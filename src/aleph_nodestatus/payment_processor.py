"""On-chain interactions with the AlephPaymentProcessor and the V3 quoter."""

import json
import logging
from functools import lru_cache
from pathlib import Path
from typing import Optional

from .pool_oracle import check_swap_price_deviation
from .settings import settings

LOGGER = logging.getLogger(__name__)


def apply_slippage(amount: int, slippage_bps: int) -> int:
    """Reduce `amount` by `slippage_bps` basis points (max 9999)."""
    if slippage_bps < 0 or slippage_bps >= 10_000:
        raise ValueError(
            f"slippage_bps must be in [0, 10000), got {slippage_bps}"
        )
    return amount * (10_000 - slippage_bps) // 10_000


def quote_amount_out(quoters: dict, swap_config: dict, amount_in: int,
                     token_in: str = None) -> int:
    """Return expected output for a swap, in wei of the destination token.

    quoters: dict with keys "v2", "v3", "v4" mapping to contract objects
             (may be None for versions not yet instantiated).
    token_in: checksummed address of the input token; required for V4.
    """
    v = swap_config.get("v")
    if v == 3:
        quoter = quoters.get("v3")
        if quoter is None:
            raise ValueError("V3 quoter contract not available")
        path = swap_config["v3"]
        result = quoter.functions.quoteExactInput(path, amount_in).call()
        return result[0]
    if v == 2:
        router = quoters.get("v2")
        if router is None:
            raise ValueError("V2 router contract not available")
        amounts = router.functions.getAmountsOut(amount_in, swap_config["v2"]).call()
        return amounts[-1]
    if v == 4:
        quoter = quoters.get("v4")
        if quoter is None:
            raise ValueError("V4 quoter contract not available")
        params = (token_in, swap_config["v4"], amount_in)
        result = quoter.functions.quoteExactInput(params).call()
        return result[0]
    raise ValueError(f"Unknown swap version: {v}")


@lru_cache(maxsize=None)
def _load_abi(name: str):
    path = Path(__file__).parent / "abi" / f"{name}.json"
    with open(path) as f:
        return json.load(f)


def get_processor_contract(w3):
    return w3.eth.contract(
        address=w3.to_checksum_address(settings.payment_processor_address),
        abi=_load_abi("AlephPaymentProcessor"),
    )


def get_quoter_contract(w3):
    return w3.eth.contract(
        address=w3.to_checksum_address(settings.uniswap_v3_quoter_address),
        abi=_load_abi("UniswapV3QuoterV2"),
    )


def get_v4_quoter_contract(w3):
    addr = settings.uniswap_v4_quoter_address
    if not addr:
        return None
    return w3.eth.contract(
        address=w3.to_checksum_address(addr),
        abi=_load_abi("UniswapV4Quoter"),
    )


def get_v2_router_contract(w3):
    addr = settings.uniswap_v2_router_address
    if not addr:
        return None
    return w3.eth.contract(
        address=w3.to_checksum_address(addr),
        abi=_load_abi("UniswapV2Router"),
    )


def simulate_process(
    w3, processor,
    from_address: str,
    token: str, amount_in: int, min_out: int, ttl: int,
    contract_logic_error_cls=None,
) -> Optional[str]:
    """eth_call the process() tx to detect revert.

    Returns None on success, error message string on revert.
    """
    if contract_logic_error_cls is None:
        try:
            from web3.exceptions import ContractLogicError
            contract_logic_error_cls = ContractLogicError
        except ImportError:
            contract_logic_error_cls = Exception

    data = processor.encode_abi(
        abi_element_identifier="process",
        args=[token, amount_in, min_out, ttl],
    )
    try:
        w3.eth.call({
            "from": from_address,
            "to": w3.to_checksum_address(settings.payment_processor_address),
            "data": data,
        })
        return None
    except contract_logic_error_cls as e:
        return str(e)
    except Exception as e:
        return f"unexpected error during simulate: {e!r}"


def execute_process(
    w3, processor, account,
    token: str, amount_in: int, min_out: int, ttl: int,
    receipt_timeout: int = 300,
) -> dict:
    """Sign and broadcast the process() tx, wait for receipt.

    Gas is estimated and clamped to settings.process_gas_ceiling. If
    estimate_gas raises (legitimate mempool race), we fall back to the
    ceiling and log a WARNING — better to over-pay gas than to revert
    a working swap.
    """
    nonce = w3.eth.get_transaction_count(account.address)
    latest = w3.eth.get_block("latest")
    base_fee = latest.baseFeePerGas
    max_priority = w3.to_wei(1, "gwei")
    max_fee = 5 * base_fee + max_priority

    tx_builder = processor.functions.process(token, amount_in, min_out, ttl)
    try:
        estimated = tx_builder.estimate_gas({"from": account.address})
        gas = min(estimated * 5 // 4, settings.process_gas_ceiling)
    except Exception as e:
        LOGGER.warning(
            "estimate_gas failed for process(%s, amount_in=%d): %r; "
            "falling back to ceiling %d",
            token, amount_in, e, settings.process_gas_ceiling,
        )
        gas = settings.process_gas_ceiling

    tx = tx_builder.build_transaction({
        "chainId": settings.ethereum_chain_id,
        "gas": gas,
        "nonce": nonce,
        "maxFeePerGas": max_fee,
        "maxPriorityFeePerGas": max_priority,
    })
    signed = account.sign_transaction(tx)
    tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction).hex()
    LOGGER.info(f"process() tx broadcast: {tx_hash} (gas={gas})")
    receipt = w3.eth.wait_for_transaction_receipt(
        tx_hash, timeout=receipt_timeout
    )
    return {
        "tx_hash":  tx_hash,
        "status":   int(receipt["status"]),
        "receipt":  receipt,
        "built_tx": {
            "from":     account.address,
            "to":       tx["to"],
            "gas":      gas,
            "nonce":    nonce,
            "chainId":  settings.ethereum_chain_id,
            "fn":       "process",
            "args":     {
                "token":    token,
                "amountIn": amount_in,
                "minOut":   min_out,
                "ttl":      ttl,
            },
        },
    }


ETH_SENTINEL = "0x0000000000000000000000000000000000000000"


def _erc20_contract(w3, address):
    minimal_abi = [{
        "constant": True, "inputs": [{"name": "owner", "type": "address"}],
        "name": "balanceOf",
        "outputs": [{"name": "balance", "type": "uint256"}],
        "type": "function", "stateMutability": "view",
    }, {
        "constant": True, "inputs": [],
        "name": "decimals",
        "outputs": [{"name": "", "type": "uint8"}],
        "type": "function", "stateMutability": "view",
    }]
    return w3.eth.contract(
        address=w3.to_checksum_address(address),
        abi=minimal_abi,
    )


def _token_decimals(w3, token_address: str) -> int:
    """ERC20 `decimals()` with a sentinel for the ETH/native asset.

    The processor's `process_tokens` lists ETH via the zero address
    (an internal sentinel); the on-chain `process()` path wraps to
    WETH for the actual swap, but for display purposes ETH and WETH
    are both 18-decimal."""
    if token_address.lower() == ETH_SENTINEL.lower():
        return 18
    return int(_erc20_contract(w3, token_address).functions.decimals().call())


def _fmt_amount(value: int, decimals: int, *, max_dp: int = 6) -> str:
    """Render a wei value as a human-readable float at the given decimals.

    Used only in the audit print path; production code keeps raw wei.
    `max_dp` truncates without rounding so the integer part stays
    exact and the printed value is never misleadingly precise."""
    if decimals <= 0:
        return str(value)
    s = str(value).rjust(decimals + 1, "0")
    head, tail = s[:-decimals], s[-decimals:][:max_dp]
    return f"{head}.{tail}"


# ABI fragment for the AlephPaymentProcessor's reconciliation event.
# Duplicated here (instead of pulled from the full vendored ABI) so the
# audit path doesn't pay the cost of loading the entire ABI when it
# only needs to decode one event type — and so a future ABI change
# that drops fields fails loudly here rather than silently in the
# audit reconciliation.
_TOKEN_PAYMENTS_PROCESSED_EVENT_ABI = {
    "name": "TokenPaymentsProcessed",
    "type": "event",
    "anonymous": False,
    "inputs": [
        {"name": "_token",               "type": "address", "indexed": True},
        {"name": "sender",               "type": "address", "indexed": True},
        {"name": "amount",               "type": "uint256", "indexed": False},
        {"name": "swapAmount",           "type": "uint256", "indexed": False},
        {"name": "alephReceived",        "type": "uint256", "indexed": False},
        {"name": "amountBurned",         "type": "uint256", "indexed": False},
        {"name": "amountToDistribution", "type": "uint256", "indexed": False},
        {"name": "amountToDevelopers",   "type": "uint256", "indexed": False},
        {"name": "swapVersion",          "type": "uint8",   "indexed": False},
        {"name": "isStable",             "type": "bool",    "indexed": False},
    ],
}


def audit_process_tx(
    w3, processor,
    *,
    tx_info: dict, entry: dict, token: str,
    expected_out: int, min_out: int,
    reconcile_bps: int = 200,
    aleph_decimals: int = 18,
) -> bool:
    """Reconcile a broadcast `process()` tx against the quoter prediction.

    Reads `TokenPaymentsProcessed` from the receipt, populates
    `entry["audit"]` with realized amounts, prints a structured audit
    block, and hard-checks |alephReceived - expected_out| / expected_out
    <= reconcile_bps / 10_000. Returns True on pass, False on fail.

    Sets `entry["audit"]["reconcile_failed"] = True` on fail so the
    caller can aggregate exit codes.
    """
    import click

    receipt = tx_info["receipt"]
    # `processor.events.TokenPaymentsProcessed()` would work but loads
    # the whole ABI into the matcher; manual filter by topic0 is faster
    # and keeps the event coupling explicit.
    target_topic0 = w3.keccak(text=(
        "TokenPaymentsProcessed(address,address,uint256,uint256,uint256,"
        "uint256,uint256,uint256,uint8,bool)"
    ))
    token_lc = token.lower()
    matching = []
    for log in receipt["logs"]:
        if not log["topics"]:
            continue
        if bytes(log["topics"][0]) != bytes(target_topic0):
            continue
        # topics[1] is the indexed `_token`, left-padded to 32 bytes.
        ev_token = "0x" + bytes(log["topics"][1])[-20:].hex()
        if ev_token.lower() != token_lc:
            continue
        matching.append(log)

    if not matching:
        entry["audit"] = {
            "reconcile_failed": True,
            "reason": "no TokenPaymentsProcessed event for this token",
            "tx_hash": tx_info["tx_hash"],
        }
        click.echo(
            f"\n=== Extract tx audit: {entry['symbol']} === FAIL\n"
            f"  No TokenPaymentsProcessed event found in receipt for "
            f"token {token}. tx={tx_info['tx_hash']}"
        )
        return False

    # Decode the non-indexed payload via the contract's codec.
    from web3._utils.events import get_event_data
    decoded = get_event_data(
        w3.codec, _TOKEN_PAYMENTS_PROCESSED_EVENT_ABI, matching[0],
    )
    args = decoded["args"]

    realized_aleph        = int(args["alephReceived"])
    swap_amount_realized  = int(args["swapAmount"])
    amount_realized       = int(args["amount"])
    to_distribution       = int(args["amountToDistribution"])
    to_developers         = int(args["amountToDevelopers"])
    burned                = int(args["amountBurned"])

    # Token-in side decimals for human display. For ALEPH (which is the
    # passthrough case where token_in == ALEPH), alephReceived == amount
    # and both sides are 18-decimal.
    in_decimals = _token_decimals(w3, token)

    # --- Reconciliation deltas ---
    # vs expected_out (this is the "did the quoter lie about price impact?"
    # check; the threshold is configurable).
    if expected_out > 0:
        delta_expected_bps = (realized_aleph - expected_out) * 10_000 // expected_out
    else:
        delta_expected_bps = 0
    # vs min_out (this must be >= 0 or the tx would have reverted; we
    # still print it because operators want to see how close to the
    # floor each swap landed).
    delta_min_bps = (
        (realized_aleph - min_out) * 10_000 // min_out
        if min_out > 0 else 0
    )
    reconcile_failed = abs(delta_expected_bps) > reconcile_bps
    status_expected = "PASS" if not reconcile_failed else "FAIL"

    entry["audit"] = {
        "tx_hash":              tx_info["tx_hash"],
        "receipt_status":       tx_info["status"],
        "block_number":         int(receipt["blockNumber"]),
        "gas_used":             int(receipt["gasUsed"]),
        "amount":               str(amount_realized),
        "swap_amount":          str(swap_amount_realized),
        "aleph_received":       str(realized_aleph),
        "amount_to_distribution": str(to_distribution),
        "amount_to_developers": str(to_developers),
        "amount_burned":        str(burned),
        "swap_version":         int(args["swapVersion"]),
        "is_stable":            bool(args["isStable"]),
        "delta_expected_bps":   delta_expected_bps,
        "delta_min_bps":        delta_min_bps,
        "reconcile_bps_limit":  reconcile_bps,
        "reconcile_failed":     reconcile_failed,
    }

    bt = tx_info.get("built_tx", {})
    bt_args = bt.get("args", {})
    click.echo(f"\n=== Extract tx audit: {entry['symbol']} === {status_expected}")
    click.echo(f"  built_tx:")
    click.echo(f"    from    : {bt.get('from')}")
    click.echo(f"    to      : {bt.get('to')}")
    click.echo(f"    fn      : process(")
    click.echo(f"                token    = {bt_args.get('token')},")
    click.echo(f"                amountIn = {bt_args.get('amountIn')},")
    click.echo(f"                minOut   = {bt_args.get('minOut')},")
    click.echo(f"                ttl      = {bt_args.get('ttl')}s)")
    click.echo(f"    gas     : {bt.get('gas')}")
    click.echo(f"    nonce   : {bt.get('nonce')}")
    click.echo(f"    chainId : {bt.get('chainId')}")
    click.echo(f"  receipt:")
    click.echo(f"    tx_hash : {tx_info['tx_hash']} (FORK)")
    click.echo(f"    status  : {tx_info['status']}")
    click.echo(f"    block   : {int(receipt['blockNumber']):,}")
    click.echo(f"    gas_used: {int(receipt['gasUsed']):,}")
    click.echo(f"  TokenPaymentsProcessed:")
    click.echo(f"    amount              : {amount_realized} "
               f"({_fmt_amount(amount_realized, in_decimals)} input)")
    click.echo(f"    swapAmount          : {swap_amount_realized} "
               f"({_fmt_amount(swap_amount_realized, in_decimals)} input)")
    click.echo(f"    alephReceived       : {realized_aleph} "
               f"({_fmt_amount(realized_aleph, aleph_decimals)} ALEPH)")
    click.echo(f"    amountToDistribution: {to_distribution} "
               f"({_fmt_amount(to_distribution, aleph_decimals)} ALEPH)")
    click.echo(f"    amountToDevelopers  : {to_developers} "
               f"({_fmt_amount(to_developers, in_decimals)} input)")
    click.echo(f"    amountBurned        : {burned} "
               f"({_fmt_amount(burned, aleph_decimals)} ALEPH)")
    click.echo(f"    swapVersion         : {int(args['swapVersion'])}")
    click.echo(f"    isStable            : {bool(args['isStable'])}")
    click.echo(f"  reconciliation:")
    click.echo(f"    quoter expected_out : {expected_out} "
               f"({_fmt_amount(expected_out, aleph_decimals)} ALEPH)")
    click.echo(f"    quoter min_out      : {min_out} "
               f"({_fmt_amount(min_out, aleph_decimals)} ALEPH)")
    click.echo(f"    realized            : {realized_aleph} "
               f"({_fmt_amount(realized_aleph, aleph_decimals)} ALEPH)")
    mark_exp = "✓" if not reconcile_failed else "✗"
    mark_min = "✓" if delta_min_bps >= 0 else "✗"
    click.echo(f"    realized vs expected: {delta_expected_bps:+d} bps "
               f"({delta_expected_bps/100:+.4f}%) {mark_exp} "
               f"(limit ±{reconcile_bps} bps)")
    click.echo(f"    realized vs min_out : {delta_min_bps:+d} bps "
               f"({delta_min_bps/100:+.4f}%) {mark_min}")
    return not reconcile_failed


def _balance_of(w3, contract_address, token_address):
    if token_address.lower() == ETH_SENTINEL.lower():
        return w3.eth.get_balance(w3.to_checksum_address(contract_address))
    return _erc20_contract(w3, token_address).functions.balanceOf(
        w3.to_checksum_address(contract_address)
    ).call()


def _swap_config_to_dict(swap_config_tuple):
    """Map ABI-decoded tuple to a dict the quoter helper understands.

    `v4` is a tuple of `PathKey` structs as decoded from the on-chain
    ABI; `list(v4)` keeps each inner struct as a positional tuple
    whose field order MUST stay in sync with the V4 quoter's
    `PathKey` definition (intermediateCurrency, fee, tickSpacing,
    hooks, hookData at the time of writing). If the on-chain struct
    is ever reshaped, the entries we forward to
    `v4_quoter.quoteExactInput` will silently misalign. The
    AlephPaymentProcessor and UniswapV4Quoter ABIs vendored in
    `abi/*.json` are the source of truth — update both together.
    """
    v, t, v2, v3, v4 = swap_config_tuple
    # Early surface for an unexpected version — `quote_amount_out` would
    # raise later anyway, but failing at the parse boundary makes ABI
    # drift obvious without descending into the quoter dispatch.
    if v not in (2, 3, 4):
        raise ValueError(f"unexpected swap version from getSwapConfig: {v}")
    return {"v": v, "t": t, "v2": list(v2), "v3": bytes(v3), "v4": list(v4)}


def _aleph_token_address():
    for sym, addr in settings.process_tokens:
        if sym == "ALEPH":
            return addr
    return settings.ethereum_token_contract


def extract_aleph(
    w3, processor, quoters: dict, account,
    from_address: str,
    dry_run: bool = False,
    transfer_enabled: bool = True,
    aleph_address: Optional[str] = None,
    slippage_bps: int = None,
    audit_tx: bool = False,
    reconcile_bps: int = 200,
) -> dict:
    """Run process() per token in settings.process_tokens. Returns the
    extract block for the audit post.
    """
    aleph_address = aleph_address or _aleph_token_address()
    effective_slippage = (
        slippage_bps if slippage_bps is not None
        else settings.process_slippage_bps
    )
    out = {"tokens": [], "errors": []}

    # Read protocol-wide percentages once. For stable tokens the contract
    # takes `developersPercentage` of the input BEFORE swapping (see
    # AlephPaymentProcessor.process(), branch `isStable && !ALEPH`), so we
    # must quote on the reduced amount or our min_out becomes ~dev_pct%
    # higher than what the swap can actually produce, triggering
    # V4TooLittleReceived. Non-stable tokens swap the full amount; the
    # dev cut comes off the ALEPH side and doesn't affect min_out.
    try:
        dev_pct = int(processor.functions.developersPercentage().call())
    except Exception as e:
        LOGGER.warning(
            "Failed to read developersPercentage; assuming 0%% adjustment: %r",
            e,
        )
        dev_pct = 0

    for token_symbol, raw_token in settings.process_tokens:
        # Checksum once at the loop head so every downstream call
        # (getSwapConfig, quote_amount_out, simulate_process,
        # execute_process, the audit-post entry) sees the same
        # EIP-55 canonical form. web3.py would normalise per-call,
        # but doing it here removes a class of "is this the same
        # address?" foot-guns.
        token = w3.to_checksum_address(raw_token)
        token_lc = token.lower()
        contract_address = settings.payment_processor_address
        balance = _balance_of(w3, contract_address, token)
        entry = {
            "symbol": token_symbol, "token": token,
            "amount_in": str(balance), "skipped_reason": None,
            "swap_amount_in": None,
            "min_out": None, "expected_out": None,
            "tx_hash": None, "simulated_only": False, "error": None,
        }
        out["tokens"].append(entry)

        if balance == 0:
            entry["skipped_reason"] = "zero_balance"
            continue

        if token_lc == aleph_address.lower():
            # ALEPH passthrough: no swap. expected_out is the full balance
            # (the processor just routes it onward); min_out stays at 0
            # because there's nothing to lose to slippage. Mirror those
            # into the entry so audit reconciliation has values to compare
            # against (previously these stayed None, which the audit path
            # would have mis-treated as a missing quote).
            min_out = 0
            expected_out = balance
            entry["expected_out"] = str(expected_out)
            entry["min_out"]      = str(min_out)
        else:
            # Quote against the amount the contract will *actually* swap.
            # For stables that's `balance × (100 - dev_pct) / 100` because
            # the dev cut is removed from the input pre-swap; for the rest
            # it's the full balance.
            try:
                is_stable = bool(
                    processor.functions.isStableToken(token).call()
                )
            except Exception as e:
                LOGGER.warning(
                    "Failed to read isStableToken(%s); assuming non-stable: %r",
                    token_symbol, e,
                )
                is_stable = False
            if is_stable and dev_pct > 0:
                # Integer floor-division. For balances < 100/dev_pct wei
                # this truncates the swap_amount to 0 (e.g., balance=1
                # USDC-wei with dev_pct=5 yields 0). Such balances are
                # economically meaningless — USDC has 6 decimals so 1 wei
                # is $0.000001 — and the contract's own ZeroAmount check
                # would revert downstream, so we just accept the floor.
                swap_amount = balance * (100 - dev_pct) // 100
            else:
                swap_amount = balance
            entry["swap_amount_in"] = str(swap_amount)
            try:
                swap_config = processor.functions.getSwapConfig(token).call()
                cfg = _swap_config_to_dict(swap_config)
            except Exception as e:
                LOGGER.exception(
                    "getSwapConfig failed for token %s: %r", token_symbol, e,
                )
                entry["error"] = f"swap_config_failed: {e!r}"
                out["errors"].append(entry)
                continue

            oracle = check_swap_price_deviation(w3, cfg, token_in=token)
            if not oracle.ok:
                entry["skipped_reason"] = oracle.reason
                entry["oracle"] = {
                    "deviation_bps": oracle.deviation_bps,
                    "spot_price":    oracle.spot_price,
                    "ref_price":     oracle.ref_price,
                }
                continue

            try:
                expected_out = quote_amount_out(
                    quoters, cfg, swap_amount,
                    token_in=token,
                )
                min_out = apply_slippage(
                    expected_out, effective_slippage
                )
            except Exception as e:
                LOGGER.exception(
                    "Quote failed for token %s (balance=%d, swap_amount=%d): %r",
                    token_symbol, balance, swap_amount, e,
                )
                entry["error"] = f"quote_failed: {e!r}"
                out["errors"].append(entry)
                continue
            entry["expected_out"] = str(expected_out)
            entry["min_out"] = str(min_out)

        err = simulate_process(
            w3, processor,
            from_address=from_address,
            token=token, amount_in=balance, min_out=min_out,
            ttl=settings.process_ttl_seconds,
        )
        if err:
            entry["error"] = f"simulation_revert: {err}"
            out["errors"].append(entry)
            continue

        if dry_run or not transfer_enabled:
            entry["simulated_only"] = True
            continue

        try:
            tx_info = execute_process(
                w3, processor, account,
                token=token, amount_in=balance, min_out=min_out,
                ttl=settings.process_ttl_seconds,
            )
            entry["tx_hash"] = tx_info["tx_hash"]
            if tx_info["status"] == 0:
                entry["error"] = "tx_reverted_on_chain"
                out["errors"].append(entry)
                continue
            if audit_tx:
                # Fork-mode audit: reconcile the realized output against
                # the quoter prediction. Populates entry["audit"] and
                # prints the structured tx block. Failures are recorded
                # on the entry, not raised — the caller aggregates the
                # exit code at the end.
                audit_process_tx(
                    w3, processor,
                    tx_info=tx_info, entry=entry, token=token,
                    expected_out=int(expected_out),
                    min_out=int(min_out),
                    reconcile_bps=reconcile_bps,
                )
        except Exception as e:
            LOGGER.exception(
                "process() tx broadcast failed for token %s: %r", token_symbol, e,
            )
            entry["error"] = f"tx_failed: {e!r}"
            out["errors"].append(entry)

    return out
