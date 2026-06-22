"""On-chain interactions with the AlephPaymentProcessor and the V3 quoter."""

import json
import logging
from decimal import Decimal
from functools import lru_cache
from pathlib import Path
from typing import Mapping, Optional, Set

from eth_abi import encode as abi_encode
from eth_utils import keccak
from web3 import Web3

from .price_oracle import CreditApiUnavailable, fair_aleph_rate
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


def pool_fee_bps(swap_config: dict) -> int:
    """Total swap fee across all hops of the configured path, in bps.

    Uniswap fees are expressed in hundredths of a bip (1e-6), so
    `fee_bps = fee_raw / 100` (10000 -> 100 bps, 3000 -> 30 bps). This
    fee is fixed by the configured pool and is budgeted out of the
    deviation/impact checks rather than counted against them.
    """
    v = swap_config.get("v")
    if v == 4:
        return sum(int(hop[1]) for hop in swap_config["v4"]) // 100
    if v == 3:
        path = swap_config["v3"]
        # encoded as token(20) [fee(3) token(20)]... ; fees at 20, 43, ...
        total = 0
        i = 20
        while i + 3 <= len(path):
            total += int.from_bytes(path[i:i + 3], "big")
            i += 23
        return total // 100
    if v == 2:
        hops = max(len(swap_config["v2"]) - 1, 1)
        return 30 * hops  # v2 is a flat 0.30% per hop
    raise ValueError(f"unknown swap version: {v}")


def bisect_swap_amount(
    quoters, swap_config, *,
    token_in: str,
    upper_amount_in: int,
    min_amount_in: int,
    fair_rate,
    spot_rate: float,
    pool_fee_bps: int,
    max_deviation_bps: int,
    max_impact_bps: int,
    dev_pct: int = 0,
    is_stable: bool = False,
    max_iters: int = 30,
) -> dict:
    """Largest `amount_in` in `[min, upper]` that passes both fee-free
    checks at the real swap amount:

      out_adj = quoter_out / (1 - pool_fee)          # fee removed once
      dev_api = |out_adj - fair| / fair  <= max_deviation_bps   (vs Credit-API)
      impact  = (spot - out_adj) / spot  <= max_impact_bps      (vs pool spot)

    `fair_rate`/`spot_rate` are ALEPH-wei per input-wei. `fair_rate=None`
    disables the deviation check (operator opt-out). Returns settled amount
    (0 if none fit), a rich iteration trace, and the binding skip reason.
    """
    def _swap_amount_of(amount_in: int) -> int:
        if is_stable and dev_pct > 0:
            return amount_in * (100 - dev_pct) // 100
        return amount_in

    iters: list = []
    fee_den = 10_000 - pool_fee_bps

    def _measure(amount_in: int):
        swap = _swap_amount_of(amount_in)
        out = quote_amount_out(quoters, swap_config, swap, token_in=token_in)
        out_adj = out * 10_000 // fee_den if fee_den > 0 else out
        spot = int(swap * spot_rate)
        impact_bps = (
            max((spot - out_adj) * 10_000 // spot, 0) if spot > 0 else 10 ** 9
        )
        if fair_rate is None:
            fair = 0
            dev_api_bps = 0
        else:
            fair = int(swap * fair_rate)
            dev_api_bps = (
                abs(out_adj - fair) * 10_000 // fair if fair > 0 else 10 ** 9
            )
        if impact_bps > max_impact_bps:
            fail = "price_impact_too_high"
        elif dev_api_bps > max_deviation_bps:
            fail = "price_deviation"
        else:
            fail = None
        rec = {
            "amount_in": amount_in, "swap_amount": swap,
            "out": out, "out_adj": out_adj, "fair": fair, "spot": spot,
            "dev_api_bps": dev_api_bps, "impact_bps": impact_bps, "fail": fail,
        }
        iters.append(rec)
        return fail

    def _result(settled, binding):
        return {"settled_amount_in": settled,
                "iterations": iters, "binding": binding}

    # Phase 1: does the full (capped) amount already pass?
    if _measure(upper_amount_in) is None:
        return _result(upper_amount_in, None)

    # Phase 1b: halve down to the first amount that passes.
    high_bad = upper_amount_in
    low_ok = 0
    candidate = upper_amount_in // 2
    last_fail = iters[-1]["fail"]
    while candidate >= min_amount_in and len(iters) < max_iters:
        fail = _measure(candidate)
        if fail is None:
            low_ok = candidate
            break
        last_fail = fail
        high_bad = candidate
        candidate //= 2

    if low_ok == 0:
        return _result(0, last_fail or "price_impact_too_high")

    # Phase 2: bisect (low_ok, high_bad) for the largest passing amount.
    delta = max(min_amount_in, low_ok // 100)
    while high_bad - low_ok > delta and len(iters) < max_iters:
        mid = (low_ok + high_bad) // 2
        if mid < min_amount_in:
            break
        if _measure(mid) is None:
            low_ok = mid
        else:
            high_bad = mid

    return _result(low_ok, None)


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


def _human_to_wei(w3, token_address: str, human_amount: Decimal) -> int:
    """Convert a human-units amount (e.g. Decimal("1000") for 1000 USDC)
    to integer wei, using the token's on-chain `decimals()`. The ETH
    sentinel resolves to 18 dp via `_token_decimals`.

    Decimal arithmetic keeps the result exact; the final `int()` truncates
    any sub-wei fraction, matching Solidity's integer semantics.
    """
    dec = _token_decimals(w3, token_address)
    scale = Decimal(10) ** dec
    return int(Decimal(human_amount) * scale)


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
    token_filter: Optional[Set[str]] = None,
    max_amounts: Optional[Mapping[str, Decimal]] = None,
    min_amounts: Optional[Mapping[str, Decimal]] = None,
    max_price_impact_bps: int = 0,
) -> dict:
    """Run process() per token in settings.process_tokens. Returns the
    extract block for the audit post.

    Per-token sizing knobs (all keyed by token symbol, e.g. "USDC"):

    - `token_filter`: if non-empty, restricts the loop to these symbols.
      Other tokens are skipped entirely (no balance read, no entry).
    - `max_amounts`: cap the swap input per token, in *human units*
      (e.g. Decimal("1000") for 1000 USDC). Converted to wei via the
      token's on-chain `decimals()`. The cap is `amount_in = min(balance, cap_wei)`.
    - `min_amounts`: skip the token with reason "below_min_amount" if
      the effective (post-cap) amount is below this threshold, also in
      human units. Reads alongside `max_amounts` so a cap that lands
      below the floor short-circuits without burning gas on a dust swap.

    Auto-sizing (price-impact aware):

    - `max_price_impact_bps`: when > 0, the loop probes the quoter at a
      small-size reference amount and at the candidate (post-cap)
      amount, derives the implied price impact, and bisects down until
      the impact fits. The leftover stays in the contract for the next
      cron cycle. 0 disables the search and falls back to the cap/min
      behaviour above.
    """
    aleph_address = aleph_address or _aleph_token_address()
    effective_slippage = (
        slippage_bps if slippage_bps is not None
        else settings.process_slippage_bps
    )
    max_amounts = max_amounts or {}
    min_amounts = min_amounts or {}
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
        # token_filter is a positive allowlist: when non-empty, skip
        # everything else BEFORE the on-chain balance read so a
        # narrow --token run doesn't waste RPC calls on tokens the
        # operator didn't ask for.
        if token_filter and token_symbol not in token_filter:
            continue

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
            "balance": str(balance),
            "amount_in": str(balance), "skipped_reason": None,
            "swap_amount_in": None,
            "min_out": None, "expected_out": None,
            "tx_hash": None, "simulated_only": False, "error": None,
        }
        out["tokens"].append(entry)

        if balance == 0:
            entry["skipped_reason"] = "zero_balance"
            continue

        # Per-token sizing: cap then min-skip. Conversion human → wei
        # happens here so the caller can pass plain Decimals without
        # knowing decimals() for each token.
        effective_amount = balance
        if token_symbol in max_amounts:
            cap_wei = _human_to_wei(w3, token, max_amounts[token_symbol])
            if cap_wei < balance:
                effective_amount = cap_wei
                entry["amount_in"] = str(effective_amount)
                entry["capped_from_balance"] = True
        if token_symbol in min_amounts:
            min_wei = _human_to_wei(w3, token, min_amounts[token_symbol])
            if effective_amount < min_wei:
                entry["skipped_reason"] = "below_min_amount"
                entry["min_amount_wei"] = str(min_wei)
                continue

        if token_lc == aleph_address.lower():
            # ALEPH passthrough: no swap. expected_out is the effective
            # amount (the processor just routes it onward); min_out stays
            # at 0 because there's nothing to lose to slippage. Mirror
            # those into the entry so audit reconciliation has values to
            # compare against (previously these stayed None, which the
            # audit path would have mis-treated as a missing quote).
            min_out = 0
            expected_out = effective_amount
            entry["expected_out"] = str(expected_out)
            entry["min_out"]      = str(min_out)
        else:
            # Hoisted reads: is_stable + swap_config are needed both by
            # the auto-size search (when enabled) and by the final
            # quote, so fetch them once.
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

            fee_bps = pool_fee_bps(cfg)
            try:
                spot_rate = pool_spot_rate(w3, cfg, token)
            except Exception as e:
                LOGGER.warning(
                    "Spot read failed for %s; skipping token: %r",
                    token_symbol, e,
                )
                entry["skipped_reason"] = "spot_read_failed"
                continue

            if settings.extract_price_deviation_enabled:
                try:
                    fair_rate = fair_aleph_rate(token_symbol)
                except CreditApiUnavailable:
                    raise
                except Exception as e:
                    raise CreditApiUnavailable(
                        f"Credit-API fair rate failed for {token_symbol}: {e!r}"
                    )
            else:
                fair_rate = None

            if token_symbol in min_amounts:
                size_min_wei = _human_to_wei(w3, token, min_amounts[token_symbol])
            else:
                size_min_wei = 1
            search = bisect_swap_amount(
                quoters, cfg,
                token_in=token,
                upper_amount_in=effective_amount,
                min_amount_in=size_min_wei,
                fair_rate=fair_rate,
                spot_rate=spot_rate,
                pool_fee_bps=fee_bps,
                max_deviation_bps=settings.extract_max_deviation_bps,
                max_impact_bps=max_price_impact_bps or settings.extract_max_price_impact_bps,
                dev_pct=dev_pct,
                is_stable=is_stable,
            )
            entry["price_size_search"] = search
            if search["settled_amount_in"] == 0:
                entry["skipped_reason"] = search["binding"] or "price_impact_too_high"
                continue
            if search["settled_amount_in"] < effective_amount:
                effective_amount = search["settled_amount_in"]
                entry["amount_in"] = str(effective_amount)
                entry["auto_sized"] = True

            # Quote against the amount the contract will *actually* swap.
            # For stables that's `effective_amount × (100 - dev_pct) / 100`
            # because the dev cut is removed from the input pre-swap; for
            # the rest it's the full effective amount.
            if is_stable and dev_pct > 0:
                # Integer floor-division. For balances < 100/dev_pct wei
                # this truncates the swap_amount to 0 (e.g., balance=1
                # USDC-wei with dev_pct=5 yields 0). Such balances are
                # economically meaningless — USDC has 6 decimals so 1 wei
                # is $0.000001 — and the contract's own ZeroAmount check
                # would revert downstream, so we just accept the floor.
                swap_amount = effective_amount * (100 - dev_pct) // 100
            else:
                swap_amount = effective_amount
            entry["swap_amount_in"] = str(swap_amount)

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
            token=token, amount_in=effective_amount, min_out=min_out,
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
                token=token, amount_in=effective_amount, min_out=min_out,
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


def _v4_pool_id(currency0: str, currency1: str, fee: int,
                tick_spacing: int, hooks: str) -> bytes:
    """Uniswap v4 PoolId = keccak(abi.encode(PoolKey)). Currencies must be
    pre-sorted ascending; native ETH is the zero address and sorts first."""
    return keccak(abi_encode(
        ["address", "address", "uint24", "int24", "address"],
        [Web3.to_checksum_address(currency0),
         Web3.to_checksum_address(currency1),
         int(fee), int(tick_spacing),
         Web3.to_checksum_address(hooks)],
    ))


def pool_spot_rate(w3, swap_config: dict, token_in: str) -> float:
    """ALEPH-wei per input-wei from the pool's on-chain spot price.

    v4 single-hop only: build the PoolKey from (token_in, PathKey), read
    sqrtPriceX96 via the StateView, and convert to a wei-per-wei rate. The
    `raw = (sqrtP/2**96)**2` value is currency1-wei per currency0-wei; we
    orient it so the result is output(ALEPH)-wei per input(token_in)-wei.
    No decimals factor — both sides of the later checks are in wei.
    """
    v = swap_config.get("v")
    if v != 4 or len(swap_config["v4"]) != 1:
        raise NotImplementedError(
            "pool_spot_rate supports only v4 single-hop pools")
    intermediate, fee, tick_spacing, hooks, _hook_data = swap_config["v4"][0]
    in_addr = ETH_SENTINEL if token_in.lower() == ETH_SENTINEL.lower() else token_in
    pair = sorted([in_addr.lower(), intermediate.lower()])
    currency0, currency1 = pair[0], pair[1]
    pool_id = _v4_pool_id(currency0, currency1, fee, tick_spacing, hooks)
    sv = w3.eth.contract(
        address=w3.to_checksum_address(settings.extract_v4_stateview_address),
        abi=_load_abi("IUniswapV4StateView"),
    )
    sqrt_price_x96 = sv.functions.getSlot0(pool_id).call()[0]
    raw = (sqrt_price_x96 / 2 ** 96) ** 2  # currency1-wei per currency0-wei
    if in_addr.lower() == currency0:
        # ALEPH is currency1: ALEPH-wei per input-wei = raw
        return raw
    # ALEPH is currency0: ALEPH-wei per input-wei = 1/raw
    return 1.0 / raw
