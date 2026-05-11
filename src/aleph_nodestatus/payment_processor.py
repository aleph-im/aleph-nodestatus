"""On-chain interactions with the AlephPaymentProcessor and the V3 quoter."""

import json
import logging
import os
from functools import lru_cache
from pathlib import Path
from typing import Optional

from .settings import settings

LOGGER = logging.getLogger(__name__)


def apply_slippage(amount: int, slippage_bps: int) -> int:
    """Reduce `amount` by `slippage_bps` basis points (max 9999)."""
    if slippage_bps < 0 or slippage_bps >= 10_000:
        raise ValueError(
            f"slippage_bps must be in [0, 10000), got {slippage_bps}"
        )
    return amount * (10_000 - slippage_bps) // 10_000


def quote_amount_out(quoter, swap_config: dict, amount_in: int) -> int:
    """Return expected output for a swap, in wei of the destination token."""
    v = swap_config.get("v")
    if v == 3:
        path = swap_config["v3"]
        result = quoter.functions.quoteExactInput(path, amount_in).call()
        return result[0] if isinstance(result, (list, tuple)) else result
    if v == 2:
        raise NotImplementedError("Quoter for V2 path not yet supported")
    if v == 4:
        raise NotImplementedError("Quoter for V4 path not yet supported")
    raise ValueError(f"Unknown swap version: {v}")


@lru_cache(maxsize=2)
def _load_abi(name: str):
    return json.load(
        open(os.path.join(Path(__file__).resolve().parent, "abi", f"{name}.json"))
    )


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

    data = processor.encodeABI(
        fn_name="process",
        args=[token, amount_in, min_out, ttl],
    )
    try:
        w3.eth.call({
            "from": from_address,
            "to": settings.payment_processor_address,
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
    """Sign and broadcast the process() tx, wait for receipt."""
    nonce = w3.eth.get_transaction_count(account.address)
    latest = w3.eth.get_block("latest")
    base_fee = latest.baseFeePerGas
    max_priority = w3.to_wei(1, "gwei")
    max_fee = 5 * base_fee + max_priority

    tx = processor.functions.process(token, amount_in, min_out, ttl)
    tx = tx.build_transaction({
        "chainId": settings.ethereum_chain_id,
        "gas": 500_000,
        "nonce": nonce,
        "maxFeePerGas": max_fee,
        "maxPriorityFeePerGas": max_priority,
    })
    signed = account.sign_transaction(tx)
    tx_hash = w3.eth.send_raw_transaction(signed.rawTransaction).hex()
    LOGGER.info(f"process() tx broadcast: {tx_hash}")
    receipt = w3.eth.wait_for_transaction_receipt(
        tx_hash, timeout=receipt_timeout
    )
    return {
        "tx_hash": tx_hash,
        "status":  int(receipt["status"]),
    }


ETH_SENTINEL = "0x0000000000000000000000000000000000000000"


def _erc20_contract(w3, address):
    minimal_abi = [{
        "constant": True, "inputs": [{"name": "owner", "type": "address"}],
        "name": "balanceOf",
        "outputs": [{"name": "balance", "type": "uint256"}],
        "type": "function", "stateMutability": "view",
    }]
    return w3.eth.contract(
        address=w3.to_checksum_address(address),
        abi=minimal_abi,
    )


def _balance_of(w3, contract_address, token_address):
    if token_address.lower() == ETH_SENTINEL.lower():
        return w3.eth.get_balance(w3.to_checksum_address(contract_address))
    return _erc20_contract(w3, token_address).functions.balanceOf(
        w3.to_checksum_address(contract_address)
    ).call()


def _swap_config_to_dict(swap_config_tuple):
    """Map ABI-decoded tuple to a dict the quoter helper understands."""
    v, t, v2, v3, v4 = swap_config_tuple
    return {"v": v, "t": t, "v2": list(v2), "v3": bytes(v3), "v4": list(v4)}


def _aleph_token_address():
    for sym, addr in settings.process_tokens:
        if sym == "ALEPH":
            return addr
    return settings.ethereum_token_contract


def extract_aleph(
    w3, processor, quoter, account,
    from_address: str,
    dry_run: bool = False,
    transfer_enabled: bool = True,
    aleph_address: Optional[str] = None,
    slippage_bps: int = None,
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

    for symbol, token in settings.process_tokens:
        token_lc = token.lower()
        contract_address = settings.payment_processor_address
        balance = _balance_of(w3, contract_address, token)
        entry = {
            "symbol": symbol, "token": token,
            "amount_in": str(balance), "skipped_reason": None,
            "min_out": None, "expected_out": None,
            "tx_hash": None, "simulated_only": False, "error": None,
        }
        out["tokens"].append(entry)

        if balance == 0:
            entry["skipped_reason"] = "zero_balance"
            continue

        if token_lc == aleph_address.lower():
            min_out = 0
            expected_out = balance
        else:
            try:
                swap_config = processor.functions.getSwapConfig(
                    w3.to_checksum_address(token)
                ).call()
                cfg = _swap_config_to_dict(swap_config)
                expected_out = quote_amount_out(quoter, cfg, balance)
                min_out = apply_slippage(
                    expected_out, effective_slippage
                )
            except Exception as e:
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
        except Exception as e:
            entry["error"] = f"tx_failed: {e!r}"
            out["errors"].append(entry)

    return out
