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
