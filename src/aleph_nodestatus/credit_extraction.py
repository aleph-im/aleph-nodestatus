"""Orchestration for the credit-extraction CLI.

Wraps payment_processor.extract_aleph with the operational glue that
used to live inside process_credit_distribution: admin-account
resolution, ETH preflight, async offload of the sync web3 calls, and a
human-readable stdout summary.

No Aleph audit post is written. The on-chain process() transactions
are the canonical record of every extraction; ops review happens via
stdout / log aggregation.
"""

import asyncio
import logging
from typing import Optional

import click

from eth_account import Account
from hexbytes import HexBytes

from .ethereum import get_web3
from .payment_processor import (
    extract_aleph,
    get_processor_contract,
    get_quoter_contract,
    get_v2_router_contract,
    get_v4_quoter_contract,
)
from .settings import settings

LOGGER = logging.getLogger(__name__)


async def process_credit_extraction(
    *, act: bool, dry_run: bool, transfer: bool,
    slippage_bps: Optional[int] = None,
) -> dict:
    """Run one extract pass. Returns the dict produced by extract_aleph."""
    web3 = get_web3()

    admin_account = None
    admin_address = settings.payment_processor_admin_address
    if act and not dry_run and transfer:
        pk = settings.payment_processor_admin_pkey or settings.ethereum_pkey
        if not settings.payment_processor_admin_pkey:
            LOGGER.warning(
                "payment_processor_admin_pkey not set; falling back to "
                "ethereum_pkey"
            )
        admin_account = Account.from_key(HexBytes(pk))
        admin_address = admin_account.address

        admin_eth_wei = web3.eth.get_balance(admin_address)
        gas_headroom = (
            settings.process_gas_ceiling
            * int(web3.to_wei(50, "gwei"))
            * len(settings.process_tokens)
        )
        if admin_eth_wei < gas_headroom:
            LOGGER.warning(
                "Admin %s has %.4f ETH; recommended >= %.4f ETH to cover "
                "~%d process() transactions at 50 gwei. Extract may fail "
                "partway if gas runs out.",
                admin_address,
                admin_eth_wei / 1e18,
                gas_headroom / 1e18,
                len(settings.process_tokens),
            )

    processor = get_processor_contract(web3)
    quoters = {
        "v2": get_v2_router_contract(web3),
        "v3": get_quoter_contract(web3),
        "v4": get_v4_quoter_contract(web3),
    }

    extract_block = await asyncio.to_thread(
        extract_aleph,
        web3, processor, quoters,
        account=admin_account,
        from_address=admin_address,
        dry_run=dry_run or not transfer,
        transfer_enabled=transfer,
        slippage_bps=(
            slippage_bps if slippage_bps is not None
            else settings.process_slippage_bps
        ),
    )
    _print_summary(extract_block)
    return extract_block


def _print_summary(extract_block: dict) -> None:
    click.echo("=== Extract summary ===")
    for entry in extract_block.get("tokens", []):
        line = f"  {entry['symbol']:6} balance={entry['amount_in']}"
        if entry.get("skipped_reason"):
            line += f" skipped={entry['skipped_reason']}"
        elif entry.get("error"):
            line += f" ERROR={entry['error']}"
        elif entry.get("simulated_only"):
            line += f" simulated_only min_out={entry['min_out']}"
        else:
            line += f" tx_hash={entry['tx_hash']}"
        click.echo(line)
    n_err = len(extract_block.get("errors", []))
    click.echo(f"  {n_err} error(s)" if n_err else "  no errors")
