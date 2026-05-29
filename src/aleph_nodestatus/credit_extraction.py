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
import random
from typing import Optional

import click

from eth_account import Account
from hexbytes import HexBytes
from web3 import HTTPProvider, Web3

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
    immediate: bool = False,
    slippage_bps: Optional[int] = None,
    fork_rpc: Optional[str] = None,
    reconcile_bps: int = 200,
) -> int:
    """Run one extract pass. Returns a process exit code (0 = clean).

    In `fork_rpc` mode the CLI passed `dry_run=True, transfer=True`. We
    point web3 at the fork RPC, force-derive the admin account (signing
    is real even on a fork), bypass the gas-headroom warning (the fork
    inherits the admin's mainnet ETH balance), and tell `extract_aleph`
    to broadcast + audit each tx. The published Aleph post is unrelated
    here — extract never publishes — so there is no testnet/mainnet
    cross-contamination to worry about.
    """
    if fork_rpc:
        web3 = Web3(HTTPProvider(fork_rpc))
    else:
        web3 = get_web3()

    admin_account = None
    admin_address = settings.payment_processor_admin_address

    # Fork mode also needs a signer — the broadcast path runs for real,
    # just against a forked node. Falls through to the same pkey
    # resolution as live --act.
    needs_signer = (act and not dry_run and transfer) or bool(fork_rpc)
    if needs_signer:
        pk = settings.payment_processor_admin_pkey or settings.ethereum_pkey
        if not pk:
            click.echo(
                "ERROR: signing key required "
                "(payment_processor_admin_pkey or ethereum_pkey). Fork "
                "mode signs real txs against the fork." if fork_rpc else
                "ERROR: payment_processor_admin_pkey or ethereum_pkey "
                "required for --act."
            )
            return 2
        if not settings.payment_processor_admin_pkey:
            LOGGER.warning(
                "payment_processor_admin_pkey not set; falling back to "
                "ethereum_pkey"
            )
        admin_account = Account.from_key(HexBytes(pk))
        admin_address = admin_account.address

    if act and not dry_run and transfer:
        # Gas-headroom warning only for real mainnet --act runs. The
        # fork inherits whatever ETH the admin has on mainnet; if the
        # mainnet balance is fine the fork is fine.
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

    # Anti-MEV random delay: hourly cron is predictable; this jitter
    # uniformly spreads the actual execution over [0, max] seconds so a
    # bot watching the schedule can't pre-position a sandwich for a
    # specific block. Bypass via --immediate (manual retries),
    # --dry-run (no on-chain tx, no need to obscure timing), or fork
    # mode (audit run, no real MEV surface).
    if (not dry_run
        and not immediate
        and not fork_rpc
        and settings.extract_random_delay_max_seconds > 0):
        delay = random.randint(0, settings.extract_random_delay_max_seconds)
        click.echo(f"Sleeping {delay}s before extract (anti-MEV jitter)…")
        await asyncio.sleep(delay)

    processor = get_processor_contract(web3)
    quoters = {
        "v2": get_v2_router_contract(web3),
        "v3": get_quoter_contract(web3),
        "v4": get_v4_quoter_contract(web3),
    }

    # Internal extract_aleph contract: `dry_run=True` means "simulate
    # only" inside the loop. Fork mode wants the full broadcast path,
    # so flip it off here regardless of the user-facing `--dry-run`.
    internal_dry_run = (dry_run or not transfer) and not fork_rpc

    extract_block = await asyncio.to_thread(
        extract_aleph,
        web3, processor, quoters,
        account=admin_account,
        from_address=admin_address,
        dry_run=internal_dry_run,
        transfer_enabled=transfer,
        slippage_bps=(
            slippage_bps if slippage_bps is not None
            else settings.process_slippage_bps
        ),
        audit_tx=bool(fork_rpc),
        reconcile_bps=reconcile_bps,
    )
    _print_summary(extract_block, audit=bool(fork_rpc))

    # Exit code: 2 if any reconciliation failed (fork mode), else 0.
    if fork_rpc:
        failed = [
            e for e in extract_block.get("tokens", [])
            if e.get("audit", {}).get("reconcile_failed")
        ]
        if failed:
            return 2
    return 0


def _print_summary(extract_block: dict, *, audit: bool = False) -> None:
    click.echo("=== Extract summary ===")
    for entry in extract_block.get("tokens", []):
        line = f"  {entry['symbol']:6} balance={entry['amount_in']}"
        if entry.get("skipped_reason"):
            line += f" skipped={entry['skipped_reason']}"
            if entry.get("skipped_reason") == "price_deviation":
                o = entry.get("oracle") or {}
                if o.get("deviation_bps") is not None:
                    line += f" Δ={o['deviation_bps']}bps"
        elif entry.get("error"):
            line += f" ERROR={entry['error']}"
        elif entry.get("simulated_only"):
            line += f" simulated_only min_out={entry['min_out']}"
        else:
            line += f" tx_hash={entry['tx_hash']}"
            if audit and entry.get("audit"):
                a = entry["audit"]
                line += (
                    f" realized={a['aleph_received']}"
                    f" Δexp={a['delta_expected_bps']:+d}bps"
                )
                if a.get("reconcile_failed"):
                    line += " ✗RECONCILE_FAIL"
        click.echo(line)
    n_err = len(extract_block.get("errors", []))
    click.echo(f"  {n_err} error(s)" if n_err else "  no errors")
    if audit:
        failed = [
            e for e in extract_block.get("tokens", [])
            if e.get("audit", {}).get("reconcile_failed")
        ]
        if failed:
            click.echo(
                f"\n!!! RECONCILIATION FAILED for {len(failed)} token(s): "
                f"{[e['symbol'] for e in failed]} — exit code 2"
            )
