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
from decimal import Decimal
from typing import Mapping, Optional, Tuple

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


_HEX_CHARS = frozenset("0123456789abcdefABCDEF")


def _validate_pkey(
    pk: Optional[str], *,
    source: str,
    fork_rpc: Optional[str] = None,
) -> Optional[str]:
    """Return None if `pk` is a syntactically valid 32-byte hex private
    key, else a CLI-ready error string describing what's wrong.

    Catches the three common operator mistakes that previously surfaced
    as a `binascii.Error: Non-hexadecimal digit found` traceback from
    `HexBytes(...)`:

      - the value is missing (`.env` was never edited from the template);
      - the value still contains placeholder syntax like
        `<processor_admin_private_key>` (angle brackets, underscores);
      - the value has the right length but the wrong character set
        (stray whitespace, quotes, BOM, smart quotes from a copy/paste).
    """
    intro = (
        "Fork mode signs real txs against the fork; a real key is required."
        if fork_rpc else
        "A real signing key is required."
    )
    if not pk:
        return (
            f"ERROR: {source} is empty. {intro} "
            "Edit docker/.env.extract (or .env.dist) and set "
            "payment_processor_admin_pkey or ethereum_pkey to a 32-byte "
            "hex private key (0x-prefixed)."
        )
    body = pk[2:] if pk.lower().startswith("0x") else pk
    if len(body) != 64:
        # Length-first because it's the most diagnostic single signal.
        return (
            f"ERROR: {source} is the wrong length "
            f"(len={len(body)}, expected 64 hex chars / 32 bytes). "
            f"Got something starting with {pk[:8]!r}. "
            f"If you copied .env.extract from .env.extract.example, "
            f"replace the placeholder. {intro}"
        )
    bad = [c for c in body if c not in _HEX_CHARS]
    if bad:
        return (
            f"ERROR: {source} contains non-hex characters {bad[:5]!r}. "
            f"Likely a stray placeholder, quote, BOM, or whitespace in "
            f"docker/.env.extract (or .env.dist). Open the file and make "
            f"sure the value is exactly 0x + 64 hex chars on one line. "
            f"{intro}"
        )
    return None


async def process_credit_extraction(
    *, act: bool, dry_run: bool, transfer: bool,
    immediate: bool = False,
    slippage_bps: Optional[int] = None,
    tokens: Tuple[str, ...] = (),
    max_amounts: Optional[Mapping[str, Decimal]] = None,
    min_amounts: Optional[Mapping[str, Decimal]] = None,
    max_price_impact_bps: int = 0,
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
        pk_raw = (
            settings.payment_processor_admin_pkey or settings.ethereum_pkey
        )
        err = _validate_pkey(
            pk_raw,
            source="payment_processor_admin_pkey"
                   if settings.payment_processor_admin_pkey
                   else "ethereum_pkey",
            fork_rpc=fork_rpc,
        )
        if err:
            click.echo(err)
            return 2
        if not settings.payment_processor_admin_pkey:
            LOGGER.warning(
                "payment_processor_admin_pkey not set; falling back to "
                "ethereum_pkey"
            )
        admin_account = Account.from_key(HexBytes(pk_raw))
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
        token_filter=set(tokens) if tokens else None,
        max_amounts=max_amounts or {},
        min_amounts=min_amounts or {},
        max_price_impact_bps=max_price_impact_bps,
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
        # `balance` is the on-chain balance; `amount_in` is what we
        # actually passed to process() (capped by --max-amount). Render
        # both only when they differ so the common path stays terse.
        balance = entry.get("balance", entry["amount_in"])
        if balance != entry["amount_in"]:
            line = (f"  {entry['symbol']:6} balance={balance}"
                    f" amount_in={entry['amount_in']} (capped)")
        else:
            line = f"  {entry['symbol']:6} balance={balance}"
        if entry.get("auto_sized"):
            # Pull the impact_bps from the settled iteration so the
            # operator sees what the bisection landed on. Iterations
            # are appended in order; the last one matching settled
            # is the chosen amount.
            search = entry.get("price_impact_search") or {}
            settled = search.get("settled_amount_in")
            chosen = next(
                (it for it in reversed(search.get("iterations") or [])
                 if it.get("amount_in") == settled),
                None,
            )
            if chosen:
                line += (f" auto_sized impact={chosen['impact_bps']}bps "
                         f"iters={len(search.get('iterations') or [])}")
        if entry.get("skipped_reason"):
            line += f" skipped={entry['skipped_reason']}"
            if entry.get("skipped_reason") == "price_deviation":
                o = entry.get("oracle") or {}
                if o.get("deviation_bps") is not None:
                    line += f" Δ={o['deviation_bps']}bps"
            elif entry.get("skipped_reason") == "below_min_amount":
                if entry.get("min_amount_wei"):
                    line += f" min={entry['min_amount_wei']}"
            elif entry.get("skipped_reason") == "price_impact_too_high":
                search = entry.get("price_impact_search") or {}
                iters = search.get("iterations") or []
                if iters:
                    worst = min(iters, key=lambda i: i["impact_bps"])
                    line += (f" best_impact={worst['impact_bps']}bps "
                             f"iters={len(iters)}")
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
