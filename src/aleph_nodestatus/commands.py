"""CLI entrypoints for nodestatus monitoring, distribution, and credit rewards."""

import argparse
import asyncio
import json
import logging
import math
import os
import shutil
import sys
import time as time_mod
from decimal import Decimal

import click
from eth_account import Account
from hexbytes import HexBytes

from aleph_nodestatus import __version__
from aleph_nodestatus.sablier import sablier_monitoring_process

from .balances import do_reset_balances
from . import credit_distribution
from .credit_distribution import (
    CREDIT_DISTRIBUTION_POST_TYPE,
    build_total_summary,
    compute_rewards,
    get_latest_successful_credit_distribution,
    should_skip_run,
    zero_totals,
)
from .credit_extraction import process_credit_extraction
from .distribution import (
    create_distribution_tx_post,
    get_latest_successful_distribution,
    prepare_distribution,
)
from .erc20 import erc20_monitoring_process, process_contract_history
from .ethereum import (
    get_account,
    get_eth_account,
    get_token_contract,
    get_web3,
    transfer_tokens,
)
from .indexer_balance import indexer_monitoring_process
from .messages import parse_window_size
from .rewards_merge import merge_rewards
from .settings import settings, PublishMode
from .solana import solana_monitoring_process
from .status import process
from .storage import close_dbs, get_dbs
from .wage_subsidy import (
    compute_period_subsidy,
    compute_subsidy_daily,
    months_since_start,
)


def _clear_messages_db():
    """Clear the messages and scores databases to force a full resync.

    This keeps the erc20 and balances databases intact since they are
    expensive to rebuild (blockchain sync).
    """
    db_path = settings.db_path

    # Clear messages DB
    messages_path = os.path.join(db_path, "messages")
    if os.path.exists(messages_path):
        print(f"Removing {messages_path}...")
        shutil.rmtree(messages_path)
        print("Messages database cleared.")

    # Clear scores DB (depends on messages for consistency)
    scores_path = os.path.join(db_path, "scores")
    if os.path.exists(scores_path):
        print(f"Removing {scores_path}...")
        shutil.rmtree(scores_path)
        print("Scores database cleared.")

    # Keep erc20 and balances intact
    print("Keeping erc20 and balances databases intact.")
    print("Full resync of messages will start from 2020-01-01...")


__author__ = "Jonathan Schemoul"
__copyright__ = "Jonathan Schemoul"
__license__ = "mit"

LOGGER = logging.getLogger(__name__)

# First Ethereum mainnet block at or after 2026-05-01 00:00:00 UTC — the
# start of the new credit-distribution tokenomics. Hardcoded so the floor
# never drifts and we don't have to mix dates with block heights at runtime.
# Verified: block 24_996_367 ts=1_777_593_599 (still in 2026-04-30),
#           block 24_996_368 ts=1_777_593_611 (first block past the boundary).
# Refresh via etherscan: first block with timestamp >= 1_777_593_600.
CREDIT_DIST_FLOOR_HEIGHT = 24_996_368


def setup_logging(verbose):
    """Setup basic logging

    Args:
      loglevel (int): minimum loglevel for emitting messages
    """
    loglevel = [logging.WARNING, logging.INFO, logging.DEBUG][verbose]
    logformat = "[%(asctime)s] %(levelname)s:%(name)s:%(message)s"
    logging.basicConfig(
        level=loglevel, stream=sys.stdout, format=logformat, datefmt="%Y-%m-%d %H:%M:%S"
    )


@click.command()
@click.option("-v", "--verbose", count=True)
@click.option(
    "-t", "--testnet",
    help="Publish to testnet API instead of mainnet",
    is_flag=True
)
@click.option(
    "--force-resync",
    help="Force resync of messages DB from scratch (keeps erc20 intact)",
    is_flag=True
)
@click.option(
    "--window-size", "-w",
    help="Window size for message fetching (e.g., 1d, 2d, 1w, 12h). Default: 1d",
    default="1d"
)
@click.version_option(version=__version__)
def main(verbose, testnet, force_resync, window_size):
    """
    NodeStatus: Keeps an aggregate up to date with current nodes statuses

    Use --testnet to publish status updates to testnet for testing.
    Use --force-resync to rebuild the messages database from scratch.
    Use --window-size to control message fetching speed (larger = faster but more memory).
    """
    setup_logging(verbose)

    if testnet:
        PublishMode.set_testnet(True)
        LOGGER.info(f"TESTNET MODE: Publishing to {settings.aleph_testnet_api_server}")
    else:
        LOGGER.info(f"MAINNET MODE: Publishing to {settings.aleph_api_server}")

    if force_resync:
        LOGGER.warning("FORCE RESYNC: Clearing messages and scores databases...")
        _clear_messages_db()

    # Parse and validate window size
    try:
        window_seconds = parse_window_size(window_size)
        LOGGER.info(f"Using window size: {window_size} ({window_seconds} seconds)")
    except ValueError as e:
        LOGGER.error(str(e))
        return

    LOGGER.debug("Starting nodestatus")
    account = get_eth_account()
    dbs = get_dbs()
    LOGGER.debug(f"Starting with ETH account {account.address}")
    asyncio.run(process(dbs, window_size=window_seconds))


async def process_distribution(start_height, end_height, act=False, reward_sender=None):
    account = get_eth_account()
    dbs = get_dbs()
    LOGGER.debug(f"Starting with ETH account {account.address}")

    if end_height == -1:
        end_height = get_web3().eth.block_number

    if start_height == -1:
        last_end_height, dist = await get_latest_successful_distribution(reward_sender)

        if last_end_height and dist:
            start_height = last_end_height + 1

        else:
            start_height = 0

    reward_start, end_height, rewards = await prepare_distribution(
        dbs, start_height, end_height
    )

    # Determine status and tags based on mode
    is_testnet = PublishMode.is_testnet()
    if is_testnet:
        status = "simulation"
        tags = ["simulation", settings.filter_tag]
    elif act:
        status = "distribution"
        tags = ["distribution", settings.filter_tag]
    else:
        status = "calculation"
        tags = ["calculation", settings.filter_tag]

    distribution = dict(
        incentive="corechannel",
        status=status,
        start_height=reward_start,
        end_height=end_height,
        rewards=rewards,
        tags=tags,
    )

    # Print summary
    total_rewards = sum(rewards.values())
    api_server = PublishMode.get_publish_api_server()
    print(f"\n{'='*60}")
    print(f"Distribution Summary ({status.upper()})")
    print(f"{'='*60}")
    print(f"Target API: {api_server}")
    print(f"Block range: {reward_start} -> {end_height}")
    print(f"Total recipients: {len(rewards)}")
    print(f"Total rewards: {total_rewards:,.2f} ALEPH")
    print(f"{'='*60}\n")

    transfer_metadata = {"targets": []}
    if act and not is_testnet:
        print("Executing actual token transfers...")
        max_items = settings.ethereum_batch_size
        distribution_list = list(rewards.items())

        for i in range(math.ceil(len(distribution_list) / max_items)):
            step_items = distribution_list[max_items * i : max_items * (i + 1)]
            print(f"Batch {i+1}: transferring to {len(step_items)} recipients")
            await transfer_tokens(dict(step_items), metadata=transfer_metadata)

    distribution["targets"] = transfer_metadata["targets"]
    await create_distribution_tx_post(distribution)


@click.command()
@click.option("-v", "--verbose", count=True)
@click.option("-a", "--act", help="Do actual batch transfer", is_flag=True)
@click.option(
    "-t", "--testnet",
    help="Publish to testnet API instead of mainnet (no token transfers)",
    is_flag=True
)
@click.option(
    "-s", "--start-height:", "start_height", help="Starting height", default=-1
)
@click.option("-e", "--end-height", "end_height", help="Ending height", default=-1)
@click.option(
    "--reward-sender",
    "reward_sender",
    help="Reward emitting address (to see last distributions)",
    default=None,
)
def distribute(verbose, act=False, testnet=False, start_height=-1, end_height=-1, reward_sender=None):
    """
    Staking distribution script.

    Modes:
      (default)   Calculate rewards, post to mainnet as "calculation"
      --testnet   Calculate rewards, post to TESTNET as "simulation" (safe to test)
      --act       Calculate AND transfer tokens, post to mainnet as "distribution"
    """
    setup_logging(verbose)

    if act and testnet:
        print("ERROR: Cannot use --act and --testnet together")
        return

    # Set publish mode
    if testnet:
        PublishMode.set_testnet(True)

    mode = "TESTNET (simulation)" if testnet else ("LIVE DISTRIBUTION" if act else "CALCULATION ONLY")
    print(f"Mode: {mode}")
    print(f"Start height: {start_height}, End height: {end_height}")

    asyncio.run(
        process_distribution(
            start_height, end_height, act=act, reward_sender=reward_sender
        )
    )


def _payout_after_slash(final_rewards, slashed):
    """Per-address payout = full reward minus its withheld slash, floored at 0.

    Zero-value entries are dropped so the on-chain transfer never targets a
    recipient owed nothing. When `slashed` is empty this returns `final_rewards`
    unchanged (modulo zero filtering), keeping the slash-disabled path identical.
    """
    payout = {}
    for addr, amount in final_rewards.items():
        net = max(0.0, amount - slashed.get(addr, 0.0))
        if net > 0:
            payout[addr] = net
    return payout


def max_successful_nonce(distribution_content, get_tx_nonce=None):
    """Highest ETH nonce among the SUCCESSFUL transfer batches of a published
    distribution post.

    Uses the per-batch `nonce` when the post recorded it. Older posts (published
    before nonce tracking) carry the batch `tx` hash but no nonce; when a
    `get_tx_nonce(tx_hash) -> int | None` resolver is given, the nonce is
    recovered on-chain from that hash. Returns None when no nonce can be
    determined (no content, or no recorded nonce and no resolvable tx hash).
    """
    if not distribution_content:
        return None
    nonces = []
    for t in distribution_content.get("targets", []):
        if not t.get("success"):
            continue
        n = t.get("nonce")
        if isinstance(n, int) and not isinstance(n, bool):
            nonces.append(n)
            continue
        tx = t.get("tx")
        if tx and get_tx_nonce is not None:
            resolved = get_tx_nonce(tx)
            if isinstance(resolved, int) and not isinstance(resolved, bool):
                nonces.append(resolved)
    return max(nonces) if nonces else None


def nonce_gap_reason(current_nonce, last_content, get_tx_nonce=None):
    """Return a human-readable reason string if the signer's current on-chain
    nonce does NOT cleanly continue from the last distribution's transfers
    (i.e. transactions happened from that account since), else None.

    After a distribution whose highest successful tx used nonce N, the account's
    transaction count (next nonce) is N+1. If the current count exceeds N+1,
    extra transactions were sent in between — a prior batchTransfer that was
    never published, or a manual operator tx — and re-running this distribution
    would double-pay. Cases:

      - No prior distribution at all -> None (first run, no baseline).
      - Prior distribution whose nonce can't be determined (no recorded nonce
        AND no resolvable tx hash) -> reason (cannot verify; review manually).
      - current_nonce > last_max + 1 -> reason (gap: intermediate transactions).

    `get_tx_nonce` resolves a batch tx hash to its on-chain nonce, used to back
    older posts that recorded only the tx hash.
    """
    if last_content is None:
        return None
    last_max = max_successful_nonce(last_content, get_tx_nonce)
    if last_max is None:
        return (
            "cannot verify nonce continuity: the last distribution post records "
            "no transfer nonce and no resolvable transaction hash"
        )
    if current_nonce > last_max + 1:
        gap = current_nonce - (last_max + 1)
        return (
            f"signer nonce {current_nonce} is beyond the last distribution's "
            f"max nonce {last_max} + 1: {gap} intermediate transaction(s) since "
            f"the last distribution"
        )
    return None


async def process_credit_distribution(
    start_height, end_height, *,
    act=False, dry_run=False, force=False, force_cap=False,
    force_nonce_gap=False,
    flags=None, reward_sender=None, full_resync=False,
    safety_blocks=None, require_witness=None,
    fork_rpc=None, reconcile_bps=0,
):
    flags = flags or {}
    from .slashing import (
        SlashAccumulator, compute_slashing, enabled_slash_streams,
    )
    slash_on = settings.credit_dist_slash_enabled
    slash_accumulator = SlashAccumulator() if slash_on else None
    dbs = get_dbs()
    if fork_rpc:
        # Fork mode: stand up a Web3 pointed at the local fork, leaving
        # the cached `get_web3()` alone so anything else this process
        # touches still talks to the production RPC settings dictated.
        # The cron and live --act path are untouched.
        from web3 import HTTPProvider, Web3 as _W3
        web3 = _W3(HTTPProvider(fork_rpc))
    else:
        web3 = get_web3()

    if safety_blocks is None:
        safety_blocks = settings.credit_dist_safety_blocks
    if require_witness is None:
        require_witness = settings.credit_dist_require_expense_witness

    # Single RPC call: reused for both the default end_height and the safety
    # cap so a load-balanced provider (e.g. Infura) can't return two different
    # block numbers and produce end_height < start_height.
    current_block = web3.eth.block_number

    if end_height in (None, -1):
        end_height = current_block

    # Right-edge finality margin. See settings.credit_dist_safety_blocks for
    # rationale. If the caller passed an explicit end_height that's already
    # below current_block - safety, leave it alone — they know what they're
    # doing (replays, audits, etc.). Otherwise cap.
    max_allowed = current_block - safety_blocks
    if end_height > max_allowed:
        original = end_height
        end_height = max_allowed
        click.echo(
            f"Capping end_height {original} -> {end_height} "
            f"(safety_blocks={safety_blocks}, ~{safety_blocks * 12 // 60}min finality buffer)"
        )

    # === Cadence guard ===
    # Fires unconditionally when --act is set, regardless of how start_height
    # was derived. Without this, an operator supplying --start-height N bypasses
    # the rate-limit silently and (combined with C1's partial-failure cursor)
    # creates a path to double distribution.
    #
    # Fetch the last distribution once and reuse for both the cadence guard
    # and the cursor derivation below (one posts-API round-trip per run).
    last_end = None
    last_content = None
    if act:
        last_end, last_content = await get_latest_successful_credit_distribution(
            reward_sender
        )
        if last_end and should_skip_run(
            last_end, end_height,
            settings.credit_dist_min_interval_blocks, force,
        ):
            click.echo(
                f"Cadence guard: only {end_height - last_end} blocks "
                f"since last distribution. Skipping; use --force to override."
            )
            return 0

    if start_height in (None, -1):
        if last_end is None:
            last_end, last_content = (
                await get_latest_successful_credit_distribution(reward_sender)
            )
        if last_end:
            start_height = last_end + 1
            click.echo(f"Resuming from last_end={last_end}; start={start_height}")
        else:
            start_height = CREDIT_DIST_FLOOR_HEIGHT
            click.echo(
                f"No prior distribution; starting at floor "
                f"{CREDIT_DIST_FLOOR_HEIGHT}"
            )

    # Hard floor: any resolved start_height below the launch block would replay
    # pre-launch expenses. Explicit values are an operator error; resumed
    # values lower than the floor mean the prior cursor predates the launch.
    if start_height < CREDIT_DIST_FLOOR_HEIGHT:
        click.echo(
            f"ERROR: start_height {start_height} is below floor "
            f"{CREDIT_DIST_FLOOR_HEIGHT}. Pass --start-height >= "
            f"{CREDIT_DIST_FLOOR_HEIGHT}."
        )
        sys.exit(2)

    if end_height <= start_height:
        raise ValueError(
            f"end_height ({end_height}) must be greater than start_height "
            f"({start_height})"
        )

    start_time = web3.eth.get_block(start_height).timestamp
    end_time = web3.eth.get_block(end_height).timestamp

    # Right-edge finality witness. An expense whose `end_date ∈ [start_time,
    # end_time]` may not yet be published when NS runs, so the safety-blocks
    # cap alone is not enough — confirm the publisher has actually moved
    # past `end_time` by finding at least one confirmed expense post with
    # `expense.end_date > end_time`. Skipped if credit_revenue is off
    # (no expense math anyway).
    if flags.get("credit_revenue"):
        witness_ok = await credit_distribution.has_expense_witness_after(
            PublishMode.get_publish_api_server(),
            end_time,
            settings.credit_expense_sender,
        )
        if not witness_ok:
            msg = (
                f"No confirmed expense witness found with end_date > end_time "
                f"({end_time}). Publisher may be stalled or end_time is in "
                f"the future."
            )
            if require_witness:
                click.echo(f"ABORT: {msg}")
                sys.exit(2)
            else:
                LOGGER.warning(msg)

    # Aleph API `messages.json` AGGREGATE+date queries are slow (~80s per
    # page), so fetch node snapshots once and share between the credit
    # revenue path (snapshot mode only) and the wage subsidy below.
    # full_resync drives credit revenue from the local state machine, so
    # it doesn't need snapshots there — but wage still does.
    api_server = PublishMode.get_publish_api_server()
    # Dry-run only: collect the source item_hashes (snapshots + expenses)
    # actually consumed by the calculation so they can be diffed against
    # other services' input sets. The fetchers append to these lists
    # opportunistically — `None` is a no-op in the production path.
    snapshot_hashes = [] if dry_run else None
    expense_hashes  = [] if dry_run else None
    snapshots = None
    if (flags.get("credit_revenue") and not full_resync) or flags.get("wage"):
        # Call through the module so test-time monkeypatching of
        # `credit_distribution.fetch_node_snapshots` is observed here.
        snapshots = await credit_distribution.fetch_node_snapshots(
            api_server, start_time, end_time,
            out_hashes=snapshot_hashes,
        )

    # === Step 2: credit_revenue + holder_tier rewards ===
    streams = {
        "credit_revenue": ({}, zero_totals()),
        "holder_tier":    ({}, zero_totals()),
        "detailed": {"credit_revenue": {}, "holder_tier": {}},
    }
    if flags.get("credit_revenue"):
        streams = await compute_rewards(
            start_time, end_time,
            full_resync=full_resync,
            include_holder_tier=flags.get("holder_tier", False),
            sender=settings.credit_expense_sender,
            dbs=dbs, end_height=end_height, web3=web3,
            snapshots=snapshots,
            last_end_height=last_end,
            out_expense_hashes=expense_hashes,
            slash_accumulator=slash_accumulator,
        )

    credit_rewards, credit_totals = streams["credit_revenue"]
    holder_rewards, holder_totals = streams["holder_tier"]
    revenue_detailed = streams.get(
        "detailed", {"credit_revenue": {}, "holder_tier": {}},
    )

    # === Step 3: wage subsidy ===
    wage_rewards = {}
    wage_detailed = {}
    wage_totals = {
        "period_total_aleph": 0,
        "unallocated_aleph": 0,
        "start_t_months": 0,
        "end_t_months": 0,
        "split": {"ccn": 0, "crn": 0, "staker": 0},
    }
    if flags.get("wage"):
        if snapshots:
            # Per-UTC-day attribution: each day's wage pool is split with
            # the snapshot most recently observed at or before that day's
            # end (mirrors aleph-api-credit's `computeAndPersistDay` +
            # `findSnapshotAtOrBeforeByTs(snaps, t1Ms)`). The integral is
            # additive, so the network total is unchanged; only the per-
            # address attribution tracks mid-period state changes (CRN
            # link/unlink, score crossings, staker movements) instead of
            # collapsing them onto the final snapshot.
            wage_rewards, wage_totals, wage_detailed = compute_subsidy_daily(
                start_time, end_time, snapshots, web3=web3,
                accumulator=slash_accumulator,
            )
        else:
            period_total = compute_period_subsidy(start_time, end_time)
            wage_totals = {
                "period_total_aleph": period_total,
                "unallocated_aleph":  period_total,
                "start_t_months":     months_since_start(start_time),
                "end_t_months":       months_since_start(end_time),
                "split": {
                    "ccn":    period_total * settings.wage_ccn_share,
                    "crn":    period_total * settings.wage_crn_share,
                    "staker": period_total * settings.wage_staker_share,
                },
            }
            LOGGER.warning(
                f"No snapshots for wage subsidy; entire {period_total:.4f} "
                f"ALEPH recorded as unallocated."
            )

    # === Step 4: merge + dust filter ===
    # `by_address_detailed` inverts the per-source breakdowns so each
    # surviving recipient address carries its full {source: {component: amount}}
    # attribution. Sums per address equal final_rewards[addr] by construction.
    final_rewards, by_source, by_address_detailed = merge_rewards(
        {"credit_revenue": credit_rewards,
         "holder_tier":    holder_rewards,
         "wage_subsidy":   wage_rewards},
        details={
            "credit_revenue": revenue_detailed.get("credit_revenue", {}),
            "holder_tier":    revenue_detailed.get("holder_tier", {}),
            "wage_subsidy":   wage_detailed,
        },
        dust_threshold=settings.credit_dist_dust_threshold_aleph,
    )

    # Enrich the per-address detail with a `total` field inside each
    # stream block so the dry-run `rewards_detailed` output is fully
    # self-contained: each address carries both its per-component
    # breakdown (existing) and the per-stream total (formerly available
    # via the published rewards_by_source field, now removed from the
    # post). Streams that contributed zero for an address are omitted.
    # Iterating over final_rewards (not by_address_detailed) covers the
    # edge case of survivors whose details dict was empty for every
    # stream.
    #
    # NOTE: this mutates `by_address_detailed` in place after the
    # `merge_rewards` boundary. Kept here (rather than inside
    # merge_rewards) because the "per-stream total" enrichment is a
    # publish-shape concern specific to this caller — merge_rewards is
    # stream-agnostic. Anything downstream that holds a reference to
    # `by_address_detailed` will see the enriched dict.
    for addr in final_rewards:
        detail = by_address_detailed.setdefault(addr, {})
        for stream in by_source:
            amount = by_source[stream].get(addr, 0.0)
            if amount:
                detail.setdefault(stream, {})["total"] = amount

    # === Step 4b: slashing pass ===
    # `rewards`/`final_rewards` stay intact (full calculation, published as-is).
    # `slashed` is withheld from the on-chain payout below.
    slashed, slashed_meta_nodes = {}, []
    if slash_on:
        close_resource_nodes = snapshots[-1][2] if snapshots else {}
        slashed, slashed_meta_nodes = compute_slashing(
            slash_accumulator, close_resource_nodes, end_height,
            enabled_streams=enabled_slash_streams(),
            retroactive=settings.credit_dist_slash_retroactive,
            threshold_days=settings.credit_dist_slash_threshold_days,
            blocks_per_day=settings.ethereum_blocks_per_day,
        )

    # Payout view: full rewards minus the withheld slash per address, floored
    # at 0. Used for the balance check and the actual transfer only.
    payout_rewards = _payout_after_slash(final_rewards, slashed)

    # === Global cap (before balance check) ===
    # Aborts when upstream math produces inflated rewards, regardless of
    # whether the sender happens to be flush. Independent of --force
    # (cadence): an operator must explicitly --force-cap to override.
    if act and flags.get("transfer") and final_rewards:
        owed_total = sum(final_rewards.values())
        if owed_total > settings.credit_dist_max_total_aleph and not force_cap:
            click.echo(
                f"ABORT: owed {owed_total:.2f} ALEPH > max_total cap "
                f"{settings.credit_dist_max_total_aleph}. Inspect upstream "
                f"inputs (credit_price_aleph, wage integral, holder_tier "
                f"sums) and rerun with --force-cap to override if "
                f"intentional."
            )
            sys.exit(1)

    # === Balance safety check (before any real transfer) ===
    # Check the balance of the actual transfer sender (get_eth_account, derived
    # from ethereum_pkey), not settings.distribution_recipient. In the intended
    # deployment they are the same address — but they are independently
    # configurable settings, and transfer_tokens uses the pkey-derived account.
    if act and flags.get("transfer") and payout_rewards:
        sender = get_eth_account().address
        if sender.lower() != settings.distribution_recipient.lower():
            click.echo(
                f"WARNING: transfer sender {sender} differs from "
                f"settings.distribution_recipient "
                f"{settings.distribution_recipient}. Checking sender balance."
            )
        token = get_token_contract(web3)
        # Decimal comparison to avoid float accumulation pulling `owed`
        # slightly under `bal` (or vice versa) at large totals — the
        # previous 1e-6 epsilon worked for current scale but doesn't
        # generalise once the wage subsidy ramps and per-run totals
        # reach the 1M+ ALEPH range.
        bal_wei = token.functions.balanceOf(
            web3.to_checksum_address(sender)
        ).call()
        bal  = Decimal(bal_wei) / Decimal(10 ** settings.ethereum_decimals)
        owed = sum(
            (Decimal(str(v)) for v in payout_rewards.values()),
            Decimal("0"),
        )
        if owed > bal:
            click.echo(
                f"ABORT: owed {owed} ALEPH > balance {bal} at {sender}"
            )
            sys.exit(1)

        # Pre-flight nonce-continuity guard (B1 double-pay protection). If the
        # signer has sent transactions since the last distribution — a prior
        # batchTransfer that crashed before publishing, or a manual operator tx
        # — re-running would re-broadcast the same payouts. Block here; the
        # operator reconciles manually and reruns with --force-nonce-gap.
        # Skipped in fork mode (the fork account nonce isn't the prod cursor).
        if not fork_rpc:
            current_nonce = web3.eth.get_transaction_count(
                web3.to_checksum_address(sender)
            )

            # Older distribution posts recorded only the batch tx hash, not the
            # nonce. Recover the nonce on-chain from that hash so the guard works
            # for the next run after this distribution too.
            def _tx_nonce(tx_hash):
                h = tx_hash if tx_hash.startswith("0x") else "0x" + tx_hash
                try:
                    return web3.eth.get_transaction(h)["nonce"]
                except Exception:
                    return None

            reason = nonce_gap_reason(current_nonce, last_content, _tx_nonce)
            if reason and not force_nonce_gap:
                click.echo(
                    f"ABORT: {reason}. Review the account's transactions since "
                    f"the last distribution and rerun with --force-nonce-gap "
                    f"to override."
                )
                sys.exit(1)
            if reason:
                click.echo(
                    f"WARNING: nonce-continuity guard overridden "
                    f"(--force-nonce-gap): {reason}"
                )

    # === Step 5: transfer (or simulate) ===
    is_testnet = PublishMode.is_testnet()
    transfer_metadata = {"sources": list(by_source.keys()), "targets": []}
    # Fork-mode audit accumulator. Populated only when fork_rpc is set;
    # surfaces at the end of the function as the process exit code.
    audit_results = []
    if fork_rpc and flags.get("transfer"):
        # Fork-verified batchTransfer: full sign + broadcast + receipt
        # + per-recipient reconciliation against ERC20 Transfer events.
        # Mirrors the extract fork audit. Does NOT publish (--no-publish
        # is implied by --dry-run).
        # NOTE: get_token_contract is already module-imported at the top
        # of this file — re-importing it here would make Python treat it
        # as a function-local for the whole of `process_credit_distribution`
        # and break the live --act path at line ~560.
        from .credit_extraction import _validate_pkey
        from .ethereum import (
            audit_distribution_tx, transfer_tokens_audited,
        )
        err = _validate_pkey(
            settings.ethereum_pkey,
            source="ethereum_pkey",
            fork_rpc=fork_rpc,
        )
        if err:
            click.echo(err)
            return 2
        signer = Account.from_key(HexBytes(settings.ethereum_pkey))
        contract = get_token_contract(web3)
        click.echo(
            f"Executing batchTransfer on fork (signer={signer.address}) …"
        )
        max_items = settings.ethereum_batch_size
        items = [(a, v) for a, v in payout_rewards.items() if v > 0]
        nonce = web3.eth.get_transaction_count(signer.address)
        for i in range(math.ceil(len(items) / max_items)):
            step = dict(items[max_items*i:max_items*(i+1)])
            click.echo(
                f"Batch {i+1}: transferring to {len(step)} recipients "
                f"(nonce={nonce})"
            )
            try:
                tx_info = transfer_tokens_audited(
                    web3, contract, signer, step, nonce=nonce,
                )
            except Exception as e:
                click.echo(f"  batch broadcast FAILED: {e!r}")
                audit_results.append({
                    "batch_index": i + 1, "reconcile_failed": True,
                    "error": repr(e),
                })
                continue
            nonce += 1
            if tx_info["status"] == 0:
                click.echo(f"  batch reverted on fork (tx_hash="
                           f"{tx_info['tx_hash']})")
                audit_results.append({
                    "batch_index": i + 1, "reconcile_failed": True,
                    "error": "tx_reverted_on_fork",
                    "tx_hash": tx_info["tx_hash"],
                })
                continue
            audit_results.append(audit_distribution_tx(
                web3,
                tx_info=tx_info, batch_targets=step, batch_index=i + 1,
                token_address=settings.ethereum_token_contract,
                sender_address=signer.address,
                reconcile_bps=reconcile_bps,
            ))
            # Mirror the production transfer_metadata shape so downstream
            # consumers (the distribution post body, the failed-batch
            # summary) see a familiar structure.
            transfer_metadata["targets"].append({
                "success": True, "status": "fork_mined",
                "tx": tx_info["tx_hash"], "chain": "ETH-FORK",
                "sender": signer.address,
                "targets": step,
                "total": sum(step.values()),
            })
    elif flags.get("transfer") and act and not is_testnet:
        click.echo("Executing batchTransfer …")
        max_items = settings.ethereum_batch_size
        items = [(a, v) for a, v in payout_rewards.items() if v > 0]
        for i in range(math.ceil(len(items) / max_items)):
            step = items[max_items*i:max_items*(i+1)]
            click.echo(f"Batch {i+1}: transferring to {len(step)} recipients")
            await transfer_tokens(dict(step), metadata=transfer_metadata)
    elif not flags.get("transfer"):
        click.echo("--no-transfer / dry-run: skipping batchTransfer")
        items = [(a, v) for a, v in payout_rewards.items() if v > 0]
        n = len(items)
        batch_size = settings.ethereum_batch_size
        for i in range(math.ceil(n / batch_size)):
            count = min(batch_size, n - i * batch_size)
            click.echo(f"Batch {i+1}: would transfer to {count} recipients")

    # === Step 6: publish ===
    # Per-account × per-source breakdowns (`rewards_by_source` and the
    # finer-grained `rewards_detailed`) are intentionally NOT in the
    # published distribution. The post aggregates over whatever
    # arbitrary window the operator ran with (--start-height → now), so
    # per-account-per-source data at that granularity isn't useful for
    # end consumers and bloats the payload. A dedicated service will
    # expose this view later with proper time-window filtering
    # (1D/1W/1M); out of scope for this PR. The dry-run output still
    # prints `rewards_detailed` for local debugging.
    status = "distribution" if act else "calculation"
    distribution = {
        "incentive": "credits",
        "status": status,
        "start_height": start_height, "end_height": end_height,
        "start_time": start_time, "end_time": end_time,
        "rewards": final_rewards,
        "slashed": slashed,
        "slashed_meta": {
            "enabled": slash_on,
            "threshold_days": settings.credit_dist_slash_threshold_days,
            "retroactive": settings.credit_dist_slash_retroactive,
            "streams": list(enabled_slash_streams()) if slash_on else [],
            "nodes": slashed_meta_nodes,
        },
        "credit_revenue_totals": credit_totals,
        "holder_tier_totals": {**holder_totals,
                                "included": flags.get("holder_tier", False)},
        "wage_subsidy": wage_totals,
        "total": build_total_summary(final_rewards, by_address_detailed),
        "feature_flags": flags,
        "tags": [status, "credits", settings.filter_tag],
        "sources": transfer_metadata["sources"],
        "targets": transfer_metadata["targets"],
    }

    if flags.get("publish") and not dry_run:
        await create_distribution_tx_post(
            distribution, post_type=CREDIT_DISTRIBUTION_POST_TYPE
        )
    else:
        click.echo("--no-publish / dry-run: skipping Aleph post")
        click.echo(json.dumps(distribution, indent=2, default=str))
        if dry_run:
            click.echo(
                "\n=== rewards_detailed (dry-run debug; not published) ==="
            )
            click.echo(
                json.dumps(by_address_detailed, indent=2, default=str)
            )
            # Source item_hashes actually consumed by the calculation.
            # Sorted so the output can be diffed against other services'
            # input sets (e.g. aleph-api-credit) to localize input-set
            # discrepancies. Lists are None outside dry-run and empty
            # when the corresponding fetch path didn't run (e.g. snapshots
            # under full_resync without wage).
            click.echo(
                "\n=== source item_hashes (dry-run debug; not published) ==="
            )
            click.echo(json.dumps({
                "snapshots": {
                    "count":  len(snapshot_hashes or []),
                    "hashes": sorted(snapshot_hashes or []),
                },
                "expenses": {
                    "count":  len(expense_hashes or []),
                    "hashes": sorted(expense_hashes or []),
                },
            }, indent=2, default=str))

    # === Step 7: failed-batch summary ===
    # Surface failed batches at run end so operators don't have to scroll
    # through per-batch echo lines to find them. Failed recipients must be
    # retransferred manually and the audit post AMENDed.
    if flags.get("transfer") and act and not is_testnet:
        failed = [
            (i, t) for i, t in enumerate(transfer_metadata["targets"])
            if not t.get("success")
        ]
        if failed:
            click.echo(
                f"\n=== {len(failed)} batch(es) failed — "
                f"manual action required ==="
            )
            for i, t in failed:
                click.echo(
                    f"  Batch {i+1}: {len(t['targets'])} recipients, "
                    f"{t['total']} ALEPH, tx={t.get('tx') or 'none'}"
                )
            click.echo(
                "Manually retransfer the failed recipients (see the "
                "published distribution post for recipient lists), then "
                "AMEND the post to mark them success=true."
            )

    # === Fork-mode summary ===
    if fork_rpc:
        n_failed = sum(1 for a in audit_results if a.get("reconcile_failed"))
        n_total  = len(audit_results)
        click.echo(
            f"\n=== Distribute fork audit summary ==="
            f"\n  batches audited : {n_total}"
            f"\n  matched         : {n_total - n_failed}"
            f"\n  failed          : {n_failed}"
        )
        if n_failed:
            click.echo("  failing batches:")
            for a in audit_results:
                if not a.get("reconcile_failed"):
                    continue
                click.echo(
                    f"    batch {a.get('batch_index')}: "
                    f"{a.get('error') or 'reconciliation mismatch'}"
                )
            click.echo("  !!! RECONCILIATION FAILED — exit code 2")
            return 2
    return 0


@click.command()
@click.option("-v", "--verbose", count=True)
@click.option("-a", "--act", help="Execute transfers + post status=distribution",
              is_flag=True)
@click.option("-t", "--testnet",
              help="Publish to testnet API; no transfers",
              is_flag=True)
@click.option("--dry-run",
              help="No post, no transfers; simulate process() via eth_call",
              is_flag=True)
@click.option("--force", help="Bypass the cadence guard", is_flag=True)
@click.option("--force-cap", "force_cap", is_flag=True,
              help="Bypass the global per-run distribution cap")
@click.option("--force-nonce-gap", "force_nonce_gap", is_flag=True,
              help="Bypass the pre-flight nonce-continuity guard (only after "
                   "manually reconciling transactions sent since the last "
                   "distribution)")
@click.option("-s", "--start-height", "start_height", default=-1, type=int,
              help="Starting block height, default: resume from last distribution")
@click.option("-e", "--end-height", "end_height", default=-1, type=int,
              help="Ending block height, default: latest")
@click.option("--full-resync", is_flag=True,
              help="Replay full state machine from genesis")
@click.option("--no-credit-revenue", "no_credit_revenue", is_flag=True,
              help="Skip credit-expense reward computation")
@click.option("--no-wage", "no_wage", is_flag=True,
              help="Skip wage subsidy")
@click.option("--enable-holder-tier", "enable_holder_tier", is_flag=True,
              help="Force holder-tier ON (overrides env if disabled). "
                   "Mutually exclusive with --no-holder-tier; if both are "
                   "set programmatically, --no-holder-tier wins.")
@click.option("--no-holder-tier", "no_holder_tier", is_flag=True,
              help="Force holder-tier OFF (overrides env=True, the default). "
                   "Wins over --enable-holder-tier when both are set, since "
                   "over-distribution is worse than under-distribution.")
@click.option("--no-transfer", "no_transfer", is_flag=True,
              help="Compute everything but don't broadcast txs")
@click.option("--no-publish", "no_publish", is_flag=True,
              help="Don't post the Aleph distribution record")
@click.option("--reward-sender", default=None,
              help="Address used to look up the previous distribution")
@click.option("--safety-blocks", "safety_blocks", type=int, default=None,
              help="Right-edge finality buffer. Default from settings (500).")
@click.option("--no-witness-required", "no_witness_required", is_flag=True,
              help="Downgrade missing-expense-witness abort to a warning.")
@click.option("--fork-rpc", "fork_rpc", default=None,
              help="URL of a local mainnet fork (e.g. http://localhost:8545 "
                   "for `anvil --fork-url …`). Requires --dry-run; refused "
                   "with --act or --testnet. Upgrades dry-run into a full "
                   "sign+broadcast+receipt audit of each batchTransfer "
                   "against the fork. Overrides settings.distribute_fork_rpc.")
@click.option("--reconcile-bps", "reconcile_bps", default=None, type=int,
              help="Max acceptable delta between realized and expected "
                   "per-recipient amount in fork mode (bps). Default 0 "
                   "(exact wei match). Override "
                   "settings.distribute_max_reconcile_delta_bps.")
def distribute_credits(verbose, act, testnet, dry_run, force, force_cap,
                       force_nonce_gap,
                       start_height, end_height, full_resync,
                       no_credit_revenue, no_wage,
                       enable_holder_tier, no_holder_tier,
                       no_transfer, no_publish,
                       reward_sender, safety_blocks, no_witness_required,
                       fork_rpc, reconcile_bps):
    """Credit-based distribution script (new tokenomics)."""
    setup_logging(verbose)

    if act and testnet:
        click.echo("ERROR: --act and --testnet are mutually exclusive")
        sys.exit(2)
    if act and dry_run:
        click.echo("ERROR: --act and --dry-run are mutually exclusive")
        sys.exit(2)
    if enable_holder_tier and no_holder_tier:
        click.echo("ERROR: --enable-holder-tier and --no-holder-tier are "
                   "mutually exclusive")
        sys.exit(2)

    # Resolve fork-rpc: CLI flag wins, else settings (env). Same
    # mutual-exclusion guards as extract: never accept fork mode in any
    # configuration that could touch mainnet or the Aleph testnet API,
    # because the broadcast path signs real txs.
    resolved_fork_rpc = fork_rpc or (settings.distribute_fork_rpc or None)
    if resolved_fork_rpc:
        if act:
            click.echo(
                "ERROR: --fork-rpc / distribute_fork_rpc cannot be combined "
                "with --act. Fork mode broadcasts to the configured RPC; "
                "running --act with a fork URL active would either silently "
                "execute against the fork or — if the URL was a typo for a "
                "real node — execute live. Clear distribute_fork_rpc to "
                "distribute for real."
            )
            sys.exit(2)
        if testnet:
            click.echo(
                "ERROR: --fork-rpc / distribute_fork_rpc cannot be combined "
                "with --testnet. Pick one verification path."
            )
            sys.exit(2)
        if not dry_run:
            click.echo(
                "ERROR: --fork-rpc / distribute_fork_rpc requires --dry-run."
            )
            sys.exit(2)
        if not _fork_rpc_is_safe(resolved_fork_rpc):
            click.echo(
                f"ERROR: fork RPC host must be one of {_FORK_RPC_SAFE_HOSTS}. "
                f"Got: {resolved_fork_rpc}. Refusing to sign real txs "
                f"against a non-loopback endpoint."
            )
            sys.exit(2)

    if testnet:
        PublishMode.set_testnet(True)

    flags = _resolve_feature_flags(
        no_credit_revenue=no_credit_revenue,
        no_wage=no_wage, enable_holder_tier=enable_holder_tier,
        no_holder_tier=no_holder_tier,
        no_transfer=no_transfer, no_publish=no_publish,
        act=act, dry_run=dry_run,
    )
    # In fork mode we want the transfer step to actually run (against
    # the fork). The flag-resolution above set transfer=False because
    # `act=False`; flip it back on.
    if resolved_fork_rpc:
        flags["transfer"] = True

    mode = (
        "DRY-RUN (FORK)"        if resolved_fork_rpc else
        "DRY-RUN"               if dry_run else
        "TESTNET (calculation)" if testnet else
        "LIVE DISTRIBUTION"     if act else
        "CALCULATION ONLY"
    )
    click.echo(f"Mode: {mode}")
    if resolved_fork_rpc:
        click.echo(f"Fork RPC: {resolved_fork_rpc}")
    click.echo(f"Flags: {flags}")

    # `--no-witness-required` is a flag, so absence means "use settings
    # default" (None passes through; process_credit_distribution resolves
    # it). Setting it to True only when the flag is present preserves the
    # settings-driven default behavior.
    require_witness = False if no_witness_required else None

    exit_code = asyncio.run(process_credit_distribution(
        start_height=start_height, end_height=end_height,
        act=act, dry_run=dry_run, force=force, force_cap=force_cap,
        force_nonce_gap=force_nonce_gap,
        flags=flags, reward_sender=reward_sender,
        full_resync=full_resync,
        safety_blocks=safety_blocks,
        require_witness=require_witness,
        fork_rpc=resolved_fork_rpc,
        reconcile_bps=(
            reconcile_bps if reconcile_bps is not None
            else settings.distribute_max_reconcile_delta_bps
        ),
    ))
    if exit_code:
        sys.exit(exit_code)


def _resolve_feature_flags(*, no_credit_revenue, no_wage,
                           enable_holder_tier, no_holder_tier,
                           no_transfer, no_publish, act, dry_run):
    f = {
        "credit_revenue": settings.credit_dist_credit_revenue_enabled,
        "wage":           settings.credit_dist_wage_subsidy_enabled,
        "holder_tier":    settings.credit_dist_holder_tier_enabled,
        "transfer":       settings.credit_dist_transfer_enabled,
        "publish":        settings.credit_dist_publish_enabled,
    }
    if no_credit_revenue: f["credit_revenue"] = False
    if no_wage:           f["wage"] = False
    # holder_tier overrides are last-wins. Order matters only for
    # programmatic callers — the CLI rejects mutual exclusivity at parse
    # time. no_holder_tier wins last because accidentally enabling
    # holder_tier (over-distribution) is worse than accidentally disabling
    # it (under-distribution, recoverable on the next run).
    if enable_holder_tier: f["holder_tier"] = True
    if no_holder_tier:    f["holder_tier"] = False
    if no_transfer:       f["transfer"] = False
    if no_publish:        f["publish"] = False
    if dry_run:
        f["transfer"] = False
        f["publish"]  = False
    if not act and not dry_run:
        f["transfer"] = False
    # holder_tier piggy-backs on the credit_revenue expense-message fetch;
    # there is no separate pipeline path. If credit_revenue is off, holder_tier
    # cannot run — make that explicit rather than silently producing an empty
    # holder_tier stream downstream.
    if f["holder_tier"] and not f["credit_revenue"]:
        click.echo(
            "WARNING: holder_tier requires credit_revenue (same expense "
            "messages drive both). Forcing holder_tier=False."
        )
        f["holder_tier"] = False
    return f


@click.command()
@click.option("-v", "--verbose", count=True)
@click.option("-t", "--testnet", help="Publish to testnet API", is_flag=True)
@click.version_option(version=__version__)
def monitor_erc20(verbose, testnet):
    """
    ERC20BalancesMonitor: Pushes current token balances at each change.
    """
    setup_logging(verbose)
    if testnet:
        PublishMode.set_testnet(True)
        LOGGER.info(f"TESTNET MODE: Publishing to {settings.aleph_testnet_api_server}")
    dbs = get_dbs()
    LOGGER.debug("Starting erc20 balance monitor")
    asyncio.run(erc20_monitoring_process(dbs))


@click.command()
@click.option("-v", "--verbose", count=True)
@click.option("-t", "--testnet", help="Publish to testnet API", is_flag=True)
@click.version_option(version=__version__)
def monitor_sablier(verbose, testnet):
    """
    SablierBalancesMonitor: Pushes current token balances at each change.
    """
    setup_logging(verbose)
    if testnet:
        PublishMode.set_testnet(True)
        LOGGER.info(f"TESTNET MODE: Publishing to {settings.aleph_testnet_api_server}")
    LOGGER.debug("Starting sablier balance monitor")
    asyncio.run(sablier_monitoring_process())


@click.command()
@click.option("-v", "--verbose", count=True)
@click.option("-t", "--testnet", help="Publish to testnet API", is_flag=True)
@click.version_option(version=__version__)
def monitor_solana(verbose, testnet):
    """
    SolanaBalancesMonitor: Pushes current token balances at each change.
    """
    setup_logging(verbose)
    if testnet:
        PublishMode.set_testnet(True)
        LOGGER.info(f"TESTNET MODE: Publishing to {settings.aleph_testnet_api_server}")
    LOGGER.debug("Starting solana balance monitor")
    asyncio.run(solana_monitoring_process())


@click.command()
@click.option("-v", "--verbose", count=True)
@click.option("-t", "--testnet", help="Publish to testnet API", is_flag=True)
@click.version_option(version=__version__)
def monitor_indexer(verbose, testnet):
    """
    IndexerBalancesMonitor: Pushes current token balances at each change on all indexed chains.
    """
    setup_logging(verbose)
    if testnet:
        PublishMode.set_testnet(True)
        LOGGER.info(f"TESTNET MODE: Publishing to {settings.aleph_testnet_api_server}")
    LOGGER.debug("Starting indexer balance monitor")
    asyncio.run(indexer_monitoring_process())


@click.command()
@click.option("-v", "--verbose", count=True)
@click.option("-t", "--testnet", help="Publish to testnet API", is_flag=True)
@click.option(
    "-c",
    "--chain",
    "chain",
    help="Chain name (ethereum, solana, base)",
    default=None,
)
def reset_balances(verbose, testnet, chain: str):
    """
    AlephResetBalances: Reset all balances for a specific chain.
    """
    setup_logging(verbose)
    if testnet:
        PublishMode.set_testnet(True)
        LOGGER.info(f"TESTNET MODE: Publishing to {settings.aleph_testnet_api_server}")
    if chain is None:
        LOGGER.warning("No chain provided!")
        exit()

    LOGGER.warning(f"Reset all balances for chain: {chain}")
    asyncio.run(do_reset_balances(chain))


_FORK_RPC_SAFE_HOSTS = ("localhost", "127.0.0.1", "host.docker.internal")


def _fork_rpc_is_safe(url: str) -> bool:
    """Refuse anything that isn't a loopback / Docker host. The fork-mode
    path signs and broadcasts real txs; a non-loopback URL is almost
    always either a remote node by mistake or a mainnet RPC, both of
    which would turn the dry-run into a real extract."""
    from urllib.parse import urlparse
    try:
        host = (urlparse(url).hostname or "").lower()
    except Exception:
        return False
    return host in _FORK_RPC_SAFE_HOSTS


def _parse_amount_kv(values, *, flag_name: str, configured_symbols: set):
    """Parse a click `multiple=True` list of 'SYMBOL=AMOUNT' strings into
    `{SYMBOL: Decimal(AMOUNT)}`. Each SYMBOL must be in `configured_symbols`
    (i.e. settings.process_tokens). Duplicate SYMBOLs are rejected to
    surface "did the operator mean +500 or =500?" ambiguity at the CLI
    boundary instead of silently using the last-wins value.

    Click-style errors via UsageError so the user sees a normal CLI
    "Error: ..." message and a non-zero exit, not a stack trace.
    """
    from decimal import InvalidOperation
    out = {}
    for raw in values:
        if "=" not in raw:
            raise click.UsageError(
                f"--{flag_name} expects SYMBOL=AMOUNT (e.g. USDC=1000), "
                f"got: {raw!r}"
            )
        symbol, _, amount_s = raw.partition("=")
        symbol = symbol.strip()
        amount_s = amount_s.strip()
        if not symbol or not amount_s:
            raise click.UsageError(
                f"--{flag_name} expects SYMBOL=AMOUNT (e.g. USDC=1000), "
                f"got: {raw!r}"
            )
        if symbol not in configured_symbols:
            raise click.UsageError(
                f"--{flag_name}: symbol {symbol!r} is not in "
                f"settings.process_tokens ({sorted(configured_symbols)}). "
                f"Add it to the configured token list first."
            )
        if symbol in out:
            raise click.UsageError(
                f"--{flag_name}: duplicate entry for {symbol!r}. "
                f"Pass the option once per token."
            )
        try:
            value = Decimal(amount_s)
        except InvalidOperation:
            raise click.UsageError(
                f"--{flag_name}: {amount_s!r} is not a valid decimal number"
            )
        if value <= 0:
            raise click.UsageError(
                f"--{flag_name}: {symbol}={value} must be > 0"
            )
        out[symbol] = value
    return out


@click.command()
@click.option("-v", "--verbose", count=True)
@click.option("-a", "--act", help="Broadcast process() txs", is_flag=True)
@click.option("--dry-run", "dry_run", is_flag=True,
              help="No broadcast; only eth_call simulate")
@click.option("--no-transfer", "no_transfer", is_flag=True,
              help="Compute + simulate, don't broadcast")
@click.option("--immediate", "immediate", is_flag=True,
              help="Skip the anti-MEV random delay (manual retries)")
@click.option("--slippage-bps", "slippage_bps", default=None, type=int,
              help="Override per-run slippage tolerance")
@click.option("-t", "--token", "tokens", multiple=True,
              help="Restrict extract to this token symbol (repeatable). "
                   "Must be one of settings.process_tokens. Default: all.")
@click.option("--max-amount", "max_amount_kv", multiple=True,
              help="Cap swap amount per token in human units: "
                   "SYMBOL=AMOUNT (repeatable). Example: "
                   "--max-amount USDC=1000 swaps at most 1000 USDC even if "
                   "the contract balance is higher. Useful when pool "
                   "liquidity is thin and a full-balance swap would move "
                   "the price too much.")
@click.option("--min-amount", "min_amount_kv", multiple=True,
              help="Skip the token if its (post-cap) amount is below this "
                   "threshold in human units: SYMBOL=AMOUNT (repeatable). "
                   "Example: --min-amount USDC=100 skips USDC when the "
                   "extractable amount drops below 100 USDC — avoids "
                   "paying gas on dust-sized swaps. Stacks with "
                   "--max-amount: cap first, then min-skip.")
@click.option("--max-price-impact-bps", "max_price_impact_bps", default=None,
              type=int,
              help="Enable auto-sizing: bisect the swap amount until the "
                   "implied price impact (vs a small-size reference quote) "
                   "fits under this threshold, in bps. 100 = 1 percent. "
                   "Overrides settings.extract_max_price_impact_bps. "
                   "0 disables the search and uses the full (or capped) "
                   "balance. Leftover stays in the contract for next "
                   "cron cycle — the pool gets time to rebalance via "
                   "arbitrage between swaps.")
@click.option("--fork-rpc", "fork_rpc", default=None,
              help="URL of a local mainnet fork (e.g. http://localhost:8545 "
                   "for `anvil --fork-url …`). Requires --dry-run; refused "
                   "with --act. Upgrades dry-run into a full sign+broadcast "
                   "+ receipt audit against the fork. Overrides "
                   "settings.extract_fork_rpc.")
@click.option("--reconcile-bps", "reconcile_bps", default=None, type=int,
              help="Max acceptable delta between realized alephReceived and "
                   "quoter expected_out, in bps. Default from "
                   "settings.extract_max_reconcile_delta_bps (200 = 2%). "
                   "Only applies in --fork-rpc mode.")
def extract_credits(verbose, act, dry_run, no_transfer, immediate,
                    slippage_bps, tokens, max_amount_kv, min_amount_kv,
                    max_price_impact_bps, fork_rpc, reconcile_bps):
    """Extract ALEPH from the AlephPaymentProcessor (no Aleph publish)."""
    setup_logging(verbose)

    if act and dry_run:
        click.echo("ERROR: --act and --dry-run are mutually exclusive")
        sys.exit(2)

    configured_symbols = {sym for sym, _ in settings.process_tokens}
    token_filter = set(tokens) if tokens else None
    if token_filter:
        unknown = token_filter - configured_symbols
        if unknown:
            click.echo(
                f"ERROR: --token {sorted(unknown)} not in "
                f"settings.process_tokens ({sorted(configured_symbols)}). "
                f"Add to the configured list first."
            )
            sys.exit(2)
    try:
        max_amounts = _parse_amount_kv(
            max_amount_kv, flag_name="max-amount",
            configured_symbols=configured_symbols,
        )
        cli_min_amounts = _parse_amount_kv(
            min_amount_kv, flag_name="min-amount",
            configured_symbols=configured_symbols,
        )
    except click.UsageError as e:
        click.echo(f"ERROR: {e.message}")
        sys.exit(2)
    # Defaults from settings.extract_default_min_amounts merge under the
    # CLI values: any --min-amount SYM=N overrides the default for that
    # symbol, but tokens the operator didn't mention still get the
    # configured floor. Filter defaults to configured_symbols so a stale
    # entry (e.g. a removed token) is silently ignored instead of
    # erroring out the CLI.
    default_min_amounts = {
        sym: Decimal(str(amt))
        for sym, amt in settings.extract_default_min_amounts.items()
        if sym in configured_symbols
    }
    min_amounts = {**default_min_amounts, **cli_min_amounts}
    # If both are set for the same token, cap must be >= min — otherwise
    # the cap deterministically forces the min-skip and the run is a no-op
    # for that token. Reject at the CLI so the operator notices.
    for sym in max_amounts.keys() & min_amounts.keys():
        if max_amounts[sym] < min_amounts[sym]:
            click.echo(
                f"ERROR: --max-amount {sym}={max_amounts[sym]} is below "
                f"--min-amount {sym}={min_amounts[sym]}; the cap would "
                f"always trigger the min-skip. Adjust one of them."
            )
            sys.exit(2)

    # Resolve fork-rpc: CLI flag wins, else settings (env). Empty string
    # in settings is treated as "unset" so the user can keep an
    # `extract_fork_rpc=` line commented or empty in `.env`.
    resolved_fork_rpc = fork_rpc or (settings.extract_fork_rpc or None)

    if resolved_fork_rpc:
        if act:
            click.echo(
                "ERROR: --fork-rpc / extract_fork_rpc cannot be combined "
                "with --act. The fork path signs real txs against the "
                "configured RPC; running --act with a fork URL active "
                "would either broadcast to the fork (no mainnet effect, "
                "misleading) or — if the fork URL was a typo for a real "
                "node — execute live. Either drop --act to verify, or "
                "clear extract_fork_rpc to extract for real."
            )
            sys.exit(2)
        if not dry_run:
            click.echo(
                "ERROR: --fork-rpc / extract_fork_rpc requires --dry-run "
                "(calculation-only mode has nothing to broadcast)."
            )
            sys.exit(2)
        if not _fork_rpc_is_safe(resolved_fork_rpc):
            click.echo(
                f"ERROR: fork RPC host must be one of {_FORK_RPC_SAFE_HOSTS}. "
                f"Got: {resolved_fork_rpc}. Refusing to sign real txs "
                f"against a non-loopback endpoint."
            )
            sys.exit(2)

    transfer = not no_transfer
    if dry_run or not act:
        transfer = False
    # Fork mode is a `--dry-run` that DOES broadcast (to the fork), so
    # flip the transfer flag back on. The signing path needs to run.
    if resolved_fork_rpc:
        transfer = True

    mode = (
        "DRY-RUN (FORK)"  if resolved_fork_rpc else
        "DRY-RUN"         if dry_run else
        "LIVE EXTRACT"    if act else
        "CALCULATION ONLY"
    )
    click.echo(f"Mode: {mode}")
    if resolved_fork_rpc:
        click.echo(f"Fork RPC: {resolved_fork_rpc}")

    exit_code = asyncio.run(process_credit_extraction(
        act=act, dry_run=dry_run, transfer=transfer,
        immediate=(immediate or dry_run),
        slippage_bps=slippage_bps,
        tokens=tuple(tokens),
        max_amounts=max_amounts,
        min_amounts=min_amounts,
        max_price_impact_bps=(
            max_price_impact_bps if max_price_impact_bps is not None
            else settings.extract_max_price_impact_bps
        ),
        fork_rpc=resolved_fork_rpc,
        reconcile_bps=(
            reconcile_bps if reconcile_bps is not None
            else settings.extract_max_reconcile_delta_bps
        ),
    ))
    if exit_code:
        sys.exit(exit_code)


def run():
    """Entry point for console_scripts"""
    main(sys.argv[1:])


if __name__ == "__main__":
    run()
