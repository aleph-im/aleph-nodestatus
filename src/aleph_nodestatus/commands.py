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
from aleph_nodestatus import __version__
from aleph_nodestatus.sablier import sablier_monitoring_process

from .balances import do_reset_balances
from . import credit_distribution
from .credit_distribution import (
    CREDIT_DISTRIBUTION_POST_TYPE,
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
    compute_subsidy,
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


async def process_credit_distribution(
    start_height, end_height, *,
    act=False, dry_run=False, force=False, force_cap=False,
    flags=None, reward_sender=None, full_resync=False,
    refresh_cache=False,
):
    flags = flags or {}
    dbs = get_dbs()
    web3 = get_web3()

    if end_height in (None, -1):
        end_height = web3.eth.block_number

    # === Cadence guard ===
    # Fires unconditionally when --act is set, regardless of how start_height
    # was derived. Without this, an operator supplying --start-height N bypasses
    # the rate-limit silently and (combined with C1's partial-failure cursor)
    # creates a path to double distribution.
    #
    # Fetch the last distribution once and reuse for both the cadence guard
    # and the cursor derivation below (one posts-API round-trip per run).
    last_end = None
    if act:
        last_end, _ = await get_latest_successful_credit_distribution(
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
            return

    if start_height in (None, -1):
        if last_end is None:
            last_end, _ = await get_latest_successful_credit_distribution(
                reward_sender
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

    # Aleph API `messages.json` AGGREGATE+date queries are slow (~80s per
    # page), so fetch node snapshots once and share between the credit
    # revenue path (snapshot mode only) and the wage subsidy below.
    # full_resync drives credit revenue from the local state machine, so
    # it doesn't need snapshots there — but wage still does.
    api_server = PublishMode.get_publish_api_server()
    snapshots = None
    if (flags.get("credit_revenue") and not full_resync) or flags.get("wage"):
        # Call through the module so test-time monkeypatching of
        # `credit_distribution.fetch_node_snapshots` is observed here.
        snapshots = await credit_distribution.fetch_node_snapshots(
            api_server, start_time, end_time,
            refresh_cache=refresh_cache,
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
            refresh_cache=refresh_cache,
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
            # Design choice: use the most recent snapshot as the canonical
            # weighting for the whole period. Intermediate node/staker state
            # changes within the window are intentionally collapsed — the
            # wage subsidy is a CCN/CRN/staker pool split, not a per-block
            # reward, so the latest snapshot is treated as authoritative.
            _, nodes, resource_nodes = snapshots[-1]
            wage_rewards, wage_totals, wage_detailed = compute_subsidy(
                start_time, end_time, nodes, resource_nodes, web3=web3,
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
    for addr in final_rewards:
        detail = by_address_detailed.setdefault(addr, {})
        for stream in by_source:
            amount = by_source[stream].get(addr, 0.0)
            if amount:
                detail.setdefault(stream, {})["total"] = amount

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
    if act and flags.get("transfer") and final_rewards:
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
            (Decimal(str(v)) for v in final_rewards.values()),
            Decimal("0"),
        )
        if owed > bal:
            click.echo(
                f"ABORT: owed {owed} ALEPH > balance {bal} at {sender}"
            )
            sys.exit(1)

    # === Step 5: transfer (or simulate) ===
    is_testnet = PublishMode.is_testnet()
    transfer_metadata = {"sources": list(by_source.keys()), "targets": []}
    if flags.get("transfer") and act and not is_testnet:
        click.echo("Executing batchTransfer …")
        max_items = settings.ethereum_batch_size
        items = list(final_rewards.items())
        for i in range(math.ceil(len(items) / max_items)):
            step = items[max_items*i:max_items*(i+1)]
            click.echo(f"Batch {i+1}: transferring to {len(step)} recipients")
            await transfer_tokens(dict(step), metadata=transfer_metadata)
    elif not flags.get("transfer"):
        click.echo("--no-transfer / dry-run: skipping batchTransfer")
        n = len(final_rewards)
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
        "credit_revenue_totals": credit_totals,
        "holder_tier_totals": {**holder_totals,
                                "included": flags.get("holder_tier", False)},
        "wage_subsidy": wage_totals,
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
@click.option("--refresh-cache", "refresh_cache", is_flag=True,
              help="Bypass any cached Aleph message JSONL and re-fetch "
                   "from the API; the new response overwrites the cache.")
def distribute_credits(verbose, act, testnet, dry_run, force, force_cap,
                       start_height, end_height, full_resync,
                       no_credit_revenue, no_wage,
                       enable_holder_tier, no_holder_tier,
                       no_transfer, no_publish,
                       reward_sender, refresh_cache):
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

    if testnet:
        PublishMode.set_testnet(True)

    flags = _resolve_feature_flags(
        no_credit_revenue=no_credit_revenue,
        no_wage=no_wage, enable_holder_tier=enable_holder_tier,
        no_holder_tier=no_holder_tier,
        no_transfer=no_transfer, no_publish=no_publish,
        act=act, dry_run=dry_run,
    )

    mode = (
        "DRY-RUN" if dry_run else
        "TESTNET (calculation)" if testnet else
        "LIVE DISTRIBUTION" if act else
        "CALCULATION ONLY"
    )
    click.echo(f"Mode: {mode}")
    click.echo(f"Flags: {flags}")

    asyncio.run(process_credit_distribution(
        start_height=start_height, end_height=end_height,
        act=act, dry_run=dry_run, force=force, force_cap=force_cap,
        flags=flags, reward_sender=reward_sender,
        full_resync=full_resync,
        refresh_cache=refresh_cache,
    ))


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
def extract_credits(verbose, act, dry_run, no_transfer, immediate, slippage_bps):
    """Extract ALEPH from the AlephPaymentProcessor (no Aleph publish)."""
    setup_logging(verbose)

    if act and dry_run:
        click.echo("ERROR: --act and --dry-run are mutually exclusive")
        sys.exit(2)

    transfer = not no_transfer
    if dry_run or not act:
        transfer = False

    mode = (
        "DRY-RUN"        if dry_run else
        "LIVE EXTRACT"   if act else
        "CALCULATION ONLY"
    )
    click.echo(f"Mode: {mode}")

    asyncio.run(process_credit_extraction(
        act=act, dry_run=dry_run, transfer=transfer,
        immediate=(immediate or dry_run),
        slippage_bps=slippage_bps,
    ))


def run():
    """Entry point for console_scripts"""
    main(sys.argv[1:])


if __name__ == "__main__":
    run()
