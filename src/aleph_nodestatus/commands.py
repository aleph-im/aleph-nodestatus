# -*- coding: utf-8 -*-
"""
This is a skeleton file that can serve as a starting point for a Python
console script. To run this script uncomment the following lines in the
[options.entry_points] section in setup.cfg:

    console_scripts =
         fibonacci = aleph_nodestatus.skeleton:run

Then run `python setup.py install` which will install the command `fibonacci`
inside your current environment.
Besides console scripts, the header (i.e. until _logger...) of this file can
also be used as template for Python modules.

Note: This skeleton file can be safely removed if not needed!
"""

import argparse
import asyncio
import json
import logging
import math
import os
import shutil
import sys
import time as time_mod

import click
from eth_account import Account
from hexbytes import HexBytes

from aleph_nodestatus import __version__
from aleph_nodestatus.sablier import sablier_monitoring_process

from .balances import do_reset_balances
from .credit_distribution import (
    CREDIT_DISTRIBUTION_POST_TYPE,
    compute_rewards,
    fetch_node_snapshots,
    get_latest_successful_credit_distribution,
    prepare_credit_distribution,
    should_skip_run,
    zero_totals,
)
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
from .payment_processor import (
    extract_aleph,
    get_processor_contract,
    get_quoter_contract,
    get_v2_router_contract,
    get_v4_quoter_contract,
)
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

    if act and not is_testnet:
        print("Executing actual token transfers...")
        max_items = settings.ethereum_batch_size
        distribution_list = list(rewards.items())

        for i in range(math.ceil(len(distribution_list) / max_items)):
            step_items = distribution_list[max_items * i : max_items * (i + 1)]
            print(f"Batch {i+1}: transferring to {len(step_items)} recipients")
            await transfer_tokens(dict(step_items), metadata=distribution)

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
    act=False, dry_run=False, force=False,
    flags=None, slippage_bps=None, reward_sender=None, full_resync=False,
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
    # last_end has dual semantics here: None means "not yet fetched", 0 means
    # "fetched and no prior distribution exists" (the function returns 0 in that
    # case). Both are falsy, so the inner truthy guards below correctly skip
    # advancing the cursor — but the memoization check at the cursor-derivation
    # step uses `is None` specifically to distinguish "not yet fetched" from
    # "fetched, no prior run", which avoids a redundant network call.
    last_end = None
    if act:
        last_end, _, _ = await get_latest_successful_credit_distribution(
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
            last_end, _, _ = await get_latest_successful_credit_distribution(
                reward_sender
            )
        if last_end:
            start_height = last_end + 1
            click.echo(f"Resuming from last_end={last_end}; start={start_height}")
        else:
            click.echo("ERROR: --start-height required for first run.")
            return

    if end_height <= start_height:
        raise ValueError(
            f"end_height ({end_height}) must be greater than start_height "
            f"({start_height})"
        )

    start_time = web3.eth.get_block(start_height).timestamp
    end_time = web3.eth.get_block(end_height).timestamp

    admin_address = settings.payment_processor_admin_address
    admin_account = None
    if flags.get("extract") and (act and not dry_run):
        pk = settings.payment_processor_admin_pkey or settings.ethereum_pkey
        if not settings.payment_processor_admin_pkey:
            LOGGER.warning(
                "payment_processor_admin_pkey not set; falling back to ethereum_pkey"
            )
        admin_account = Account.from_key(HexBytes(pk))
        admin_address = admin_account.address

    # === Step 1: extract ALEPH from the processor ===
    # extract_aleph is synchronous (web3.py is sync-native) and the receipt
    # wait can take up to 300s per token. We run it on a worker thread so the
    # asyncio event loop remains responsive for any concurrent coroutines
    # sharing this loop.
    extract_block = {"tokens": [], "errors": []}
    if flags.get("extract"):
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
            dry_run=dry_run or not flags.get("transfer"),
            transfer_enabled=flags.get("transfer"),
            slippage_bps=(
                slippage_bps if slippage_bps is not None
                else settings.process_slippage_bps
            ),
        )

    # === Step 2: credit_revenue + holder_tier rewards ===
    streams = {
        "credit_revenue": ({}, zero_totals()),
        "holder_tier":    ({}, zero_totals()),
    }
    if flags.get("credit_revenue"):
        streams = await compute_rewards(
            start_time, end_time,
            full_resync=full_resync,
            include_holder_tier=flags.get("holder_tier", False),
            sender=settings.credit_expense_sender,
            dbs=dbs, end_height=end_height, web3=web3,
        )

    credit_rewards, credit_totals = streams["credit_revenue"]
    holder_rewards, holder_totals = streams["holder_tier"]

    # === Step 3: wage subsidy ===
    wage_rewards = {}
    wage_totals = {
        "period_total_aleph": 0,
        "unallocated_aleph": 0,
        "start_t_months": 0,
        "end_t_months": 0,
        "split": {"ccn": 0, "crn": 0, "stakers": 0},
    }
    if flags.get("wage"):
        api_server = PublishMode.get_publish_api_server()
        snapshots = await fetch_node_snapshots(api_server, start_time, end_time)
        if snapshots:
            # Design choice: use the most recent snapshot as the canonical
            # weighting for the whole period. Intermediate node/staker state
            # changes within the window are intentionally collapsed — the
            # wage subsidy is a CCN/CRN/staker pool split, not a per-block
            # reward, so the latest snapshot is treated as authoritative.
            _, nodes, resource_nodes = snapshots[-1]
            wage_rewards, wage_totals = compute_subsidy(
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
                    "ccn":     period_total * settings.wage_ccn_share,
                    "crn":     period_total * settings.wage_crn_share,
                    "stakers": period_total * settings.wage_staker_share,
                },
            }
            LOGGER.warning(
                f"No snapshots for wage subsidy; entire {period_total:.4f} "
                f"ALEPH recorded as unallocated."
            )

    # === Step 4: merge + dust filter ===
    final_rewards, by_source = merge_rewards(
        {"credit_revenue": credit_rewards,
         "holder_tier":    holder_rewards,
         "wage_subsidy":   wage_rewards},
        dust_threshold=settings.credit_dist_dust_threshold_aleph,
    )

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
        bal = token.functions.balanceOf(
            web3.to_checksum_address(sender)
        ).call() / (10 ** settings.ethereum_decimals)
        owed = sum(final_rewards.values())
        if owed > bal + 1e-6:
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
    status = "distribution" if act else "calculation"
    distribution = {
        "incentive": "credits",
        "status": status,
        "start_height": start_height, "end_height": end_height,
        "start_time": start_time, "end_time": end_time,
        "rewards": final_rewards,
        "rewards_by_source": by_source,
        "credit_revenue_totals": credit_totals,
        "holder_tier_totals": {**holder_totals,
                                "included": flags.get("holder_tier", False)},
        "wage_subsidy": wage_totals,
        "extract": extract_block,
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
        preview = {k: v for k, v in distribution.items()
                    if k != "rewards_by_source"}
        click.echo(json.dumps(preview, indent=2, default=str))


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
@click.option("-s", "--start-height", "start_height", default=-1, type=int,
              help="Starting block height, default: resume from last distribution")
@click.option("-e", "--end-height", "end_height", default=-1, type=int,
              help="Ending block height, default: latest")
@click.option("--full-resync", is_flag=True,
              help="Replay full state machine from genesis")
@click.option("--no-extract", "no_extract", is_flag=True,
              help="Skip process() calls")
@click.option("--no-credit-revenue", "no_credit_revenue", is_flag=True,
              help="Skip credit-expense reward computation")
@click.option("--no-wage", "no_wage", is_flag=True,
              help="Skip wage subsidy")
@click.option("--enable-holder-tier", "enable_holder_tier", is_flag=True,
              help="Force holder-tier ON (overrides env=False)")
@click.option("--no-holder-tier", "no_holder_tier", is_flag=True,
              help="Force holder-tier OFF (overrides env=True, the default)")
@click.option("--no-transfer", "no_transfer", is_flag=True,
              help="Compute everything but don't broadcast txs")
@click.option("--no-publish", "no_publish", is_flag=True,
              help="Don't post the Aleph distribution record")
@click.option("--slippage-bps", default=None, type=int,
              help="Override per-run slippage tolerance")
@click.option("--reward-sender", default=None,
              help="Address used to look up the previous distribution")
def distribute_credits(verbose, act, testnet, dry_run, force,
                       start_height, end_height, full_resync,
                       no_extract, no_credit_revenue, no_wage,
                       enable_holder_tier, no_holder_tier,
                       no_transfer, no_publish,
                       slippage_bps, reward_sender):
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
        no_extract=no_extract, no_credit_revenue=no_credit_revenue,
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
        act=act, dry_run=dry_run, force=force,
        flags=flags, slippage_bps=slippage_bps,
        reward_sender=reward_sender,
        full_resync=full_resync,
    ))


def _resolve_feature_flags(*, no_extract, no_credit_revenue, no_wage,
                           enable_holder_tier, no_holder_tier,
                           no_transfer, no_publish, act, dry_run):
    f = {
        "extract":        settings.credit_dist_extract_enabled,
        "credit_revenue": settings.credit_dist_credit_revenue_enabled,
        "wage":           settings.credit_dist_wage_subsidy_enabled,
        "holder_tier":    settings.credit_dist_holder_tier_enabled,
        "transfer":       settings.credit_dist_transfer_enabled,
        "publish":        settings.credit_dist_publish_enabled,
    }
    if no_extract:        f["extract"] = False
    if no_credit_revenue: f["credit_revenue"] = False
    if no_wage:           f["wage"] = False
    # holder_tier overrides are last-wins by design; the CLI already
    # validates --enable-holder-tier / --no-holder-tier as mutually
    # exclusive, so this only matters for programmatic callers.
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


def run():
    """Entry point for console_scripts"""
    main(sys.argv[1:])


if __name__ == "__main__":
    run()
