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
import logging
import math
import sys
import time as time_mod

import click

from aleph_nodestatus import __version__
from aleph_nodestatus.sablier import sablier_monitoring_process

from .balances import do_reset_balances
from .credit_distribution import (
    CREDIT_DISTRIBUTION_POST_TYPE,
    get_latest_successful_credit_distribution,
    prepare_credit_distribution,
)
from .distribution import (
    create_distribution_tx_post,
    get_latest_successful_distribution,
    prepare_distribution,
)
from .erc20 import erc20_monitoring_process, process_contract_history
from .ethereum import get_account, get_eth_account, get_web3, transfer_tokens
from .indexer_balance import indexer_monitoring_process
from .settings import settings, PublishMode
from .solana import solana_monitoring_process
from .status import process
from .storage import close_dbs, get_dbs

import os
import shutil


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
    from .messages import parse_window_size
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
    start_time, end_time, *,
    act=False, dry_run=False, force=False,
    flags=None, reward_sender=None, full_resync=False,
):
    from .credit_distribution import (
        compute_rewards, should_skip_run, fetch_node_snapshots,
    )
    from .payment_processor import (
        extract_aleph, get_processor_contract, get_quoter_contract,
    )
    from .rewards_merge import merge_rewards
    from .wage_subsidy import compute_subsidy
    from .ethereum import get_eth_account, get_web3, get_token_contract
    from hexbytes import HexBytes
    from eth_account import Account
    import time as _time
    import json as _json

    flags = flags or {}
    dbs = get_dbs()
    if end_time is None:
        end_time = _time.time()

    # === Cadence guard ===
    if start_time is None:
        last_end, _ = await get_latest_successful_credit_distribution(reward_sender)
        if last_end:
            if should_skip_run(last_end, _time.time(),
                               settings.credit_dist_min_interval_seconds,
                               force):
                click.echo(
                    f"Cadence guard: only {(_time.time()-last_end):.0f}s "
                    f"since last run; skipping (use --force to override)."
                )
                return
            start_time = last_end + 1
            click.echo(f"Resuming from last_end={last_end}; start={start_time}")
        else:
            click.echo("ERROR: --start-time required for first run.")
            return

    web3 = get_web3()
    admin_address = "0xC870B0Ca4B3d65f33E2a3c732ab3cD2aE555b14E"
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
    extract_block = {"tokens": [], "errors": []}
    if flags.get("extract"):
        processor = get_processor_contract(web3)
        quoter = get_quoter_contract(web3)
        extract_block = extract_aleph(
            web3, processor, quoter,
            account=admin_account,
            from_address=admin_address,
            dry_run=dry_run or not flags.get("transfer"),
            transfer_enabled=flags.get("transfer"),
        )

    # === Step 2: credit_revenue + holder_tier rewards ===
    end_block = None
    if full_resync:
        end_block = web3.eth.block_number
        block = web3.eth.get_block(end_block)
        if block.timestamp > end_time:
            lo, hi = 0, end_block
            while lo < hi:
                mid = (lo + hi + 1) // 2
                if web3.eth.get_block(mid).timestamp <= end_time:
                    lo = mid
                else:
                    hi = mid - 1
            end_block = lo

    zero_totals = lambda: {"storage_total_aleph": 0,
                            "execution_total_aleph": 0,
                            "dev_fund_total_aleph": 0}
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
            dbs=dbs, end_height=end_block, web3=web3,
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
            _, nodes, resource_nodes = snapshots[-1]
            wage_rewards, wage_totals = compute_subsidy(
                start_time, end_time, nodes, resource_nodes, web3=web3,
            )
        else:
            LOGGER.warning("No snapshots for wage subsidy; pool unallocated.")

    # === Step 4: merge + dust filter ===
    final_rewards, by_source = merge_rewards(
        {"credit_revenue": credit_rewards,
         "holder_tier":    holder_rewards,
         "wage_subsidy":   wage_rewards},
        dust_threshold=settings.credit_dist_dust_threshold_aleph,
    )

    # === Holder-tier safety check ===
    if flags.get("holder_tier") and holder_rewards:
        token = get_token_contract(web3)
        bal = token.functions.balanceOf(
            web3.to_checksum_address(settings.distribution_recipient)
        ).call() / (10 ** settings.ethereum_decimals)
        owed = sum(final_rewards.values())
        if owed > bal + 1e-6:
            click.echo(
                f"ABORT: owed {owed} ALEPH > balance {bal} at "
                f"{settings.distribution_recipient}"
            )
            return

    # === Step 5: transfer (or simulate) ===
    is_testnet = PublishMode.is_testnet()
    if flags.get("transfer") and act and not is_testnet:
        click.echo("Executing batchTransfer …")
        max_items = settings.ethereum_batch_size
        items = list(final_rewards.items())
        for i in range(math.ceil(len(items) / max_items)):
            step = items[max_items*i:max_items*(i+1)]
            click.echo(f"Batch {i+1}: transferring to {len(step)} recipients")
            await transfer_tokens(
                dict(step),
                metadata={"sources": list(by_source.keys())},
            )
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
    }
    if end_block is not None:
        distribution["end_height"] = end_block

    if flags.get("publish") and not dry_run:
        await create_distribution_tx_post(
            distribution, post_type=CREDIT_DISTRIBUTION_POST_TYPE
        )
    else:
        click.echo("--no-publish / dry-run: skipping Aleph post")
        preview = {k: v for k, v in distribution.items()
                    if k != "rewards_by_source"}
        click.echo(_json.dumps(preview, indent=2, default=str)[:4000])


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
@click.option("--start-time", "start_time", default=None, type=float,
              help="Unix seconds, default: resume from last distribution")
@click.option("--end-time", "end_time", default=None, type=float,
              help="Unix seconds, default: now")
@click.option("--full-resync", is_flag=True,
              help="Replay full state machine from genesis")
@click.option("--no-extract", "no_extract", is_flag=True,
              help="Skip process() calls")
@click.option("--no-credit-revenue", "no_credit_revenue", is_flag=True,
              help="Skip credit-expense reward computation")
@click.option("--no-wage", "no_wage", is_flag=True,
              help="Skip wage subsidy")
@click.option("--enable-holder-tier", "enable_holder_tier", is_flag=True,
              help="Process expense.rewards[] (deprecated branch)")
@click.option("--no-holder-tier", "no_holder_tier", is_flag=True,
              help="Force holder-tier OFF (overrides env=True)")
@click.option("--no-transfer", "no_transfer", is_flag=True,
              help="Compute everything but don't broadcast txs")
@click.option("--no-publish", "no_publish", is_flag=True,
              help="Don't post the Aleph distribution record")
@click.option("--slippage-bps", default=None, type=int,
              help="Override per-run slippage tolerance")
@click.option("--reward-sender", default=None,
              help="Address used to look up the previous distribution")
def distribute_credits(verbose, act, testnet, dry_run, force,
                       start_time, end_time, full_resync,
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

    if slippage_bps is not None:
        settings.process_slippage_bps = slippage_bps

    mode = (
        "DRY-RUN" if dry_run else
        "TESTNET (calculation)" if testnet else
        "LIVE DISTRIBUTION" if act else
        "CALCULATION ONLY"
    )
    click.echo(f"Mode: {mode}")
    click.echo(f"Flags: {flags}")

    asyncio.run(process_credit_distribution(
        start_time=start_time, end_time=end_time,
        act=act, dry_run=dry_run, force=force,
        flags=flags, reward_sender=reward_sender,
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
    if enable_holder_tier: f["holder_tier"] = True
    if no_holder_tier:    f["holder_tier"] = False
    if no_transfer:       f["transfer"] = False
    if no_publish:        f["publish"] = False
    if dry_run:
        f["transfer"] = False
        f["publish"]  = False
    if not act and not dry_run:
        f["transfer"] = False
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
