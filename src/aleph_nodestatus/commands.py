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

import click

from aleph_nodestatus import __version__
from aleph_nodestatus.sablier import sablier_monitoring_process

from .balances import do_reset_balances
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
@click.version_option(version=__version__)
def main(verbose, testnet):
    """
    NodeStatus: Keeps an aggregate up to date with current nodes statuses

    Use --testnet to publish status updates to testnet for testing.
    """
    setup_logging(verbose)

    if testnet:
        PublishMode.set_testnet(True)
        LOGGER.info(f"TESTNET MODE: Publishing to {settings.aleph_testnet_api_server}")
    else:
        LOGGER.info(f"MAINNET MODE: Publishing to {settings.aleph_api_server}")

    LOGGER.debug("Starting nodestatus")
    account = get_eth_account()
    dbs = get_dbs()
    LOGGER.debug(f"Starting with ETH account {account.address}")
    asyncio.run(process(dbs))


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
