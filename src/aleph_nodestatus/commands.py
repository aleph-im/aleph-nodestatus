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
import sys
import logging
import asyncio
import click
import math

from aleph_nodestatus import __version__
from .ethereum import get_account, get_web3, transfer_tokens
from .erc20 import process_contract_history, erc20_monitoring_process
from .status import process
from .settings import settings
from .distribution import (create_distribution_tx_post,
                           get_latest_successful_distribution,
                           prepare_distribution)

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
    logging.basicConfig(level=loglevel, stream=sys.stdout,
                        format=logformat, datefmt="%Y-%m-%d %H:%M:%S")



@click.command()
@click.option('-v', '--verbose', count=True)
@click.version_option(version=__version__)
def main(verbose):
    """
    NodeStatus: Keeps an aggregate up to date with current nodes statuses
    """
    setup_logging(verbose)
    LOGGER.debug("Starting nodestatus")
    account = get_account()
    LOGGER.debug(f"Starting with ETH account {account.address}")
    asyncio.run(process())


async def process_distribution(start_height, end_height, act=False):
            
    account = get_account()
    LOGGER.debug(f"Starting with ETH account {account.address}")

    if end_height == -1:
        end_height = get_web3().eth.blockNumber

    if start_height == -1:
        last_end_height, dist = await get_latest_successful_distribution()

        if last_end_height and dist:
            start_height = last_end_height + 1
        
        else:
            start_height = 0

    reward_start, end_height, rewards = await prepare_distribution(start_height, end_height)

    distribution = dict(
        incentive="corechannel",
        status="calculation",
        start_height=reward_start,
        end_height=end_height,
        rewards=rewards
    )

    if act:
        # distribution['status'] = ''
        print("Doing distribution")
        print(distribution)
        distribution['status'] = 'distribution'

        max_items = settings.ethereum_batch_size

        distribution_list = list(rewards.items())

        for i in range(math.ceil(len(distribution_list) / max_items)):
            step_items = distribution_list[max_items*i:max_items*(i+1)]
            print(f"doing batch {i} of {len(step_items)} items")
            await transfer_tokens(dict(step_items), metadata=distribution)

    await create_distribution_tx_post(distribution)


@click.command()
@click.option('-v', '--verbose', count=True)
@click.option('-a', '--act', help='Do actual batch transfer', is_flag=True)
@click.option('-s', '--start-height:',
              'start_height', help='Starting height', default=-1)
@click.option('-e', '--end-height',
              'end_height', help='Ending height', default=-1)
def distribute(verbose, act=False, start_height=-1, end_height=-1):
    """
    Staking distribution script.
    """
    setup_logging(verbose)
    print(verbose, act, start_height, end_height)
    
    asyncio.run(process_distribution(start_height, end_height, act=act))
    

@click.command()
@click.option('-v', '--verbose', count=True)
@click.version_option(version=__version__)
def monitor_erc20(verbose):
    """
    ERC20BalancesMonitor: Pushes current token balances at each change.
    """
    setup_logging(verbose)
    LOGGER.debug("Starting erc20 balance monitor")
    asyncio.run(erc20_monitoring_process())


def run():
    """Entry point for console_scripts
    """
    main(sys.argv[1:])


if __name__ == "__main__":
    run()
