import asyncio
import json
import logging
import os
from collections import deque
from pathlib import Path

from aleph.sdk.client import AuthenticatedAlephHttpClient
from web3._utils.events import construct_event_topic_set, get_event_data

from .ethereum import get_logs, get_web3
from .settings import settings

LOGGER = logging.getLogger(__name__)

DECIMALS = 10**settings.ethereum_decimals


def get_contract_abi():
    return json.load(
        open(os.path.join(Path(__file__).resolve().parent, "abi/ALEPHERC20.json"))
    )


def get_contract(address, web3):
    return web3.eth.contract(address, abi=get_contract_abi())


async def process_contract_history(
    contract_address, start_height, platform="ETH", balances=None, last_seen=None
):
    web3 = get_web3()
    contract = get_contract(contract_address, web3)
    abi = contract.events.Transfer._get_event_abi()
    topic = construct_event_topic_set(abi, web3.codec)
    if balances is None:
        balances = {
            settings.ethereum_deployer: settings.ethereum_total_supply * DECIMALS
        }
    last_height = start_height
    end_height = web3.eth.block_number

    changed_addresses = set()

    to_append = list()

    async for i in get_logs(web3, contract, start_height, topics=topic):
        evt_data = get_event_data(web3.codec, abi, i)
        args = evt_data["args"]
        height = evt_data["blockNumber"]

        if height != last_height:
            yield (last_height, (balances, platform, changed_addresses))
            changed_addresses = set()

            if last_seen is not None:
                last_seen.extend(to_append)

            to_append = list()

        if last_seen is not None:
            tx_hash = evt_data.transactionHash.hex()
            if tx_hash in last_seen:
                continue
            else:
                to_append.append(tx_hash)

        balances[args["_from"]] = balances.get(args["_from"], 0) - args["_value"]
        balances[args["_to"]] = balances.get(args["_to"], 0) + args["_value"]
        changed_addresses.add(args["_from"])
        changed_addresses.add(args["_to"])
        last_height = height

    if len(changed_addresses):
        yield (last_height, (balances, platform, changed_addresses))


async def update_balances(account, height, balances, changed_addresses = None):
    if changed_addresses is None:
        changed_addresses = list(balances.keys())

    async with AuthenticatedAlephHttpClient(
        account, settings.aleph_api_server
    ) as client:
        return await client.create_post(
            {
                "tags": ["ERC20", settings.ethereum_token_contract, settings.filter_tag],
                "height": height,
                "main_height": height,  # ethereum height
                "platform": "{}_{}".format(settings.token_symbol, settings.chain_name),
                "token_contract": settings.ethereum_token_contract,
                "token_symbol": settings.token_symbol,
                "network_id": settings.ethereum_chain_id,
                "chain": settings.chain_name,
                "balances": {
                    # we only send the balances that are > 0.00001 or are in the changed_addresses list
                    addr: value / DECIMALS for addr, value in balances.items() if (value > 0.00001 or addr in changed_addresses)
                },
            },
            settings.balances_post_type,
            channel=settings.aleph_channel,
        )


async def erc20_monitoring_process():
    from .messages import get_aleph_account

    last_seen_txs = deque([], maxlen=100)
    account = get_aleph_account()
    LOGGER.info("processing history")
    items = process_contract_history(
        settings.ethereum_token_contract,
        settings.ethereum_min_height,
        last_seen=last_seen_txs,
    )
    balances = {}
    last_height = settings.ethereum_min_height
    async for height, (balances, platform, changed_items) in items:
        last_height = height
        balances = balances
    LOGGER.info("pushing current state")

    await update_balances(account, height, balances)

    while True:
        changed_items = None
        async for height, (
            balances,
            platform,
            changed_items,
        ) in process_contract_history(
            settings.ethereum_token_contract,
            last_height + 1,
            balances=balances,
            last_seen=last_seen_txs,
        ):
            pass

        if changed_items:
            LOGGER.info("New data available for addresses %s, posting" % changed_items)
            await update_balances(account, height, balances, changed_addresses=changed_items)
            last_height = height

        await asyncio.sleep(5)
