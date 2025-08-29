import asyncio
import json
import logging
import os
from collections import deque
from pathlib import Path
from aleph.sdk.client import AuthenticatedAlephHttpClient
from web3 import Web3
from web3._utils.events import construct_event_topic_set
try:
    from web3.contract import get_event_data
except ImportError:
    from web3._utils.events import get_event_data
    
from web3.gas_strategies.rpc import rpc_gas_price_strategy
from web3.middleware import geth_poa_middleware, local_filter_middleware

from .ethereum import get_logs, get_web3, get_aleph_account
from .settings import settings
from .utils import chunks

LOGGER = logging.getLogger(__name__)

DECIMALS = 10**settings.ethereum_decimals


def get_contract_abi():
    return json.load(
        open(os.path.join(Path(__file__).resolve().parent, "abi/ALEPHERC20.json"))
    )


def get_contract(address, web3):
    return web3.eth.contract(address, abi=get_contract_abi())


async def process_contract_history(
    contract_address, start_height, platform="ETH", balances=None, last_seen=None, db=None, fetch_from_db=True
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

    changed_addresses = set()

    to_append = list()
        
    async def handle_event(height, args):
        nonlocal changed_addresses
        nonlocal last_height

        balances[args["_from"]] = balances.get(args["_from"], 0) - args["_value"]
        balances[args["_to"]] = balances.get(args["_to"], 0) + args["_value"]
        changed_addresses.add(args["_from"])
        changed_addresses.add(args["_to"])
        last_height = height
        
    if db is not None and fetch_from_db:
        last_key = await db.get_last_available_key(prefix=contract_address)
        if last_key:
            start_height = int(last_key.split("_")[0])
            last_height = start_height
        
            async for key, values in db.retrieve_entries(prefix=contract_address):
                if values["height"] != last_height and last_height != start_height:
                    yield (last_height, (balances, platform, changed_addresses))
                    changed_addresses = set()

                    if last_seen is not None:
                        last_seen.extend(to_append)

                    to_append = list()

                if last_seen is not None:
                    tx_hash = values["tx_hash"]
                    if tx_hash in last_seen:
                        continue
                    else:
                        to_append.append(tx_hash)
                    
                await handle_event(values["height"], values["args"])
        
    end_height = web3.eth.block_number
    
    start_height = last_height
    

    async for i in get_logs(web3, contract, start_height, topics=topic):
        evt_data = get_event_data(web3.codec, abi, i)
        event = evt_data["event"]
        args = evt_data["args"]
        height = evt_data["blockNumber"]
        tx_hash = evt_data["transactionHash"].hex()
        tx_index = evt_data["transactionIndex"]
        log_index = evt_data["logIndex"]
        key = "{}_{}_{}".format(height, tx_index, log_index)
        # print(json.dumps({'event': event, 'args': dict(args), 'height': height, 'key': key}))
        
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

        await handle_event(height, args)
        
        if db is not None:
            await db.store_entry(key,
                                 {'event': event,
                                  'args': dict(args),
                                  'height': height,
                                  'key': key,
                                  'tx_hash': tx_hash
                                  }, prefix=contract_address)

        

    if len(changed_addresses):
        yield (last_height, (balances, platform, changed_addresses))


async def update_balances(account, height, balances, changed_addresses = None):
    if changed_addresses is None:
        changed_addresses = list(balances.keys())
        
    async with AuthenticatedAlephHttpClient(account, api_server="https://api.twentysix.testnet.network") as client:
        post_message, _ = await client.create_post(
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
            channel=settings.aleph_channel
        )

        return post_message

async def do_update_balances(account, height, balances, changed_addresses = None):
    if len(balances) > 10000:
        for _balances in chunks(balances, 10000):
            yield await update_balances(account, height, _balances, changed_addresses)
    else:
        yield await update_balances(account, height, balances, changed_addresses)

async def erc20_monitoring_process(dbs):
    last_seen_txs = deque([], maxlen=100)
    account = get_aleph_account()
    print(account.get_address())
    LOGGER.info("processing history")
    # get with DB to get last history
    items = process_contract_history(
        settings.ethereum_token_contract,
        settings.ethereum_min_height,
        last_seen=last_seen_txs,
        db=dbs['erc20']
    )
    balances = {}
    last_height = settings.ethereum_min_height
    height = last_height
    async for height, (balances, platform, changed_items) in items:
        last_height = height
        balances = balances
    LOGGER.info("pushing current state")

    async for message in do_update_balances(account, last_height, balances):
        print(f"Balances update, Message item hash {message.item_hash}")

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
            db=dbs['erc20'],
            fetch_from_db=False
        ):
            pass

        if changed_items:
            LOGGER.info("New data available for addresses %s, posting" % changed_items)
            async for message in do_update_balances(account, height, balances, changed_addresses=changed_items):
                print(f"Balances update, Message item hash {message.item_hash}")

            last_height = height

        await asyncio.sleep(5)
