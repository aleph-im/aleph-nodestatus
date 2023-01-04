import json
import os
import asyncio
from pathlib import Path
from collections import deque
from .settings import settings
from .ethereum import get_web3, get_logs

from aleph_client.asynchronous import create_post
from web3 import Web3
from web3._utils.events import (
    construct_event_topic_set,
)
from web3.middleware import geth_poa_middleware, local_filter_middleware
from web3.contract import get_event_data
from web3.gas_strategies.rpc import rpc_gas_price_strategy

DECIMALS = 10**settings.ethereum_decimals


def get_contract_abi():
    return json.load(open(os.path.join(Path(__file__).resolve().parent,
                                       'abi/ALEPHERC20.json')))


def get_contract(address, web3):
    return web3.eth.contract(address,
                             abi=get_contract_abi())


async def process_contract_history(contract_address, start_height, platform="ETH",
                                   balances=None, last_seen=None):
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
        args = evt_data['args']
        height = evt_data['blockNumber']

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

        balances[args['_from']] = (balances.get(args['_from'], 0)
                                   - args['_value'])
        balances[args['_to']] = balances.get(args['_to'], 0) + args['_value']
        changed_addresses.add(args['_from'])
        changed_addresses.add(args['_to'])
        last_height = height

    if len(changed_addresses):
        yield (last_height, (balances, platform, changed_addresses))


async def update_balances(account, height, balances):
    return await create_post(
        account, {
            'tags': ['ERC20', settings.ethereum_sablier_contract,
                     settings.filter_tag],
            'height': height,
            'main_height': height,  # ethereum height
            'platform': '{}_{}'.format(settings.token_symbol,
                                               settings.chain_name),
            'token_contract': settings.ethereum_token_contract,
            'token_symbol': settings.token_symbol,
            'network_id': settings.ethereum_chain_id,
            'chain': settings.chain_name,
            'balances': {addr: value / DECIMALS
                        for addr, value in balances.items()
                        if value > 0}},
        settings.balances_post_type,
        channel=settings.aleph_channel,
        api_server=settings.aleph_api_server)


async def erc20_monitoring_process():
    from .messages import get_aleph_account
    last_seen_txs = deque([], maxlen=100)
    account = get_aleph_account()
    items = process_contract_history(
            settings.ethereum_token_contract, settings.ethereum_min_height,
            last_seen=last_seen_txs)
    balances = {}
    last_height = settings.ethereum_min_height
    async for height, (balances, platform, changed_items) in items:
        last_height = height
        balances = balances
    
    await update_balances(account, height, balances)
    
    while True:
        changed_items = None
        async for height, (balances, platform, changed_items) in process_contract_history(
                settings.ethereum_token_contract, last_height+1,
                balances=balances, last_seen=last_seen_txs):
            pass
        
        if changed_items:
            await update_balances(account, height, balances)
        
        await asyncio.sleep(5)