import json
import os
from pathlib import Path
from .settings import settings
from .ethereum import get_web3, get_logs


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


async def process_contract_history(contract_address, start_height,
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
    end_height = web3.eth.blockNumber

    changed_addresses = set()

    to_append = list()

    async for i in get_logs(web3, contract, start_height, topics=topic):
        evt_data = get_event_data(web3.codec, abi, i)
        print("Item", i, evt_data)
        args = evt_data['args']
        height = evt_data['blockNumber']
        
        if last_seen is not None:
            tx_hash = evt_data.transactionHash.hex()
            if tx_hash in last_seen:
                continue
            else:
                to_append.append(tx_hash)

        if height != last_height:
            yield (last_height, (balances, changed_addresses))
            changed_addresses = set()
            last_seen.extend(to_append)
            to_append = list()

        balances[args['_from']] = (balances.get(args['_from'], 0)
                                   - args['_value'])
        balances[args['_to']] = balances.get(args['_to'], 0) + args['_value']
        changed_addresses.add(args['_from'])
        changed_addresses.add(args['_to'])
        last_height = height

    if len(changed_addresses):
        yield (last_height, (balances, changed_addresses))
