import json
import os
import asyncio
from pathlib import Path
from collections import deque
from .settings import settings
from .ethereum import get_web3, get_logs

from aleph_client.asynchronous import create_aggregate, create_post
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
                                       'abi/Sablier.json')))


def get_contract(address, web3):
    return web3.eth.contract(address,
                             abi=get_contract_abi())
    
def get_contract_params(web3, contract, events):
    output = {}
    for event in events:
        abi = getattr(contract.events, event)._get_event_abi()
        topic = construct_event_topic_set(abi, web3.codec)
        output[event] = (abi, topic[0])
    return output

async def process_contract_history(contract_address, start_height,
                                   balances=None, streams=None, last_seen=None):
    web3 = get_web3()
    contract = get_contract(contract_address, web3)
    
    contract_events = get_contract_params(web3, contract, ["CreateStream", "WithdrawFromStream", "CancelStream"])
    
    # abi = contract.events.Transfer._get_event_abi()
    # topic = construct_event_topic_set(abi, web3.codec)
    if balances is None:
        balances = {}
    if streams is None:
        streams = {}
        
    last_height = start_height
    end_height = web3.eth.block_number

    changed_addresses = set()
    
    to_append = list()

    async for i in get_logs(web3, contract, start_height):
        message_topics = [t.hex() for t in i['topics']]
        evt_data = None
        for topic, (abi, topic_hash) in contract_events.items():
            if topic_hash in message_topics:
                print("found", topic)
                evt_data = get_event_data(web3.codec, abi, i)
                
        if evt_data is None:
            continue
        
        # yield evt_data
        args = evt_data['args']
        height = evt_data['blockNumber']

        if height != last_height:
            yield (last_height, (balances, streams, changed_addresses))
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
                
        if (evt_data["event"] == "CreateStream" and 
             args["tokenAddress"] == settings.ethereum_token_contract):
            balances[args["recipient"]] = balances.get(args["recipient"], 0) \
                                          + args["deposit"]
            changed_addresses.add(args['recipient'])
            streams[args["streamId"]] = {
                "balance": args["deposit"],
                **args
            }
            
        elif (evt_data["event"] == "CancelStream" and
               args["streamId"] in streams):
            balances[args["recipient"]] -= streams[args["streamId"]]["balance"]
            changed_addresses.add(args['recipient'])
            del streams[args["streamId"]]
            
        elif (evt_data["event"] == "WithdrawFromStream" and
               args["streamId"] in streams):
            balances[args["recipient"]] -= args['amount']
            streams[args["streamId"]]["balance"] -= args['amount']
            if streams[args["streamId"]]["balance"] == 0:
                del streams[args["streamId"]]
                
            changed_addresses.add(args['recipient'])

        # balances[args['_from']] = (balances.get(args['_from'], 0)
        #                            - args['_value'])
        # balances[args['_to']] = balances.get(args['_to'], 0) + args['_value']
        # changed_addresses.add(args['_from'])
        # changed_addresses.add(args['_to'])
        last_height = height

    if len(changed_addresses):
        yield (last_height, (balances, streams, changed_addresses))

async def update_balances(account, height, balances):
    return await create_post(
        account, {
            'tags': ['SABLIER', settings.ethereum_sablier_contract,
                     settings.filter_tag],
            'height': height,
            'main_height': height,  # ethereum height
            'platform': '{}_{}_SABLIER'.format(settings.token_symbol,
                                               settings.chain_name),
            'dapp': 'SABLIER',
            'dapp_id': settings.ethereum_sablier_contract,
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


async def sablier_monitoring_process():
    from .messages import get_aleph_account
    last_seen_txs = deque([], maxlen=100)
    account = get_aleph_account()
    items = process_contract_history(
            settings.ethereum_sablier_contract, settings.ethereum_sablier_min_height,
            last_seen=last_seen_txs)
    balances = {}
    last_height = settings.ethereum_min_height
    # async for height, (balances, streams, changed_addresses) in items:
    #     print(changed_addresses)
    async for height, (balances, streams, changed_items) in items:
        last_height = height
        balances = balances
    
    await update_balances(account, height, balances)
    
    while True:
        changed_items = None
        async for height, (balances, streams, changed_items) in process_contract_history(
                settings.ethereum_sablier_contract, last_height+1,
                balances=balances, streams=streams, last_seen=last_seen_txs):
            pass
        
        if changed_items:
            await update_balances(account, height, balances)
        
        await asyncio.sleep(5)
    