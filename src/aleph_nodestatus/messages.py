import aiohttp
from hexbytes import HexBytes
from functools import lru_cache
from collections import deque
from .settings import settings
from .ethereum import get_web3
from .erc20 import DECIMALS
from aleph_client.chains.ethereum import ETHAccount
from aleph_client.asynchronous import create_aggregate


@lru_cache(maxsize=2)
def get_aleph_account():
    if settings.ethereum_pkey:
        pri_key = HexBytes(settings.ethereum_pkey)
        account = ETHAccount(pri_key)
        return account
    else:
        return None

@lru_cache(maxsize=32)
def get_aleph_address():
    return (get_aleph_account()).get_address()


UNCONFIRMED_MESSAGES = deque([], maxlen=500)


async def process_message_history(tags, message_types, api_server,
                                  min_height=0, request_count=100000,
                                  request_sort='1', yield_unconfirmed=True):
    web3 = get_web3()
    last_block = web3.eth.blockNumber
    async with aiohttp.ClientSession() as session:
        async with session.get(f'{api_server}/api/v0/messages.json', params={
                'msgType': 'POST',
                'tags': ','.join(tags),
                'contentTypes': ','.join(message_types),
                'pagination': request_count,
                'sort_order': request_sort
        }) as resp:
            items = await resp.json()
            messages = items['messages']
            if request_sort == '-1':
                messages = reversed(messages)

            for message in items['messages']:
                if message['item_hash'] in UNCONFIRMED_MESSAGES:
                    continue

                earliest = None
                for conf in message.get('confirmations', []):
                    if conf['chain'] == 'ETH':
                        if earliest is None or conf['height'] < earliest:
                            earliest = conf['height']
                # print(earliest, min_height)
                if earliest is None and yield_unconfirmed:
                    # let's assign the current block height... (ugly)
                    earliest = last_block
                    UNCONFIRMED_MESSAGES.append(message['item_hash'])
                    yield earliest, message

                elif earliest >= min_height:
                    yield earliest, message


async def set_status(account, nodes):
    nodes = [
        {**node.copy(), **{
            'total_staked': node['total_staked']/DECIMALS,
            'stakers': {
                addr: amt/DECIMALS
                for addr, amt in node['stakers'].items()
            }
        }}
        for node in nodes.values()
    ]
    await create_aggregate(
        account, 'corechannel',
        {'nodes': nodes}, channel=settings.aleph_channel)
