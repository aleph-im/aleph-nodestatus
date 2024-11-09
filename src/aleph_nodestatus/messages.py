import math
from collections import deque
from functools import lru_cache

import aiohttp
from aleph.sdk.chains.ethereum import ETHAccount
from aleph.sdk.client import AuthenticatedAlephHttpClient
from hexbytes import HexBytes

from .erc20 import DECIMALS
from .ethereum import get_web3, lookup_timestamp
from .settings import settings


@lru_cache(maxsize=2)
def get_aleph_account():
    if settings.ethereum_pkey:
        account = ETHAccount(settings.ethereum_pkey)
        return account
    else:
        return None


@lru_cache(maxsize=32)
def get_aleph_address():
    return (get_aleph_account()).get_address()


UNCONFIRMED_MESSAGES = deque([], maxlen=500)

async def get_message_result(
    message, yield_unconfirmed=True, last_block=0, min_height=0,
    last_seen=None, db=None, db_prefix=None
):
    earliest = None
    for conf in message.get("confirmations", []):
        if conf["chain"] == "ETH":
            if earliest is None or conf["height"] < earliest:
                earliest = conf["height"]
                
    # key = f"{earliest}_{int(message['time'])}_{message['item_hash']}"
    key = f"{int(message['time'])}_{earliest}_{message['item_hash']}"
    
    if message["item_hash"] in UNCONFIRMED_MESSAGES:
        if earliest is not None:
            # we store it in the db
            if db is not None:
                await db.store_entry(key, {"height": earliest, "message": message}, prefix=db_prefix)
            UNCONFIRMED_MESSAGES.remove(message["item_hash"])
        return None
                
    if message["item_hash"] in last_seen:
        return None
    
    # print(earliest, min_height)
    if earliest is None and not yield_unconfirmed:
        return None

    if earliest is None and yield_unconfirmed:
        # let's assign the current block height... (ugly)
        earliest = last_block
        UNCONFIRMED_MESSAGES.append(message["item_hash"])
        last_seen.append(message["item_hash"])
        return earliest, message

    elif earliest >= min_height:
        last_seen.append(message["item_hash"])
        
        if db is not None:
            await db.store_entry(key, {"height": earliest, "message": message}, prefix=db_prefix)
        return earliest, message


async def process_message_history(
    tags,
    content_types,
    api_server,
    min_height=0,
    request_count=100000,
    message_type="POST",
    request_sort="1",
    yield_unconfirmed=True,
    addresses=None,
    crawl_history=True,
    db=None
):
    web3 = get_web3()
    last_block = web3.eth.block_number
    params = {
        "msgType": message_type,
        "tags": ",".join(tags),
        "contentTypes": ",".join(content_types),
        "pagination": request_count,
        "sort_order": request_sort,
    }
    prefix = f"{message_type}_{','.join(tags)}_{','.join(content_types)}"
    
    # we ensure we don't process a tx twice
    last_seen = deque([], maxlen=4000)
    
    fetch_from_db = db is not None
    if (not crawl_history) or request_sort == "-1":
        fetch_from_db = False
        
    last_yielded_height = 0
    
    if fetch_from_db:
        last_key = await db.get_last_available_key(prefix=prefix)
        if last_key:
            last_height = int(last_key.split("_")[1])
                
            async for key, values in db.retrieve_entries(prefix=prefix):
                if values["height"] >= min_height:
                    result = await get_message_result(
                        values["message"],
                        yield_unconfirmed=yield_unconfirmed,
                        last_block=last_block,
                        min_height=min_height,
                        last_seen=last_seen
                    )
                    if result is not None:
                        yield result
                        
            if last_height > min_height:
                # params['start_height'] = last_height
                timestamp = await lookup_timestamp(web3, last_height)
                params["start_date"] = timestamp - (60 * 60 * 24 * 7)
    
    if addresses is not None:
        params["addresses"] = ",".join(addresses)

    last_iteration_total = 0
    last_per_page = 0

    async with aiohttp.ClientSession() as session:
        async with session.get(
            f"{api_server}/api/v0/messages.json", params=params
        ) as resp:
            items = await resp.json()
            messages = items["messages"]
            last_iteration_total = items["pagination_total"]
            last_per_page = items["pagination_per_page"]
            if request_sort == "-1" and not crawl_history:
                messages = reversed(messages)

            for message in items["messages"]:
                result = await get_message_result(
                    message,
                    yield_unconfirmed=yield_unconfirmed,
                    last_block=last_block,
                    min_height=min_height,
                    last_seen=last_seen,
                    db=db,
                    db_prefix=prefix
                )
                if result is not None:
                    yield result
                    
              
        if (last_iteration_total > last_per_page) and crawl_history:
            for page in range(2, math.ceil(last_iteration_total / last_per_page) + 1):
                async with session.get(
                    f"{api_server}/api/v0/messages.json",
                    params={**params, "page": page},
                ) as resp:
                    items = await resp.json()
                    for message in items["messages"]:
                        result = await get_message_result(
                            message,
                            yield_unconfirmed=yield_unconfirmed,
                            last_block=last_block,
                            min_height=min_height,
                            last_seen=last_seen,
                            db=db,
                            db_prefix=prefix
                        )
                        if result is not None:
                            yield result
    print("network finished")


async def set_status(account, nodes, resource_nodes):
    nodes = [
        {
            **node.copy(),
            **{
                "total_staked": node["total_staked"] / DECIMALS,
                "stakers": {
                    addr: amt / DECIMALS for addr, amt in node["stakers"].items()
                },
            },
        }
        for node in nodes.values()
    ]
    async with AuthenticatedAlephHttpClient(account, api_server=settings.aleph_api_server) as client:
        await client.create_aggregate(
            "corechannel",
            {"nodes": nodes, "resource_nodes": list(resource_nodes.values())},
            channel=settings.aleph_channel,
        )
