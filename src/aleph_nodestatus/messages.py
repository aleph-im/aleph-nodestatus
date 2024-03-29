import math
from collections import deque
from functools import lru_cache

import aiohttp
from aleph_client.asynchronous import create_aggregate
from aleph_client.chains.ethereum import ETHAccount
from hexbytes import HexBytes

from .erc20 import DECIMALS
from .ethereum import get_web3
from .settings import settings


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


async def get_message_result(
    message, yield_unconfirmed=True, last_block=0, min_height=0
):
    if message["item_hash"] in UNCONFIRMED_MESSAGES:
        return None

    earliest = None
    for conf in message.get("confirmations", []):
        if conf["chain"] == "ETH":
            if earliest is None or conf["height"] < earliest:
                earliest = conf["height"]
    # print(earliest, min_height)
    if earliest is None and not yield_unconfirmed:
        return None

    if earliest is None and yield_unconfirmed:
        # let's assign the current block height... (ugly)
        earliest = last_block
        UNCONFIRMED_MESSAGES.append(message["item_hash"])
        return earliest, message

    elif earliest >= min_height:
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
                        )
                        if result is not None:
                            yield result


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
    await create_aggregate(
        account,
        "corechannel",
        {"nodes": nodes, "resource_nodes": list(resource_nodes.values())},
        channel=settings.aleph_channel,
        api_server=settings.aleph_api_server,
    )
