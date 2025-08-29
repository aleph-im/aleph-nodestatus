import aiohttp
from aleph.sdk.chains.ethereum import ETHAccount
from aleph.sdk.client import AuthenticatedAlephHttpClient
from aleph_message.models import Chain

from .settings import settings
from .utils import chunks


async def publish_balances(account, height, chain_name, chain_identifier, balances):
    async with AuthenticatedAlephHttpClient(
        account=account, api_server="https://api.twentysix.testnet.network"
    ) as client:
        post_message, _ = await client.create_post(
            post_content={
                "tags": [chain_identifier, chain_name, settings.filter_tag],
                "height": height,
                "main_height": height,
                "platform": "{}_{}".format(settings.token_symbol, chain_identifier),
                "token_contract": "",
                "token_symbol": settings.token_symbol,
                "chain": chain_identifier,
                "balances": {
                    addr: value for addr, value in balances.items()
                }
            },
            post_type=settings.balances_post_type,
            channel=settings.aleph_channel,
        )

        return post_message


async def do_update_balances(account, height, chain_name, chain_identifier, balances):
    if len(balances) > 10000:
        for _balances in chunks(balances, 10000):
            yield await publish_balances(account, height, chain_name, chain_identifier, _balances)
    else:
        yield await publish_balances(account, height, chain_name, chain_identifier, balances)


async def get_existing_balances(chain: Chain):
    async with aiohttp.ClientSession() as session:
        page = 1
        limit = 1000
        params = {
            "page": page,
            "limit": limit,
            "chains": chain
        }
        url = f"{settings.aleph_api_server}/api/v0/balances"
        while True:
            async with session.get(url, params={**params, "page": page}) as resp:
                items = await resp.json()
                if len(items["balances"]) > 0:
                    for balance in items["balances"]:
                        yield balance

                    page += 1


async def do_reset_balances(chain_name: str):
    chain = settings.platform_indexer_chains[chain_name]
    balances = {}
    async for item in get_existing_balances(chain):
        if float(item["balance"]) > 0:
            balances[item["address"]] = 0

    if len(balances) > 0:
        account = ETHAccount(settings.ethereum_pkey)

        async for message in do_update_balances(account, 0, chain_name, chain, balances):
            print(f"Message item hash {message.item_hash}")
