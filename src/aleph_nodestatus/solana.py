import asyncio
import aiohttp
from .settings import settings
from aleph_client.asynchronous import create_post
from .ethereum import get_web3, get_logs

DECIMALS = 10**settings.platform_solana_decimals


async def update_balances(account, main_height, balances):
    return await create_post(
        account, {
            'tags': ['SOL', 'SPL', settings.platform_solana_mint,
                     settings.filter_tag],
            'main_height': main_height,
            'platform': '{}_{}'.format(settings.token_symbol,
                                       'SOL'),
            'token_contract': settings.platform_solana_mint,
            'token_symbol': settings.token_symbol,
            'chain': 'SOL',
            'balances': {addr: value / DECIMALS
                         for addr, value in balances.items()
                         if value > 0}},
        settings.balances_post_type,
        channel=settings.aleph_channel,
        api_server=settings.aleph_api_server)


async def query_balances(endpoint, mint):
    query = """
query {
  tokenHolders(
    mint: "%s"
  ) {
    account
    owner
    balance
  }
}""" % mint
    async with aiohttp.ClientSession() as session:
        async with session.post(endpoint,
                                json={"query": query}) as resp:
            result = await resp.json()
            holders = result["data"]["tokenHolders"]
            seen_accounts = set()
            values = {}
            for h in holders:
                if h is None:
                    continue
                if h['account'] not in seen_accounts:
                    seen_accounts.add(h['account'])
                    values[h['owner']] = (values.get(h['owner'], 0)
                                          + int(h['balance']))
            return values
            # return {h['owner']: int(h['balance']) for h in holders}


async def solana_monitoring_process():
    from .messages import get_aleph_account
    web3 = get_web3()
    account = get_aleph_account()

    previous_balances = None

    while True:
        changed_items = set()

        balances = await query_balances(settings.platform_solana_endpoint,
                                        settings.platform_solana_mint)
        if previous_balances is None:
            changed_items = set(balances.keys())
        else:
            for address in previous_balances.keys():
                if address not in balances.keys():
                    changed_items.add(address)

            for address, amount in balances.items():
                if (amount != previous_balances.get(address, 0)
                        and address
                        not in settings.platform_solana_ignored_addresses):
                    changed_items.add(address)

        if changed_items:
            print(changed_items)
            await update_balances(account, web3.eth.blockNumber, balances)
            previous_balances = balances
        
        await asyncio.sleep(300)
    