import asyncio

import aiohttp
from aleph.sdk.chains.ethereum import ETHAccount
from aleph.sdk.client import AuthenticatedAlephHttpClient

from .ethereum import get_web3
from .settings import settings
from .solana_voucher import query_voucher_balances

DECIMALS = 10**settings.platform_solana_decimals


async def update_balances(account, main_height, chain_name, chain_identifier, balances):
    async with AuthenticatedAlephHttpClient(
        account=account, api_server=settings.aleph_api_server
    ) as client:
        post_message, _ = await client.create_post(
            post_content={
                "tags": [chain_identifier, chain_name, settings.filter_tag],
                "main_height": main_height,
                "platform": "{}_{}".format(settings.token_symbol, chain_identifier),
                "token_contract": "",
                "token_symbol": settings.token_symbol,
                "chain": chain_identifier,
                "balances": {
                    addr: value for addr, value in balances.items() if value > 0
                }
            },
            post_type=settings.balances_post_type,
            channel=settings.aleph_channel,
        )

        return post_message


async def query_balances(endpoint, chain_name):
    query = (
        """
query ($bc: String!) {
  balances(blockchain: $bc, limit: 99999) {
    account
    balance
    balanceNum
  }
}
"""
    )
    async with aiohttp.ClientSession() as session:
        async with session.post(endpoint, json={
                "query": query,
                "variables": {"bc": chain_name},
            }) as resp:
            result = await resp.json()
            holders = result["data"]["balances"]
            seen_accounts = set()
            values = {}
            for h in holders:
                if h is None:
                    continue
                if h["account"] not in seen_accounts:
                    seen_accounts.add(h["account"])
                    values[h["account"]] = values.get("account", 0) + h["balanceNum"]

            if chain_name == 'solana':
                voucher_balances = await query_voucher_balances(
                    settings.voucher_indexer_endpoint, chain_name
                )
                for voucher_owner, balance in voucher_balances.items():
                    values[voucher_owner] = values.get(voucher_owner, 0) + balance
            return values
            # return {h['owner']: int(h['balance']) for h in holders}


async def indexer_monitoring_process():
    web3 = get_web3()
    account = ETHAccount(settings.ethereum_pkey)

    previous_balances = {
        chain_identifier: None for chain_identifier in settings.platform_indexer_chains.values()
    }

    while True:
        changed_items = {
            chain_identifier: set() for chain_identifier in settings.platform_indexer_chains.values()
        }

        for chain_name, chain_identifier in settings.platform_indexer_chains.items():
            balances = await query_balances(
                settings.platform_indexer_endpoint, chain_name
            )
            if previous_balances[chain_identifier] is None:
                changed_items[chain_identifier] = set(balances.keys())
            else:
                for address in previous_balances[chain_identifier].keys():
                    if address not in balances.keys():
                        changed_items[chain_identifier].add(address)

                for address, amount in balances.items():
                    if (
                        abs(amount - previous_balances[chain_identifier].get(address, 0)) > 1
                        and address not in settings.platform_indexer_ignored_addresses
                    ):
                        changed_items[chain_identifier].add(address)

            if len(changed_items[chain_identifier]):
                print("SENDING BALANCES FOR {}".format(chain_identifier))
                await update_balances(account, web3.eth.block_number, chain_name, chain_identifier, balances)
                print("UPDATED!")
                previous_balances[chain_identifier] = balances

        await asyncio.sleep(60)
