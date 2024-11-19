import logging
import time

import aiohttp

from .settings import settings

DECIMALS = 10**settings.platform_solana_decimals

LOGGER = logging.getLogger(__name__)

metadata = {}


async def fetch_metadata(url):
    if url in metadata:
        return metadata[url]

    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            try:
                metadata[url] = await resp.json()
                return metadata[url]
            except Exception as e:
                LOGGER.error(f"Error when retrieving metadata: {e}")


async def get_voucher_balance(addr, metadata, now):
    balance, start, end = 0, 0, 0
    for k in metadata["attributes"]:
        if k["trait_type"] == "$ALEPH Virtual Balance":
            balance = k["value"]
        elif k["trait_type"] == "Start":
            start = k["value"]
        elif k["trait_type"] == "End":
            end = k["value"]
    if start < now and now < end and balance > 0:
        return balance
    return 0


def build_query(chain, skip: int = 0, limit: int = 1000):
    return (
        """
query {
  tokens(
    blockchain: "%s",
    skip: %s
    limit: %s
  ) {
     account
     owner
     url
  }
}"""
        % (chain, skip, limit)
    )


async def query_voucher_balances(endpoint, chain):
    seen_nfts = set()
    values = {}

    async with aiohttp.ClientSession() as session:
        skip = 0
        limit = 1000
        while True:
            query = build_query(chain, skip, limit)
            async with session.post(endpoint, json={"query": query}) as resp:
                result = await resp.json()
                balances = result["data"]["tokens"]
                for b in balances:
                    nft_address = b["account"]
                    owner = b["owner"]
                    url = b["url"]
                    if nft_address not in seen_nfts:
                        seen_nfts.add(nft_address)
                        metadata = await fetch_metadata(url)
                        voucher_balance = await get_voucher_balance(owner, metadata, int(time.time())*1000)
                        values[owner] = values.get(owner, 0) + int(voucher_balance)
                        values[owner] = 0
            if len(balances) >= limit:
                skip += limit
            else:
                break

        return values
