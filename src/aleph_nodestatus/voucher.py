import logging
import time

import requests
from aleph_client.asynchronous import create_post, get_posts

from .erc20_utils import get_contract, get_token_contract_abi
from .ethereum import get_web3
from .messages import get_aleph_account
from .settings import settings

LOGGER = logging.getLogger(__name__)

DECIMALS = 10**settings.ethereum_decimals

class VoucherSettings:
    _instance = None
    account = None
    balances = {}
    vouchers = {}
    metadata = {}

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super().__new__(cls, *args, **kwargs)
        return cls._instance

    def __init__(self):
        if not hasattr(self, "initialized"):
            self.initialized = True
            self.account = get_aleph_account()
            self.web3 = get_web3(settings.avax_api_server)
            self.abi = get_token_contract_abi(settings.voucher_abi_name)
            self.contract = get_contract(
                settings.voucher_contract_address, self.web3, self.abi
            )
            self._active = True
            self._next_height = settings.voucher_avax_min_height

    @property
    def active(self):
        return self._active

    @active.setter
    def active(self, value):
        self._active = value

    @property
    def next_height(self):
        return self._next_height

    @next_height.setter
    def next_height(self, value):
        self._next_height = value

async def getVoucherNFTUpdates():
    voucher_settings = VoucherSettings()
    now = time.time()
    start_height = voucher_settings.next_height
    current_height = voucher_settings.web3.eth.block_number

    if start_height == settings.voucher_avax_min_height:
        last_commit = await fetch_last_commit_nft_balances(voucher_settings)
        if last_commit:
            voucher_settings.balances = last_commit['virtual_balances']
            voucher_settings.vouchers = last_commit['nft_vouchers']
            voucher_settings.metadata = last_commit['metadata']
            start_height = last_commit['chain_height'] + 1
            for claimer, balance in voucher_settings.balances.items():
                yield (claimer, balance * DECIMALS)
    if start_height > current_height:
        return

    metadata_events = voucher_settings.contract.events.BatchMetadataUpdate().getLogs(
        fromBlock=start_height
    )
    if metadata_events:
        if start_height > settings.voucher_avax_min_height:
            refetch_all_metadata(voucher_settings)
            for claimer, balance in recompute_all_balances(voucher_settings, now):
                yield (claimer, balance * DECIMALS)

    mint_events = voucher_settings.contract.events.Mint().getLogs(
            fromBlock=start_height
    )
    for mint in mint_events:
        token_id = mint["args"]["tokenId"]
        claimer = mint["args"]["claimer"]
        metadata_id = str(mint["args"]["metadataId"])
        voucher_settings.vouchers[token_id] = dict(
            claimer=claimer, metadata_id=metadata_id
        )
        metadata = check_metadata(metadata_id, voucher_settings)
        virtual_balance_change = check_balance(claimer, metadata, voucher_settings.balances, now)
        LOGGER.info(
                    f"New NFT id={token_id} for {claimer}: +{virtual_balance_change}"
                )
        if virtual_balance_change > 0:
            yield (claimer, virtual_balance_change * DECIMALS)

    voucher_settings.next_height = current_height + 1
    if len(mint_events) + len(metadata_events) > 0:
        await update_nft_balances(voucher_settings)

def fetch_metadata(metadata_id):
    url = f"{settings.voucher_api_server}/sbt/metadata/{metadata_id}.json"
    try:
        response = requests.get(url)
        return response.json()
    except Exception as e:
        LOGGER.error(f"Error when retrieving metadata: {e}")

def check_metadata(metadata_id, voucher_settings):
    if metadata_id not in voucher_settings.metadata:
        voucher_settings.metadata[metadata_id] = fetch_metadata(metadata_id)
    return voucher_settings.metadata[metadata_id]

def check_balance(addr, metadata, balances, now):
    balance, start, end = 0, 0, 0
    for k in metadata["attributes"]:
        if k["trait_type"] == "$ALEPH Virtual Balance":
            balance = k["value"]
        elif k["trait_type"] == "Start":
            start = k["value"]
        elif k["trait_type"] == "End":
            end = k["value"]
    if start < now and now < end and balance > 0:
        if addr not in balances:
            balances[addr] = balance
        else:
            balances[addr] += balance
        return balance
    return 0

def refetch_all_metadata(voucher_settings):
    LOGGER.info(f"Metadata Update: Refetch all metadata")
    for metadata_id in voucher_settings.metadata:
        voucher_settings.metadata[metadata_id] = fetch_metadata(metadata_id)

def recompute_all_balances(voucher_settings, now):
    LOGGER.info(f"Metadata Update: Recompute all balances")
    balances, diffs = {}, []
    for voucher in voucher_settings.vouchers.values():
        claimer, metadata_id = voucher["claimer"], voucher["metadata_id"]
        metadata = voucher_settings.metadata[metadata_id]
        check_balance(claimer, metadata, balances, now)
    for claimer, old_balance in voucher_settings.balances.items():
        new_balance = balances[claimer]
        if old_balance != new_balance:
            diffs.append((claimer, new_balance - old_balance))
    voucher_settings.balances = balances
    return diffs

async def fetch_last_commit_nft_balances(voucher_settings):
    LOGGER.info(f"Process vouchers: Fetch last commit NFT balances")
    chain_height, last_commit = 0, None
    posts = await get_posts(
        types=[settings.vouchers_post_type],
        addresses=[voucher_settings.account.get_address()],
        api_server=settings.aleph_api_server,
    )
    if posts and 'posts' in posts and len(posts['posts']) > 0:
        for post in posts['posts']:
            try:
                current = post['content']
                if current['token_contract'] == settings.voucher_contract_address and current['chain_height'] > chain_height:
                    last_commit, chain_height = current, current['chain_height']
            except:
                pass
        LOGGER.info(f"Process vouchers: commit found for last_height={chain_height}")
        return last_commit
    LOGGER.info(f"Process vouchers: no commit found")

async def update_nft_balances(voucher_settings):
    return await create_post(
        voucher_settings.account,
        {
        "tags": ["ERC721", settings.voucher_contract_address, "voucher"],
        "chain": settings.voucher_chain_name,
        "chain_id": settings.voucher_chain_id,
        "chain_height": voucher_settings.next_height - 1,
        "platform": f"{settings.voucher_token_symbol}_{settings.voucher_chain_name}",
        "token_symbol": settings.voucher_token_symbol,
        "token_contract": settings.voucher_contract_address,
        "virtual_balances": voucher_settings.balances,
        "nft_vouchers": voucher_settings.vouchers,
        "metadata": voucher_settings.metadata,
    },
        settings.vouchers_post_type,
        channel=settings.aleph_channel,
        api_server=settings.aleph_api_server,
    )