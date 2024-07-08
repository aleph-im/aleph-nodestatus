from functools import lru_cache

import requests

from .erc20_utils import get_contract, get_token_contract_abi
from .ethereum import get_web3
from .settings import settings

DECIMALS = 10**18

@lru_cache(maxsize=30)
def get_metadata(metadata_id):
    url = f"{settings.voucher_api_server}/sbt/metadata/{metadata_id}.json"
    try:
        response = requests.get(url)
        return response.json()
    except requests.exceptions.JSONDecodeError:
        return None


async def getVoucherNFTBalances(start_height=None):
    # Connect to the network
    web3 = get_web3(settings.avax_api_server)

    # Get the contract
    abi = get_token_contract_abi(settings.voucher_abi_name)
    contract = get_contract(settings.voucher_contract_address, web3, abi)

    if start_height is None:
        start_height = settings.voucher_ethereum_min_height

    # Get the events
    mint_events = contract.events.Mint().getLogs(
        fromBlock=start_height
    )

    for mint in mint_events:
        claimer = mint["args"]["claimer"]
        #token_id = mint["args"]["tokenId"]
        metadata_id = mint["args"]["metadataId"]
        metadata = get_metadata(metadata_id)
        balance = 0
        if metadata:
            for k in metadata["attributes"]:
                if "$ALEPH" in k["trait_type"]:
                    balance = k["value"] * DECIMALS

        yield (claimer, balance, mint["blockNumber"])
