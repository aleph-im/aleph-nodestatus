import json
import os
from functools import lru_cache
from pathlib import Path


@lru_cache(maxsize=20)
def get_token_contract_abi(abi_name):
    return json.load(
        open(os.path.join(Path(__file__).resolve().parent, f"abi/{abi_name}.json"))
    )


def get_contract(address, web3, abi):
    return web3.eth.contract(address, abi=abi)
