from ctypes import addressof
from .messages import process_message_history
from .settings import settings
from .erc20 import process_contract_history, DECIMALS

async def process_balances_history(min_height, request_sort="1",
                                   platform_balances=None):
    async for height, message in process_message_history(
        [settings.filter_tag], settings.balances_post_type,
        settings.aleph_api_server, yield_unconfirmed=False,
        addresses=settings.balances_senders, min_height=min_height,
        message_type="POST", request_sort=request_sort):
        
        message_content = message["content"]
        post_content = message_content["content"]
        platform = post_content.get("platform", None)
        if message_content["address"] not in settings.balances_senders:
            continue
        if message_content["type"] != settings.balances_post_type:
            continue
        if platform not in settings.balances_platforms:
            # unexpected platform
            continue
        
        balances = {
            address: amount * DECIMALS # convert to ERC-20 decimals
            for address, amount in post_content["balances"].items()
        }
        
        changed_addresses = list(balances.keys())
        # TODO: fine computing of platform balances (per platform, evolving)
        # if platform_balances is None:
        #     changed_addresses = list(balances.keys())
        # else:
        #     for address, new_value in balances.items():
        #         if platform_balances
            
        
        yield (height, (balances, platform, changed_addresses))