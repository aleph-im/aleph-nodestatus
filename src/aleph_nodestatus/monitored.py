from .erc20 import DECIMALS as ETH_DECIMALS
from .solana import DECIMALS as SOL_DECIMALS
from .messages import process_message_history
from .settings import settings


async def process_balances_history(
    min_height,
    request_sort="1",
    request_count=10000,
    crawl_history=True,
    platform_balances=None,
):
    last_height = 0
    async for height, message in process_message_history(
        [settings.filter_tag],
        [settings.balances_post_type],
        settings.aleph_api_server,
        yield_unconfirmed=True,
        addresses=settings.balances_senders,
        min_height=min_height,
        message_type="POST",
        request_sort=request_sort,
        request_count=request_count,
        crawl_history=crawl_history,
    ):
        message_content = message["content"]
        post_content = message_content["content"]
        platform = post_content.get("platform", None)

        if post_content.get("main_height", None) is not None:
            nheight = post_content["main_height"]
        else:
            nheight = height

        if nheight <= last_height:
            # fake iteration on height in case stuff gets mixed up
            nheight = last_height + 1

        if message_content["address"] not in settings.balances_senders:
            continue
        if message_content["type"] != settings.balances_post_type:
            continue

        decimals: int
        if platform in [settings.ethereum_platform, settings.ethereum_sablier_platform]:
            decimals = ETH_DECIMALS
        elif platform == settings.solana_platform:
            decimals = SOL_DECIMALS
        else:
            # unexpected platform
            continue

        # TODO: Is this really required here? We already multiply the decimals when doing the actual transfers
        balances = {
            address: amount * decimals  # convert to ERC-20 decimals
            for address, amount in post_content["balances"].items()
        }

        changed_addresses = list(balances.keys())
        # TODO: fine computing of platform balances (per platform, evolving)
        # if platform_balances is None:
        #     changed_addresses = list(balances.keys())
        # else:
        #     for address, new_value in balances.items():
        #         if platform_balances

        yield (nheight, (balances, platform, changed_addresses))
        last_height = nheight
