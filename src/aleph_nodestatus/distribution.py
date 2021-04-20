import math
import asyncio
from collections import deque
from .status import NodesStatus, prepare_items
from .erc20 import process_contract_history, DECIMALS
from .ethereum import get_web3
from .messages import process_message_history, get_aleph_account, set_status, get_aleph_address
from .settings import settings
from .utils import merge
from aleph_client.asynchronous import create_aggregate, create_post, get_posts

import logging
LOGGER = logging.getLogger(__name__)


async def create_distribution_tx_post(distribution):
    print(f"Preparing pending TX post {distribution}")
    post = await create_post(
        get_aleph_account(),
        distribution,
        post_type='staking-rewards-distribution',
        channel=settings.aleph_channel,
        api_server=settings.aleph_api_server)
    return post


async def get_latest_successful_distribution(sender=None):
    if sender is None:
        sender = get_aleph_address()
    posts = await get_posts(
        types=["staking-rewards-distribution"],
        addresses=[sender],
        api_server=settings.aleph_api_server)

    current_post = None
    current_end_height = 0
    for post in posts['posts']:
        successful = False
        if post['content']['status'] != 'distribution':
            pass

        for target in post['content'].get('targets', []):
            if target['success']:
                successful = True
                break
        
        if successful:
            if post['content']['end_height'] >= current_end_height:
                current_post = post
                current_end_height = post['content']['end_height']
                continue
    
    if current_post is not None:
        return current_end_height, current_post['content']
    else:
        return 0, None

async def prepare_distribution(start_height, end_height):
    state_machine = NodesStatus()
    account = get_aleph_account()
    reward_start = max(settings.reward_start_height, start_height)
    web3 = get_web3()

    rewards = dict()

    last_seen_txs = deque([], maxlen=100)

    iterators = [
        prepare_items('balance-update', process_contract_history(
            settings.ethereum_token_contract, settings.ethereum_min_height,
            last_seen=last_seen_txs)),
        prepare_items('staking-update', process_message_history(
            [settings.filter_tag],
            [settings.node_post_type, 'amend'],
            settings.aleph_api_server,
            yield_unconfirmed=False))
    ]
    nodes = None
    # TODO: handle decay
    nodes_rewards = settings.reward_nodes_daily / settings.ethereum_blocks_per_day

    def process_distribution(nodes, since, current):
        # Ignore if we aren't in distribution period yet.
        # Handle calculation for previous period now.
        block_count = current - since
        LOGGER.debug(f"Calculating for block {current}, {block_count} blocks")
        active_nodes = [node for node
                        in nodes.values()
                        if node['status'] == 'active']
        if not active_nodes:
            return

        # TODO: handle decay
        per_day = ((math.log10(len(active_nodes))+1)/3) * settings.reward_stakers_daily_base
        stakers_reward = (per_day / settings.ethereum_blocks_per_day) * block_count

        per_node = (nodes_rewards / len(active_nodes)) * block_count
        total_staked = sum([
            sum(node['stakers'].values())
            for node in active_nodes
        ])
        per_bonus_node = per_node
        if current > settings.bonus_start:
            modifier = (
                settings.bonus_modifier
                - ((current-settings.bonus_start) * settings.bonus_decay)
            )
            if modifier > 1:
                per_bonus_node = per_node * modifier
                
        for node in active_nodes:
            reward_address = node['owner']
            this_node = per_node
            if node['has_bonus']:
                this_node = per_bonus_node
                
            try:
                taddress = web3.toChecksumAddress(node.get('reward', None))
                if taddress:
                    reward_address = taddress
            except Exception:
                LOGGER.debug("Bad reward address, defaulting to owner")
            rewards[reward_address] = rewards.get(reward_address, 0) + this_node

            for addr, value in node['stakers'].items():
                rewards[addr] = rewards.get(addr, 0) + ((value / total_staked) * stakers_reward)

    last_height = reward_start
    async for height, nodes in state_machine.process(iterators):
        if height == last_height:
            continue

        if height > end_height:
            break
        if height > reward_start:
            process_distribution(nodes, max(last_height, reward_start), height)

        last_height = height

    process_distribution(nodes, last_height, end_height)
    LOGGER.info(f'Rewards from {reward_start} to {end_height}, total {sum(rewards.values())}')
    return reward_start, end_height, rewards

    