import logging
import math
from collections import deque

from aleph.sdk.client import AuthenticatedAlephHttpClient
from aleph.sdk.query.filters import PostFilter

from .erc20 import DECIMALS, process_contract_history
from .ethereum import get_web3
from .messages import get_aleph_account, get_aleph_address, process_message_history
from .monitored import process_balances_history
from .settings import settings
from .status import NodesStatus, prepare_items

LOGGER = logging.getLogger(__name__)


def compute_score_multiplier(score: float) -> float:
    """
    Compute the score multiplier
    """
    if score < 0.2:
        # The score is zero below 20%
        return 0
    elif score >= 0.8:
        # The score is 1 above 80%
        return 1
    else:
        # The score is normalized between 20% and 80%
        assert 0.2 <= score <= 0.8
        return (score - 0.2) / 0.6


async def create_distribution_tx_post(distribution):
    print(f"Preparing pending TX post {distribution}")
    async with AuthenticatedAlephHttpClient(get_aleph_account(), api_server=settings.aleph_api_server) as client:
        post = await client.create_post(
            distribution,
            post_type="staking-rewards-distribution",
            channel=settings.aleph_channel
        )
    return post


async def get_latest_successful_distribution(sender=None):
    if sender is None:
        sender = get_aleph_address()
        
    async with AuthenticatedAlephHttpClient(get_aleph_account(), api_server=settings.aleph_api_server) as client:
        posts = await client.get_posts(
            post_filter=PostFilter(
                types=["staking-rewards-distribution"],
                addresses=[sender]
            )
        )
        
    current_post = None
    current_end_height = 0
    for post in posts.posts:
        successful = False
        if post.content["status"] != "distribution":
            pass

        for target in post.content.get("targets", []):
            if target["success"]:
                successful = True
                break

        if successful:
            if post.content["end_height"] >= current_end_height:
                current_post = post
                current_end_height = post.content["end_height"]
                continue

    if current_post is not None:
        return current_end_height, current_post.content
    else:
        return 0, None


async def prepare_distribution(dbs, start_height, end_height):
    state_machine = NodesStatus()
    account = get_aleph_account()
    reward_start = max(settings.reward_start_height, start_height)
    web3 = get_web3()

    rewards = dict()

    last_seen_txs = deque([], maxlen=100)

    iterators = [
        prepare_items(
            "balance-update",
            process_contract_history(
                settings.ethereum_token_contract,
                settings.ethereum_min_height,
                last_seen=last_seen_txs,
                db=dbs["erc20"],
                fetch_from_db=True
            ),
        ),
        prepare_items(
            "balance-update",
            process_balances_history(
                settings.ethereum_min_height,
                request_count=500,
                db=dbs["messages"],
            )
        ),
        prepare_items(
            "staking-update",
            process_message_history(
                [settings.filter_tag],
                [settings.node_post_type, "amend"],
                settings.aleph_api_server,
                yield_unconfirmed=False,
                request_count=5000,
                db=dbs["messages"],
            ),
        ),
        prepare_items(
            "score-update",
            process_message_history(
                [settings.filter_tag],
                [settings.scores_post_type],
                message_type="POST",
                addresses=settings.scores_senders,
                api_server=settings.aleph_api_server,
                request_count=1000,
                db=dbs["messages"],
            ),
        ),
    ]
    nodes = None
    # TODO: handle decay
    nodes_rewards = settings.reward_nodes_daily / settings.ethereum_blocks_per_day

    def compute_resource_node_rewards(decentralization_factor):
        return (
            (
                settings.reward_resource_node_monthly_base
                + (
                    settings.reward_resource_node_monthly_variable
                    * decentralization_factor
                )
            )
            / (365/12)
            / settings.ethereum_blocks_per_day
        )

    def process_distribution(nodes, resource_nodes, since, current):
        # Ignore if we aren't in distribution period yet.
        # Handle calculation for previous period now.
        block_count = current - since
        LOGGER.debug(f"Calculating for block {current}, {block_count} blocks")
        active_nodes = [node for node in nodes.values() if node["status"] == "active"]
        if not active_nodes:
            return

        # TODO: handle decay
        per_day = (
            (math.log10(len(active_nodes)) + 1) / 3
        ) * settings.reward_stakers_daily_base
        stakers_reward = (per_day / settings.ethereum_blocks_per_day) * block_count

        per_node = (nodes_rewards / len(active_nodes)) * block_count
        # per_resource_node = resource_node_rewards * block_count
        total_staked = sum([sum(node["stakers"].values()) for node in active_nodes])
        per_bonus_node = per_node
        if current > settings.bonus_start:
            modifier = settings.bonus_modifier - (
                (current - settings.bonus_start) * settings.bonus_decay
            )
            if modifier > 1:
                per_bonus_node = per_node * modifier

        for node in active_nodes:
            reward_address = node["owner"]
            this_node = per_node
            if node["has_bonus"]:
                this_node = per_bonus_node

            rnodes = node["resource_nodes"]
            paid_node_count = 0
            for rnode_id in rnodes:
                rnode = resource_nodes.get(rnode_id, None)
                if rnode is None:
                    continue
                if rnode["status"] != "linked": # how could this happen?
                    continue

                rnode_reward_address = rnode["owner"]
                try:
                    rtaddress = web3.toChecksumAddress(rnode.get("reward", None))
                    if rtaddress:
                        rnode_reward_address = rtaddress
                except Exception:
                    LOGGER.debug("Bad reward address, defaulting to owner")

                crn_multiplier = compute_score_multiplier(rnode["score"])

                assert 0 <= crn_multiplier <= 1, "Invalid value of the score multiplier"

                this_resource_node = (
                    compute_resource_node_rewards(rnode["decentralization"])
                    * block_count
                    * crn_multiplier
                )

                if crn_multiplier > 0:
                    paid_node_count += 1
                
                if paid_node_count <= settings.node_max_paid: # we only pay the first N nodes
                    rewards[rnode_reward_address] = (
                        rewards.get(rnode_reward_address, 0) + this_resource_node
                    )

            if paid_node_count > settings.node_max_paid:
                paid_node_count = settings.node_max_paid

            score_multiplier = compute_score_multiplier(node["score"])
            assert 0 <= score_multiplier <= 1, "Invalid value of the score multiplier"

            linkage = 0.7 + (0.1 * paid_node_count)

            if linkage > 1: # Cap the linkage at 1
                linkage = 1

            assert 0.7 <= linkage <= 1, "Invalid value of the linkage"

            this_node_modifier = linkage * score_multiplier

            this_node = this_node * this_node_modifier

            try:
                taddress = web3.toChecksumAddress(node.get("reward", None))
                if taddress:
                    reward_address = taddress
            except Exception:
                LOGGER.debug("Bad reward address, defaulting to owner")
            rewards[reward_address] = rewards.get(reward_address, 0) + this_node

            for addr, value in node["stakers"].items():
                sreward = ((value / total_staked) * stakers_reward) * this_node_modifier
                rewards[addr] = rewards.get(addr, 0) + sreward

    last_height = reward_start
    async for height, nodes, resource_nodes in state_machine.process(iterators):
        if height == last_height:
            continue

        if height > end_height:
            break
        if height > reward_start:
            process_distribution(
                nodes, resource_nodes, max(last_height, reward_start), height
            )

        last_height = height

    process_distribution(nodes, resource_nodes, last_height, end_height)
    LOGGER.info(
        f"Rewards from {reward_start} to {end_height}, total {sum(rewards.values())}"
    )
    return reward_start, end_height, rewards
