import asyncio
import logging
from collections import deque

import aiohttp
from aleph.sdk.client import AuthenticatedAlephHttpClient
from aleph.sdk.query.filters import PostFilter

from .distribution import compute_score_multiplier
from .erc20 import process_contract_history
from .ethereum import get_web3
from .messages import get_aleph_account, get_aleph_address, process_message_history
from .monitored import process_balances_history
from .settings import settings
from .status import NodesStatus, prepare_items

LOGGER = logging.getLogger(__name__)

CREDIT_DISTRIBUTION_POST_TYPE = "credit-rewards-distribution"


async def get_latest_successful_credit_distribution(sender=None):
    """Find the last credit distribution that had successful transfers."""
    if sender is None:
        sender = get_aleph_address()

    async with AuthenticatedAlephHttpClient(
        get_aleph_account(), api_server=settings.aleph_api_server
    ) as client:
        posts = await client.get_posts(
            post_filter=PostFilter(
                types=[CREDIT_DISTRIBUTION_POST_TYPE],
                addresses=[sender],
            )
        )

    current_post = None
    current_end_time = 0
    for post in posts.posts:
        if post.content.get("status") != "distribution":
            continue

        successful = False
        for target in post.content.get("targets", []):
            if target["success"]:
                successful = True
                break

        if successful and post.content.get("end_time", 0) >= current_end_time:
            current_post = post
            current_end_time = post.content["end_time"]

    if current_post is not None:
        return current_end_time, current_post.content
    else:
        return 0, None


async def fetch_credit_expenses(api_server, start_time, end_time, sender=None):
    """
    Fetch aleph_credit_expense messages from the Aleph API.

    Returns:
        List of (height, "storage"|"execution", expense_dict) sorted by height.
        Messages without ETH confirmation are skipped.
    """
    expenses = []

    params = {
        "msgType": "POST",
        "contentTypes": "aleph_credit_expense",
        "startDate": int(start_time),
        "endDate": int(end_time),
        "pagination": 500,
        "page": 1,
        "sort_order": "1",
        "sort_by": "tx-time",
    }

    if sender:
        params["addresses"] = sender

    seen_hashes = set()
    max_retries = 3
    retry_delay = 5

    async with aiohttp.ClientSession() as session:
        while True:
            data = None
            for attempt in range(max_retries):
                try:
                    async with session.get(
                        f"{api_server}/api/v0/messages.json",
                        params=params,
                        timeout=60,
                    ) as resp:
                        if resp.status != 200:
                            LOGGER.warning(
                                f"API error {resp.status} (attempt {attempt + 1}/{max_retries})"
                            )
                            if attempt < max_retries - 1:
                                await asyncio.sleep(retry_delay)
                                continue
                            raise Exception(
                                f"API error {resp.status} fetching credit expenses"
                            )
                        data = await resp.json()
                        break
                except (asyncio.TimeoutError, aiohttp.ClientError) as e:
                    LOGGER.warning(
                        f"Network error fetching credit expenses: {e} "
                        f"(attempt {attempt + 1}/{max_retries})"
                    )
                    if attempt < max_retries - 1:
                        await asyncio.sleep(retry_delay)
                    else:
                        raise

            messages = data.get("messages", [])

            if not messages:
                break

            for msg in messages:
                if msg["item_hash"] in seen_hashes:
                    continue
                seen_hashes.add(msg["item_hash"])

                # Extract ETH confirmation height
                height = None
                for conf in msg.get("confirmations", []):
                    if conf["chain"] == "ETH":
                        if height is None or conf["height"] < height:
                            height = conf["height"]

                if height is None:
                    LOGGER.debug(
                        f"Skipping unconfirmed expense {msg['item_hash']}"
                    )
                    continue

                content = msg.get("content", {}).get("content", {})
                tags = content.get("tags", [])
                expense = content.get("expense", {})

                if not expense:
                    continue

                if "type_storage" in tags:
                    expenses.append((height, "storage", expense))
                elif "type_execution" in tags:
                    expenses.append((height, "execution", expense))

            total = data.get("pagination_total", 0)
            per_page = data.get("pagination_per_page", 500)
            if params["page"] * per_page >= total:
                break
            params["page"] += 1

    expenses.sort(key=lambda x: x[0])
    storage_count = sum(1 for e in expenses if e[1] == "storage")
    execution_count = sum(1 for e in expenses if e[1] == "execution")
    LOGGER.info(
        f"Fetched {storage_count} storage and "
        f"{execution_count} execution expense messages"
    )
    return expenses


def _get_reward_address(node, web3=None):
    """Get the reward address for a node, falling back to owner."""
    reward = node.get("reward")
    if reward and web3:
        try:
            validator = getattr(
                web3, "to_checksum_address",
                getattr(web3, "toChecksumAddress", None),
            )
            if validator:
                return validator(reward)
        except Exception:
            LOGGER.debug("Bad reward address, defaulting to owner")
            return node["owner"]
    return reward or node["owner"]


def _distribute_expense(expense_type, expense, nodes, resource_nodes,
                         rewards, web3=None):
    """
    Distribute a single expense message using the current node state.

    Mutates `rewards` in place.
    Returns (storage_aleph, execution_aleph, dev_fund_aleph) totals.
    """
    total_aleph = sum(
        credit["price"] for credit in expense.get("credits", [])
    )

    dev_fund = total_aleph * settings.credit_dev_fund_share

    if expense_type == "storage":
        ccn_share = settings.credit_storage_ccn_share
        staker_share = settings.credit_storage_staker_share
    else:
        ccn_share = settings.credit_execution_ccn_share
        staker_share = settings.credit_execution_staker_share

        # CRN share to the specific CRN per credit
        for credit in expense.get("credits", []):
            node_id = credit.get("node_id")
            if node_id and node_id in resource_nodes:
                rnode = resource_nodes[node_id]
                addr = _get_reward_address(rnode, web3)
                rewards[addr] = (
                    rewards.get(addr, 0)
                    + credit["price"] * settings.credit_execution_crn_share
                )
            elif node_id:
                LOGGER.warning(
                    f"CRN {node_id} not found in resource_nodes, "
                    f"skipping CRN share"
                )

    # --- CCN pool (weighted by score) ---
    ccn_weights = {}
    for node_hash, node in nodes.items():
        if node["status"] != "active":
            continue
        score = compute_score_multiplier(node["score"])
        if score > 0:
            ccn_weights[node_hash] = {
                "score": score,
                "reward_address": _get_reward_address(node, web3),
            }

    total_ccn_score = sum(v["score"] for v in ccn_weights.values())
    ccn_pool = total_aleph * ccn_share

    if total_ccn_score > 0 and ccn_pool > 0:
        for info in ccn_weights.values():
            share = info["score"] / total_ccn_score
            addr = info["reward_address"]
            rewards[addr] = rewards.get(addr, 0) + ccn_pool * share

    # --- Staker pool ---
    all_stakers = {}
    for node in nodes.values():
        if node["status"] != "active":
            continue
        for addr, amount in node["stakers"].items():
            all_stakers[addr] = all_stakers.get(addr, 0) + amount
    total_staked = sum(all_stakers.values())

    staker_pool = total_aleph * staker_share

    if total_staked > 0 and staker_pool > 0:
        for addr, staked in all_stakers.items():
            share = staked / total_staked
            rewards[addr] = rewards.get(addr, 0) + staker_pool * share

    if expense_type == "storage":
        return total_aleph, 0, dev_fund
    else:
        return 0, total_aleph, dev_fund


async def prepare_credit_distribution(dbs, end_height, start_time, end_time):
    """
    Build node state and distribute credit expenses at their confirmed heights.

    Advances with the state machine so each expense is distributed using
    the node/staker/score state at its confirmation height.

    Returns:
        Tuple of (rewards, total_storage_aleph, total_execution_aleph)
    """
    state_machine = NodesStatus()
    web3 = get_web3()

    # Fetch expenses first (sorted by height)
    api_server = settings.aleph_api_server
    expenses = await fetch_credit_expenses(
        api_server,
        start_time,
        end_time,
        sender=settings.credit_expense_sender,
    )

    if not expenses:
        LOGGER.warning("No credit expenses found in the given time range")
        return {}, 0, 0

    last_seen_txs = deque([], maxlen=100)

    iterators = [
        prepare_items(
            "balance-update",
            process_contract_history(
                settings.ethereum_token_contract,
                settings.ethereum_min_height,
                last_seen=last_seen_txs,
                db=dbs["erc20"],
                fetch_from_db=True,
            ),
        ),
        prepare_items(
            "balance-update",
            process_balances_history(
                settings.ethereum_min_height,
                request_count=500,
                db=dbs["balances"],
            ),
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
                db=dbs["scores"],
            ),
        ),
    ]

    rewards = {}
    total_storage = 0
    total_execution = 0
    total_dev_fund = 0
    expense_idx = 0
    nodes = None
    resource_nodes = None

    async for height, nodes, resource_nodes in state_machine.process(iterators):
        if height > end_height:
            break

        # Distribute any expenses confirmed at or before this height
        while expense_idx < len(expenses) and expenses[expense_idx][0] <= height:
            exp_height, exp_type, expense = expenses[expense_idx]
            s, e, d = _distribute_expense(
                exp_type, expense, nodes, resource_nodes, rewards, web3=web3
            )
            total_storage += s
            total_execution += e
            total_dev_fund += d
            expense_idx += 1

    # Distribute any remaining expenses (confirmed after last state change
    # but still within end_height)
    if nodes is not None:
        while expense_idx < len(expenses):
            exp_height, exp_type, expense = expenses[expense_idx]
            if exp_height > end_height:
                LOGGER.warning(
                    f"Expense at height {exp_height} is beyond end_height "
                    f"{end_height}, skipping"
                )
                expense_idx += 1
                continue
            s, e, d = _distribute_expense(
                exp_type, expense, nodes, resource_nodes, rewards, web3=web3
            )
            total_storage += s
            total_execution += e
            total_dev_fund += d
            expense_idx += 1

    if nodes is None:
        raise ValueError("No node state available")

    LOGGER.info(
        f"Credit distribution: "
        f"{total_storage:.4f} ALEPH storage, "
        f"{total_execution:.4f} ALEPH execution, "
        f"{total_dev_fund:.4f} ALEPH dev fund (not distributed), "
        f"{sum(rewards.values()):.4f} ALEPH total distributed"
    )

    return rewards, total_storage, total_execution, total_dev_fund
