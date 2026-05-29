import asyncio
import bisect
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
from .settings import settings, PublishMode
from .status import NodesStatus, prepare_items

LOGGER = logging.getLogger(__name__)

CREDIT_DISTRIBUTION_POST_TYPE = "credit-rewards-distribution"


async def get_latest_successful_credit_distribution(sender=None):
    """Find the last credit distribution that had successful transfers."""
    if sender is None:
        sender = get_aleph_address()

    async with AuthenticatedAlephHttpClient(
        get_aleph_account(), api_server=PublishMode.get_publish_api_server()
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


async def _fetch_paginated_messages(session, api_server, params, max_retries=3,
                                     retry_delay=5):
    """Fetch paginated messages from the Aleph API with retry logic."""
    seen_hashes = set()
    messages = []

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
                            f"API error {resp.status} fetching messages"
                        )
                    data = await resp.json()
                    break
            except (asyncio.TimeoutError, aiohttp.ClientError) as e:
                LOGGER.warning(
                    f"Network error: {e} (attempt {attempt + 1}/{max_retries})"
                )
                if attempt < max_retries - 1:
                    await asyncio.sleep(retry_delay)
                else:
                    raise

        page_messages = data.get("messages", [])

        if not page_messages:
            break

        for msg in page_messages:
            if msg["item_hash"] not in seen_hashes:
                seen_hashes.add(msg["item_hash"])
                messages.append(msg)

        total = data.get("pagination_total", 0)
        per_page = data.get("pagination_per_page", 500)
        if params["page"] * per_page >= total:
            break
        params["page"] += 1

    return messages


def _extract_eth_height(msg):
    """Extract the earliest ETH confirmation height from a message."""
    height = None
    for conf in msg.get("confirmations", []):
        if conf["chain"] == "ETH":
            if height is None or conf["height"] < height:
                height = conf["height"]
    return height


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

    async with aiohttp.ClientSession() as session:
        messages = await _fetch_paginated_messages(session, api_server, params)

    for msg in messages:
        height = _extract_eth_height(msg)
        if height is None:
            LOGGER.debug(f"Skipping unconfirmed expense {msg['item_hash']}")
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

    expenses.sort(key=lambda x: x[0])
    storage_count = sum(1 for e in expenses if e[1] == "storage")
    execution_count = sum(1 for e in expenses if e[1] == "execution")
    LOGGER.info(
        f"Fetched {storage_count} storage and "
        f"{execution_count} execution expense messages"
    )
    return expenses


async def fetch_node_snapshots(api_server, start_time, end_time, sender=None):
    """
    Fetch historical corechannel aggregate snapshots for the period.

    Fetches from 1 day before start_time to ensure we have a snapshot
    before the first expense.

    Returns:
        List of (height, nodes_dict, resource_nodes_dict) sorted by height.
    """
    if sender is None:
        sender = settings.status_sender

    snapshots = []

    params = {
        "msgType": "AGGREGATE",
        "addresses": sender,
        "startDate": int(start_time - 86400),
        "endDate": int(end_time),
        "pagination": 500,
        "page": 1,
        "sort_order": "1",
        "sort_by": "tx-time",
    }

    async with aiohttp.ClientSession() as session:
        messages = await _fetch_paginated_messages(session, api_server, params)

    for msg in messages:
        content = msg.get("content", {})
        if content.get("key") != "corechannel":
            continue

        height = _extract_eth_height(msg)
        if height is None:
            continue

        inner = content.get("content", {})
        nodes_list = inner.get("nodes", [])
        resource_nodes_list = inner.get("resource_nodes", [])

        nodes_dict = {node["hash"]: node for node in nodes_list}
        resource_nodes_dict = {rn["hash"]: rn for rn in resource_nodes_list}

        snapshots.append((height, nodes_dict, resource_nodes_dict))

    snapshots.sort(key=lambda x: x[0])
    LOGGER.info(f"Fetched {len(snapshots)} node status snapshots")
    return snapshots


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
                         payouts, web3=None):
    """
    Distribute a single expense message using the current node state.

    ALEPH cost per entry = entry["amount"] * expense["credit_price_aleph"]
    (entry["price"] is the compute rate, NOT the ALEPH cost)

    Two entry pools are processed with the SAME split:
    - `credits`: user paid in credits (revenue from users)
    - `rewards`: holder-tier executions (paid from the incentives pool, not
      deducted from any user). Execution messages only; gated by
      settings.credit_hold_rewards_enabled.

    Mutates `payouts` in place (address -> ALEPH owed).
    Returns (storage_aleph, execution_aleph, hold_rewards_aleph, dev_fund_aleph).
    """
    credit_price_aleph = expense.get("credit_price_aleph", 0)

    if expense_type == "storage":
        ccn_share = settings.credit_storage_ccn_share
        staker_share = settings.credit_storage_staker_share
    else:
        ccn_share = settings.credit_execution_ccn_share
        staker_share = settings.credit_execution_staker_share

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

    all_stakers = {}
    for node in nodes.values():
        if node["status"] != "active":
            continue
        for addr, amount in node["stakers"].items():
            all_stakers[addr] = all_stakers.get(addr, 0) + amount
    total_staked = sum(all_stakers.values())

    def apply_split(entries):
        total = sum(e["amount"] * credit_price_aleph for e in entries)
        dev = total * settings.credit_dev_fund_share

        if expense_type == "execution":
            for entry in entries:
                entry_aleph = entry["amount"] * credit_price_aleph
                node_id = entry.get("node_id")
                if node_id and node_id in resource_nodes:
                    rnode = resource_nodes[node_id]
                    addr = _get_reward_address(rnode, web3)
                    payouts[addr] = (
                        payouts.get(addr, 0)
                        + entry_aleph * settings.credit_execution_crn_share
                    )
                elif node_id:
                    LOGGER.warning(
                        f"CRN {node_id} not found in resource_nodes, "
                        f"skipping CRN share"
                    )

        ccn_pool = total * ccn_share
        if total_ccn_score > 0 and ccn_pool > 0:
            for info in ccn_weights.values():
                share = info["score"] / total_ccn_score
                addr = info["reward_address"]
                payouts[addr] = payouts.get(addr, 0) + ccn_pool * share

        staker_pool = total * staker_share
        if total_staked > 0 and staker_pool > 0:
            for addr, staked in all_stakers.items():
                share = staked / total_staked
                payouts[addr] = payouts.get(addr, 0) + staker_pool * share

        return total, dev

    credits_total, credits_dev = apply_split(expense.get("credits", []))

    hold_total = 0
    hold_dev = 0
    if expense_type == "execution" and settings.credit_hold_rewards_enabled:
        hold_entries = expense.get("rewards", [])
        if hold_entries:
            hold_total, hold_dev = apply_split(hold_entries)

    dev_fund_total = credits_dev + hold_dev

    if expense_type == "storage":
        return credits_total, 0, 0, dev_fund_total
    else:
        return 0, credits_total, hold_total, dev_fund_total


async def prepare_credit_distribution(dbs, end_height, start_time, end_time,
                                       full_resync=False):
    """
    Distribute credit expenses using node state snapshots.

    By default, fetches published corechannel aggregate snapshots and
    advances through them. With full_resync=True, replays the full state
    machine from genesis instead.

    Returns:
        Tuple of (payouts, total_storage_aleph, total_execution_aleph,
                  total_hold_rewards_aleph, total_dev_fund_aleph).
        `total_hold_rewards_aleph` is sourced from the incentives pool, not
        from user credits, and must not be netted against user expenses.
    """
    api_server = PublishMode.get_publish_api_server()

    expenses = await fetch_credit_expenses(
        api_server, start_time, end_time,
        sender=settings.credit_expense_sender,
    )

    if not expenses:
        LOGGER.warning("No credit expenses found in the given time range")
        return {}, 0, 0, 0, 0

    if full_resync:
        web3 = get_web3()
        return await _prepare_with_state_machine(
            dbs, end_height, expenses, web3
        )
    else:
        # web3 only needed for checksum validation, not required in snapshot mode
        try:
            web3 = get_web3()
        except Exception:
            web3 = None
        return await _prepare_with_snapshots(
            api_server, start_time, end_time, expenses, web3
        )


async def _prepare_with_snapshots(api_server, start_time, end_time,
                                    expenses, web3):
    """Distribute using published corechannel aggregate snapshots."""
    snapshots = await fetch_node_snapshots(
        api_server, start_time, end_time
    )

    if not snapshots:
        raise ValueError(
            "No node status snapshots found. "
            "Use --full-resync or ensure nodestatus is running."
        )

    snapshot_heights = [s[0] for s in snapshots]

    payouts = {}
    total_storage = 0
    total_execution = 0
    total_hold_rewards = 0
    total_dev_fund = 0

    for exp_height, exp_type, expense in expenses:
        # Find the most recent snapshot at or before this expense's height
        idx = bisect.bisect_right(snapshot_heights, exp_height) - 1
        if idx < 0:
            idx = 0  # Use earliest available snapshot

        _, nodes, resource_nodes = snapshots[idx]

        s, e, h, d = _distribute_expense(
            exp_type, expense, nodes, resource_nodes, payouts, web3=web3
        )
        total_storage += s
        total_execution += e
        total_hold_rewards += h
        total_dev_fund += d

    LOGGER.info(
        f"Credit distribution (snapshots): "
        f"{total_storage:.4f} ALEPH storage, "
        f"{total_execution:.4f} ALEPH execution, "
        f"{total_hold_rewards:.4f} ALEPH hold rewards (incentives pool), "
        f"{total_dev_fund:.4f} ALEPH dev fund (not distributed), "
        f"{sum(payouts.values()):.4f} ALEPH total distributed"
    )

    return payouts, total_storage, total_execution, total_hold_rewards, total_dev_fund


async def _prepare_with_state_machine(dbs, end_height, expenses, web3):
    """Distribute by replaying the full state machine from genesis."""
    state_machine = NodesStatus()

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

    payouts = {}
    total_storage = 0
    total_execution = 0
    total_hold_rewards = 0
    total_dev_fund = 0
    expense_idx = 0
    nodes = None
    resource_nodes = None

    async for height, nodes, resource_nodes in state_machine.process(iterators):
        if height > end_height:
            break

        while expense_idx < len(expenses) and expenses[expense_idx][0] <= height:
            exp_height, exp_type, expense = expenses[expense_idx]
            s, e, h, d = _distribute_expense(
                exp_type, expense, nodes, resource_nodes, payouts, web3=web3
            )
            total_storage += s
            total_execution += e
            total_hold_rewards += h
            total_dev_fund += d
            expense_idx += 1

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
            s, e, h, d = _distribute_expense(
                exp_type, expense, nodes, resource_nodes, payouts, web3=web3
            )
            total_storage += s
            total_execution += e
            total_hold_rewards += h
            total_dev_fund += d
            expense_idx += 1

    if nodes is None:
        raise ValueError("No node state available")

    LOGGER.info(
        f"Credit distribution (full resync): "
        f"{total_storage:.4f} ALEPH storage, "
        f"{total_execution:.4f} ALEPH execution, "
        f"{total_hold_rewards:.4f} ALEPH hold rewards (incentives pool), "
        f"{total_dev_fund:.4f} ALEPH dev fund (not distributed), "
        f"{sum(payouts.values()):.4f} ALEPH total distributed"
    )

    return payouts, total_storage, total_execution, total_hold_rewards, total_dev_fund
