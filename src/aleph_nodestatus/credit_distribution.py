import asyncio
import bisect
import logging
from collections import deque

import aiohttp
from aleph.sdk.client import AlephHttpClient
from aleph.sdk.query.filters import PostFilter

from .distribution import compute_score_multiplier
from .erc20 import process_contract_history
from .ethereum import get_web3
from .messages import process_message_history
from .monitored import process_balances_history
from .settings import settings, PublishMode
from .status import NodesStatus, prepare_items
from .utils import get_reward_address

LOGGER = logging.getLogger(__name__)

CREDIT_DISTRIBUTION_POST_TYPE = "credit-rewards-distribution"


def _shares_for(expense_type):
    if expense_type == "storage":
        return dict(
            ccn_share=settings.credit_storage_ccn_share,
            staker_share=settings.credit_storage_staker_share,
            crn_share=0.0,
            dev_share=settings.credit_dev_fund_share,
        )
    return dict(
        ccn_share=settings.credit_execution_ccn_share,
        staker_share=settings.credit_execution_staker_share,
        crn_share=settings.credit_execution_crn_share,
        dev_share=settings.credit_dev_fund_share,
    )


async def get_latest_successful_credit_distribution(sender=None):
    """Find the last credit distribution that had successful transfers.

    Queries posts from both status_sender (calculation publisher) and
    distribution_recipient (distribution publisher) so either cron deployment
    can discover the most recent successful distribution regardless of which
    address signed it.
    """
    if sender is None:
        senders = [settings.status_sender, settings.distribution_recipient]
    elif isinstance(sender, (list, tuple)):
        senders = list(sender)
    else:
        senders = [sender]

    async with AlephHttpClient(
        api_server=PublishMode.get_publish_api_server()
    ) as client:
        posts = await client.get_posts(
            post_filter=PostFilter(
                types=[CREDIT_DISTRIBUTION_POST_TYPE],
                addresses=senders,
            )
        )

    current_post = None
    current_end_height = 0
    for post in posts.posts:
        if post.content.get("status") != "distribution":
            continue

        end_height = post.content.get("end_height")
        if end_height is None:
            continue

        successful = False
        for target in post.content.get("targets", []):
            if target["success"]:
                successful = True
                break

        if successful and end_height >= current_end_height:
            current_post = post
            current_end_height = end_height

    if current_post is not None:
        return current_end_height, current_post.content
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

    # AGGREGATE messages on Aleph wrap their body in two `content` layers:
    #   msg.content = {"key": "<name>", "content": {<body>}, ...}
    # The outer layer carries the aggregate name (here "corechannel") and the
    # inner layer holds the actual node/resource_node lists.
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


def _distribute_expense(
    expense_type, expense, nodes, resource_nodes, rewards,
    web3=None,
    *,
    ccn_share: float,
    staker_share: float,
    crn_share: float = 0.0,
    dev_share: float,
):
    """Distribute a single expense entry. Pure function: shares are explicit.

    Returns (storage_aleph, execution_aleph, dev_fund_aleph).
    """
    credit_price_aleph = expense.get("credit_price_aleph", 0)
    total_aleph = sum(
        credit["amount"] * credit_price_aleph
        for credit in expense.get("credits", [])
    )
    dev_fund = total_aleph * dev_share

    if expense_type == "execution":
        # Credits without a node_id, or with a node_id that isn't in the
        # current resource_nodes snapshot, drop their CRN share (the credit
        # still contributes to total_aleph and therefore to CCN/staker/dev
        # pools — only the per-CRN portion is lost). Warn in both cases so
        # the missing share is visible in logs.
        for credit in expense.get("credits", []):
            credit_aleph = credit["amount"] * credit_price_aleph
            node_id = credit.get("node_id")
            if node_id and node_id in resource_nodes:
                rnode = resource_nodes[node_id]
                addr = get_reward_address(rnode, web3)
                rewards[addr] = (
                    rewards.get(addr, 0) + credit_aleph * crn_share
                )
            elif node_id:
                LOGGER.warning(
                    f"CRN {node_id} not found in resource_nodes, "
                    f"dropping {credit_aleph * crn_share:.6f} ALEPH CRN share"
                )
            else:
                LOGGER.warning(
                    f"Credit missing node_id, dropping "
                    f"{credit_aleph * crn_share:.6f} ALEPH CRN share"
                )

    # CCN pool (score-weighted)
    ccn_weights = {}
    for node_hash, node in nodes.items():
        if node["status"] != "active":
            continue
        score = compute_score_multiplier(node["score"])
        if score > 0:
            ccn_weights[node_hash] = {
                "score": score,
                "reward_address": get_reward_address(node, web3),
            }
    total_ccn_score = sum(v["score"] for v in ccn_weights.values())
    ccn_pool = total_aleph * ccn_share
    if total_ccn_score > 0 and ccn_pool > 0:
        for info in ccn_weights.values():
            rewards[info["reward_address"]] = (
                rewards.get(info["reward_address"], 0)
                + ccn_pool * info["score"] / total_ccn_score
            )

    # Staker pool
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
            rewards[addr] = (
                rewards.get(addr, 0) + staker_pool * staked / total_staked
            )

    if expense_type == "storage":
        return total_aleph, 0, dev_fund
    else:
        return 0, total_aleph, dev_fund


async def _fetch_expense_messages(api_server, start_time, end_time, sender=None):
    """Single paginated fetch of aleph_credit_expense messages."""
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
        return await _fetch_paginated_messages(session, api_server, params)


def _parse_message(msg):
    """Return (height, expense_type, expense_dict) or (None, None, None)."""
    height = _extract_eth_height(msg)
    if height is None:
        return None, None, None
    content = msg.get("content", {}).get("content", {})
    tags = content.get("tags", [])
    expense = content.get("expense", {})
    if not expense:
        return None, None, None
    if "type_storage" in tags:
        return height, "storage", expense
    if "type_execution" in tags:
        return height, "execution", expense
    return None, None, None


def _project_expense(expense, src_key):
    """Synthesize an expense with the chosen list aliased as credits[]."""
    return {
        "credit_price_aleph": expense.get("credit_price_aleph", 0),
        "credits":            expense.get(src_key, []),
    }


def zero_totals():
    return {"storage_total_aleph": 0,
            "execution_total_aleph": 0,
            "dev_fund_total_aleph": 0}


def _validate_rewards_aggregates(expense):
    declared_count = expense.get("rewards_count")
    declared_amount = expense.get("rewards_amount")
    rewards = expense.get("rewards", [])
    if declared_count is not None and declared_count != len(rewards):
        LOGGER.warning(
            f"rewards_count mismatch: declared={declared_count}, "
            f"actual={len(rewards)}"
        )
    if declared_amount is not None:
        actual = sum(r.get("amount", 0) for r in rewards)
        if abs(actual - declared_amount) > 1:
            LOGGER.warning(
                f"rewards_amount mismatch: declared={declared_amount}, "
                f"actual={actual}"
            )


def _apply_expense_to(rewards, totals, exp_type, expense,
                      nodes, resource_nodes, web3):
    s, e, d = _distribute_expense(
        exp_type, expense, nodes, resource_nodes, rewards,
        web3=web3, **_shares_for(exp_type),
    )
    totals["storage_total_aleph"]   += s
    totals["execution_total_aleph"] += e
    totals["dev_fund_total_aleph"]  += d


async def compute_rewards(
    start_time, end_time, full_resync=False,
    include_holder_tier=False, sender=None,
    dbs=None, end_height=None, web3=None,
):
    """Pure function: produce credit-revenue and optional holder-tier rewards.

    Returns:
        {"credit_revenue": (rewards_dict, totals_dict),
         "holder_tier":    (rewards_dict, totals_dict)}
    """
    api_server = PublishMode.get_publish_api_server()
    sender = sender or settings.credit_expense_sender

    msgs = await _fetch_expense_messages(api_server, start_time, end_time, sender)
    if not msgs:
        LOGGER.warning("No credit expenses found in the given time range")
        return {
            "credit_revenue": ({}, zero_totals()),
            "holder_tier":    ({}, zero_totals()),
        }

    if web3 is None:
        try:
            web3 = get_web3()
        except Exception:
            web3 = None

    if full_resync and end_height is None:
        raise ValueError(
            "end_height is required when full_resync=True (state-machine "
            "drain depends on a known upper bound)"
        )

    if full_resync:
        result = await _compute_rewards_full_resync(
            dbs, end_height, msgs, include_holder_tier, web3
        )
    else:
        result = await _compute_rewards_snapshots(
            api_server, start_time, end_time, msgs, include_holder_tier, web3
        )

    credit_totals = result["credit_revenue"][1]
    holder_totals = result["holder_tier"][1]
    LOGGER.info(
        "compute_rewards: revenue=%.4f storage / %.4f execution / %.4f dev | "
        "holder=%.4f storage / %.4f execution / %.4f dev",
        credit_totals["storage_total_aleph"],
        credit_totals["execution_total_aleph"],
        credit_totals["dev_fund_total_aleph"],
        holder_totals["storage_total_aleph"],
        holder_totals["execution_total_aleph"],
        holder_totals["dev_fund_total_aleph"],
    )
    return result


async def _compute_rewards_snapshots(
    api_server, start_time, end_time, msgs, include_holder_tier, web3
):
    snapshots = await fetch_node_snapshots(api_server, start_time, end_time)
    if not snapshots:
        raise ValueError(
            "No node status snapshots found. "
            "Use --full-resync or ensure nodestatus is running."
        )
    snapshot_heights = [s[0] for s in snapshots]

    credit_rewards, credit_totals = {}, zero_totals()
    holder_rewards, holder_totals = {}, zero_totals()

    for msg in msgs:
        height, exp_type, expense = _parse_message(msg)
        if expense is None:
            continue

        idx = max(0, bisect.bisect_right(snapshot_heights, height) - 1)
        _, nodes, resource_nodes = snapshots[idx]

        _apply_expense_to(
            credit_rewards, credit_totals, exp_type,
            _project_expense(expense, "credits"),
            nodes, resource_nodes, web3,
        )

        if include_holder_tier and expense.get("rewards"):
            _validate_rewards_aggregates(expense)
            _apply_expense_to(
                holder_rewards, holder_totals, exp_type,
                _project_expense(expense, "rewards"),
                nodes, resource_nodes, web3,
            )

    return {
        "credit_revenue": (credit_rewards, credit_totals),
        "holder_tier":    (holder_rewards, holder_totals),
    }


async def _compute_rewards_full_resync(
    dbs, end_height, msgs, include_holder_tier, web3
):
    """Identical math as snapshot mode but driven by the state machine."""
    parsed = []
    for msg in msgs:
        height, exp_type, expense = _parse_message(msg)
        if expense is not None:
            parsed.append((height, exp_type, expense))
    parsed.sort(key=lambda x: x[0])

    state_machine = NodesStatus()
    last_seen_txs = deque([], maxlen=100)
    iterators = [
        prepare_items("balance-update",
            process_contract_history(
                settings.ethereum_token_contract,
                settings.ethereum_min_height,
                last_seen=last_seen_txs, db=dbs["erc20"], fetch_from_db=True)),
        prepare_items("balance-update",
            process_balances_history(
                settings.ethereum_min_height,
                request_count=500, db=dbs["balances"])),
        prepare_items("staking-update",
            process_message_history(
                [settings.filter_tag],
                [settings.node_post_type, "amend"],
                settings.aleph_api_server,
                yield_unconfirmed=False, request_count=5000,
                db=dbs["messages"])),
        prepare_items("score-update",
            process_message_history(
                [settings.filter_tag],
                [settings.scores_post_type],
                message_type="POST",
                addresses=settings.scores_senders,
                api_server=settings.aleph_api_server,
                request_count=1000, db=dbs["scores"])),
    ]

    credit_rewards, credit_totals = {}, zero_totals()
    holder_rewards, holder_totals = {}, zero_totals()
    idx = 0
    nodes = resource_nodes = None

    async for height, nodes, resource_nodes in state_machine.process(iterators):
        if end_height is not None and height > end_height:
            break
        while idx < len(parsed) and parsed[idx][0] <= height:
            _, exp_type, expense = parsed[idx]
            _apply_expense_to(credit_rewards, credit_totals, exp_type,
                              _project_expense(expense, "credits"),
                              nodes, resource_nodes, web3)
            if include_holder_tier and expense.get("rewards"):
                _validate_rewards_aggregates(expense)
                _apply_expense_to(holder_rewards, holder_totals, exp_type,
                                  _project_expense(expense, "rewards"),
                                  nodes, resource_nodes, web3)
            idx += 1

    # Tail: expenses with height > last state-machine yield (but <= end_height)
    # are applied against the final (nodes, resource_nodes) snapshot from the
    # loop above. The state machine only emits a yield when corechannel state
    # changes; expenses landing in the gap between the last yield and
    # end_height all share that terminal state.
    while idx < len(parsed) and nodes is not None:
        h, exp_type, expense = parsed[idx]
        if end_height is not None and h > end_height:
            idx += 1
            continue
        _apply_expense_to(credit_rewards, credit_totals, exp_type,
                          _project_expense(expense, "credits"),
                          nodes, resource_nodes, web3)
        if include_holder_tier and expense.get("rewards"):
            _validate_rewards_aggregates(expense)
            _apply_expense_to(holder_rewards, holder_totals, exp_type,
                              _project_expense(expense, "rewards"),
                              nodes, resource_nodes, web3)
        idx += 1

    if nodes is None:
        raise ValueError("No node state available")

    return {
        "credit_revenue": (credit_rewards, credit_totals),
        "holder_tier":    (holder_rewards, holder_totals),
    }


async def prepare_credit_distribution(
    dbs, end_height, start_time, end_time, full_resync=False
):
    result = await compute_rewards(
        start_time, end_time, full_resync=full_resync,
        include_holder_tier=False, dbs=dbs, end_height=end_height,
    )
    rewards, totals = result["credit_revenue"]
    return (
        rewards,
        totals["storage_total_aleph"],
        totals["execution_total_aleph"],
        totals["dev_fund_total_aleph"],
    )


def should_skip_run(last_end_height, current_height, min_interval_blocks,
                    force=False):
    """Return True if we should skip the run because the min interval
    hasn't elapsed since the last successful distribution."""
    if force:
        return False
    if not last_end_height:
        return False
    return (current_height - last_end_height) < min_interval_blocks
