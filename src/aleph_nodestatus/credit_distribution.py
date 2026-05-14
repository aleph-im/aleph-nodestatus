import bisect
import logging
from collections import defaultdict, deque
from typing import TypedDict

from aleph.sdk.client import AlephHttpClient
from aleph.sdk.query.filters import MessageFilter, PostFilter
from aleph_message.models import MessageType

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

# Sentinel keys used in `unallocated_*_by_id` for drops that don't map to a
# real node hash. Distinct from valid node hashes so downstream tooling can
# pick them out.
UNALLOCATED_MISSING_NODE_ID = "__missing_node_id__"
UNALLOCATED_NO_ACTIVE_CCNS  = "__no_active_ccns__"


class _Shares(TypedDict):
    """Reward-pool fractions for a single expense type. Splatted into
    `_distribute_expense` as kwargs; the TypedDict gives the call site a
    checked shape without forcing a refactor of the splat pattern."""
    ccn_share:    float
    staker_share: float
    crn_share:    float
    dev_share:    float


def _shares_for(expense_type: str) -> _Shares:
    if expense_type == "storage":
        return _Shares(
            ccn_share=settings.credit_storage_ccn_share,
            staker_share=settings.credit_storage_staker_share,
            crn_share=0.0,
            dev_share=settings.credit_dev_fund_share,
        )
    return _Shares(
        ccn_share=settings.credit_execution_ccn_share,
        staker_share=settings.credit_execution_staker_share,
        crn_share=settings.credit_execution_crn_share,
        dev_share=settings.credit_dev_fund_share,
    )


async def get_latest_successful_credit_distribution(sender=None):
    """Find the last credit distribution that had successful transfers.

    Treats a post as 'successful' if ANY batch in `targets[]` succeeded.
    Failed batches in such posts must be manually retransferred and the
    post AMENDed out-of-band; the cursor advances normally regardless.
    The run-end summary in process_credit_distribution surfaces failed
    batches in stdout so operators can find them without scrolling.

    In the documented two-instance deployment, only `distribution_recipient`
    publishes status='distribution'; `status_sender` is queried defensively
    to handle single-instance / dev setups.
    """
    if sender is None:
        senders = [settings.status_sender, settings.distribution_recipient]
    elif isinstance(sender, (list, tuple)):
        senders = list(sender)
    else:
        senders = [sender]

    post_filter = PostFilter(
        types=[CREDIT_DISTRIBUTION_POST_TYPE],
        addresses=senders,
    )

    current_post = None
    current_end_height = 0
    async with AlephHttpClient(
        api_server=PublishMode.get_publish_api_server()
    ) as client:
        # Iterator auto-paginates so we don't drop older distributions
        # once the post history grows past one page.
        async for post in client.get_posts_iterator(post_filter=post_filter):
            content = post.content or {}
            if content.get("status") != "distribution":
                continue

            end_height = content.get("end_height")
            if end_height is None:
                continue

            if any(t.get("success") for t in content.get("targets", [])):
                # `>=` (not `>`) so that an AMEND publishing a later
                # post with the same end_height overrides the prior
                # pick rather than being ignored. The iterator yields
                # posts in API-defined order, so the AMEND wins iff
                # it is yielded after the original — true for the
                # default sort (newest first ⇒ amend appears first
                # and is replaced by older same-height entries until
                # the most-recent one is reached).
                if end_height >= current_end_height:
                    current_post = post
                    current_end_height = end_height

    if current_post is not None:
        return current_end_height, current_post.content
    return 0, None


async def _iter_messages_dedup(client, message_filter):
    """Wrap the SDK's async message iterator with item_hash dedup.

    The SDK's `get_messages_iterator` auto-paginates: in 1.4.0 it walks
    pages by offset and the docstring warns it "might return duplicates";
    in 2.3.2+ (PR #282 of aleph-sdk-python) it walks via cursor and the
    duplicates disappear. Dedup is cheap insurance across both versions.
    """
    seen = set()
    async for msg in client.get_messages_iterator(message_filter=message_filter):
        h = msg.item_hash
        if h in seen:
            continue
        seen.add(h)
        # Serialise to a dict so downstream parsers consume the same
        # shape regardless of pydantic version under the SDK.
        yield msg.dict()


def _extract_eth_height(msg):
    """Extract the earliest ETH confirmation height from a message dict."""
    height = None
    for conf in msg.get("confirmations") or []:
        if conf.get("chain") == "ETH":
            ch = conf.get("height")
            if ch is not None and (height is None or ch < height):
                height = ch
    return height


async def fetch_node_snapshots(api_server, start_time, end_time, sender=None):
    """
    Fetch historical corechannel aggregate snapshots for the period via
    the SDK's auto-paginated message iterator.

    Fetches from 1 day before start_time to ensure we have a snapshot
    before the first expense.

    Returns:
        List of (height, nodes_dict, resource_nodes_dict) sorted by height.
    """
    if sender is None:
        sender = settings.status_sender

    snapshots = []
    # SDK 1.4.0 rejects bare `int` in start_date/end_date; cast to float.
    message_filter = MessageFilter(
        message_types=[MessageType.aggregate],
        addresses=[sender],
        start_date=float(start_time - 86400),
        end_date=float(end_time),
    )

    # AGGREGATE messages on Aleph wrap their body in two `content` layers:
    #   msg["content"] = {"key": "<name>", "content": {<body>}, ...}
    # The outer layer carries the aggregate name (here "corechannel") and the
    # inner layer holds the actual node/resource_node lists.
    async with AlephHttpClient(api_server=api_server) as client:
        async for msg in _iter_messages_dedup(client, message_filter):
            content = msg.get("content") or {}
            if content.get("key") != "corechannel":
                continue

            height = _extract_eth_height(msg)
            if height is None:
                continue

            inner = content.get("content") or {}
            nodes_list          = inner.get("nodes", [])
            resource_nodes_list = inner.get("resource_nodes", [])

            nodes_dict          = {n["hash"]: n  for n  in nodes_list}
            resource_nodes_dict = {rn["hash"]: rn for rn in resource_nodes_list}

            snapshots.append((height, nodes_dict, resource_nodes_dict))

    snapshots.sort(key=lambda x: x[0])
    LOGGER.info(f"Fetched {len(snapshots)} node status snapshots")
    return snapshots


def _build_crn_to_ccn_map(nodes):
    """Reverse-index: CRN hash → CCN hash, derived from each CCN's
    `resource_nodes` list."""
    m = {}
    for ccn_hash, ccn in nodes.items():
        for crn_hash in ccn.get("resource_nodes", []):
            m[crn_hash] = ccn_hash
    return m


def _distribute_expense(
    expense_type, expense, nodes, resource_nodes, rewards,
    detailed=None,
    unallocated=None,
    web3=None,
    *,
    ccn_share: float,
    staker_share: float,
    crn_share: float = 0.0,
    dev_share: float,
):
    """Distribute a single expense entry. Pure function: shares are explicit.

    Returns (storage_aleph, execution_aleph, dev_fund_aleph).
    Side effects (in-place mutation):
      - `rewards`: addr → total_amount, accumulated.
      - `detailed` (optional): addr → {component_key: amount}.
      - `unallocated` (optional): three buckets ("crn"/"ccn"/"staker"),
        each keyed by the most specific id available (CRN node_id, CCN
        hash, or a sentinel). Accumulates shares the formula assigned to
        a pool but couldn't pay out.

    Distribution rules:
      execution credits — per-credit attribution of all three pools:
        - CRN share → the CRN identified by credit.node_id.
        - CCN share → the CCN linked to that CRN (via the CCN's
          `resource_nodes` list).
        - Staker share → the linked CCN's stakers, proportional to their
          stake on THAT CCN.
        Missing/orphan/inactive links push the corresponding share to
        `unallocated`.

      storage credits — pool-level attribution:
        - CCN share → split across active CCNs by score multiplier.
        - Staker share → sliced per-CCN by the same score weight; within
          each CCN, distributed to its stakers proportional to their
          stake on that CCN (NOT the global staker network).
    """
    if detailed is None:
        detailed = defaultdict(lambda: defaultdict(float))
    if unallocated is None:
        unallocated = _empty_unallocated()

    credit_price_aleph = expense.get("credit_price_aleph", 0)
    total_aleph = sum(
        credit["amount"] * credit_price_aleph
        for credit in expense.get("credits", [])
    )
    dev_fund = total_aleph * dev_share

    if expense_type == "execution":
        _distribute_execution_credits(
            expense, credit_price_aleph, nodes, resource_nodes,
            rewards, detailed, unallocated, web3,
            ccn_share=ccn_share, staker_share=staker_share,
            crn_share=crn_share,
        )
        return 0, total_aleph, dev_fund

    _distribute_storage_pools(
        total_aleph, nodes, rewards, detailed, unallocated, web3,
        ccn_share=ccn_share, staker_share=staker_share,
    )
    return total_aleph, 0, dev_fund


def _distribute_execution_credits(
    expense, credit_price_aleph, nodes, resource_nodes,
    rewards, detailed, unallocated, web3,
    *, ccn_share, staker_share, crn_share,
):
    """Per-credit execution attribution: CRN → linked CCN → linked-CCN
    stakers. Each missing link drops the relevant share to `unallocated`."""
    crn_to_ccn = _build_crn_to_ccn_map(nodes)

    for credit in expense.get("credits", []):
        credit_aleph  = credit["amount"] * credit_price_aleph
        node_id       = credit.get("node_id")
        crn_amount    = credit_aleph * crn_share
        ccn_amount    = credit_aleph * ccn_share
        staker_amount = credit_aleph * staker_share
        id_key        = node_id or UNALLOCATED_MISSING_NODE_ID

        # 1) CRN share — to the CRN that hosted the workload.
        if node_id and node_id in resource_nodes:
            rnode = resource_nodes[node_id]
            crn_addr = get_reward_address(rnode, web3)
            rewards[crn_addr] = rewards.get(crn_addr, 0) + crn_amount
            detailed[crn_addr]["execution_crn"] += crn_amount
        else:
            LOGGER.warning(
                f"CRN {id_key} not resolved, "
                f"dropping {crn_amount:.6f} ALEPH CRN share"
            )
            if crn_amount > 0:
                unallocated["crn"][id_key] += crn_amount

        # 2) CCN + staker shares — to the CCN linked to that CRN and its
        #    stakers, respectively. Both are gated on the linked CCN being
        #    findable AND active; otherwise the entire 15% + 20% goes to
        #    unallocated under the same id_key.
        linked_ccn_hash = crn_to_ccn.get(node_id) if node_id else None
        linked_ccn = nodes.get(linked_ccn_hash) if linked_ccn_hash else None
        if linked_ccn and linked_ccn.get("status") == "active":
            # CCN share
            ccn_addr = get_reward_address(linked_ccn, web3)
            rewards[ccn_addr] = rewards.get(ccn_addr, 0) + ccn_amount
            detailed[ccn_addr]["execution_ccn"] += ccn_amount

            # Staker share — only stakers OF THIS CCN, weighted by their
            # stake on this CCN. Stake amounts on other CCNs are ignored.
            stakers = linked_ccn.get("stakers", {})
            total_stake = sum(stakers.values())
            if total_stake > 0:
                for staker_addr, stake in stakers.items():
                    share = staker_amount * stake / total_stake
                    rewards[staker_addr] = rewards.get(staker_addr, 0) + share
                    detailed[staker_addr]["execution_staker"] += share
            elif staker_amount > 0:
                LOGGER.warning(
                    f"Linked CCN {linked_ccn_hash} has no stakers, "
                    f"dropping {staker_amount:.6f} ALEPH staker share"
                )
                unallocated["staker"][linked_ccn_hash] += staker_amount
        else:
            LOGGER.warning(
                f"Linked CCN for {id_key} not found/inactive, "
                f"dropping {ccn_amount + staker_amount:.6f} ALEPH "
                f"(CCN + staker shares)"
            )
            if ccn_amount > 0:
                unallocated["ccn"][id_key] += ccn_amount
            if staker_amount > 0:
                unallocated["staker"][id_key] += staker_amount


def _distribute_storage_pools(
    total_aleph, nodes, rewards, detailed, unallocated, web3,
    *, ccn_share, staker_share,
):
    """Storage-only distribution:
        - CCN pool: split across active CCNs weighted by score multiplier.
        - Staker pool: sliced per-CCN by the same score weight, then split
          among that CCN's stakers proportional to their stake on it.
    A CCN with no stakers loses its staker sub-pool to `unallocated`."""
    ccn_weights = {}
    for ccn_hash, node in nodes.items():
        if node["status"] != "active":
            continue
        score = compute_score_multiplier(node["score"])
        if score > 0:
            ccn_weights[ccn_hash] = {
                "score":          score,
                "reward_address": get_reward_address(node, web3),
                "stakers":        node.get("stakers", {}),
            }

    total_ccn_score = sum(v["score"] for v in ccn_weights.values())
    ccn_pool    = total_aleph * ccn_share
    staker_pool = total_aleph * staker_share

    if total_ccn_score == 0:
        # No active CCNs with score > 0 — both pools are unrecoverable.
        if ccn_pool > 0:
            unallocated["ccn"][UNALLOCATED_NO_ACTIVE_CCNS] += ccn_pool
        if staker_pool > 0:
            unallocated["staker"][UNALLOCATED_NO_ACTIVE_CCNS] += staker_pool
        return

    for ccn_hash, info in ccn_weights.items():
        weight_ratio = info["score"] / total_ccn_score

        # CCN reward (unchanged: score-weighted slice of the CCN pool).
        if ccn_pool > 0:
            ccn_amount = ccn_pool * weight_ratio
            ccn_addr   = info["reward_address"]
            rewards[ccn_addr] = rewards.get(ccn_addr, 0) + ccn_amount
            detailed[ccn_addr]["storage_ccn"] += ccn_amount

        # Per-CCN staker sub-pool. Stakers of this CCN share it
        # proportional to their stake on THIS CCN. A staker on multiple
        # CCNs accumulates one share per CCN; cross-CCN aggregation no
        # longer happens.
        if staker_pool > 0:
            ccn_staker_pool = staker_pool * weight_ratio
            stakers         = info["stakers"]
            total_stake     = sum(stakers.values())
            if total_stake > 0:
                for staker_addr, stake in stakers.items():
                    share = ccn_staker_pool * stake / total_stake
                    rewards[staker_addr] = rewards.get(staker_addr, 0) + share
                    detailed[staker_addr]["storage_staker"] += share
            elif ccn_staker_pool > 0:
                unallocated["staker"][ccn_hash] += ccn_staker_pool


async def _fetch_expense_messages(api_server, start_time, end_time, sender=None):
    """Fetch aleph_credit_expense POST messages via the SDK iterator.

    Returns a list of message dicts. The shape mirrors the raw API
    response, so the downstream parser (`_parse_message`) consumes
    the same keys as before the SDK migration.
    """
    # SDK 1.4.0 rejects bare `int` in start_date/end_date; cast to float.
    message_filter = MessageFilter(
        message_types=[MessageType.post],
        content_types=["aleph_credit_expense"],
        addresses=[sender] if sender else None,
        start_date=float(start_time),
        end_date=float(end_time),
    )
    messages = []
    async with AlephHttpClient(api_server=api_server) as client:
        async for msg in _iter_messages_dedup(client, message_filter):
            messages.append(msg)
    return messages


def _parse_message(msg):
    """Return (height, expense_type, expense_dict) or (None, None, None).

    Logs a WARNING when an expense payload is present but carries neither
    `type_storage` nor `type_execution` — these messages are silently
    skipped so a missed tag at the indexer side surfaces during debugging.
    """
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
    LOGGER.warning(
        f"Credit expense at height {height} has neither type_storage nor "
        f"type_execution tag (tags={tags}); skipping"
    )
    return None, None, None


def _project_expense(expense, src_key):
    """Synthesize an expense with the chosen list aliased as credits[]."""
    return {
        "credit_price_aleph": expense.get("credit_price_aleph", 0),
        "credits":            expense.get(src_key, []),
    }


def zero_totals():
    """Initial accumulator for credit_revenue / holder_tier totals.

    Unallocated tracking — three buckets for ALEPH the formula computed
    but couldn't pay out. Keys in the `*_by_id` dicts come from the most
    specific identifier available at the drop site, and that depends on
    *which* link failed:

      - `unallocated_crn_by_node_id` — always keyed by CRN node_id (or
        UNALLOCATED_MISSING_NODE_ID for credits that arrived without
        one). The dropped share is the CRN slice of that credit.

      - `unallocated_ccn_by_id` — keyed by the CRN node_id when the
        linked CCN can't be found at all (no reverse-link or sentinel
        for missing node_id), or by UNALLOCATED_NO_ACTIVE_CCNS for the
        storage path when no CCN is eligible. Never keyed by CCN hash
        directly: if we found the CCN hash, we could pay it.

      - `unallocated_staker_by_id` — DUAL-KEYED depending on failure:
            * CRN node_id (or UNALLOCATED_MISSING_NODE_ID) when the
              linked CCN couldn't be resolved at all — same key as the
              corresponding ccn drop entry.
            * CCN hash when the linked CCN was found and active but has
              zero stakers (execution path), OR when an otherwise-paid
              CCN has no stakers (storage path).
            * UNALLOCATED_NO_ACTIVE_CCNS for the storage path when no
              CCN is eligible at all.

    Operators reading the audit post: an entry in `unallocated_staker_by_id`
    that ALSO appears in `unallocated_ccn_by_id` is a CRN-side miss
    (linked CCN unresolved); an entry that DOESN'T is a CCN-side miss
    (linked CCN resolved but has no stakers).
    """
    return {"storage_total_aleph":          0,
            "execution_total_aleph":        0,
            "dev_fund_total_aleph":         0,
            "unallocated_aleph":            0,
            "unallocated_crn_aleph":        0,
            "unallocated_crn_by_node_id":   {},
            "unallocated_ccn_aleph":        0,
            "unallocated_ccn_by_id":        {},
            "unallocated_staker_aleph":     0,
            "unallocated_staker_by_id":     {}}


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


def _apply_expense_to(rewards, totals, detailed, unallocated, exp_type, expense,
                      nodes, resource_nodes, web3):
    s, e, d = _distribute_expense(
        exp_type, expense, nodes, resource_nodes, rewards,
        detailed=detailed, unallocated=unallocated,
        web3=web3, **_shares_for(exp_type),
    )
    totals["storage_total_aleph"]   += s
    totals["execution_total_aleph"] += e
    totals["dev_fund_total_aleph"]  += d


def _empty_detailed():
    """Defaultdict shape that _distribute_expense / _apply_expense_to mutate."""
    return defaultdict(lambda: defaultdict(float))


def _empty_unallocated():
    """Three buckets keyed by which share kind was dropped: 'crn', 'ccn',
    'staker'. Each is a defaultdict[float] indexed by the most specific
    identifier available (CRN node_id, CCN hash, or a sentinel)."""
    return {
        "crn":    defaultdict(float),
        "ccn":    defaultdict(float),
        "staker": defaultdict(float),
    }


def _fold_unallocated(totals, unallocated):
    """Stamp the three per-id unallocated dicts into `totals` for the audit
    post. Idempotent: callers may invoke at any time."""
    crn_total    = sum(unallocated["crn"].values())
    ccn_total    = sum(unallocated["ccn"].values())
    staker_total = sum(unallocated["staker"].values())
    totals["unallocated_crn_aleph"]      = crn_total
    totals["unallocated_crn_by_node_id"] = dict(unallocated["crn"])
    totals["unallocated_ccn_aleph"]      = ccn_total
    totals["unallocated_ccn_by_id"]      = dict(unallocated["ccn"])
    totals["unallocated_staker_aleph"]   = staker_total
    totals["unallocated_staker_by_id"]   = dict(unallocated["staker"])
    totals["unallocated_aleph"]          = crn_total + ccn_total + staker_total


def _detailed_to_plain(detailed):
    """Convert a defaultdict-of-defaultdict into a plain dict-of-dict so it
    serialises cleanly and round-trips through JSON."""
    return {addr: dict(comps) for addr, comps in detailed.items()}


async def compute_rewards(
    start_time, end_time, full_resync=False,
    include_holder_tier=False, sender=None,
    dbs=None, end_height=None, web3=None,
):
    """Pure function: produce credit-revenue and optional holder-tier rewards.

    Returns:
        {"credit_revenue": (rewards_dict, totals_dict),
         "holder_tier":    (rewards_dict, totals_dict),
         "detailed": {"credit_revenue": {addr: {component: amount}},
                      "holder_tier":    {addr: {component: amount}}}}

    The `detailed` key carries per-account, per-component (storage_ccn,
    storage_staker, execution_ccn, execution_crn, execution_staker) shares
    so the audit post can attribute every ALEPH to its source pool.
    """
    api_server = PublishMode.get_publish_api_server()
    sender = sender or settings.credit_expense_sender

    msgs = await _fetch_expense_messages(api_server, start_time, end_time, sender)
    if not msgs:
        LOGGER.warning("No credit expenses found in the given time range")
        return {
            "credit_revenue": ({}, zero_totals()),
            "holder_tier":    ({}, zero_totals()),
            "detailed": {"credit_revenue": {}, "holder_tier": {}},
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
    credit_detailed = _empty_detailed()
    holder_detailed = _empty_detailed()
    credit_unallocated = _empty_unallocated()
    holder_unallocated = _empty_unallocated()

    for msg in msgs:
        height, exp_type, expense = _parse_message(msg)
        if expense is None:
            continue

        idx = max(0, bisect.bisect_right(snapshot_heights, height) - 1)
        _, nodes, resource_nodes = snapshots[idx]

        _apply_expense_to(
            credit_rewards, credit_totals, credit_detailed, credit_unallocated,
            exp_type,
            _project_expense(expense, "credits"),
            nodes, resource_nodes, web3,
        )

        if include_holder_tier and expense.get("rewards"):
            _validate_rewards_aggregates(expense)
            _apply_expense_to(
                holder_rewards, holder_totals, holder_detailed,
                holder_unallocated, exp_type,
                _project_expense(expense, "rewards"),
                nodes, resource_nodes, web3,
            )

    _fold_unallocated(credit_totals, credit_unallocated)
    _fold_unallocated(holder_totals, holder_unallocated)

    return {
        "credit_revenue": (credit_rewards, credit_totals),
        "holder_tier":    (holder_rewards, holder_totals),
        "detailed": {
            "credit_revenue": _detailed_to_plain(credit_detailed),
            "holder_tier":    _detailed_to_plain(holder_detailed),
        },
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
    credit_detailed = _empty_detailed()
    holder_detailed = _empty_detailed()
    credit_unallocated = _empty_unallocated()
    holder_unallocated = _empty_unallocated()
    idx = 0
    nodes = resource_nodes = None

    async for height, nodes, resource_nodes in state_machine.process(iterators):
        if end_height is not None and height > end_height:
            break
        while idx < len(parsed) and parsed[idx][0] <= height:
            _, exp_type, expense = parsed[idx]
            _apply_expense_to(credit_rewards, credit_totals, credit_detailed,
                              credit_unallocated, exp_type,
                              _project_expense(expense, "credits"),
                              nodes, resource_nodes, web3)
            if include_holder_tier and expense.get("rewards"):
                _validate_rewards_aggregates(expense)
                _apply_expense_to(holder_rewards, holder_totals,
                                  holder_detailed, holder_unallocated, exp_type,
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
            # Beyond the run's upper bound — skip rather than apply
            # against the stale terminal snapshot. `parsed` may still
            # contain even later items, so advance idx and keep
            # scanning instead of breaking out of the loop.
            idx += 1
            continue
        _apply_expense_to(credit_rewards, credit_totals, credit_detailed,
                          credit_unallocated, exp_type,
                          _project_expense(expense, "credits"),
                          nodes, resource_nodes, web3)
        if include_holder_tier and expense.get("rewards"):
            _validate_rewards_aggregates(expense)
            _apply_expense_to(holder_rewards, holder_totals, holder_detailed,
                              holder_unallocated, exp_type,
                              _project_expense(expense, "rewards"),
                              nodes, resource_nodes, web3)
        idx += 1

    if nodes is None:
        raise ValueError("No node state available")

    _fold_unallocated(credit_totals, credit_unallocated)
    _fold_unallocated(holder_totals, holder_unallocated)

    return {
        "credit_revenue": (credit_rewards, credit_totals),
        "holder_tier":    (holder_rewards, holder_totals),
        "detailed": {
            "credit_revenue": _detailed_to_plain(credit_detailed),
            "holder_tier":    _detailed_to_plain(holder_detailed),
        },
    }


def should_skip_run(last_end_height, current_height, min_interval_blocks,
                    force=False):
    """Return True if we should skip the run because the min interval
    hasn't elapsed since the last successful distribution."""
    if force:
        return False
    if not last_end_height:
        return False
    return (current_height - last_end_height) < min_interval_blocks
