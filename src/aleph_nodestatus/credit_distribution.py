import asyncio
import bisect
import logging
from collections import defaultdict, deque
from datetime import datetime
from typing import TypedDict

import aiohttp
from aleph.sdk.client import AlephHttpClient
from aleph.sdk.exceptions import MessageNotFoundError
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


class _LoggingAlephHttpClient(AlephHttpClient):
    """AlephHttpClient extended with logged cursor-paginated fetches.

    Overrides the SDK's `get_messages_cursor` and `get_posts_cursor`
    (introduced in aleph-im/aleph-sdk-python#282) so each cursor step
    logs a pre-request line, request duration, and the next cursor —
    slow API responses surface as visible progress instead of silence,
    and timeouts get a distinct error line before the traceback. The
    configured `aleph_http_page_size` is plumbed in when the caller
    doesn't pass one explicitly.
    """

    @staticmethod
    def _cursor_tag(cursor):
        return cursor[:16] if cursor else "<empty>"

    async def get_messages_cursor(self, page_size=None, cursor="", message_filter=None, **kwargs):
        if page_size is None:
            page_size = settings.aleph_http_page_size
        timeout_s = settings.aleph_http_timeout_seconds
        tag = self._cursor_tag(cursor)
        LOGGER.info(
            "Aleph messages cursor=%s requested (page_size=%d, timeout=%ds)…",
            tag, page_size, timeout_s,
        )
        t0 = asyncio.get_event_loop().time()
        try:
            resp = await super().get_messages_cursor(
                page_size=page_size, cursor=cursor,
                message_filter=message_filter, **kwargs,
            )
        except asyncio.TimeoutError:
            elapsed = asyncio.get_event_loop().time() - t0
            LOGGER.error(
                "Aleph messages cursor=%s TIMED OUT after %.1fs "
                "(aleph_http_timeout_seconds=%ds)",
                tag, elapsed, timeout_s,
            )
            raise
        elapsed = asyncio.get_event_loop().time() - t0
        nxt = resp.next_cursor or ""
        LOGGER.info(
            "Aleph messages cursor=%s → %d msgs in %.1fs (next=%s)",
            tag, len(resp.messages), elapsed,
            self._cursor_tag(nxt) if nxt else "<end>",
        )
        return resp

    async def get_posts_cursor(self, page_size=None, cursor="", post_filter=None, **kwargs):
        if page_size is None:
            page_size = settings.aleph_http_page_size
        timeout_s = settings.aleph_http_timeout_seconds
        tag = self._cursor_tag(cursor)
        LOGGER.info(
            "Aleph posts cursor=%s requested (page_size=%d, timeout=%ds)…",
            tag, page_size, timeout_s,
        )
        t0 = asyncio.get_event_loop().time()
        try:
            resp = await super().get_posts_cursor(
                page_size=page_size, cursor=cursor,
                post_filter=post_filter, **kwargs,
            )
        except asyncio.TimeoutError:
            elapsed = asyncio.get_event_loop().time() - t0
            LOGGER.error(
                "Aleph posts cursor=%s TIMED OUT after %.1fs "
                "(aleph_http_timeout_seconds=%ds)",
                tag, elapsed, timeout_s,
            )
            raise
        elapsed = asyncio.get_event_loop().time() - t0
        nxt = resp.next_cursor or ""
        LOGGER.info(
            "Aleph posts cursor=%s → %d posts in %.1fs (next=%s)",
            tag, len(resp.posts), elapsed,
            self._cursor_tag(nxt) if nxt else "<end>",
        )
        return resp


def _aleph_client(api_server):
    """Construct a `_LoggingAlephHttpClient` with an explicit total timeout.

    aiohttp's default 5-minute timeout fires before the API can answer
    some date+address-filtered AGGREGATE queries over multi-week windows;
    use the configured `aleph_http_timeout_seconds` instead so operators
    can tune it.
    """
    return _LoggingAlephHttpClient(
        api_server=api_server,
        timeout=aiohttp.ClientTimeout(total=settings.aleph_http_timeout_seconds),
    )

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

    current_content = None
    current_end_height = 0
    async with _aleph_client(PublishMode.get_publish_api_server()) as client:
        # Walk via cursor so we don't drop older distributions once the
        # post history grows past one page.
        cursor = ""
        while True:
            resp = await client.get_posts_cursor(
                post_filter=post_filter, cursor=cursor,
            )
            for post in resp.posts:
                content = post.content or {}
                if content.get("status") != "distribution":
                    continue

                end_height = content.get("end_height")
                if end_height is None:
                    continue

                if any(tgt.get("success") for tgt in content.get("targets", [])):
                    # `>=` (not `>`) so that an AMEND publishing a later
                    # post with the same end_height overrides the prior
                    # pick rather than being ignored. Posts are yielded
                    # in API-defined order, so the AMEND wins iff it is
                    # yielded after the original — true for the default
                    # sort (newest first ⇒ amend appears first and is
                    # replaced by older same-height entries until the
                    # most-recent one is reached).
                    if end_height >= current_end_height:
                        current_content = content
                        current_end_height = end_height

            cursor = resp.next_cursor or ""
            if not cursor:
                break

    if current_content is not None:
        return current_end_height, current_content
    return 0, None


async def _iter_messages_dedup(client, message_filter):
    """Yield Aleph messages via cursor pagination, deduped by item_hash.

    Cursor pagination (SDK `get_messages_cursor`, see
    `_LoggingAlephHttpClient.get_messages_cursor`) avoids the deep-page
    slowdown that made offset-based scans of multi-week aggregate
    history time out. Dedup is kept as cheap insurance against any
    duplicates returned across cursor boundaries.
    """
    seen = set()
    cursor = ""
    while True:
        resp = await client.get_messages_cursor(
            message_filter=message_filter, cursor=cursor,
        )
        for msg in resp.messages:
            h = msg.item_hash
            if h in seen:
                continue
            seen.add(h)
            yield msg.model_dump(mode="json")
        cursor = resp.next_cursor or ""
        if not cursor:
            break


def _extract_eth_height(msg):
    """Extract the earliest ETH confirmation height from a message dict."""
    height = None
    for conf in msg.get("confirmations") or []:
        if conf.get("chain") == "ETH":
            ch = conf.get("height")
            if ch is not None and (height is None or ch < height):
                height = ch
    return height


def _is_eth_confirmed(msg):
    """True iff the message has at least one ETH confirmation.

    Used as an explicit guard so that unconfirmed posts/messages never
    enter the rewards pipeline — confirmation = the data is durable on
    chain, so the run is reproducible.
    """
    for conf in msg.get("confirmations") or []:
        if conf.get("chain") == "ETH":
            return True
    return False


def _normalize_ts_to_seconds(ts):
    """Aleph mixes units and types across fields:
    - `Post.time` is a float (seconds since epoch).
    - `AlephMessage.time` is a datetime; `model_dump(mode="json")`
      serializes that as an ISO 8601 string (with trailing `Z`).
    - `expense.start_date` / `expense.end_date` are *milliseconds*.

    Magnitude check is unambiguous because 1e12 seconds is year ~33000:
    anything above 1e12 has to be ms.
    """
    if ts is None:
        return None
    if isinstance(ts, str):
        ts = datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp()
    ts = float(ts)
    if ts > 1e12:
        ts /= 1000.0
    return ts


_ORIGINAL_HEIGHT_RETRIES = 5
_ORIGINAL_HEIGHT_BACKOFFS = (1.0, 2.0, 4.0, 8.0, 16.0)

# Cross-run leak mitigation: re-fetch a 48h window before `start_time` and
# rely on `_original_height <= last_end_height` for dedup. Covers the
# typical case of an expense published just before a run but
# ETH-confirmed afterwards: it falls outside the new run's time window
# (msg.time < new_start_time) but is recovered via the lookback. Width
# tuned to comfortably exceed the publication-to-confirmation gap (minutes
# in normal operation) plus typical inter-run gap (hours).
_EXPENSE_FETCH_LOOKBACK_SECONDS = 48 * 3600


async def _resolve_original_height(client, original_item_hash, cache):
    """Return the earliest ETH-confirmation height of `original_item_hash`.

    Persists the result in `cache` so re-encountering the same original
    (across pages or multiple amendments) avoids a second API call.

    Raises after `_ORIGINAL_HEIGHT_RETRIES` consecutive transient failures:
    rewards cannot trust an amended post's height for trazabilidad, so
    aborting the run is preferable to silently using the amend's height.

    Returns None if the original exists but is not ETH-confirmed; the
    caller treats that as "drop the expense" (we will not credit work
    whose origin isn't durable on chain even if the amend is confirmed).
    """
    if original_item_hash in cache:
        return cache[original_item_hash]

    last_err = None
    for attempt in range(_ORIGINAL_HEIGHT_RETRIES):
        try:
            original = await client.get_message(item_hash=original_item_hash)
            original_dict = original.model_dump(mode="json")
            if not _is_eth_confirmed(original_dict):
                LOGGER.warning(
                    "Original message %s is not ETH-confirmed; dropping amend",
                    original_item_hash,
                )
                cache[original_item_hash] = None
                return None
            height = _extract_eth_height(original_dict)
            cache[original_item_hash] = height
            return height
        except MessageNotFoundError:
            LOGGER.warning(
                "Original message %s not found; dropping amend",
                original_item_hash,
            )
            cache[original_item_hash] = None
            return None
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            last_err = e
            if attempt < _ORIGINAL_HEIGHT_RETRIES - 1:
                backoff = _ORIGINAL_HEIGHT_BACKOFFS[attempt]
                LOGGER.warning(
                    "Fetch of original %s failed (attempt %d/%d): %s — "
                    "retrying in %.1fs",
                    original_item_hash, attempt + 1,
                    _ORIGINAL_HEIGHT_RETRIES, e, backoff,
                )
                await asyncio.sleep(backoff)
    raise RuntimeError(
        f"Could not fetch original message {original_item_hash} after "
        f"{_ORIGINAL_HEIGHT_RETRIES} attempts: {last_err}"
    )


async def _iter_posts_dedup(client, post_filter, last_end_height=None):
    """Yield Aleph posts via cursor pagination, deduped by item_hash.

    For amended posts (`original_item_hash != item_hash`), enriches the
    yielded dict with `_original_height`: the ETH-confirmation height of
    the original message (resolved via an extra `get_message` call,
    cached for the lifetime of the iterator). Posts without ETH
    confirmation, and amendments whose original is unconfirmed/missing,
    are skipped — see `_resolve_original_height`.

    Cross-run leak mitigation: when `last_end_height` is given, posts
    whose `_original_height <= last_end_height` are skipped — they were
    already visible to the previous successful run (so either applied
    then, or were the run's responsibility to handle). Posts whose
    original confirmation came AFTER that cursor are kept; they are the
    ones the previous run could not see (unconfirmed at fetch time) and
    must be recovered now. Caller widens the fetch window backwards via
    `_EXPENSE_FETCH_LOOKBACK_SECONDS` to give this rule something to
    work with.
    """
    seen = set()
    cursor = ""
    original_height_cache = {}
    while True:
        resp = await client.get_posts_cursor(
            post_filter=post_filter, cursor=cursor,
        )
        for post in resp.posts:
            h = post.item_hash
            if h in seen:
                continue
            seen.add(h)
            post_dict = post.model_dump(mode="json")

            if not _is_eth_confirmed(post_dict):
                LOGGER.debug(
                    "Skipping unconfirmed post %s", h,
                )
                continue

            original_item_hash = post_dict.get("original_item_hash")
            if original_item_hash and original_item_hash != h:
                original_height = await _resolve_original_height(
                    client, original_item_hash, original_height_cache,
                )
                if original_height is None:
                    continue
                post_dict["_original_height"] = original_height
            else:
                post_dict["_original_height"] = _extract_eth_height(post_dict)

            if (last_end_height is not None
                    and post_dict["_original_height"] is not None
                    and post_dict["_original_height"] <= last_end_height):
                LOGGER.debug(
                    "Skipping already-seen post %s "
                    "(_original_height=%d <= last_end_height=%d)",
                    h, post_dict["_original_height"], last_end_height,
                )
                continue

            yield post_dict
        cursor = resp.next_cursor or ""
        if not cursor:
            break


async def fetch_node_snapshots(
    api_server, start_time, end_time, sender=None, out_hashes=None,
):
    """
    Fetch historical corechannel aggregate snapshots for the period via
    the SDK's auto-paginated message iterator.

    Fetches from 1 day before start_time to ensure we have a snapshot
    before the first expense.

    Returns:
        List of (ts, nodes_dict, resource_nodes_dict) sorted by ts, where
        `ts` is the aggregate's `msg.time` (seconds). Lookup is by ts to
        match the consumption-time semantics used by the expense path —
        an amended aggregate could otherwise drift the snapshot away
        from its real point in time.

    `out_hashes`, when supplied, is appended with the item_hash of each
    accepted snapshot message. Used by the dry-run debug path to dump
    the source-message set actually consumed by the calculation so it
    can be diffed against other services' inputs.
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
    async with _aleph_client(api_server) as client:
        async for msg in _iter_messages_dedup(client, message_filter):
            content = msg.get("content") or {}
            if content.get("key") != "corechannel":
                continue

            if not _is_eth_confirmed(msg):
                continue

            ts = _normalize_ts_to_seconds(msg.get("time"))
            if ts is None:
                continue

            inner = content.get("content") or {}
            nodes_list          = inner.get("nodes", [])
            resource_nodes_list = inner.get("resource_nodes", [])

            nodes_dict          = {n["hash"]: n  for n  in nodes_list}
            resource_nodes_dict = {rn["hash"]: rn for rn in resource_nodes_list}

            snapshots.append((ts, nodes_dict, resource_nodes_dict))
            if out_hashes is not None:
                h = msg.get("item_hash")
                if h:
                    out_hashes.append(h)

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

        # 1) CRN share — to the CRN that hosted the workload. Gated on
        #    status == "linked": a delinked CRN still present in
        #    resource_nodes loses its share to unallocated, mirroring the
        #    wage_subsidy / storage-pool eligibility rules.
        rnode = resource_nodes.get(node_id) if node_id else None
        if rnode and rnode.get("status") == "linked":
            crn_addr = get_reward_address(rnode, web3)
            rewards[crn_addr] = rewards.get(crn_addr, 0) + crn_amount
            detailed[crn_addr]["execution_crn"] += crn_amount
        else:
            LOGGER.warning(
                f"CRN {id_key} not resolved or not linked, "
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


async def _fetch_expense_messages(
    api_server, start_time, end_time, sender=None, last_end_height=None,
):
    """Fetch `aleph_credit_expense` posts via cursor pagination.

    Pagination uses `/posts.json` (not `/messages.json`) so that amended
    expenses surface only once, in their materialized form. With
    `/messages.json` an amend and its original both show up — sorted by
    their respective publication times — which would have us applying
    the same expense twice, with contradictory metadata, to different
    snapshots.

    For amended posts the downstream parser uses the ETH confirmation
    height of the *original* (stamped here as `_original_height` by
    `_iter_posts_dedup`) for trazabilidad; the time at which rewards are
    routed comes from `expense.end_date` (the billing window), not from
    `msg.time`.

    When `last_end_height` is given the fetch widens backwards by
    `_EXPENSE_FETCH_LOOKBACK_SECONDS` and `_iter_posts_dedup` drops
    anything whose `_original_height <= last_end_height` — this is the
    cross-run recovery path for expenses whose ETH confirmation landed
    after the previous run had already closed.
    """
    if last_end_height is not None:
        fetch_start_time = start_time - _EXPENSE_FETCH_LOOKBACK_SECONDS
    else:
        fetch_start_time = start_time

    post_filter = PostFilter(
        types=["aleph_credit_expense"],
        addresses=[sender] if sender else None,
        start_date=float(fetch_start_time),
        end_date=float(end_time),
    )
    posts = []
    async with _aleph_client(api_server) as client:
        async for post in _iter_posts_dedup(
            client, post_filter, last_end_height=last_end_height,
        ):
            posts.append(post)
    return posts


def _parse_message(msg):
    """Return (ts_s, expense_type, expense_dict) or (None, None, None).

    `ts_s` is taken from `expense.end_date` (preferred) or
    `expense.start_date`, i.e. the billing window — never from the
    publication timestamp of the envelope. Two reasons: (1) amendments
    overwrite the envelope `time` to the amend's publication, which is
    days after the consumption it describes; (2) batches of stuck
    expenses get released together, so `msg.time` clusters consumption
    from different billing periods at one instant. Both cases would
    route expenses to the wrong snapshot. Falls back to `msg.time` only
    when both billing-window fields are absent (legacy/malformed payloads).

    Skips unconfirmed messages — see `_is_eth_confirmed`.

    Note: `msg` here is a Post dict (from `/posts.json`), so the payload is
    `msg["content"]` directly — not `msg["content"]["content"]` as on the
    AlephMessage envelopes returned by `/messages.json`.
    """
    if not _is_eth_confirmed(msg):
        return None, None, None
    content = msg.get("content", {})
    tags = content.get("tags", [])
    expense = content.get("expense", {})
    if not expense:
        return None, None, None
    ts_s = (
        _normalize_ts_to_seconds(expense.get("end_date"))
        or _normalize_ts_to_seconds(expense.get("start_date"))
        or _normalize_ts_to_seconds(msg.get("time"))
    )
    if ts_s is None:
        LOGGER.warning(
            "Credit expense %s has no usable timestamp "
            "(end_date/start_date/time all missing); skipping",
            msg.get("item_hash"),
        )
        return None, None, None
    if "type_storage" in tags:
        return ts_s, "storage", expense
    if "type_execution" in tags:
        return ts_s, "execution", expense
    LOGGER.warning(
        f"Credit expense at ts={ts_s} has neither type_storage nor "
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


def _validate_hold_aggregates(expense):
    """Cross-check the declared hold aggregates against `expense.hold[]`.

    Reads from `expense.stats.hold` (the canonical shape introduced in
    2026-05 by aleph-api-credit) when present, otherwise falls back to
    the legacy top-level `hold_count` / `hold_amount` keys still
    present on pre-migration messages.
    """
    stats_hold = (expense.get("stats") or {}).get("hold") or {}
    declared_count = stats_hold.get("count", expense.get("hold_count"))
    declared_amount = stats_hold.get("amount", expense.get("hold_amount"))

    hold = expense.get("hold", [])
    if declared_count is not None and declared_count != len(hold):
        LOGGER.warning(
            f"hold count mismatch: declared={declared_count}, "
            f"actual={len(hold)}"
        )
    if declared_amount is not None:
        actual = sum(h.get("amount", 0) for h in hold)
        if abs(actual - declared_amount) > 1:
            LOGGER.warning(
                f"hold amount mismatch: declared={declared_amount}, "
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
    snapshots=None, last_end_height=None,
    out_expense_hashes=None,
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

    `snapshots`, when supplied, lets the orchestrator share a single
    `fetch_node_snapshots` result with the downstream wage-subsidy step.
    Aggregate-by-date queries on the Aleph API take tens of seconds per
    page, so doing them twice in one run doubles wall-time for nothing.

    `out_expense_hashes`, when supplied, is appended with the item_hash
    of every expense post that contributed to the calculation. Used by
    the dry-run debug path to surface the exact input set for diffing
    against other services.
    """
    api_server = PublishMode.get_publish_api_server()
    sender = sender or settings.credit_expense_sender

    msgs = await _fetch_expense_messages(
        api_server, start_time, end_time, sender,
        last_end_height=last_end_height,
    )
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
            dbs, end_height, msgs, include_holder_tier, web3,
            out_expense_hashes=out_expense_hashes,
            start_time=start_time, end_time=end_time,
        )
    else:
        result = await _compute_rewards_snapshots(
            api_server, start_time, end_time, msgs, include_holder_tier, web3,
            snapshots=snapshots,
            out_expense_hashes=out_expense_hashes,
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


def _parse_expenses(msgs, out_hashes=None, window_start=None, window_end=None):
    """Parse a list of raw expense post dicts into sorted (ts_s, type, expense).

    When `window_start`/`window_end` are supplied, expenses whose
    routing timestamp (`expense.end_date`) falls outside `[window_start,
    window_end]` are dropped. This matters because
    `_fetch_expense_messages` opens its fetch window 48h backwards
    whenever `last_end_height` is set, to recover late-confirmed amends
    whose billing window still falls *inside* the current period. That
    same lookback also surfaces expenses whose billing window falls in
    the *previous* period (already settled or supposed to be); without
    this filter those get re-applied here and double-pay recipients.

    `out_hashes`, when supplied, is appended with each expense post's
    `item_hash` for every message that survives parsing and the window
    check. Used by the dry-run debug path to dump the exact expense set
    fed into the distribution math.
    """
    parsed = []
    for msg in msgs:
        ts_s, exp_type, expense = _parse_message(msg)
        if expense is None:
            continue
        if window_start is not None and ts_s < window_start:
            continue
        if window_end is not None and ts_s > window_end:
            continue
        parsed.append((ts_s, exp_type, expense))
        if out_hashes is not None:
            h = msg.get("item_hash")
            if h:
                out_hashes.append(h)
    parsed.sort(key=lambda x: x[0])
    return parsed


def _apply_expenses_to_snapshots(
    parsed, snapshots, include_holder_tier, web3,
):
    """Apply a sorted list of (ts_s, exp_type, expense) to snapshots.

    `snapshots` is a list of (ts, nodes, resource_nodes) sorted by ts;
    each expense is routed to the snapshot with the largest ts ≤ ts_s,
    i.e. the node state that was in force at the end of the expense's
    billing window. Both the corechannel-aggregate path and the
    full-resync state-machine path produce snapshots in this shape, so
    this helper is the one and only place where expense-to-snapshot
    routing happens.

    Window enforcement is upstream: `_fetch_expense_messages` already
    filters by `start_date/end_date` at the API level, so callers can
    trust that `parsed` only contains expenses inside the run window.
    """
    if not snapshots:
        raise ValueError("Cannot apply expenses with empty snapshot list")

    snapshot_times = [s[0] for s in snapshots]

    credit_rewards, credit_totals = {}, zero_totals()
    holder_rewards, holder_totals = {}, zero_totals()
    credit_detailed = _empty_detailed()
    holder_detailed = _empty_detailed()
    credit_unallocated = _empty_unallocated()
    holder_unallocated = _empty_unallocated()

    for ts_s, exp_type, expense in parsed:
        idx = bisect.bisect_right(snapshot_times, ts_s) - 1
        if idx < 0:
            LOGGER.warning(
                "Expense at ts=%.0f falls before first snapshot (ts=%.0f); "
                "skipping — snapshot range does not cover expense range",
                ts_s, snapshot_times[0],
            )
            continue
        _, nodes, resource_nodes = snapshots[idx]

        _apply_expense_to(
            credit_rewards, credit_totals, credit_detailed, credit_unallocated,
            exp_type,
            _project_expense(expense, "credits"),
            nodes, resource_nodes, web3,
        )

        if include_holder_tier and expense.get("hold"):
            _validate_hold_aggregates(expense)
            _apply_expense_to(
                holder_rewards, holder_totals, holder_detailed,
                holder_unallocated, exp_type,
                _project_expense(expense, "hold"),
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


async def _compute_rewards_snapshots(
    api_server, start_time, end_time, msgs, include_holder_tier, web3,
    snapshots=None, out_expense_hashes=None,
):
    if snapshots is None:
        snapshots = await fetch_node_snapshots(api_server, start_time, end_time)
    if not snapshots:
        raise ValueError(
            "No node status snapshots found. "
            "Use --full-resync or ensure nodestatus is running."
        )
    parsed = _parse_expenses(
        msgs,
        out_hashes=out_expense_hashes,
        window_start=start_time,
        window_end=end_time,
    )
    return _apply_expenses_to_snapshots(
        parsed, snapshots, include_holder_tier, web3,
    )


async def _compute_rewards_full_resync(
    dbs, end_height, msgs, include_holder_tier, web3,
    out_expense_hashes=None, start_time=None, end_time=None,
):
    """Same math as snapshot mode, but snapshots are reconstructed from
    base-data replay (ERC20, staking, balances, scores) instead of read
    from corechannel aggregates.

    Each state-machine yield is paired with the timestamp of its ETH
    block via `web3.eth.get_block`, cached in-memory so re-asks (and the
    `end_time` lookup) reuse the same value.
    """
    if web3 is None:
        raise ValueError(
            "web3 is required for full_resync (used to derive block "
            "timestamps for snapshot lookup)"
        )

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

    block_ts_cache = {}

    def _block_ts(height):
        if height not in block_ts_cache:
            block_ts_cache[height] = float(web3.eth.get_block(height).timestamp)
        return block_ts_cache[height]

    snapshots = []
    async for height, nodes, resource_nodes in state_machine.process(iterators):
        if end_height is not None and height > end_height:
            break
        snapshots.append((_block_ts(height), nodes, resource_nodes))

    if not snapshots:
        raise ValueError("State machine produced no snapshots")

    parsed = _parse_expenses(
        msgs,
        out_hashes=out_expense_hashes,
        window_start=start_time,
        window_end=end_time,
    )
    return _apply_expenses_to_snapshots(
        parsed, snapshots, include_holder_tier, web3,
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
