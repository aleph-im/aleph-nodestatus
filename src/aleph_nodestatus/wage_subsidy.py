"""Linear-decay wage subsidy for the 6-month tokenomics transition.

The curve is W0·(1 - t/T) ALEPH per month, where t is months since
settings.wage_start_date and T = settings.wage_duration_months.
"""

import bisect
import logging
from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Tuple

from .distribution import compute_score_multiplier
from .settings import settings
from .utils import get_reward_address

LOGGER = logging.getLogger(__name__)

MONTH_SECONDS = 30 * 86400
DAY_SECONDS   = 86400


def wage_integral(t: float) -> float:
    """Cumulative ALEPH paid from t=0 to t months (clamped to [0, T])."""
    T = settings.wage_duration_months
    W0 = settings.wage_initial_monthly_aleph
    if t <= 0:
        return 0.0
    if t >= T:
        return W0 * T / 2.0
    return W0 * (t - t * t / (2.0 * T))


def parse_wage_start() -> float:
    return datetime.fromisoformat(
        settings.wage_start_date.replace("Z", "+00:00")
    ).timestamp()


def months_since_start(unix_ts: float) -> float:
    return (unix_ts - parse_wage_start()) / MONTH_SECONDS


def compute_period_subsidy(start_time: float, end_time: float) -> float:
    """Total ALEPH owed as wage subsidy over [start_time, end_time]."""
    if end_time <= start_time:
        raise ValueError(
            f"end_time ({end_time}) must be > start_time ({start_time})"
        )
    T = settings.wage_duration_months
    t1 = max(0.0, months_since_start(start_time))
    t2 = min(float(T), months_since_start(end_time))
    if t2 <= t1:
        return 0.0
    return wage_integral(t2) - wage_integral(t1)


def split_subsidy(
    period_subsidy: float,
    nodes: dict,
    resource_nodes: dict,
    web3=None,
    accumulator=None,
) -> Tuple[Dict[str, float], float, Dict[str, Dict[str, float]]]:
    """Split a period's wage subsidy across CCN / CRN / staker pools.

    Returns (rewards_by_address, unallocated_aleph, detailed_by_address).
    `detailed_by_address` maps address → {"ccn"|"crn"|"staker": amount} so
    consumers can attribute each address's wage payout to its role(s).
    Each pool with zero eligible recipients contributes to unallocated.
    """
    if period_subsidy <= 0:
        return {}, 0.0, {}

    ccn_pool    = period_subsidy * settings.wage_ccn_share
    crn_pool    = period_subsidy * settings.wage_crn_share
    staker_pool = period_subsidy * settings.wage_staker_share

    rewards: Dict[str, float] = {}
    detailed: Dict[str, Dict[str, float]] = defaultdict(
        lambda: defaultdict(float)
    )
    unallocated = 0.0

    ccn_weights = []
    for node in nodes.values():
        if node["status"] != "active":
            continue
        score = compute_score_multiplier(node["score"])
        if score > 0:
            addr = get_reward_address(node, web3)
            if addr is None:
                LOGGER.warning(
                    "CCN %s has no valid reward address, excluding it "
                    "from the wage subsidy", node.get("hash"),
                )
                continue
            ccn_weights.append((addr, score))
    total_ccn = sum(s for _, s in ccn_weights)
    if total_ccn > 0:
        for addr, s in ccn_weights:
            share = ccn_pool * s / total_ccn
            rewards[addr] = rewards.get(addr, 0.0) + share
            detailed[addr]["ccn"] += share
    else:
        unallocated += ccn_pool

    crn_weights = []
    for rnode_hash, rnode in resource_nodes.items():
        if rnode["status"] != "linked":
            continue
        score = compute_score_multiplier(rnode["score"])
        if score > 0:
            addr = get_reward_address(rnode, web3)
            if addr is None:
                LOGGER.warning(
                    "CRN %s has no valid reward address, excluding it "
                    "from the wage subsidy", rnode_hash,
                )
                continue
            inactive_since = rnode.get("inactive_since")
            crn_weights.append((rnode_hash, addr, score, inactive_since))
    total_crn = sum(s for _, _, s, _ in crn_weights)
    if total_crn > 0:
        for rnode_hash, addr, s, inactive_since in crn_weights:
            share = crn_pool * s / total_crn
            rewards[addr] = rewards.get(addr, 0.0) + share
            detailed[addr]["crn"] += share
            if accumulator is not None:
                accumulator.add(
                    "wage_subsidy", rnode_hash, addr, share,
                    inactive_at_expense=inactive_since is not None,
                    inactive_since=inactive_since,
                )
    else:
        unallocated += crn_pool

    all_stakers: Dict[str, float] = {}
    for node in nodes.values():
        if node["status"] != "active":
            continue
        for addr, amt in node["stakers"].items():
            all_stakers[addr] = all_stakers.get(addr, 0.0) + amt
    total_stake = sum(all_stakers.values())
    if total_stake > 0:
        for addr, amt in all_stakers.items():
            share = staker_pool * amt / total_stake
            rewards[addr] = rewards.get(addr, 0.0) + share
            detailed[addr]["staker"] += share
    else:
        unallocated += staker_pool

    return rewards, unallocated, {a: dict(d) for a, d in detailed.items()}


def compute_subsidy(
    start_time: float,
    end_time: float,
    nodes: dict,
    resource_nodes: dict,
    web3=None,
) -> Tuple[Dict[str, float], dict, Dict[str, Dict[str, float]]]:
    """Compute the wage subsidy for [start_time, end_time] and split it.

    Returns (rewards_by_address, totals, detailed_by_address) where:
      - totals contains period_total_aleph, unallocated_aleph,
        start_t_months, end_t_months, split={ccn, crn, stakers}.
      - detailed_by_address maps address → {"ccn"|"crn"|"staker": amount}.
    """
    period_total = compute_period_subsidy(start_time, end_time)
    rewards, unallocated, detailed = split_subsidy(
        period_total, nodes, resource_nodes, web3,
    )

    totals = {
        "start_t_months":     months_since_start(start_time),
        "end_t_months":       months_since_start(end_time),
        "period_total_aleph": period_total,
        "unallocated_aleph":  unallocated,
        # Keys here mirror the per-address `detailed[addr]` component
        # keys ("ccn"/"crn"/"staker") so operators don't have to
        # mentally translate between the two views in the audit post.
        "split": {
            "ccn":    period_total * settings.wage_ccn_share,
            "crn":    period_total * settings.wage_crn_share,
            "staker": period_total * settings.wage_staker_share,
        },
    }
    return rewards, totals, detailed


def compute_subsidy_daily(
    start_time: float,
    end_time: float,
    snapshots: List[Tuple[float, dict, dict]],
    web3=None,
    accumulator=None,
) -> Tuple[Dict[str, float], dict, Dict[str, Dict[str, float]]]:
    """Per-UTC-day variant of `compute_subsidy` mirroring aleph-api-credit.

    Splits [start_time, end_time] into UTC day buckets. For each bucket
    (clipped to the period at the edges), the wage subsidy is computed
    with the snapshot most recently observed at or before that bucket's
    end. The integral is additive, so summing per-day totals reproduces
    `compute_period_subsidy(start_time, end_time)` exactly; only the
    *attribution* differs from the single-snapshot path because each day
    uses its own network composition.

    `snapshots` is a list of (ts_seconds, nodes_dict, resource_nodes_dict)
    sorted by ts ascending — the shape produced by
    `credit_distribution.fetch_node_snapshots`.

    If `snapshots` is empty the entire period total is returned as
    unallocated, matching the no-snapshot branch in commands.py.
    """
    if end_time <= start_time:
        raise ValueError(
            f"end_time ({end_time}) must be > start_time ({start_time})"
        )

    rewards: Dict[str, float] = {}
    detailed: Dict[str, Dict[str, float]] = defaultdict(
        lambda: defaultdict(float)
    )
    total_period = 0.0
    total_unallocated = 0.0

    if not snapshots:
        period_total = compute_period_subsidy(start_time, end_time)
        totals = {
            "start_t_months":     months_since_start(start_time),
            "end_t_months":       months_since_start(end_time),
            "period_total_aleph": period_total,
            "unallocated_aleph":  period_total,
            "split": {
                "ccn":    period_total * settings.wage_ccn_share,
                "crn":    period_total * settings.wage_crn_share,
                "staker": period_total * settings.wage_staker_share,
            },
        }
        return {}, totals, {}

    snap_times = [s[0] for s in snapshots]
    last_snap_idx = len(snapshots) - 1

    # Walk UTC-day buckets [day_start, day_start + 86400) covering the
    # period. Edge days get clipped to [start_time, end_time]; clipping
    # at the integral boundary keeps `sum(per-day total) == period total`.
    day_start = (int(start_time) // DAY_SECONDS) * DAY_SECONDS
    while day_start < end_time:
        day_end = day_start + DAY_SECONDS
        win_start = max(day_start, start_time)
        win_end   = min(day_end,   end_time)
        day_start = day_end
        if win_end <= win_start:
            continue

        # Snapshot for this day: latest snapshot at or before the window
        # end (matches api-credit's `findSnapshotAtOrBeforeByTs(snaps, t1Ms)`).
        # Fall back to the last available snapshot when the period starts
        # before any snapshot exists — same semantics as api-credit's
        # `?? snapshots[snapshots.length - 1]`.
        idx = bisect.bisect_right(snap_times, win_end) - 1
        if idx < 0:
            idx = last_snap_idx
        _, nodes, resource_nodes = snapshots[idx]

        day_total = compute_period_subsidy(win_start, win_end)
        day_rewards, day_unalloc, day_detailed = split_subsidy(
            day_total, nodes, resource_nodes, web3,
            accumulator=accumulator,
        )

        total_period += day_total
        total_unallocated += day_unalloc
        for addr, amt in day_rewards.items():
            rewards[addr] = rewards.get(addr, 0.0) + amt
        for addr, comps in day_detailed.items():
            for k, v in comps.items():
                detailed[addr][k] += v

    totals = {
        "start_t_months":     months_since_start(start_time),
        "end_t_months":       months_since_start(end_time),
        "period_total_aleph": total_period,
        "unallocated_aleph":  total_unallocated,
        "split": {
            "ccn":    total_period * settings.wage_ccn_share,
            "crn":    total_period * settings.wage_crn_share,
            "staker": total_period * settings.wage_staker_share,
        },
    }
    return rewards, totals, {a: dict(d) for a, d in detailed.items()}
