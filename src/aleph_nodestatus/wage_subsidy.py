"""Linear-decay wage subsidy for the 6-month tokenomics transition.

The curve is W0·(1 - t/T) ALEPH per month, where t is months since
settings.wage_start_date and T = settings.wage_duration_months.
"""

from collections import defaultdict
from datetime import datetime
from typing import Dict, Tuple

from .distribution import compute_score_multiplier
from .settings import settings
from .utils import get_reward_address

MONTH_SECONDS = 30 * 86400


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
            ccn_weights.append((get_reward_address(node, web3), score))
    total_ccn = sum(s for _, s in ccn_weights)
    if total_ccn > 0:
        for addr, s in ccn_weights:
            share = ccn_pool * s / total_ccn
            rewards[addr] = rewards.get(addr, 0.0) + share
            detailed[addr]["ccn"] += share
    else:
        unallocated += ccn_pool

    crn_weights = []
    for rnode in resource_nodes.values():
        if rnode["status"] != "linked":
            continue
        score = compute_score_multiplier(rnode["score"])
        if score > 0:
            crn_weights.append((get_reward_address(rnode, web3), score))
    total_crn = sum(s for _, s in crn_weights)
    if total_crn > 0:
        for addr, s in crn_weights:
            share = crn_pool * s / total_crn
            rewards[addr] = rewards.get(addr, 0.0) + share
            detailed[addr]["crn"] += share
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
