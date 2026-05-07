"""Linear-decay wage subsidy for the 6-month tokenomics transition.

The curve is W0·(1 - t/T) ALEPH per month, where t is months since
settings.wage_start_date and T = settings.wage_duration_months.
"""

from datetime import datetime
from typing import Dict, Tuple

from .settings import settings

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


from .distribution import compute_score_multiplier


def _get_reward_address(entity, web3=None):
    reward = entity.get("reward")
    if reward and web3 is not None:
        validator = getattr(web3, "to_checksum_address",
                            getattr(web3, "toChecksumAddress", None))
        if validator:
            try:
                return validator(reward)
            except Exception:
                pass
    return reward or entity["owner"]


def split_subsidy(
    period_subsidy: float,
    nodes: dict,
    resource_nodes: dict,
    web3=None,
) -> Tuple[Dict[str, float], float]:
    """Split a period's wage subsidy across CCN / CRN / staker pools.

    Returns (rewards_by_address, unallocated_aleph).
    Each pool with zero eligible recipients contributes to unallocated.
    """
    if period_subsidy <= 0:
        return {}, 0.0

    ccn_pool    = period_subsidy * settings.wage_ccn_share
    crn_pool    = period_subsidy * settings.wage_crn_share
    staker_pool = period_subsidy * settings.wage_staker_share

    rewards: Dict[str, float] = {}
    unallocated = 0.0

    ccn_weights = []
    for node in nodes.values():
        if node["status"] != "active":
            continue
        score = compute_score_multiplier(node["score"])
        if score > 0:
            ccn_weights.append((_get_reward_address(node, web3), score))
    total_ccn = sum(s for _, s in ccn_weights)
    if total_ccn > 0:
        for addr, s in ccn_weights:
            rewards[addr] = rewards.get(addr, 0.0) + ccn_pool * s / total_ccn
    else:
        unallocated += ccn_pool

    crn_weights = []
    for rnode in resource_nodes.values():
        if rnode["status"] != "linked":
            continue
        score = compute_score_multiplier(rnode["score"])
        if score > 0:
            crn_weights.append((_get_reward_address(rnode, web3), score))
    total_crn = sum(s for _, s in crn_weights)
    if total_crn > 0:
        for addr, s in crn_weights:
            rewards[addr] = rewards.get(addr, 0.0) + crn_pool * s / total_crn
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
            rewards[addr] = rewards.get(addr, 0.0) + staker_pool * amt / total_stake
    else:
        unallocated += staker_pool

    return rewards, unallocated
