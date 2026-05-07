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
