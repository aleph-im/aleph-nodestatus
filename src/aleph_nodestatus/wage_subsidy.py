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
