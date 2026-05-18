"""Spot-vs-Chainlink deviation guard for the extract swap path.

For each hop in the configured swap path (read at runtime from the
AlephPaymentProcessor), compares the pool's current spot price to the
Chainlink-USD-implied ratio for the same hop. Returns a verdict that
the caller (extract_aleph) uses to either proceed or skip the token.

Phase 1 reads SPOT from the pool — no TWAP. Phase 2 adds true TWAP
(V3 observe() / V4 oracle hooks) which closes the ALEPH-side hop
gap (ALEPH has no Chainlink feed today).
"""

import logging
from dataclasses import dataclass
from typing import Optional

from .settings import settings

LOGGER = logging.getLogger(__name__)


@dataclass
class OracleResult:
    ok: bool
    reason: Optional[str] = None
    deviation_bps: Optional[int] = None
    spot_price: Optional[float] = None
    ref_price: Optional[float] = None


def check_swap_price_deviation(w3, swap_config: dict, token_in: str) -> OracleResult:
    """Skeleton: always returns ok=True. Per-version logic in later tasks."""
    return OracleResult(ok=True)
