"""Spot-vs-Chainlink deviation guard for the extract swap path.

For each hop in the configured swap path (read at runtime from the
AlephPaymentProcessor), compares the pool's current spot price to the
Chainlink-USD-implied ratio for the same hop. Returns a verdict that
the caller (extract_aleph) uses to either proceed or skip the token.

Phase 1 reads SPOT from the pool — no TWAP. Phase 2 adds true TWAP
(V3 observe() / V4 oracle hooks) which closes the ALEPH-side hop
gap (ALEPH has no Chainlink feed today).
"""

import json
import logging
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
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


@lru_cache(maxsize=None)
def _load_abi(name: str):
    path = Path(__file__).parent / "abi" / f"{name}.json"
    with open(path) as f:
        return json.load(f)


def _read_chainlink_price(w3, feed_address: str):
    """Return (price_float, None) on success, (None, reason_str) on failure.

    reason ∈ {"chainlink_stale", "chainlink_invalid"}.
    """
    feed = w3.eth.contract(
        address=w3.to_checksum_address(feed_address),
        abi=_load_abi("ChainlinkAggregator"),
    )
    decimals = int(feed.functions.decimals().call())
    _, answer, _, updated_at, _ = feed.functions.latestRoundData().call()
    if answer <= 0:
        return None, "chainlink_invalid"
    now = w3.eth.get_block("latest").timestamp
    if now - updated_at > settings.chainlink_max_age_seconds:
        return None, "chainlink_stale"
    return float(answer) / (10 ** decimals), None
