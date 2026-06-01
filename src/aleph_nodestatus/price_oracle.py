"""Credit-API output-deviation guard for the extract swap path.

Compares the quoter's `expected_out` (version-aware v2/v3/v4, produced
upstream by `quote_amount_out`) against the Credit-API multi-source
USD-implied output for the same swap. The Credit API
(`POST /estimation/token-to-credit`) returns a USD unit `price` for each
of USDC, ETH, and ALEPH, averaged internally across DEX and CEX sources.

The guard runs only for non-ALEPH input tokens (ALEPH has no swap), both
of which are directly priceable, so no per-hop logic or address map is
needed - intermediate hops (e.g. WETH) are already baked into the
quoter's `expected_out`.

Failure is fail-closed: if the API is unavailable the caller skips the
token for that run. Synchronous `requests` is correct here - the whole
extract path runs inside `asyncio.to_thread`, matching the module's sync
web3 calls.
"""

import logging
from dataclasses import dataclass
from typing import Optional

import requests

from .settings import settings

LOGGER = logging.getLogger(__name__)

# Per-process unit-price cache. The extract job is a one-shot cron run, so
# caching for the lifetime of the process means one HTTP call per symbol
# per run (ALEPH is the shared denominator across every token).
_PRICE_CACHE = {}

# Decimals for the priceable set. Only used to send a valid non-zero unit
# `amount` so the API returns a `price`; exact value is not price-critical.
_CREDIT_API_DECIMALS = {"USDC": 6, "ETH": 18, "ALEPH": 18}


@dataclass
class OracleResult:
    ok: bool
    reason: Optional[str] = None
    deviation_bps: Optional[int] = None
    expected_out: Optional[float] = None
    implied_out: Optional[float] = None


def _credit_api_price(symbol: str) -> float:
    """USD unit price for `symbol` from the Credit API. Cached per process.
    Raises on any HTTP/parse error (caller converts to fail-closed)."""
    if symbol in _PRICE_CACHE:
        return _PRICE_CACHE[symbol]
    decimals = _CREDIT_API_DECIMALS.get(symbol, 18)
    resp = requests.post(
        f"{settings.credit_api_url}/estimation/token-to-credit",
        json={
            "blockchain": settings.credit_api_blockchain,
            "token": symbol,
            "amount": str(10 ** decimals),
        },
        timeout=settings.credit_api_timeout_seconds,
    )
    resp.raise_for_status()
    price = float(resp.json()["price"])
    _PRICE_CACHE[symbol] = price
    return price


def check_output_deviation(
    *, token_in_symbol: str, swap_amount_wei: int, token_in_decimals: int,
    expected_out_wei: int, aleph_decimals: int = 18,
) -> OracleResult:
    """Compare the quoter's `expected_out_wei` against the Credit-API
    USD-implied ALEPH output for swapping `swap_amount_wei` of
    `token_in_symbol`. Symmetric deviation; fail-closed on API error."""
    if not settings.extract_price_deviation_enabled:
        return OracleResult(ok=True)

    try:
        price_in_usd = _credit_api_price(token_in_symbol)
        price_aleph_usd = _credit_api_price("ALEPH")
    except Exception as e:
        LOGGER.warning(
            "Credit API price fetch failed for %s: %r", token_in_symbol, e,
        )
        return OracleResult(ok=False, reason="credit_api_unavailable")

    if price_in_usd <= 0 or price_aleph_usd <= 0:
        LOGGER.warning(
            "Credit API returned non-positive price (in=%s, aleph=%s)",
            price_in_usd, price_aleph_usd,
        )
        return OracleResult(ok=False, reason="credit_api_unavailable")

    swap_amount_human = swap_amount_wei / 10 ** token_in_decimals
    implied_out = (swap_amount_human * price_in_usd) / price_aleph_usd
    expected_out = expected_out_wei / 10 ** aleph_decimals

    if implied_out <= 0:
        return OracleResult(ok=False, reason="credit_api_unavailable")

    deviation_bps = int(abs(expected_out - implied_out) / implied_out * 10_000)
    if deviation_bps > settings.extract_max_deviation_bps:
        return OracleResult(
            ok=False, reason="price_deviation",
            deviation_bps=deviation_bps,
            expected_out=expected_out, implied_out=implied_out,
        )
    return OracleResult(
        ok=True, deviation_bps=deviation_bps,
        expected_out=expected_out, implied_out=implied_out,
    )
