"""Credit-API output-deviation guard for the extract swap path.

Compares the quoter's `expected_out` (version-aware v2/v3/v4, produced
upstream by `quote_amount_out`) against the Credit-API's independent,
multi-source reference for the same swap.

The reference comes from a single bulk-prices fetch, used exactly as the
API serves it (amounts in the token's smallest unit, comparisons in wei):

    GET /api/v0/estimation/prices?blockchain=<chain>
      -> { "prices": [ { tokenSymbol, tokenAmount, creditAmount,
                         creditBonusAmount, ... }, ... ] }

Each item pairs `tokenAmount` (wei) with `creditAmount` (credits, which
INCLUDE the holder bonus reported in `creditBonusAmount`). The guarded
swap has no bonus and the bonus is not uniform across tokens, so the
bonus is removed first: `base = creditAmount - creditBonusAmount`, and
`rate = base / tokenAmount` is a per-wei rate. The fair ALEPH output is
`swap_wei * rate[token_in] / rate["ALEPH"]`; the credit unit cancels in
the ratio, so no constants or per-token decimals enter nodestatus. The
result is in wei, directly comparable to the quoter's `expected_out_wei`.

The map is cached for the process lifetime (one HTTP call per run; the
extract job is a one-shot cron). The guard runs only for non-ALEPH input
tokens (ALEPH has no swap).

Failure is fail-closed: if the API is unavailable, or a required symbol
is missing, the caller skips the token for that run. Synchronous
`requests` is correct here - the whole extract path runs inside
`asyncio.to_thread`, matching the module's sync web3 calls.
"""

import logging
from dataclasses import dataclass
from functools import lru_cache
from typing import Dict, Optional

import requests

from .settings import settings

LOGGER = logging.getLogger(__name__)


class CreditApiUnavailable(RuntimeError):
    """Raised when the Credit-API fair price cannot be obtained and the
    extract run must abort (the price is essential to sizing)."""


@dataclass
class OracleResult:
    ok: bool
    reason: Optional[str] = None
    deviation_bps: Optional[int] = None
    expected_out: Optional[float] = None
    implied_out: Optional[float] = None


@lru_cache(maxsize=4)
def _price_map(blockchain: str) -> Dict[str, dict]:
    """Per-process cache of the Credit-API bulk price map, keyed by token
    symbol. One GET per run (the extract job is a one-shot cron). Raises on
    any HTTP/parse error (caller converts to fail-closed)."""
    resp = requests.get(
        f"{settings.credit_api_url}/estimation/prices",
        params={"blockchain": blockchain},
        timeout=settings.credit_api_timeout_seconds,
    )
    resp.raise_for_status()
    body = resp.json()
    return {item["tokenSymbol"]: item for item in body["prices"]}


def _rate_credits_per_wei(prices: Dict[str, dict], symbol: str) -> float:
    """Bonus-free credits per wei for `symbol`. `creditAmount` includes the
    holder bonus (reported in `creditBonusAmount`); the guarded swap has no
    bonus, and the bonus is not uniform across tokens, so it must be removed
    before comparing. Raises KeyError if the symbol is absent
    (caller -> fail-closed)."""
    item = prices[symbol]
    base_credits = float(item["creditAmount"]) - float(item.get("creditBonusAmount", 0))
    return base_credits / float(int(item["tokenAmount"]))


def fair_aleph_rate(token_in_symbol: str) -> float:
    """ALEPH-wei per input-wei (bonus-free) from the cached bulk price map.

    The credit unit cancels in the ratio, so no constants/decimals enter.
    Raises on HTTP/parse/missing-symbol; caller converts to an abort.
    """
    prices = _price_map(settings.credit_api_blockchain)
    rate_in = _rate_credits_per_wei(prices, token_in_symbol)
    rate_aleph = _rate_credits_per_wei(prices, "ALEPH")
    if rate_aleph <= 0:
        raise ValueError("Credit-API returned non-positive ALEPH rate")
    return rate_in / rate_aleph


def _fair_aleph_out_wei(token_in_symbol: str, swap_amount_wei: int) -> int:
    """Fair ALEPH output (wei) for swapping `swap_amount_wei` of
    `token_in_symbol`, from the cached bulk price map. The credit unit
    cancels in the ratio, so no constants/decimals are involved.
    Raises on HTTP/parse/missing-symbol error (caller -> fail-closed)."""
    return int(round(swap_amount_wei * fair_aleph_rate(token_in_symbol)))


def check_output_deviation(
    *, token_in_symbol: str, swap_amount_wei: int,
    expected_out_wei: int, aleph_decimals: int = 18,
) -> OracleResult:
    """Compare the quoter's `expected_out_wei` against the Credit-API fair
    ALEPH output for swapping `swap_amount_wei` of `token_in_symbol`.
    Symmetric deviation; fail-closed on API error."""
    if not settings.extract_price_deviation_enabled:
        return OracleResult(ok=True)

    try:
        fair_out_wei = _fair_aleph_out_wei(token_in_symbol, swap_amount_wei)
    except Exception as e:
        LOGGER.warning(
            "Credit API estimation failed for %s: %r", token_in_symbol, e,
        )
        return OracleResult(ok=False, reason="credit_api_unavailable")

    if fair_out_wei <= 0:
        LOGGER.warning(
            "Credit API returned non-positive fair output for %s", token_in_symbol,
        )
        return OracleResult(ok=False, reason="credit_api_unavailable")

    expected_out = expected_out_wei / 10 ** aleph_decimals
    implied_out = fair_out_wei / 10 ** aleph_decimals

    deviation_bps = int(abs(expected_out_wei - fair_out_wei) / fair_out_wei * 10_000)
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
