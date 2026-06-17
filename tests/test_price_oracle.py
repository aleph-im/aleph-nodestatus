"""Unit tests for price_oracle: Credit-API output-deviation guard.

The guard derives a fair ALEPH output from a single cached bulk-prices
fetch (GET /estimation/prices) and compares it (in wei) against the
quoter's expected_out.
"""

import sys
import types

if "plyvel" not in sys.modules:
    _plyvel_stub = types.ModuleType("plyvel")
    _plyvel_stub.DB = object
    sys.modules["plyvel"] = _plyvel_stub

from unittest.mock import MagicMock

import pytest


@pytest.fixture(autouse=True)
def _clear_price_cache():
    import aleph_nodestatus.price_oracle as po
    po._price_map.cache_clear()
    yield
    po._price_map.cache_clear()


def _item(symbol, token_amount, credit_amount, decimals, bonus=0):
    # creditAmount INCLUDES the bonus; base = creditAmount - bonus.
    return {
        "tokenSymbol": symbol,
        "tokenDecimals": decimals,
        "tokenAmount": str(token_amount),
        "creditAmount": credit_amount,
        "creditBonusAmount": bonus,
    }


# Base (bonus-free) credits drive the rate:
#   USDC:  1e6 wei  -> base 1e6   (rate 1.0/wei)
#   ALEPH: 1e18 wei -> base 1e5   (creditAmount 1.2e5 incl. 2e4 bonus; rate 1e-13/wei)
# => 1000 USDC (1e9 wei) fair output = 1e9 * 1.0 / 1e-13 = 1e22 wei = 10000 ALEPH.
_DEFAULT_ITEMS = [
    _item("USDC", 10**6, 10**6, 6),
    _item("ETH", 10**18, 3000 * 10**6, 18),
    _item("ALEPH", 10**18, 120000, 18, bonus=20000),
]


def _mock_get(items, capture=None):
    """Fake requests.get returning a bulk-prices body. `capture` (a list)
    records (url, params) of each call."""
    def _get(url, params=None, timeout=None):
        if capture is not None:
            capture.append((url, params))
        resp = MagicMock()
        resp.raise_for_status.return_value = None
        resp.json.return_value = {
            "blockchain": (params or {}).get("blockchain"),
            "timestamp": 0,
            "prices": items,
        }
        return resp
    return _get


def test_fetches_bulk_prices_endpoint_once(monkeypatch):
    """Regression: the guard GETs /api/v0/estimation/prices with the
    blockchain query param, and only once across multiple tokens (cached)."""
    import aleph_nodestatus.price_oracle as po
    from aleph_nodestatus.settings import settings

    calls = []
    monkeypatch.setattr(po, "requests", MagicMock(get=_mock_get(_DEFAULT_ITEMS, calls)))
    po.check_output_deviation(
        token_in_symbol="USDC", swap_amount_wei=1000 * 10**6,
        expected_out_wei=10000 * 10**18)
    po.check_output_deviation(
        token_in_symbol="ETH", swap_amount_wei=1 * 10**18,
        expected_out_wei=1 * 10**18)

    assert len(calls) == 1, "price map must be fetched once per run (cached)"
    url, params = calls[0]
    assert url == "https://credit.aleph.im/api/v0/estimation/prices"
    assert params == {"blockchain": settings.credit_api_blockchain}


def test_oracle_result_fields():
    from aleph_nodestatus.price_oracle import OracleResult
    r = OracleResult(ok=True)
    assert r.ok is True
    assert r.reason is None
    assert r.deviation_bps is None
    assert r.expected_out is None
    assert r.implied_out is None


def test_within_threshold_ok(monkeypatch):
    """expected_out matches the fair output -> ok=True."""
    import aleph_nodestatus.price_oracle as po
    monkeypatch.setattr(po, "requests", MagicMock(get=_mock_get(_DEFAULT_ITEMS)))
    result = po.check_output_deviation(
        token_in_symbol="USDC",
        swap_amount_wei=1000 * 10**6,         # 1000 USDC
        expected_out_wei=10000 * 10**18,      # exactly the fair output
    )
    assert result.ok is True
    assert result.deviation_bps == 0
    assert result.expected_out == 10000.0
    assert result.implied_out == 10000.0


def test_exceeds_threshold_skips(monkeypatch):
    """expected_out 3% below fair (>200 bps) -> ok=False, price_deviation."""
    import aleph_nodestatus.price_oracle as po
    monkeypatch.setattr(po, "requests", MagicMock(get=_mock_get(_DEFAULT_ITEMS)))
    result = po.check_output_deviation(
        token_in_symbol="USDC",
        swap_amount_wei=1000 * 10**6,
        expected_out_wei=9700 * 10**18,       # 3% short of 10000
    )
    assert result.ok is False
    assert result.reason == "price_deviation"
    assert result.deviation_bps == 300


def test_excludes_holder_bonus(monkeypatch):
    """The rate uses base credits (creditAmount - creditBonusAmount). With
    ALEPH carrying a 20k bonus on a 120k creditAmount, base is 100k and the
    fair output for 1000 USDC is 10000 ALEPH. If the bonus were NOT excluded
    (rate from 120k), fair would be ~8333 ALEPH and 10000 would read as a
    ~2000 bps deviation -> skip. ok=True proves the bonus is removed."""
    import aleph_nodestatus.price_oracle as po
    monkeypatch.setattr(po, "requests", MagicMock(get=_mock_get(_DEFAULT_ITEMS)))
    result = po.check_output_deviation(
        token_in_symbol="USDC",
        swap_amount_wei=1000 * 10**6,
        expected_out_wei=10000 * 10**18,
    )
    assert result.ok is True
    assert result.deviation_bps == 0


def test_fail_closed_on_api_error(monkeypatch):
    """Any exception from requests.get -> ok=False, credit_api_unavailable."""
    import aleph_nodestatus.price_oracle as po

    def _boom(*a, **kw):
        raise RuntimeError("connection refused")

    monkeypatch.setattr(po, "requests", MagicMock(get=_boom))
    result = po.check_output_deviation(
        token_in_symbol="USDC",
        swap_amount_wei=1000 * 10**6,
        expected_out_wei=10000 * 10**18,
    )
    assert result.ok is False
    assert result.reason == "credit_api_unavailable"


def test_fail_closed_on_missing_symbol(monkeypatch):
    """A token missing from the price map -> credit_api_unavailable
    (no crash, fail-closed)."""
    import aleph_nodestatus.price_oracle as po
    items = [_item("USDC", 10**6, 10**6, 6)]  # no ALEPH
    monkeypatch.setattr(po, "requests", MagicMock(get=_mock_get(items)))
    result = po.check_output_deviation(
        token_in_symbol="USDC",
        swap_amount_wei=1000 * 10**6,
        expected_out_wei=10000 * 10**18,
    )
    assert result.ok is False
    assert result.reason == "credit_api_unavailable"


def test_flag_off_short_circuits(monkeypatch):
    """Flag disabled -> ok=True and no HTTP call made."""
    import aleph_nodestatus.price_oracle as po
    from aleph_nodestatus.settings import settings
    monkeypatch.setattr(settings, "extract_price_deviation_enabled", False)
    get_mock = MagicMock()
    monkeypatch.setattr(po, "requests", MagicMock(get=get_mock))
    result = po.check_output_deviation(
        token_in_symbol="USDC",
        swap_amount_wei=1000 * 10**6,
        expected_out_wei=1,                   # wildly off, but guard is off
    )
    assert result.ok is True
    get_mock.assert_not_called()
