"""Unit tests for price_oracle: Credit-API output-deviation guard."""

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
    po._PRICE_CACHE.clear()
    yield
    po._PRICE_CACHE.clear()


def _mock_post(prices):
    """Return a fake requests.post that maps token symbol -> {"price": ...}."""
    def _post(url, json=None, timeout=None):
        resp = MagicMock()
        resp.raise_for_status.return_value = None
        resp.json.return_value = {"price": prices[json["token"]]}
        return resp
    return _post


def test_oracle_result_fields():
    from aleph_nodestatus.price_oracle import OracleResult
    r = OracleResult(ok=True)
    assert r.ok is True
    assert r.reason is None
    assert r.deviation_bps is None
    assert r.expected_out is None
    assert r.implied_out is None


def test_within_threshold_ok(monkeypatch):
    """expected_out matches the implied output -> ok=True."""
    import aleph_nodestatus.price_oracle as po
    # USDC=$1, ALEPH=$0.10 -> 1000 USDC implies 10000 ALEPH.
    monkeypatch.setattr(po, "requests",
                        MagicMock(post=_mock_post({"USDC": 1.0, "ALEPH": 0.10})))
    result = po.check_output_deviation(
        token_in_symbol="USDC",
        swap_amount_wei=1000 * 10**6,         # 1000 USDC
        token_in_decimals=6,
        expected_out_wei=10000 * 10**18,      # 10000 ALEPH (exact)
    )
    assert result.ok is True
    assert result.deviation_bps == 0


def test_exceeds_threshold_skips(monkeypatch):
    """expected_out 3% below implied (>200 bps) -> ok=False, price_deviation."""
    import aleph_nodestatus.price_oracle as po
    monkeypatch.setattr(po, "requests",
                        MagicMock(post=_mock_post({"USDC": 1.0, "ALEPH": 0.10})))
    result = po.check_output_deviation(
        token_in_symbol="USDC",
        swap_amount_wei=1000 * 10**6,
        token_in_decimals=6,
        expected_out_wei=9700 * 10**18,       # 3% short of 10000
    )
    assert result.ok is False
    assert result.reason == "price_deviation"
    assert result.deviation_bps > 200


def test_fail_closed_on_api_error(monkeypatch):
    """Any exception from requests.post -> ok=False, credit_api_unavailable."""
    import aleph_nodestatus.price_oracle as po

    def _boom(*a, **kw):
        raise RuntimeError("connection refused")

    monkeypatch.setattr(po, "requests", MagicMock(post=_boom))
    result = po.check_output_deviation(
        token_in_symbol="USDC",
        swap_amount_wei=1000 * 10**6,
        token_in_decimals=6,
        expected_out_wei=10000 * 10**18,
    )
    assert result.ok is False
    assert result.reason == "credit_api_unavailable"


def test_fail_closed_on_non_positive_price(monkeypatch):
    """API returns a non-positive price -> ok=False, credit_api_unavailable."""
    import aleph_nodestatus.price_oracle as po
    monkeypatch.setattr(po, "requests",
                        MagicMock(post=_mock_post({"USDC": 1.0, "ALEPH": 0.0})))
    result = po.check_output_deviation(
        token_in_symbol="USDC",
        swap_amount_wei=1000 * 10**6,
        token_in_decimals=6,
        expected_out_wei=10000 * 10**18,
    )
    assert result.ok is False
    assert result.reason == "credit_api_unavailable"


def test_flag_off_short_circuits(monkeypatch):
    """Flag disabled -> ok=True and no HTTP call made."""
    import aleph_nodestatus.price_oracle as po
    from aleph_nodestatus.settings import settings
    monkeypatch.setattr(settings, "extract_price_deviation_enabled", False)
    post_mock = MagicMock()
    monkeypatch.setattr(po, "requests", MagicMock(post=post_mock))
    result = po.check_output_deviation(
        token_in_symbol="USDC",
        swap_amount_wei=1000 * 10**6,
        token_in_decimals=6,
        expected_out_wei=1,                   # wildly off, but guard is off
    )
    assert result.ok is True
    post_mock.assert_not_called()


def test_aleph_price_cached_once(monkeypatch):
    """ALEPH price is fetched once and reused across tokens in a run."""
    import aleph_nodestatus.price_oracle as po
    calls = []

    def _post(url, json=None, timeout=None):
        calls.append(json["token"])
        resp = MagicMock()
        resp.raise_for_status.return_value = None
        resp.json.return_value = {"price": {"USDC": 1.0, "ETH": 3000.0,
                                            "ALEPH": 0.10}[json["token"]]}
        return resp

    monkeypatch.setattr(po, "requests", MagicMock(post=_post))
    po.check_output_deviation(
        token_in_symbol="USDC", swap_amount_wei=1000 * 10**6,
        token_in_decimals=6, expected_out_wei=10000 * 10**18)
    po.check_output_deviation(
        token_in_symbol="ETH", swap_amount_wei=1 * 10**18,
        token_in_decimals=18, expected_out_wei=30000 * 10**18)
    # ALEPH requested only once across both calls; USDC and ETH once each.
    assert calls.count("ALEPH") == 1
    assert calls.count("USDC") == 1
    assert calls.count("ETH") == 1
