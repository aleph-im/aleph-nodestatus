"""Unit tests for pool_oracle: spot-vs-Chainlink deviation guard."""

import sys
import types

if "plyvel" not in sys.modules:
    _plyvel_stub = types.ModuleType("plyvel")
    _plyvel_stub.DB = object
    sys.modules["plyvel"] = _plyvel_stub

from unittest.mock import MagicMock

import pytest


def test_oracle_result_is_dataclass():
    from aleph_nodestatus.pool_oracle import OracleResult

    r = OracleResult(ok=True)
    assert r.ok is True
    assert r.reason is None
    assert r.deviation_bps is None
    assert r.spot_price is None
    assert r.ref_price is None


def test_check_swap_price_deviation_returns_ok_for_now():
    """Skeleton: the function exists and returns ok=True regardless of input.
    Per-version logic lands in subsequent tasks."""
    from aleph_nodestatus.pool_oracle import check_swap_price_deviation

    w3 = MagicMock()
    cfg = {"v": 3, "t": "0x0", "v2": [], "v3": b"", "v4": []}
    result = check_swap_price_deviation(w3, cfg, token_in="0xToken")
    assert result.ok is True


def test_read_chainlink_returns_price(monkeypatch):
    """Healthy feed: latestRoundData answer × 10^-decimals."""
    import aleph_nodestatus.pool_oracle as po

    feed = MagicMock()
    feed.functions.decimals.return_value.call.return_value = 8
    # answer = 1.234567 × 10^8
    feed.functions.latestRoundData.return_value.call.return_value = (
        1, 123_456_700, 0, 9_999_999_999, 1,  # updatedAt very recent
    )

    w3 = MagicMock()
    w3.eth.contract.return_value = feed
    w3.eth.get_block.return_value.timestamp = 10_000_000_000

    price, reason = po._read_chainlink_price(
        w3, feed_address="0xfeed",
    )
    assert reason is None
    assert abs(price - 1.234567) < 1e-9


def test_read_chainlink_stale(monkeypatch):
    """updatedAt older than chainlink_max_age_seconds → reason=chainlink_stale."""
    import aleph_nodestatus.pool_oracle as po
    from aleph_nodestatus.settings import settings as s

    monkeypatch.setattr(s, "chainlink_max_age_seconds", 3600)

    feed = MagicMock()
    feed.functions.decimals.return_value.call.return_value = 8
    feed.functions.latestRoundData.return_value.call.return_value = (
        1, 100_000_000, 0, 1000, 1,  # updatedAt = 1000
    )

    w3 = MagicMock()
    w3.eth.contract.return_value = feed
    w3.eth.get_block.return_value.timestamp = 1000 + 4000  # 4000s old, > 3600

    price, reason = po._read_chainlink_price(w3, "0xfeed")
    assert price is None
    assert reason == "chainlink_stale"


def test_read_chainlink_invalid_answer():
    """answer <= 0 → reason=chainlink_invalid."""
    import aleph_nodestatus.pool_oracle as po

    feed = MagicMock()
    feed.functions.decimals.return_value.call.return_value = 8
    feed.functions.latestRoundData.return_value.call.return_value = (
        1, 0, 0, 9_999_999_999, 1,
    )

    w3 = MagicMock()
    w3.eth.contract.return_value = feed
    w3.eth.get_block.return_value.timestamp = 10_000_000_000

    price, reason = po._read_chainlink_price(w3, "0xfeed")
    assert price is None
    assert reason == "chainlink_invalid"
