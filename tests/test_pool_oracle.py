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
