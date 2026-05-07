import pytest
from unittest.mock import MagicMock

from aleph_nodestatus.payment_processor import (
    apply_slippage,
    quote_amount_out,
)


def test_apply_slippage_bps_200():
    assert apply_slippage(1_000_000, 200) == 980_000


def test_apply_slippage_zero():
    assert apply_slippage(1_000_000, 0) == 1_000_000


def test_apply_slippage_max_10000_rejects():
    with pytest.raises(ValueError):
        apply_slippage(1_000_000, 10_000)


def test_quote_amount_out_v3_calls_quoter():
    quoter = MagicMock()
    quoter.functions.quoteExactInput.return_value.call.return_value = (
        9_999_999, [0], [0], 0
    )
    swap_config = {"v": 3, "v3": b"\x01\x02"}
    out = quote_amount_out(quoter, swap_config, amount_in=1_000_000)
    assert out == 9_999_999
    quoter.functions.quoteExactInput.assert_called_once_with(b"\x01\x02", 1_000_000)


def test_quote_amount_out_v2_uses_v2_path(monkeypatch):
    quoter = MagicMock()
    swap_config = {"v": 2, "v2": ["0xA", "0xB"]}
    with pytest.raises(NotImplementedError, match="V2"):
        quote_amount_out(quoter, swap_config, amount_in=1_000_000)


def test_quote_amount_out_v4_raises():
    quoter = MagicMock()
    swap_config = {"v": 4, "v4": []}
    with pytest.raises(NotImplementedError, match="V4"):
        quote_amount_out(quoter, swap_config, amount_in=1_000_000)
