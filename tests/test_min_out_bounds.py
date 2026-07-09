import pytest
from hypothesis import given, strategies as st

from aleph_nodestatus.payment_processor import apply_slippage


def test_apply_slippage_100bps_is_exact():
    assert apply_slippage(10_000, 100) == 9900
    assert apply_slippage(1_000_000, 100) == 990_000


def test_apply_slippage_zero_is_identity():
    assert apply_slippage(12345, 0) == 12345


def test_apply_slippage_max_is_rejected():
    with pytest.raises(ValueError):
        apply_slippage(1000, 10_000)


def test_apply_slippage_negative_is_rejected():
    with pytest.raises(ValueError):
        apply_slippage(1000, -1)


@given(
    amount=st.integers(min_value=0, max_value=10**24),
    bps=st.integers(min_value=0, max_value=9999),
)
def test_apply_slippage_in_bounds(amount, bps):
    result = apply_slippage(amount, bps)
    assert 0 <= result <= amount
