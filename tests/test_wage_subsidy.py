import pytest

from aleph_nodestatus.settings import settings
from aleph_nodestatus.wage_subsidy import wage_integral


def test_integral_at_zero():
    assert wage_integral(0) == 0


def test_integral_at_full_duration_equals_triangle_area():
    W0 = settings.wage_initial_monthly_aleph
    T = settings.wage_duration_months
    assert wage_integral(T) == pytest.approx(W0 * T / 2)
    assert wage_integral(T) == pytest.approx(2_700_000)


def test_integral_first_month_partial():
    assert wage_integral(1) == pytest.approx(825_000)


def test_integral_clamps_past_end():
    T = settings.wage_duration_months
    assert wage_integral(T + 0.5) == wage_integral(T)
    assert wage_integral(T + 100) == wage_integral(T)


def test_integral_negative_t_is_zero():
    assert wage_integral(-1) == 0
    assert wage_integral(-100) == 0
