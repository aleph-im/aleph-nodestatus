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


from aleph_nodestatus.wage_subsidy import (
    compute_period_subsidy,
    months_since_start,
    parse_wage_start,
)


def test_parse_wage_start_iso_z():
    settings.wage_start_date = "2026-04-01T00:00:00+00:00"
    assert parse_wage_start() == 1775001600.0   # 2026-04-01 UTC


def test_months_since_start_after_30_days():
    settings.wage_start_date = "2026-04-01T00:00:00+00:00"
    assert months_since_start(1775001600.0 + 30 * 86400) == pytest.approx(1.0)


def test_period_subsidy_within_first_month():
    settings.wage_start_date = "2026-04-01T00:00:00+00:00"
    start = parse_wage_start()
    end = start + 30 * 86400
    assert compute_period_subsidy(start, end) == pytest.approx(825_000)


def test_period_subsidy_clamped_past_end():
    settings.wage_start_date = "2026-04-01T00:00:00+00:00"
    start = parse_wage_start() + 7 * 30 * 86400
    end = start + 30 * 86400
    assert compute_period_subsidy(start, end) == 0.0


def test_period_subsidy_rejects_inverted_range():
    settings.wage_start_date = "2026-04-01T00:00:00+00:00"
    with pytest.raises(ValueError):
        compute_period_subsidy(100, 50)
