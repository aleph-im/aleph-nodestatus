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


from aleph_nodestatus.wage_subsidy import split_subsidy


def _node(hash, status, score, owner, stakers=None, reward=None,
          resource_nodes=None):
    return {
        "hash": hash, "status": status, "score": score,
        "owner": owner, "reward": reward,
        "stakers": stakers or {},
        "resource_nodes": resource_nodes or [],
        "has_bonus": False, "decentralization": 0.5,
    }


def _rnode(hash, status, score, owner, reward=None):
    return {
        "hash": hash, "status": status, "score": score,
        "owner": owner, "reward": reward,
        "decentralization": 0.5,
    }


def test_split_subsidy_three_equal_pools():
    nodes = {
        "n1": _node("n1", "active", 0.9, "0xCCN1",
                    stakers={"0xS1": 100, "0xS2": 100},
                    resource_nodes=["r1"]),
    }
    rnodes = {"r1": _rnode("r1", "linked", 0.9, "0xCRN1")}
    rewards, unallocated = split_subsidy(900.0, nodes, rnodes)
    assert rewards["0xCCN1"] == pytest.approx(300.0)
    assert rewards["0xCRN1"] == pytest.approx(300.0)
    assert rewards["0xS1"]   == pytest.approx(150.0)
    assert rewards["0xS2"]   == pytest.approx(150.0)
    assert unallocated == pytest.approx(0.0)


def test_split_subsidy_zero_subsidy_returns_empty():
    rewards, unallocated = split_subsidy(0.0, {}, {})
    assert rewards == {}
    assert unallocated == 0.0


def test_split_subsidy_no_active_ccns_records_unallocated():
    rewards, unallocated = split_subsidy(900.0, {}, {})
    assert rewards == {}
    assert unallocated == pytest.approx(900.0)


def test_split_subsidy_no_linked_crn_only_crn_pool_unallocated():
    nodes = {
        "n1": _node("n1", "active", 0.9, "0xCCN1",
                    stakers={"0xS1": 100}),
    }
    rnodes = {}
    rewards, unallocated = split_subsidy(900.0, nodes, rnodes)
    assert rewards["0xCCN1"] == pytest.approx(300.0)
    assert rewards["0xS1"]   == pytest.approx(300.0)
    assert "0xCRN1" not in rewards
    assert unallocated == pytest.approx(300.0)


def test_split_subsidy_score_weighted_ccn():
    nodes = {
        "n1": _node("n1", "active", 0.9, "0xCCN1", stakers={"0xS1": 1}),
        "n2": _node("n2", "active", 0.5, "0xCCN2", stakers={"0xS2": 1}),
    }
    rnodes = {}
    rewards, _ = split_subsidy(600.0, nodes, rnodes)
    assert rewards["0xCCN1"] == pytest.approx(200 * 1.0 / 1.5)
    assert rewards["0xCCN2"] == pytest.approx(200 * 0.5 / 1.5)
