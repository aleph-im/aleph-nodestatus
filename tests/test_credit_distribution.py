"""Unit tests for credit distribution helpers."""

import pytest

from aleph_nodestatus.credit_distribution import build_total_summary


def test_build_total_summary_aggregates_components():
    by_address_detailed = {
        "0xAAA": {
            "credit_revenue": {
                "execution_ccn": 1.0,
                "execution_crn": 2.0,
                "execution_staker": 0.0,
                "storage_ccn": 0.5,
                "storage_staker": 0.0,
                # spurious per-stream total injected by the caller; must be ignored
                "total": 999.0,
            },
            "holder_tier": {
                "execution_ccn": 0.0,
                "execution_crn": 0.0,
                "execution_staker": 3.0,
                "storage_ccn": 0.0,
                "storage_staker": 1.0,
            },
            "wage_subsidy": {
                "ccn": 2.0,
                "crn": 1.0,
                "staker": 0.5,
            },
        },
        "0xBBB": {
            "credit_revenue": {
                "execution_ccn": 4.0,
                "execution_crn": 0.0,
                "execution_staker": 1.0,
                "storage_ccn": 0.0,
                "storage_staker": 2.0,
                "total": 123.0,  # spurious, must be ignored
            },
            "wage_subsidy": {
                "ccn": 0.0,
                "crn": 3.0,
                "staker": 0.0,
            },
        },
        "0xCCC": {
            "holder_tier": {
                "execution_ccn": 5.0,
                "execution_crn": 0.0,
                "execution_staker": 0.0,
                "storage_ccn": 1.5,
                "storage_staker": 0.0,
            },
        },
    }

    final_rewards = {
        "0xAAA": 11.5,
        "0xBBB": 10.0,
        "0xCCC": 6.5,
    }

    result = build_total_summary(final_rewards, by_address_detailed)

    # Exact per-component sums in `full`.
    assert result["full"]["credit_revenue"] == {
        "execution_ccn": 5.0,
        "execution_crn": 2.0,
        "execution_staker": 1.0,
        "storage_ccn": 0.5,
        "storage_staker": 2.0,
    }
    assert result["full"]["holder_tier"] == {
        "execution_ccn": 5.0,
        "execution_crn": 0.0,
        "execution_staker": 3.0,
        "storage_ccn": 1.5,
        "storage_staker": 1.0,
    }
    assert result["full"]["wage_subsidy"] == {
        "ccn": 2.0,
        "crn": 4.0,
        "staker": 0.5,
    }

    # bySource equals the sum of that source's components.
    for src, comps in result["full"].items():
        assert result["bySource"][src] == pytest.approx(sum(comps.values()))

    assert result["bySource"]["credit_revenue"] == pytest.approx(10.5)
    assert result["bySource"]["holder_tier"] == pytest.approx(10.5)
    assert result["bySource"]["wage_subsidy"] == pytest.approx(6.5)

    # totals.aleph equals sum(final_rewards).
    assert result["totals"]["aleph"] == pytest.approx(sum(final_rewards.values()))
    assert result["totals"]["aleph"] == pytest.approx(28.0)

    # Invariant: sum(bySource) == sum of all full components.
    full_total = sum(
        amount
        for comps in result["full"].values()
        for amount in comps.values()
    )
    assert sum(result["bySource"].values()) == pytest.approx(full_total)

    # The spurious "total" key inside credit_revenue blocks was ignored:
    # had it been counted, credit_revenue components would include 999/123.
    assert result["bySource"]["credit_revenue"] == pytest.approx(10.5)
    assert "total" not in result["full"]["credit_revenue"]
