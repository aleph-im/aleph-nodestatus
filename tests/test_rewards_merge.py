import pytest

from aleph_nodestatus.rewards_merge import merge_rewards


def test_merge_rewards_sums_addresses_across_sources():
    sources = {
        "credit_revenue": {"0xA": 1.0, "0xB": 2.0},
        "holder_tier":    {"0xA": 0.5},
        "wage_subsidy":   {"0xB": 3.0, "0xC": 4.0},
    }
    final, by_source = merge_rewards(sources, dust_threshold=0)
    assert final == {"0xa": 1.5, "0xb": 5.0, "0xc": 4.0}
    expected_normalized = {
        "credit_revenue": {"0xa": 1.0, "0xb": 2.0},
        "holder_tier":    {"0xa": 0.5},
        "wage_subsidy":   {"0xb": 3.0, "0xc": 4.0},
    }
    assert by_source == expected_normalized


def test_merge_rewards_filters_dust():
    sources = {"credit_revenue": {"0xA": 0.005, "0xB": 0.05}}
    final, _ = merge_rewards(sources, dust_threshold=0.01)
    assert final == {"0xb": pytest.approx(0.05)}


def test_merge_rewards_collapses_case_via_lower():
    sources = {
        "credit_revenue": {"0xAbCdEf": 1.0},
        "wage_subsidy":   {"0xabcdef": 2.0},
    }
    final, _ = merge_rewards(sources, dust_threshold=0)
    assert len(final) == 1
    assert next(iter(final.values())) == pytest.approx(3.0)
