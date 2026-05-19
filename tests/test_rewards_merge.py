import pytest

from eth_utils import to_checksum_address as _ck

from aleph_nodestatus.rewards_merge import merge_rewards

# Address fixtures: rewards_merge normalises via to_checksum_address (strict
# 40-hex-char validation), so test fixtures must be real-shape addresses.
_ADDR_A = "0x" + "a" * 40
_ADDR_B = "0x" + "b" * 40
_ADDR_C = "0x" + "c" * 40
_ADDR_DUST = _ADDR_B
_CK_A = _ck(_ADDR_A)
_CK_B = _ck(_ADDR_B)
_CK_C = _ck(_ADDR_C)
_CK_DUST = _CK_B

# For the case-collapse test: two stylings of the same address must collapse
# into a single EIP-55 checksum entry.
_ADDR_MIXED = "0x" + "Ab" * 20
_ADDR_LOWER = _ADDR_MIXED.lower()


def test_merge_rewards_sums_addresses_across_sources():
    sources = {
        "credit_revenue": {_ADDR_A: 1.0, _ADDR_B: 2.0},
        "holder_tier":    {_ADDR_A: 0.5},
        "wage_subsidy":   {_ADDR_B: 3.0, _ADDR_C: 4.0},
    }
    final, by_source, _ = merge_rewards(sources, dust_threshold=0)
    assert final == {_CK_A: 1.5, _CK_B: 5.0, _CK_C: 4.0}
    expected_normalized = {
        "credit_revenue": {_CK_A: 1.0, _CK_B: 2.0},
        "holder_tier":    {_CK_A: 0.5},
        "wage_subsidy":   {_CK_B: 3.0, _CK_C: 4.0},
    }
    assert by_source == expected_normalized


def test_merge_rewards_filters_dust():
    sources = {"credit_revenue": {_ADDR_A: 0.005, _ADDR_B: 0.05}}
    final, _, _ = merge_rewards(sources, dust_threshold=0.01)
    assert final == {_CK_B: pytest.approx(0.05)}


def test_merge_rewards_collapses_case_via_checksum():
    sources = {
        "credit_revenue": {_ADDR_MIXED: 1.0},
        "wage_subsidy":   {_ADDR_LOWER: 2.0},
    }
    final, _, _ = merge_rewards(sources, dust_threshold=0)
    assert len(final) == 1
    assert next(iter(final.keys())) == _ck(_ADDR_MIXED)
    assert next(iter(final.values())) == pytest.approx(3.0)


def test_merge_rewards_details_inverted_by_address():
    """`details` keyed {source: {addr: {component: amount}}} is inverted to
    {addr: {source: {component: amount}}} and trimmed to dust survivors."""
    sources = {
        "credit_revenue": {_ADDR_A: 1.0, _ADDR_DUST: 0.001},
        "wage_subsidy":   {_ADDR_A: 2.0},
    }
    details = {
        "credit_revenue": {
            _ADDR_A:    {"execution_crn": 0.6, "execution_ccn": 0.15,
                         "execution_staker": 0.25},
            _ADDR_DUST: {"storage_ccn": 0.001},
        },
        "wage_subsidy": {
            _ADDR_A: {"ccn": 1.0, "staker": 1.0},
        },
    }
    _, _, by_addr = merge_rewards(sources, details=details, dust_threshold=0.01)
    assert _CK_A in by_addr
    assert _CK_DUST not in by_addr      # filtered with the total
    assert set(by_addr[_CK_A].keys()) == {"credit_revenue", "wage_subsidy"}
    assert by_addr[_CK_A]["credit_revenue"]["execution_crn"] == pytest.approx(0.6)
    assert by_addr[_CK_A]["wage_subsidy"]["ccn"] == pytest.approx(1.0)


def test_merge_rewards_details_sum_matches_final_per_address():
    """Sum of every nested component for an address equals final_rewards[addr]
    — the invariant the audit post relies on."""
    sources = {
        "credit_revenue": {_ADDR_A: 1.5},
        "wage_subsidy":   {_ADDR_A: 0.5},
    }
    details = {
        "credit_revenue": {_ADDR_A: {"storage_ccn": 1.0, "storage_staker": 0.5}},
        "wage_subsidy":   {_ADDR_A: {"ccn": 0.5}},
    }
    final, _, by_addr = merge_rewards(sources, details=details, dust_threshold=0)
    addr_total = sum(
        amt for comps in by_addr[_CK_A].values() for amt in comps.values()
    )
    assert addr_total == pytest.approx(final[_CK_A])


def test_merge_rewards_details_empty_when_not_provided():
    """Without a `details` kwarg, the audit-details return is `{}` —
    backward compatible with callers that don't track per-component data."""
    final, by_source, by_addr = merge_rewards(
        {"credit_revenue": {_ADDR_A: 1.0}}, dust_threshold=0,
    )
    assert final == {_CK_A: 1.0}
    assert by_source == {"credit_revenue": {_CK_A: 1.0}}
    assert by_addr == {}
