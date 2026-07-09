"""get_reward_address must never hand an unvalidated address string to the
rewards dict: a garbage `reward` field would otherwise flow through the
rewards merge into batchTransfer. Invalid reward falls back to owner; if
neither parses the function returns None and callers skip the entity."""
from collections import defaultdict

import pytest

from aleph_nodestatus.credit_distribution import (
    UNALLOCATED_NO_ACTIVE_CCNS,
    _distribute_execution_credits,
    _distribute_storage_pools,
)
from aleph_nodestatus.utils import get_reward_address
from aleph_nodestatus.wage_subsidy import split_subsidy

VALID = "0xdAC17F958D2ee523a2206206994597C13D831ec7"
OTHER = "0x6B175474E89094C44Da98b954EedeAC495271d0F"


class _IdentityWeb3:
    """Test seam mirroring the _Web3Stub used across the suite: symbolic
    addresses pass through unvalidated."""
    @staticmethod
    def to_checksum_address(a):
        return a


def _empty_unalloc():
    return {"crn": defaultdict(float), "ccn": defaultdict(float),
            "staker": defaultdict(float)}


# --- unit: get_reward_address ---------------------------------------------

def test_valid_reward_is_checksummed():
    entity = {"reward": VALID.lower(), "owner": OTHER}
    assert get_reward_address(entity) == VALID


def test_invalid_reward_falls_back_to_owner():
    entity = {"reward": "not-an-address", "owner": VALID.lower()}
    assert get_reward_address(entity) == VALID


def test_invalid_reward_and_owner_returns_none():
    entity = {"reward": "0xCRN", "owner": "0xOWNER"}
    assert get_reward_address(entity) is None


def test_web3_checksum_is_used_when_provided():
    entity = {"reward": "0xSYMBOLIC", "owner": "0xSYMBOLIC"}
    assert get_reward_address(entity, _IdentityWeb3()) == "0xSYMBOLIC"


# --- call sites: invalid addresses must not reach the rewards dict --------

def test_execution_crn_share_unallocated_when_address_invalid():
    expense = {"credits": [{"amount": 100.0, "node_id": "crn1"}]}
    resource_nodes = {
        "crn1": {"status": "linked", "owner": "0xCRN", "reward": "0xCRN"},
    }
    rewards = {}
    unalloc = _empty_unalloc()
    _distribute_execution_credits(
        expense, credit_price_aleph=1.0, nodes={},
        resource_nodes=resource_nodes, rewards=rewards,
        detailed=defaultdict(lambda: defaultdict(float)),
        unallocated=unalloc, web3=None,
        ccn_share=0.15, staker_share=0.20, crn_share=0.60,
    )
    assert "0xCRN" not in rewards
    assert unalloc["crn"]["crn1"] == pytest.approx(60.0)


def test_execution_ccn_share_unallocated_but_stakers_paid_when_ccn_invalid():
    expense = {"credits": [{"amount": 100.0, "node_id": "crn1"}]}
    resource_nodes = {
        "crn1": {"status": "linked", "owner": VALID, "reward": VALID},
    }
    nodes = {
        "ccn1": {"status": "active", "owner": "0xCCN", "reward": "0xCCN",
                 "resource_nodes": ["crn1"], "stakers": {OTHER: 100.0}},
    }
    rewards = {}
    unalloc = _empty_unalloc()
    _distribute_execution_credits(
        expense, credit_price_aleph=1.0, nodes=nodes,
        resource_nodes=resource_nodes, rewards=rewards,
        detailed=defaultdict(lambda: defaultdict(float)),
        unallocated=unalloc, web3=None,
        ccn_share=0.15, staker_share=0.20, crn_share=0.60,
    )
    assert "0xCCN" not in rewards
    assert unalloc["ccn"]["crn1"] == pytest.approx(15.0)
    # The stakers' own addresses are valid: their share is still paid.
    assert rewards[OTHER] == pytest.approx(20.0)
    assert rewards[VALID] == pytest.approx(60.0)


def test_storage_pool_skips_ccn_with_invalid_address():
    nodes = {
        "ccn1": {"status": "active", "score": 1.0, "owner": "0xCCN",
                 "reward": "0xCCN", "stakers": {OTHER: 100.0}},
    }
    rewards = {}
    unalloc = _empty_unalloc()
    _distribute_storage_pools(
        100.0, nodes, rewards,
        defaultdict(lambda: defaultdict(float)), unalloc, None,
        ccn_share=0.15, staker_share=0.20,
    )
    assert "0xCCN" not in rewards
    # Only CCN with an unresolvable address -> both pools unallocated.
    assert unalloc["ccn"][UNALLOCATED_NO_ACTIVE_CCNS] == pytest.approx(15.0)
    assert unalloc["staker"][UNALLOCATED_NO_ACTIVE_CCNS] == pytest.approx(20.0)


def test_wage_subsidy_skips_nodes_with_invalid_address():
    resource_nodes = {
        "crn1": {"status": "linked", "owner": "0xCRN", "reward": "0xCRN",
                 "score": 1.0},
    }
    rewards, unalloc, _detailed = split_subsidy(
        period_subsidy=300.0, nodes={}, resource_nodes=resource_nodes,
        web3=None,
    )
    assert "0xCRN" not in rewards
    # All three pools end up unallocated: no CCNs, no stakers, and the only
    # CRN has an unresolvable address.
    assert unalloc == pytest.approx(300.0)
