import asyncio

import pytest

from aleph_nodestatus.status import NodesStatus
from aleph_nodestatus.erc20 import DECIMALS
from aleph_nodestatus.settings import settings


@pytest.fixture
def state():
    return NodesStatus(initial_height=100)


def _balance_event(height, balances, platform="ALEPH_ETH", changed=None):
    """Create a balance-update event tuple."""
    if changed is None:
        changed = set(balances.keys())
    return (height, 0.5, ("balance-update", (balances, platform, changed)))


def _staking_event(height, content):
    """Create a staking-update event tuple matching the Aleph message format.

    The state machine expects:
      content["content"]["type"] == post type
      content["content"]["content"] == the action payload
      content["content"]["address"] == sender address
    """
    return (height, 0.5, ("staking-update", content))


def _score_event(height, content):
    """Create a score-update event tuple."""
    return (height, 0.5, ("score-update", content))


def _create_node_msg(item_hash, address):
    """Build a create-node message in the format NodesStatus.process expects."""
    return {
        "sender": address,
        "item_hash": item_hash,
        "time": 1000000,
        "content": {
            "type": settings.node_post_type,
            "address": address,
            "content": {
                "action": "create-node",
            },
        },
    }


def _create_rnode_msg(item_hash, address, crn_address="https://crn.example.com"):
    return {
        "sender": address,
        "item_hash": item_hash,
        "time": 1000000,
        "content": {
            "type": settings.node_post_type,
            "address": address,
            "content": {
                "action": "create-resource-node",
                "address": crn_address,
                "details": {"type": "compute"},
            },
        },
    }


def _stake_msg(item_hash, address, node_hash):
    return {
        "sender": address,
        "item_hash": item_hash,
        "time": 1000000,
        "content": {
            "type": settings.node_post_type,
            "address": address,
            "ref": node_hash,
            "content": {
                "action": "stake",
            },
        },
    }


def _link_msg(item_hash, ccn_owner, crn_hash):
    return {
        "sender": ccn_owner,
        "item_hash": item_hash,
        "time": 1000000,
        "content": {
            "type": settings.node_post_type,
            "address": ccn_owner,
            "ref": crn_hash,
            "content": {
                "action": "link",
            },
        },
    }


def _score_msg(ccn_scores=None, crn_scores=None):
    """Build a score-update message."""
    return {
        "sender": settings.scores_senders[0],
        "content": {
            "type": settings.scores_post_type,
            "address": settings.scores_senders[0],
            "content": {
                "scores": {
                    "ccn": ccn_scores or [],
                    "crn": crn_scores or [],
                },
            },
        },
    }


async def _process_events(state, events):
    """Feed events through the state machine and collect outputs."""
    outputs = []

    async def _iter(evts):
        for e in evts:
            yield e

    async for height, nodes, resource_nodes in state.process([_iter(events)]):
        outputs.append((height, nodes, resource_nodes))
    return outputs


def _run(coro):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


class TestCreateNode:
    def test_create_node_with_sufficient_balance(self, state):
        events = [
            _balance_event(101, {"0xOwner": 200000 * DECIMALS}),
            _staking_event(102, _create_node_msg("nodehash1", "0xOwner")),
        ]
        _run(_process_events(state, events))

        assert "nodehash1" in state.nodes
        assert state.nodes["nodehash1"]["owner"] == "0xOwner"
        assert state.nodes["nodehash1"]["status"] == "waiting"

    def test_create_node_insufficient_balance(self, state):
        events = [
            _balance_event(101, {"0xOwner": 100000 * DECIMALS}),
            _staking_event(102, _create_node_msg("nodehash1", "0xOwner")),
        ]
        _run(_process_events(state, events))

        assert "nodehash1" not in state.nodes

    def test_cannot_create_two_nodes_same_owner(self, state):
        events = [
            _balance_event(101, {"0xOwner": 200000 * DECIMALS}),
            _staking_event(102, _create_node_msg("nodehash1", "0xOwner")),
            _staking_event(103, _create_node_msg("nodehash2", "0xOwner")),
        ]
        _run(_process_events(state, events))

        assert "nodehash1" in state.nodes
        assert "nodehash2" not in state.nodes


class TestStaking:
    def test_stake_activates_node(self, state):
        events = [
            _balance_event(101, {
                "0xOwner": 200000 * DECIMALS,
                "0xStaker": 500000 * DECIMALS,
            }),
            _staking_event(102, _create_node_msg("nodehash1", "0xOwner")),
            _staking_event(103, _stake_msg("stakehash", "0xStaker", "nodehash1")),
        ]
        _run(_process_events(state, events))

        assert state.nodes["nodehash1"]["status"] == "active"
        assert "0xStaker" in state.nodes["nodehash1"]["stakers"]

    def test_stake_below_threshold_rejected(self, state):
        events = [
            _balance_event(101, {
                "0xOwner": 200000 * DECIMALS,
                "0xStaker": 5000 * DECIMALS,
            }),
            _staking_event(102, _create_node_msg("nodehash1", "0xOwner")),
            _staking_event(103, _stake_msg("stakehash", "0xStaker", "nodehash1")),
        ]
        _run(_process_events(state, events))

        assert "0xStaker" not in state.nodes["nodehash1"]["stakers"]

    def test_node_owner_cannot_stake(self, state):
        events = [
            _balance_event(101, {"0xOwner": 500000 * DECIMALS}),
            _staking_event(102, _create_node_msg("nodehash1", "0xOwner")),
            _staking_event(103, _stake_msg("stakehash", "0xOwner", "nodehash1")),
        ]
        _run(_process_events(state, events))

        assert "0xOwner" not in state.nodes["nodehash1"]["stakers"]


class TestNodeRemovalOnBalanceDrop:
    def test_node_removed_when_balance_drops(self, state):
        events = [
            _balance_event(101, {"0xOwner": 200000 * DECIMALS}),
            _staking_event(102, _create_node_msg("nodehash1", "0xOwner")),
            _balance_event(103, {"0xOwner": 100000 * DECIMALS}, changed={"0xOwner"}),
        ]
        _run(_process_events(state, events))

        assert "nodehash1" not in state.nodes
        assert "0xOwner" not in state.address_nodes

    def test_stake_removed_when_staker_balance_drops(self, state):
        events = [
            _balance_event(101, {
                "0xOwner": 200000 * DECIMALS,
                "0xStaker": 500000 * DECIMALS,
            }),
            _staking_event(102, _create_node_msg("nodehash1", "0xOwner")),
            _staking_event(103, _stake_msg("stakehash", "0xStaker", "nodehash1")),
            _balance_event(104, {
                "0xOwner": 200000 * DECIMALS,
                "0xStaker": 5000 * DECIMALS,
            }, changed={"0xStaker"}),
        ]
        _run(_process_events(state, events))

        assert "0xStaker" not in state.nodes["nodehash1"]["stakers"]


class TestLinking:
    def test_link_crn_to_ccn(self, state):
        events = [
            _balance_event(101, {
                "0xCCN_Owner": 200000 * DECIMALS,
                "0xCRN_Owner": 200000 * DECIMALS,
            }),
            _staking_event(102, _create_node_msg("ccn1", "0xCCN_Owner")),
            _staking_event(103, _create_rnode_msg("crn1", "0xCRN_Owner")),
            _staking_event(104, _link_msg("link1", "0xCCN_Owner", "crn1")),
        ]
        _run(_process_events(state, events))

        assert "crn1" in state.nodes["ccn1"]["resource_nodes"]
        assert state.resource_nodes["crn1"]["parent"] == "ccn1"
        assert state.resource_nodes["crn1"]["status"] == "linked"

    def test_cannot_link_to_nonexistent_ccn(self, state):
        events = [
            _balance_event(101, {"0xCRN_Owner": 200000 * DECIMALS}),
            _staking_event(102, _create_rnode_msg("crn1", "0xCRN_Owner")),
            _staking_event(103, _link_msg("link1", "0xNobody", "crn1")),
        ]
        _run(_process_events(state, events))

        assert state.resource_nodes["crn1"]["parent"] is None


class TestScoring:
    def test_score_update_applied(self, state):
        events = [
            _balance_event(101, {"0xOwner": 200000 * DECIMALS}),
            _staking_event(102, _create_node_msg("nodehash1", "0xOwner")),
            _score_event(103, _score_msg(
                ccn_scores=[{
                    "node_id": "nodehash1",
                    "total_score": 0.85,
                    "decentralization": 0.7,
                }]
            )),
        ]
        _run(_process_events(state, events))

        assert state.nodes["nodehash1"]["score"] == pytest.approx(0.85)
        assert state.nodes["nodehash1"]["decentralization"] == pytest.approx(0.7)

    def test_score_clamped_to_one(self, state):
        events = [
            _balance_event(101, {"0xOwner": 200000 * DECIMALS}),
            _staking_event(102, _create_node_msg("nodehash1", "0xOwner")),
            _score_event(103, _score_msg(
                ccn_scores=[{
                    "node_id": "nodehash1",
                    "total_score": 1.5,
                    "decentralization": 2.0,
                }]
            )),
        ]
        _run(_process_events(state, events))

        assert state.nodes["nodehash1"]["score"] == 1.0
        assert state.nodes["nodehash1"]["decentralization"] == 1.0
