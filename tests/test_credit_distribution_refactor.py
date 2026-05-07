import pytest

from aleph_nodestatus.credit_distribution import _distribute_expense


def _node(hash, score, stakers, resource_nodes=None):
    return {
        "hash": hash, "status": "active", "score": score,
        "owner": f"0xCCN-{hash}", "reward": None,
        "stakers": stakers, "resource_nodes": resource_nodes or [],
        "has_bonus": False, "decentralization": 0.5,
    }


def _rnode(hash, score, owner):
    return {
        "hash": hash, "status": "linked", "score": score,
        "owner": owner, "reward": None, "decentralization": 0.5,
    }


def test_distribute_expense_execution_split():
    nodes = {"n1": _node("n1", 0.9, {"0xS1": 100}, resource_nodes=["r1"])}
    rnodes = {"r1": _rnode("r1", 0.9, "0xCRN1")}
    expense = {
        "credit_price_aleph": 0.001,
        "credits": [{"amount": 1000, "node_id": "r1", "address": "0xU1"}],
    }
    rewards = {}
    storage, execution, dev = _distribute_expense(
        "execution", expense, nodes, rnodes, rewards,
        ccn_share=0.15, staker_share=0.20, crn_share=0.60, dev_share=0.05,
    )
    total_aleph = 1000 * 0.001
    assert execution == pytest.approx(total_aleph)
    assert storage == 0
    assert dev == pytest.approx(total_aleph * 0.05)
    assert rewards["0xCRN1"] == pytest.approx(total_aleph * 0.60)
    assert rewards["0xCCN-n1"] == pytest.approx(total_aleph * 0.15)
    assert rewards["0xS1"] == pytest.approx(total_aleph * 0.20)
