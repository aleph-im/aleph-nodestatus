import asyncio

import pytest

from aleph_nodestatus.credit_distribution import (
    _distribute_expense,
    compute_rewards,
)


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


def test_distribute_expense_populates_detailed_per_component():
    """`detailed` carries per-pool component shares for the audit post."""
    from collections import defaultdict

    nodes = {"n1": _node("n1", 0.9, {"0xS1": 100}, resource_nodes=["r1"])}
    rnodes = {"r1": _rnode("r1", 0.9, "0xCRN1")}
    expense = {
        "credit_price_aleph": 0.001,
        "credits": [{"amount": 1000, "node_id": "r1", "address": "0xU1"}],
    }
    rewards = {}
    detailed = defaultdict(lambda: defaultdict(float))
    _distribute_expense(
        "execution", expense, nodes, rnodes, rewards, detailed=detailed,
        ccn_share=0.15, staker_share=0.20, crn_share=0.60, dev_share=0.05,
    )
    total_aleph = 1000 * 0.001
    assert detailed["0xCRN1"]["execution_crn"]   == pytest.approx(total_aleph * 0.60)
    assert detailed["0xCCN-n1"]["execution_ccn"] == pytest.approx(total_aleph * 0.15)
    assert detailed["0xS1"]["execution_staker"]  == pytest.approx(total_aleph * 0.20)
    # No cross-component bleed
    assert "storage_ccn" not in detailed["0xCCN-n1"]
    assert "execution_crn" not in detailed["0xS1"]


def test_distribute_expense_storage_uses_storage_component_keys():
    """For storage expenses the keys are `storage_ccn` / `storage_staker` and
    the CRN pool is bypassed entirely (storage has no per-CRN share)."""
    from collections import defaultdict

    nodes = {"n1": _node("n1", 0.9, {"0xS1": 100})}
    rnodes = {}
    expense = {
        "credit_price_aleph": 0.001,
        "credits": [{"amount": 1000, "address": "0xU1"}],
    }
    rewards = {}
    detailed = defaultdict(lambda: defaultdict(float))
    _distribute_expense(
        "storage", expense, nodes, rnodes, rewards, detailed=detailed,
        ccn_share=0.75, staker_share=0.20, crn_share=0.0, dev_share=0.05,
    )
    total = 1000 * 0.001
    assert detailed["0xCCN-n1"]["storage_ccn"] == pytest.approx(total * 0.75)
    assert detailed["0xS1"]["storage_staker"]  == pytest.approx(total * 0.20)
    assert all("execution" not in k
               for addr in detailed.values() for k in addr)


def test_compute_rewards_returns_dict_with_two_streams(monkeypatch):
    fake_messages = []  # no expenses
    fake_snapshots = [(100, {}, {})]

    async def fake_fetch_msgs(*a, **kw):
        return fake_messages

    async def fake_fetch_snaps(*a, **kw):
        return fake_snapshots

    monkeypatch.setattr(
        "aleph_nodestatus.credit_distribution._fetch_expense_messages",
        fake_fetch_msgs,
    )
    monkeypatch.setattr(
        "aleph_nodestatus.credit_distribution.fetch_node_snapshots",
        fake_fetch_snaps,
    )

    result = asyncio.run(compute_rewards(
        start_time=1.0, end_time=2.0,
        full_resync=False, include_holder_tier=False,
    ))
    assert "credit_revenue" in result
    assert "holder_tier"    in result
    credit_rewards, credit_totals = result["credit_revenue"]
    holder_rewards, holder_totals = result["holder_tier"]
    assert credit_rewards == {}
    assert holder_rewards == {}
    assert credit_totals["storage_total_aleph"] == 0


def test_compute_rewards_holder_tier_processes_rewards_field(monkeypatch):
    expense = {
        "credit_price_aleph": 0.001,
        "credits": [{"amount": 1000, "node_id": "r1", "address": "0xU1"}],
        "rewards": [{"amount":  500, "node_id": "r1", "address": "0xH1"}],
        "rewards_amount": 500,
        "rewards_count": 1,
    }
    msg = {
        "item_hash": "h1",
        "confirmations": [{"chain": "ETH", "height": 100}],
        "content": {"content": {
            "tags": ["credit_expense", "type_execution"],
            "expense": expense,
        }},
    }

    async def fake_fetch_msgs(*a, **kw): return [msg]
    async def fake_fetch_snaps(*a, **kw):
        return [(50, {"n1": _node("n1", 0.9, {"0xS1": 100},
                                   resource_nodes=["r1"])},
                     {"r1": _rnode("r1", 0.9, "0xCRN1")})]

    monkeypatch.setattr(
        "aleph_nodestatus.credit_distribution._fetch_expense_messages",
        fake_fetch_msgs,
    )
    monkeypatch.setattr(
        "aleph_nodestatus.credit_distribution.fetch_node_snapshots",
        fake_fetch_snaps,
    )

    result = asyncio.run(compute_rewards(
        start_time=1.0, end_time=2.0,
        include_holder_tier=True,
    ))

    credit, credit_totals = result["credit_revenue"]
    holder, holder_totals = result["holder_tier"]

    assert credit_totals["execution_total_aleph"] == pytest.approx(1.0)
    assert holder_totals["execution_total_aleph"] == pytest.approx(0.5)
    assert credit["0xCRN1"] == pytest.approx(0.60)
    assert holder["0xCRN1"] == pytest.approx(0.30)


def test_compute_rewards_exposes_detailed_per_source(monkeypatch):
    """compute_rewards returns a top-level 'detailed' key with per-account
    component breakdowns for credit_revenue and holder_tier streams."""
    expense = {
        "credit_price_aleph": 0.001,
        "credits": [{"amount": 1000, "node_id": "r1", "address": "0xU1"}],
        "rewards": [{"amount":  500, "node_id": "r1", "address": "0xH1"}],
    }
    msg = {
        "item_hash": "h1",
        "confirmations": [{"chain": "ETH", "height": 100}],
        "content": {"content": {
            "tags": ["credit_expense", "type_execution"],
            "expense": expense,
        }},
    }

    async def fake_fetch_msgs(*a, **kw): return [msg]
    async def fake_fetch_snaps(*a, **kw):
        return [(50, {"n1": _node("n1", 0.9, {"0xS1": 100},
                                   resource_nodes=["r1"])},
                     {"r1": _rnode("r1", 0.9, "0xCRN1")})]
    monkeypatch.setattr(
        "aleph_nodestatus.credit_distribution._fetch_expense_messages",
        fake_fetch_msgs,
    )
    monkeypatch.setattr(
        "aleph_nodestatus.credit_distribution.fetch_node_snapshots",
        fake_fetch_snaps,
    )

    result = asyncio.run(compute_rewards(
        start_time=1.0, end_time=2.0,
        include_holder_tier=True,
    ))

    assert "detailed" in result
    rev_detailed = result["detailed"]["credit_revenue"]
    hol_detailed = result["detailed"]["holder_tier"]

    # Each address's per-component shares sum to its `rewards` entry.
    credit, _ = result["credit_revenue"]
    for addr, total in credit.items():
        assert sum(rev_detailed[addr].values()) == pytest.approx(total)
    # CRN earned execution_crn from both streams (rewards/holder_tier path
    # uses the same `_distribute_expense` math via the projection).
    assert "execution_crn" in rev_detailed["0xCRN1"]
    assert "execution_crn" in hol_detailed["0xCRN1"]


def test_compute_rewards_holder_tier_off_ignores_rewards_field(monkeypatch):
    expense = {
        "credit_price_aleph": 0.001,
        "credits": [{"amount": 1000, "node_id": "r1", "address": "0xU1"}],
        "rewards": [{"amount": 500,  "node_id": "r1", "address": "0xH1"}],
    }
    msg = {
        "item_hash": "h1",
        "confirmations": [{"chain": "ETH", "height": 100}],
        "content": {"content": {
            "tags": ["credit_expense", "type_execution"],
            "expense": expense,
        }},
    }
    async def fake_fetch_msgs(*a, **kw): return [msg]
    async def fake_fetch_snaps(*a, **kw):
        return [(50, {"n1": _node("n1", 0.9, {"0xS1": 100},
                                   resource_nodes=["r1"])},
                     {"r1": _rnode("r1", 0.9, "0xCRN1")})]
    monkeypatch.setattr(
        "aleph_nodestatus.credit_distribution._fetch_expense_messages",
        fake_fetch_msgs,
    )
    monkeypatch.setattr(
        "aleph_nodestatus.credit_distribution.fetch_node_snapshots",
        fake_fetch_snaps,
    )
    result = asyncio.run(compute_rewards(
        start_time=1.0, end_time=2.0,
        include_holder_tier=False,
    ))
    assert result["holder_tier"][0] == {}
    assert result["holder_tier"][1]["execution_total_aleph"] == 0
