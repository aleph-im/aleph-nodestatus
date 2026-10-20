import asyncio

import pytest

from aleph_nodestatus.credit_distribution import (
    _distribute_expense,
    compute_rewards,
)

HOUR = 3600


def _patch(monkeypatch, periods, snapshots):
    """Patch revenue fetch, snapshot fetch and the ts -> height mapping
    (fixed 12 s blocks)."""
    async def fake_fetch_revenue(*a, **kw):
        return periods

    async def fake_fetch_snaps(*a, **kw):
        return snapshots

    monkeypatch.setattr(
        "aleph_nodestatus.credit_distribution.fetch_revenue",
        fake_fetch_revenue,
    )
    monkeypatch.setattr(
        "aleph_nodestatus.credit_distribution.fetch_node_snapshots",
        fake_fetch_snaps,
    )
    monkeypatch.setattr(
        "aleph_nodestatus.credit_distribution.HeightResolver.height_at_or_before",
        lambda self, ts: ts // 12,
    )
    monkeypatch.setattr(
        "aleph_nodestatus.credit_distribution.get_web3", lambda: object(),
    )


def _period(period, nodes, price="0.001", storage=0):
    return {
        "period": period, "credit_price_aleph": price, "nodes": nodes,
        "storage": storage,
        "execution_total": sum(n["execution"] for n in nodes.values()),
    }


def _snap(height, owner):
    return (height,
            {"n1": _node("n1", 0.9, {"0xS1": 100}, resource_nodes=["r1"])},
            {"r1": _rnode("r1", 0.9, owner)})


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


def test_compute_rewards_returns_dict_with_two_streams(monkeypatch):
    _patch(monkeypatch, [], [(100, {}, {})])

    result = asyncio.run(compute_rewards(
        start_time=HOUR, end_time=2 * HOUR,
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
    _patch(
        monkeypatch,
        [_period(HOUR, {"r1": {"execution": 1000, "hold_equivalent": 500}})],
        [_snap(50, "0xCRN1")],
    )

    result = asyncio.run(compute_rewards(
        start_time=HOUR, end_time=2 * HOUR,
        include_holder_tier=True,
    ))

    credit, credit_totals = result["credit_revenue"]
    holder, holder_totals = result["holder_tier"]

    assert credit_totals["execution_total_aleph"] == pytest.approx(1.0)
    assert holder_totals["execution_total_aleph"] == pytest.approx(0.5)
    assert credit["0xCRN1"] == pytest.approx(0.60)
    assert holder["0xCRN1"] == pytest.approx(0.30)


def test_holder_tier_zero_when_flag_off(monkeypatch):
    _patch(
        monkeypatch,
        [_period(HOUR, {"r1": {"execution": 1000, "hold_equivalent": 500}})],
        [_snap(50, "0xCRN1")],
    )
    result = asyncio.run(compute_rewards(
        start_time=HOUR, end_time=2 * HOUR,
        include_holder_tier=False,
    ))
    assert result["holder_tier"][0] == {}
    assert result["holder_tier"][1]["execution_total_aleph"] == 0
    assert result["credit_revenue"][1]["execution_total_aleph"] == pytest.approx(1.0)


def test_period_between_snapshots_uses_earlier_snapshot(monkeypatch):
    # period HOUR*100 -> height (360000+3600)//12 = 30300, strictly between
    # the snapshots at 30000 and 31000: the earlier one (owner 0xOLD) applies.
    ts = 100 * HOUR
    assert 30000 < (ts + HOUR) // 12 < 31000
    _patch(
        monkeypatch,
        [_period(ts, {"r1": {"execution": 1000, "hold_equivalent": 0}})],
        [_snap(30000, "0xOLD"), _snap(31000, "0xNEW")],
    )
    result = asyncio.run(compute_rewards(
        start_time=ts, end_time=ts + HOUR,
    ))
    credit, _ = result["credit_revenue"]
    assert credit["0xOLD"] == pytest.approx(0.60)
    assert "0xNEW" not in credit


def test_compute_rewards_requires_web3(monkeypatch):
    _patch(monkeypatch, [], [(100, {}, {})])

    def no_web3():
        raise RuntimeError("no RPC")

    monkeypatch.setattr(
        "aleph_nodestatus.credit_distribution.get_web3", no_web3,
    )
    with pytest.raises(ValueError, match="web3 is required"):
        asyncio.run(compute_rewards(start_time=HOUR, end_time=2 * HOUR))


def test_compute_rewards_empty_revenue_warns_once(monkeypatch, caplog):
    import logging
    _patch(
        monkeypatch,
        [_period(HOUR, {"r1": {"execution": 0, "hold_equivalent": 0}})],
        [(100, {}, {})],
    )
    with caplog.at_level(logging.WARNING):
        result = asyncio.run(compute_rewards(
            start_time=HOUR, end_time=2 * HOUR,
        ))
    assert result["credit_revenue"][0] == {}
    warnings = [r for r in caplog.records
                if "No credit revenue" in r.getMessage()]
    assert len(warnings) == 1
