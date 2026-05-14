import asyncio

import pytest

from aleph_nodestatus.credit_distribution import (
    UNALLOCATED_MISSING_NODE_ID,
    UNALLOCATED_NO_ACTIVE_CCNS,
    _distribute_expense,
    _empty_unallocated,
    compute_rewards,
)


def _node(hash, score, stakers, resource_nodes=None, status="active"):
    return {
        "hash": hash, "status": status, "score": score,
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


def test_distribute_expense_unallocated_tracks_orphan_crn():
    """A credit's node_id that's absent from resource_nodes AND not linked
    to any CCN loses all three slices (CRN, CCN, staker). Under the new
    per-credit attribution rules nothing falls back to the global pools."""
    nodes = {"n1": _node("n1", 0.9, {"0xS1": 100})}  # n1 does NOT link r-ghost
    rnodes = {}                                      # r-ghost is absent
    expense = {
        "credit_price_aleph": 0.001,
        "credits": [{"amount": 1000, "node_id": "r-ghost", "address": "0xU1"}],
    }
    rewards = {}
    unallocated = _empty_unallocated()
    _distribute_expense(
        "execution", expense, nodes, rnodes, rewards,
        unallocated=unallocated,
        ccn_share=0.15, staker_share=0.20, crn_share=0.60, dev_share=0.05,
    )
    total = 1000 * 0.001
    # All three shares lost — CCN/staker no longer fall back to the global
    # pools, they're per-credit and the link is missing.
    assert dict(unallocated["crn"])    == {"r-ghost": pytest.approx(total * 0.60)}
    assert dict(unallocated["ccn"])    == {"r-ghost": pytest.approx(total * 0.15)}
    assert dict(unallocated["staker"]) == {"r-ghost": pytest.approx(total * 0.20)}
    # Nothing is paid out — only the 5% dev fund (untracked here) remains.
    assert rewards == {}


def test_distribute_expense_unallocated_buckets_missing_node_id():
    """A credit with no node_id at all goes under the sentinel bucket in
    all three drop categories."""
    nodes = {"n1": _node("n1", 0.9, {"0xS1": 100})}
    rnodes = {"r1": _rnode("r1", 0.9, "0xCRN1")}
    expense = {
        "credit_price_aleph": 0.001,
        "credits": [{"amount": 1000, "address": "0xU1"}],   # no node_id
    }
    rewards = {}
    unallocated = _empty_unallocated()
    _distribute_expense(
        "execution", expense, nodes, rnodes, rewards,
        unallocated=unallocated,
        ccn_share=0.15, staker_share=0.20, crn_share=0.60, dev_share=0.05,
    )
    total = 1000 * 0.001
    assert dict(unallocated["crn"]) == {
        UNALLOCATED_MISSING_NODE_ID: pytest.approx(total * 0.60),
    }
    assert dict(unallocated["ccn"]) == {
        UNALLOCATED_MISSING_NODE_ID: pytest.approx(total * 0.15),
    }
    assert dict(unallocated["staker"]) == {
        UNALLOCATED_MISSING_NODE_ID: pytest.approx(total * 0.20),
    }


def test_distribute_expense_unallocated_empty_when_all_match():
    """Happy path: every credit's CRN matches AND has an active linked CCN
    with at least one staker — every share pays out, unallocated stays
    empty across all three buckets."""
    nodes = {"n1": _node("n1", 0.9, {"0xS1": 100}, resource_nodes=["r1"])}
    rnodes = {"r1": _rnode("r1", 0.9, "0xCRN1")}
    expense = {
        "credit_price_aleph": 0.001,
        "credits": [{"amount": 1000, "node_id": "r1", "address": "0xU1"}],
    }
    rewards = {}
    unallocated = _empty_unallocated()
    _distribute_expense(
        "execution", expense, nodes, rnodes, rewards,
        unallocated=unallocated,
        ccn_share=0.15, staker_share=0.20, crn_share=0.60, dev_share=0.05,
    )
    assert dict(unallocated["crn"])    == {}
    assert dict(unallocated["ccn"])    == {}
    assert dict(unallocated["staker"]) == {}


def test_distribute_expense_execution_linked_ccn_attribution():
    """Two credits hosted on different CRNs attribute CCN + staker shares
    to their respective LINKED CCN (per-credit), not the global CCN pool.
    This is the key behavioural change vs the pre-v2 rules."""
    nodes = {
        "ccn-a": _node("ccn-a", 0.9, {"0xSa": 100}, resource_nodes=["r-a"]),
        "ccn-b": _node("ccn-b", 0.5, {"0xSb":  50}, resource_nodes=["r-b"]),
    }
    rnodes = {
        "r-a": _rnode("r-a", 0.8, "0xCRN-a"),
        "r-b": _rnode("r-b", 0.8, "0xCRN-b"),
    }
    expense = {
        "credit_price_aleph": 0.001,
        "credits": [
            {"amount": 1000, "node_id": "r-a"},   # 1.0 ALEPH on ccn-a
            {"amount": 2000, "node_id": "r-b"},   # 2.0 ALEPH on ccn-b
        ],
    }
    rewards = {}
    _distribute_expense(
        "execution", expense, nodes, rnodes, rewards,
        ccn_share=0.15, staker_share=0.20, crn_share=0.60, dev_share=0.05,
    )
    # CRN — per-credit to its host.
    assert rewards["0xCRN-a"] == pytest.approx(1.0 * 0.60)
    assert rewards["0xCRN-b"] == pytest.approx(2.0 * 0.60)
    # CCN — per-credit to LINKED CCN only. ccn-a's earning is from its
    # own credit (1 ALEPH * 0.15), NOT a global score-weighted share.
    assert rewards["0xCCN-ccn-a"] == pytest.approx(1.0 * 0.15)
    assert rewards["0xCCN-ccn-b"] == pytest.approx(2.0 * 0.15)
    # Stakers — to stakers of THEIR CCN only. 0xSa is the sole staker of
    # ccn-a so gets the full per-credit staker slice from credits routed
    # via r-a; 0xSb similarly for r-b. Stakes on the other CCN don't
    # contribute to this credit's distribution.
    assert rewards["0xSa"] == pytest.approx(1.0 * 0.20)
    assert rewards["0xSb"] == pytest.approx(2.0 * 0.20)


def test_distribute_expense_execution_inactive_linked_ccn_drops_shares():
    """If a CRN's linked CCN exists but is not active, the CCN + staker
    shares of that credit are dropped (the CRN share still pays out)."""
    nodes = {
        "n1": _node("n1", 0.9, {"0xS1": 100},
                    resource_nodes=["r1"], status="active"),
        "n2": _node("n2", 0.9, {"0xS2": 100},
                    resource_nodes=["r2"], status="idle"),       # inactive
    }
    rnodes = {
        "r1": _rnode("r1", 0.8, "0xCRN1"),
        "r2": _rnode("r2", 0.8, "0xCRN2"),
    }
    expense = {
        "credit_price_aleph": 0.001,
        "credits": [{"amount": 1000, "node_id": "r2"}],   # hosted by inactive CCN
    }
    rewards = {}
    unallocated = _empty_unallocated()
    _distribute_expense(
        "execution", expense, nodes, rnodes, rewards,
        unallocated=unallocated,
        ccn_share=0.15, staker_share=0.20, crn_share=0.60, dev_share=0.05,
    )
    # CRN still gets paid (the CRN itself isn't gated on the CCN's status).
    assert rewards == {"0xCRN2": pytest.approx(0.60)}
    # CCN + staker for the credit go to unallocated under the CRN node_id.
    assert dict(unallocated["ccn"])    == {"r2": pytest.approx(0.15)}
    assert dict(unallocated["staker"]) == {"r2": pytest.approx(0.20)}


def test_distribute_expense_execution_linked_ccn_no_stakers_drops_staker_share():
    """Linked CCN exists and is active, but has no stakers. CCN gets its
    share; the staker portion goes to unallocated under the CCN hash."""
    nodes = {
        "ccn-empty": _node("ccn-empty", 0.9, {},               # no stakers
                            resource_nodes=["r1"]),
    }
    rnodes = {"r1": _rnode("r1", 0.8, "0xCRN1")}
    expense = {
        "credit_price_aleph": 0.001,
        "credits": [{"amount": 1000, "node_id": "r1"}],
    }
    rewards = {}
    unallocated = _empty_unallocated()
    _distribute_expense(
        "execution", expense, nodes, rnodes, rewards,
        unallocated=unallocated,
        ccn_share=0.15, staker_share=0.20, crn_share=0.60, dev_share=0.05,
    )
    assert rewards["0xCRN1"]           == pytest.approx(0.60)
    assert rewards["0xCCN-ccn-empty"]  == pytest.approx(0.15)
    assert dict(unallocated["crn"])    == {}
    assert dict(unallocated["ccn"])    == {}
    assert dict(unallocated["staker"]) == {"ccn-empty": pytest.approx(0.20)}


def test_distribute_expense_storage_per_ccn_staker_pool():
    """Storage staker pool is sliced per-CCN by score weight, then
    distributed within each CCN proportional to stake on that CCN —
    NOT pooled globally. A staker on a high-score CCN earns more than
    one on a mid-score CCN even with the same stake."""
    nodes = {
        "ccn-a": _node("ccn-a", 0.9, {"0xSa": 100}),  # score multiplier 1.0
        "ccn-b": _node("ccn-b", 0.4, {"0xSb": 100}),  # score multiplier 0.333
    }
    rnodes = {}
    expense = {
        "credit_price_aleph": 0.001,
        "credits": [{"amount": 1000, "address": "0xU1"}],
    }
    rewards = {}
    _distribute_expense(
        "storage", expense, nodes, rnodes, rewards,
        ccn_share=0.75, staker_share=0.20, crn_share=0.0, dev_share=0.05,
    )
    total       = 1.0
    mult_a      = 1.0
    mult_b      = (0.4 - 0.2) / 0.6              # 0.3333…
    total_score = mult_a + mult_b                # 1.3333…
    weight_a    = mult_a / total_score
    weight_b    = mult_b / total_score

    # CCN reward — unchanged: score-weighted slice of the CCN pool.
    assert rewards["0xCCN-ccn-a"] == pytest.approx(total * 0.75 * weight_a)
    assert rewards["0xCCN-ccn-b"] == pytest.approx(total * 0.75 * weight_b)
    # Stakers — each gets the FULL per-CCN staker sub-pool (they're the
    # only staker on their CCN). Under the OLD global rules both would
    # have gotten total*0.20*0.5 (equal stake). Under the new rules the
    # high-score CCN's staker earns ~3x more.
    assert rewards["0xSa"] == pytest.approx(total * 0.20 * weight_a)
    assert rewards["0xSb"] == pytest.approx(total * 0.20 * weight_b)


def test_distribute_expense_storage_ccn_with_no_stakers_drops_staker_pool():
    """A CCN that earns CCN share but has zero stakers leaks its staker
    sub-pool — accounted to `unallocated["staker"][ccn_hash]`."""
    nodes = {
        "ccn-a": _node("ccn-a", 0.9, {"0xS1": 100}),   # has stakers
        "ccn-b": _node("ccn-b", 0.9, {}),              # no stakers
    }
    rnodes = {}
    expense = {
        "credit_price_aleph": 0.001,
        "credits": [{"amount": 1000, "address": "0xU1"}],
    }
    rewards = {}
    unallocated = _empty_unallocated()
    _distribute_expense(
        "storage", expense, nodes, rnodes, rewards,
        unallocated=unallocated,
        ccn_share=0.75, staker_share=0.20, crn_share=0.0, dev_share=0.05,
    )
    # ccn-a and ccn-b each have score multiplier 1.0 → 50/50 weight.
    assert rewards["0xCCN-ccn-a"] == pytest.approx(1.0 * 0.75 * 0.5)
    assert rewards["0xCCN-ccn-b"] == pytest.approx(1.0 * 0.75 * 0.5)
    # 0xS1 gets ccn-a's full staker sub-pool (only staker there).
    assert rewards["0xS1"] == pytest.approx(1.0 * 0.20 * 0.5)
    # ccn-b's staker sub-pool is unallocated.
    assert dict(unallocated["staker"]) == {
        "ccn-b": pytest.approx(1.0 * 0.20 * 0.5),
    }


def test_distribute_expense_storage_no_active_ccns_drops_everything():
    """If there are no active CCNs at all, both the CCN pool and the
    staker pool go to unallocated under the no-active-CCNs sentinel."""
    nodes = {
        "ccn-a": _node("ccn-a", 0.9, {"0xS1": 100}, status="idle"),  # not active
    }
    rnodes = {}
    expense = {
        "credit_price_aleph": 0.001,
        "credits": [{"amount": 1000, "address": "0xU1"}],
    }
    rewards = {}
    unallocated = _empty_unallocated()
    _distribute_expense(
        "storage", expense, nodes, rnodes, rewards,
        unallocated=unallocated,
        ccn_share=0.75, staker_share=0.20, crn_share=0.0, dev_share=0.05,
    )
    total = 1.0
    assert rewards == {}
    assert dict(unallocated["ccn"]) == {
        UNALLOCATED_NO_ACTIVE_CCNS: pytest.approx(total * 0.75),
    }
    assert dict(unallocated["staker"]) == {
        UNALLOCATED_NO_ACTIVE_CCNS: pytest.approx(total * 0.20),
    }


def test_compute_rewards_surfaces_unallocated_in_totals(monkeypatch):
    """The audit post `credit_revenue_totals` gets `unallocated_crn_aleph`
    (sum) and `unallocated_crn_by_node_id` ({node_id: amount}) so operators
    can see how much CRN reward was lost and to which node ids it referred."""
    expense = {
        "credit_price_aleph": 0.001,
        # Two credits: one points to a real CRN, one to a ghost.
        "credits": [
            {"amount": 1000, "node_id": "r1",      "address": "0xU1"},
            {"amount": 500,  "node_id": "r-ghost", "address": "0xU2"},
        ],
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

    _, credit_totals = result["credit_revenue"]
    # The "r-ghost" credit is 500 * 0.001 = 0.5 ALEPH. Under the new
    # per-credit attribution rules, an unknown node_id drops ALL THREE
    # shares (CRN + CCN + staker) because the CCN+staker portions are
    # attributed to the linked CCN of that specific CRN.
    assert credit_totals["unallocated_crn_aleph"]    == pytest.approx(0.30)  # 60%
    assert credit_totals["unallocated_ccn_aleph"]    == pytest.approx(0.075) # 15%
    assert credit_totals["unallocated_staker_aleph"] == pytest.approx(0.10)  # 20%
    assert credit_totals["unallocated_aleph"]        == pytest.approx(0.475)
    assert credit_totals["unallocated_crn_by_node_id"] == {
        "r-ghost": pytest.approx(0.30),
    }
    assert credit_totals["unallocated_ccn_by_id"] == {
        "r-ghost": pytest.approx(0.075),
    }
    assert credit_totals["unallocated_staker_by_id"] == {
        "r-ghost": pytest.approx(0.10),
    }


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


def test_compute_rewards_holder_tier_processes_hold_field(monkeypatch):
    expense = {
        "credit_price_aleph": 0.001,
        "credits": [{"amount": 1000, "node_id": "r1", "address": "0xU1"}],
        "hold":    [{"amount":  500, "node_id": "r1", "address": "0xH1"}],
        "hold_amount": 500,
        "hold_count": 1,
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
        "hold":    [{"amount":  500, "node_id": "r1", "address": "0xH1"}],
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


def test_compute_rewards_holder_tier_off_ignores_hold_field(monkeypatch):
    expense = {
        "credit_price_aleph": 0.001,
        "credits": [{"amount": 1000, "node_id": "r1", "address": "0xU1"}],
        "hold":    [{"amount": 500,  "node_id": "r1", "address": "0xH1"}],
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
