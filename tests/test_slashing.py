from aleph_nodestatus.slashing import is_slashable
from aleph_nodestatus.slashing import SlashAccumulator
from aleph_nodestatus.slashing import compute_slashing

BLOCKS_PER_DAY = 7130


def test_not_slashable_when_active():
    assert is_slashable(
        None, close_height=1_000_000,
        threshold_days=3, blocks_per_day=BLOCKS_PER_DAY,
    ) is False


def test_not_slashable_below_threshold():
    inactive_since = 1_000_000
    close = inactive_since + 2 * BLOCKS_PER_DAY  # 2 days < 3
    assert is_slashable(
        inactive_since, close_height=close,
        threshold_days=3, blocks_per_day=BLOCKS_PER_DAY,
    ) is False


def test_slashable_at_exact_threshold():
    inactive_since = 1_000_000
    close = inactive_since + 3 * BLOCKS_PER_DAY  # exactly 3 days
    assert is_slashable(
        inactive_since, close_height=close,
        threshold_days=3, blocks_per_day=BLOCKS_PER_DAY,
    ) is True


def test_accumulator_tracks_total_and_post_death():
    acc = SlashAccumulator()
    # Two expenses for the same CRN: first while active, second while inactive.
    acc.add("credit_revenue", "crn1", "0xAddr1", 10.0,
            inactive_at_expense=False, inactive_since=None)
    acc.add("credit_revenue", "crn1", "0xAddr1", 5.0,
            inactive_at_expense=True, inactive_since=999)
    entries = acc.entries()
    e = entries[("credit_revenue", "crn1")]
    assert e["address"] == "0xAddr1"
    assert e["total"] == 15.0
    assert e["post_death"] == 5.0
    assert e["inactive_since"] == 999


def test_accumulator_separates_streams_and_nodes():
    acc = SlashAccumulator()
    acc.add("credit_revenue", "crn1", "0xA", 1.0,
            inactive_at_expense=True, inactive_since=1)
    acc.add("holder_tier", "crn1", "0xA", 2.0,
            inactive_at_expense=True, inactive_since=1)
    acc.add("credit_revenue", "crn2", "0xB", 4.0,
            inactive_at_expense=False, inactive_since=None)
    assert set(acc.entries().keys()) == {
        ("credit_revenue", "crn1"),
        ("holder_tier", "crn1"),
        ("credit_revenue", "crn2"),
    }


BPD = 7130


def _close_rnodes(node_id, inactive_since):
    return {node_id: {"inactive_since": inactive_since}}


def test_compute_slashing_from_death_uses_post_death():
    acc = SlashAccumulator()
    acc.add("credit_revenue", "crn1", "0xA", 10.0,
            inactive_at_expense=False, inactive_since=None)
    acc.add("credit_revenue", "crn1", "0xA", 4.0,
            inactive_at_expense=True, inactive_since=1_000)
    close = _close_rnodes("crn1", 1_000)
    slashed, meta = compute_slashing(
        acc, close, close_height=1_000 + 3 * BPD,
        enabled_streams=("credit_revenue",), retroactive=False,
        threshold_days=3, blocks_per_day=BPD,
    )
    assert slashed == {"0xA": 4.0}
    assert meta == [{"node_id": "crn1", "address": "0xA",
                     "stream": "credit_revenue", "amount": 4.0,
                     "inactive_since": 1_000}]


def test_compute_slashing_retroactive_uses_total():
    acc = SlashAccumulator()
    acc.add("credit_revenue", "crn1", "0xA", 10.0,
            inactive_at_expense=False, inactive_since=None)
    acc.add("credit_revenue", "crn1", "0xA", 4.0,
            inactive_at_expense=True, inactive_since=1_000)
    close = _close_rnodes("crn1", 1_000)
    slashed, _ = compute_slashing(
        acc, close, close_height=1_000 + 3 * BPD,
        enabled_streams=("credit_revenue",), retroactive=True,
        threshold_days=3, blocks_per_day=BPD,
    )
    assert slashed == {"0xA": 14.0}


def test_compute_slashing_skips_disabled_stream():
    acc = SlashAccumulator()
    acc.add("wage_subsidy", "crn1", "0xA", 4.0,
            inactive_at_expense=True, inactive_since=1_000)
    close = _close_rnodes("crn1", 1_000)
    slashed, meta = compute_slashing(
        acc, close, close_height=1_000 + 3 * BPD,
        enabled_streams=("credit_revenue",), retroactive=False,
        threshold_days=3, blocks_per_day=BPD,
    )
    assert slashed == {}
    assert meta == []


def test_compute_slashing_skips_not_slashable():
    acc = SlashAccumulator()
    acc.add("credit_revenue", "crn1", "0xA", 4.0,
            inactive_at_expense=True, inactive_since=1_000)
    close = _close_rnodes("crn1", 1_000)
    slashed, _ = compute_slashing(
        acc, close, close_height=1_000 + 2 * BPD,  # only 2 days
        enabled_streams=("credit_revenue",), retroactive=False,
        threshold_days=3, blocks_per_day=BPD,
    )
    assert slashed == {}


def test_compute_slashing_aggregates_addresses():
    acc = SlashAccumulator()
    acc.add("credit_revenue", "crn1", "0xA", 4.0,
            inactive_at_expense=True, inactive_since=1_000)
    acc.add("holder_tier", "crn2", "0xA", 1.0,
            inactive_at_expense=True, inactive_since=1_000)
    close = {"crn1": {"inactive_since": 1_000},
             "crn2": {"inactive_since": 1_000}}
    slashed, meta = compute_slashing(
        acc, close, close_height=1_000 + 3 * BPD,
        enabled_streams=("credit_revenue", "holder_tier"),
        retroactive=False, threshold_days=3, blocks_per_day=BPD,
    )
    assert slashed == {"0xA": 5.0}
    assert len(meta) == 2


from collections import defaultdict
from aleph_nodestatus.credit_distribution import _distribute_execution_credits


class _Web3Stub:
    @staticmethod
    def to_checksum_address(a):
        return a


def test_execution_path_feeds_accumulator():
    expense = {"credits": [{"amount": 100.0, "node_id": "crn1"}]}
    resource_nodes = {
        "crn1": {"status": "linked", "owner": "0xCRN",
                 "reward": "0xCRN", "inactive_since": 555},
    }
    nodes = {}  # no linked CCN -> CCN/staker shares go unallocated, fine
    rewards = {}
    detailed = defaultdict(lambda: defaultdict(float))
    unalloc = {"crn": defaultdict(float), "ccn": defaultdict(float),
               "staker": defaultdict(float)}
    acc = SlashAccumulator()
    _distribute_execution_credits(
        expense, credit_price_aleph=1.0, nodes=nodes,
        resource_nodes=resource_nodes, rewards=rewards, detailed=detailed,
        unallocated=unalloc, web3=_Web3Stub(),
        ccn_share=0.15, staker_share=0.20, crn_share=0.60,
        accumulator=acc, stream="credit_revenue",
    )
    e = acc.entries()[("credit_revenue", "crn1")]
    assert e["address"] == "0xCRN"
    assert e["total"] == 60.0          # 100 * 0.60
    assert e["post_death"] == 60.0     # inactive_since set -> counted
    assert e["inactive_since"] == 555


from aleph_nodestatus.wage_subsidy import split_subsidy


def test_wage_path_feeds_accumulator():
    # Use a valid EIP-55 checksummed address so get_reward_address resolves it.
    addr = "0xdAC17F958D2ee523a2206206994597C13D831ec7"
    nodes = {}
    resource_nodes = {
        "crn1": {"status": "linked", "owner": addr, "reward": addr,
                 "score": 1.0, "inactive_since": 42},
    }
    acc = SlashAccumulator()
    rewards, _unalloc, _detailed = split_subsidy(
        period_subsidy=300.0, nodes=nodes, resource_nodes=resource_nodes,
        web3=None, accumulator=acc,
    )
    e = acc.entries()[("wage_subsidy", "crn1")]
    assert e["address"] == addr
    assert e["post_death"] > 0          # inactive_since set
    assert e["total"] == e["post_death"]
    assert e["inactive_since"] == 42
