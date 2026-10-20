import math
from types import SimpleNamespace

from aleph_nodestatus.credit_distribution import (
    HeightResolver,
    revenue_to_expenses,
)


def _period(period, nodes=None, storage=0, price="0.5"):
    return {
        "period": period,
        "credit_price_aleph": price,
        "nodes": nodes or {},
        "storage": storage,
        "execution_total": sum(n["execution"] for n in (nodes or {}).values()),
    }


def test_adapter_builds_execution_and_storage_tuples():
    periods = [
        _period(
            7200,
            nodes={"a": {"execution": 10, "hold_equivalent": 0},
                   "b": {"execution": 0, "hold_equivalent": 0}},
            storage=5,
            price="0.25",
        )
    ]
    out = revenue_to_expenses(periods, lambda ts: ts // 10)
    assert out == [
        (1080, "execution", {
            "credit_price_aleph": 0.25,
            "credits": [{"amount": 10, "node_id": "a"}],
            "rewards": [],
        }),
        (1080, "storage", {
            "credit_price_aleph": 0.25,
            "credits": [{"amount": 5}],
        }),
    ]


def test_adapter_skips_empty_periods():
    periods = [_period(0), _period(3600, nodes={"a": {"execution": 0, "hold_equivalent": 0}})]
    assert revenue_to_expenses(periods, lambda ts: ts) == []


def test_adapter_hold_equivalent_lands_in_rewards():
    periods = [
        _period(0, nodes={"a": {"execution": 3, "hold_equivalent": 7},
                          "b": {"execution": 0, "hold_equivalent": 9}}),
    ]
    ((h, kind, exp),) = revenue_to_expenses(periods, lambda ts: 42)
    assert (h, kind) == (42, "execution")
    assert exp["credits"] == [{"amount": 3, "node_id": "a"}]
    assert exp["rewards"] == [
        {"amount": 7, "node_id": "a"},
        {"amount": 9, "node_id": "b"},
    ]


class FakeWeb3:
    def __init__(self, n_blocks=1_000_000, t0=1_000_000_000, step=12):
        self.calls = 0
        self.t0, self.step = t0, step
        self.eth = SimpleNamespace(
            block_number=n_blocks - 1, get_block=self._get_block
        )

    def _get_block(self, n):
        self.calls += 1
        return SimpleNamespace(timestamp=self.t0 + n * self.step)


def test_height_resolver_is_monotonic_and_cached():
    w3 = FakeWeb3()
    r = HeightResolver(w3)
    bound = math.ceil(math.log2(w3.eth.block_number + 1)) + 2

    heights = []
    for k in (100, 5000, 5001, 600000):
        ts = w3.t0 + k * 12 + 5
        before = w3.calls
        h = r.height_at_or_before(ts)
        assert w3.calls - before <= bound
        assert h == k
        heights.append(h)
    assert heights == sorted(heights)

    before = w3.calls
    assert r.height_at_or_before(w3.t0 + 100 * 12 + 5) == 100
    assert w3.calls == before

    # exact timestamp hit, and ts before genesis-ish / beyond head
    assert r.height_at_or_before(w3.t0 + 7 * 12) == 7
    assert r.height_at_or_before(w3.t0 + 10**12) == w3.eth.block_number
