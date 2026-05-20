"""
Regression test for the link-ordering bug.

Previously `prepare_items` used `random.random()` as the tiebreaker within a block,
so several `corechan-operation` messages confirmed in the same Ethereum block would
be processed in a random order on every run. When a `link` op happened to be merged
ahead of its `create-node`, the link's precondition (`address in self.address_nodes`)
failed and the link was silently dropped — non-deterministically wiping CCN/CRN
linkage on each replay.

The fix: tiebreak deterministically on (message_time, item_hash).
"""
import asyncio

from aleph_nodestatus.status import prepare_items
from aleph_nodestatus.utils import merge


async def _async_iter(items):
    for it in items:
        yield it


# Reproduces the Apicultor incident: 7 corechan-operation messages all at the same
# Ethereum block, ordered correctly by their content.time.
HEIGHT = 24_992_406
APICULTOR_OPS = [
    {"item_hash": "crn2_create",  "time": 1.0, "content": {"time": 1_777_536_288, "address": "0x21801"}},
    {"item_hash": "crni_create",  "time": 1.0, "content": {"time": 1_777_536_353, "address": "0x21801"}},
    {"item_hash": "crn12_create", "time": 1.0, "content": {"time": 1_777_536_630, "address": "0x21801"}},
    {"item_hash": "ccn_create",   "time": 1.0, "content": {"time": 1_777_537_218, "address": "0xd144f"}},
    {"item_hash": "link_crn2",    "time": 1.0, "content": {"time": 1_777_537_490, "address": "0xd144f"}},
    {"item_hash": "link_crni",    "time": 1.0, "content": {"time": 1_777_537_503, "address": "0xd144f"}},
    {"item_hash": "link_crn12",   "time": 1.0, "content": {"time": 1_777_537_526, "address": "0xd144f"}},
]


async def _drain():
    """Reproduces the real call site: each message comes from its own
    iterator-window, all merged together. With N>1 iterators sharing a height,
    the heap-merge tiebreaker decides ordering."""
    iterators = [
        prepare_items("staking-update", _async_iter([(HEIGHT, msg)]))
        for msg in APICULTOR_OPS
    ]
    return [item for _, _, (_, item) in
            [tup async for tup in merge(*iterators)]]


def test_prepare_items_orders_deterministically_within_block():
    """Same input yields the same output on every run."""
    runs = [asyncio.run(_drain()) for _ in range(20)]
    first = [m["item_hash"] for m in runs[0]]
    for r in runs[1:]:
        assert [m["item_hash"] for m in r] == first, (
            "prepare_items + merge produced different orderings across runs — "
            "tiebreaker is not deterministic"
        )


def test_prepare_items_orders_create_before_link_within_block():
    """The CCN's create-node must be merged before any link op referencing it,
    even though they share an Ethereum block. Determinism alone isn't enough —
    the order must respect message content.time."""
    out = asyncio.run(_drain())
    order = [m["item_hash"] for m in out]
    ccn_idx = order.index("ccn_create")
    for link in ("link_crn2", "link_crni", "link_crn12"):
        assert order.index(link) > ccn_idx, (
            f"{link} merged before ccn_create — link precondition would fail "
            f"and the linkage would be silently dropped"
        )
