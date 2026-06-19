from aleph_nodestatus.slashing import SlashAccumulator, compute_slashing
from aleph_nodestatus.credit_distribution import _apply_expenses_to_snapshots

BPD = 7130


class _Web3Stub:
    @staticmethod
    def to_checksum_address(a):
        return a


def test_parity_retroactive_credit_revenue():
    """1:1 contract with aleph-api-credit rewards-slashing-parity.test.ts:
    identical fixture -> identical slashed {"0xcrn": 60.0}."""
    inactive_since = 1000
    close_height = inactive_since + 3 * BPD
    resource_nodes = {
        "crn1": {"hash": "crn1", "status": "linked", "owner": "0xcrn",
                 "reward": "0xcrn", "score": 1.0,
                 "inactive_since": inactive_since},
    }
    nodes = {}
    snapshots = [(1_700_000_000, nodes, resource_nodes)]
    expense = {
        "credit_price_aleph": 1.0,
        "credits": [{"amount": 100.0, "node_id": "crn1"}],
        "hold": [],
    }
    parsed = [(1_700_000_000, "execution", expense)]

    acc = SlashAccumulator()
    _apply_expenses_to_snapshots(
        parsed, snapshots, False, _Web3Stub(), accumulator=acc,
    )
    slashed, meta = compute_slashing(
        acc, snapshots[-1][2], close_height,
        enabled_streams=("credit_revenue",), retroactive=True,
        threshold_days=3, blocks_per_day=BPD,
    )
    assert slashed == {"0xcrn": 60.0}
