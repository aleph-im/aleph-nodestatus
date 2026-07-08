from aleph_nodestatus.slashing import SlashAccumulator, compute_slashing
from aleph_nodestatus.credit_distribution import _apply_expenses_to_snapshots
from aleph_nodestatus.wage_subsidy import compute_subsidy_daily, parse_wage_start

BPD = 7130
DAY = 86400


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


# --- Wage parity (changing weights) -------------------------------------
# 1:1 contract with aleph-api-credit rewards-slashing-wage-parity.test.ts.
# A single eligible CRN active for the first two UTC days of the window then
# inactive (score 0, inactive_since set) for the rest. The per-day wage
# accumulation recovers the active-day wage that a single close-of-window
# snapshot would miss. Both repos assert the SAME golden amounts below.
_T0 = int(parse_wage_start()) + 10 * DAY  # day-aligned, after wage start
_H = 1_000_000
GOLDEN_WAGE_RETRO = 9416.666666666646


def _wage_snaps():
    def rn(score, inact):
        return {"crn1": {"hash": "crn1", "status": "linked", "score": score,
                         "owner": "0xcrn", "reward": "0xcrn",
                         "inactive_since": inact}}
    return [(_T0, {}, rn(1.0, None)), (_T0 + 2 * DAY, {}, rn(0.0, _H))]


def _run_wage(retroactive, close_height):
    snaps = _wage_snaps()
    acc = SlashAccumulator()
    compute_subsidy_daily(_T0, _T0 + 5 * DAY, snaps, web3=_Web3Stub(),
                          accumulator=acc)
    slashed, _meta = compute_slashing(
        acc, snaps[-1][2], close_height,
        enabled_streams=("wage_subsidy",), retroactive=retroactive,
        threshold_days=3, blocks_per_day=BPD,
    )
    return slashed


def test_parity_wage_retroactive_changing_weights():
    slashed = _run_wage(True, _H + 5 * BPD)
    assert slashed.keys() == {"0xcrn"}
    assert abs(slashed["0xcrn"] - GOLDEN_WAGE_RETRO) < 1e-6


def test_parity_wage_from_death_is_empty():
    # CRN earns no wage while inactive, so the post-death share is empty.
    assert _run_wage(False, _H + 5 * BPD) == {}


def test_parity_wage_below_threshold_is_empty():
    assert _run_wage(True, _H + 3 * BPD - 1) == {}
