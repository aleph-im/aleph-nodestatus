"""Integration tests for the CRN-inactivity slashing wiring inside
`process_credit_distribution`.

The pure slashing math lives in `slashing.py` (covered by test_slashing.py).
Here we verify the orchestrator glue:

  - when slashing is enabled, `compute_slashing`'s output is withheld from the
    on-chain payout (via `_payout_after_slash`) but the published post keeps
    `rewards = final_rewards` intact and carries `slashed` + `slashed_meta`;
  - when `settings.credit_dist_slash_enabled` is False, the run is a no-op for
    slashing: `slashed == {}`, `slashed_meta.enabled is False`, and the payout
    equals the full rewards (no withholding).

The harness mirrors `patch_orchestrator` in test_credit_dist_safety.py: web3,
DBs, the last-distribution lookup, the witness check, the snapshot fetch and
`merge_rewards` are all stubbed so the function runs without real I/O.
"""

import sys
import types

# plyvel stub — mirrors test_credit_dist_safety. commands.py imports storage
# (which imports plyvel) at module load time; the native lib isn't needed.
if "plyvel" not in sys.modules:
    _plyvel_stub = types.ModuleType("plyvel")
    _plyvel_stub.DB = object
    sys.modules["plyvel"] = _plyvel_stub

from unittest.mock import AsyncMock, MagicMock

import pytest

import aleph_nodestatus.commands as cmd
from aleph_nodestatus import credit_distribution as cd
from aleph_nodestatus import slashing as slashing_mod
from aleph_nodestatus.settings import settings


FINAL_REWARDS = {
    "0xCRN": 10.0,      # gets slashed by 4.0 -> payout 6.0
    "0xOther": 5.0,     # untouched
}


@pytest.fixture
def patch_pipeline(monkeypatch):
    """Drive process_credit_distribution to the publish step with a known
    `final_rewards` and a captured published distribution.

    Returns the AsyncMock used for `create_distribution_tx_post` so tests can
    read back the posted `distribution` dict.
    """
    web3 = MagicMock()
    web3.eth.block_number = 2_000_000
    web3.eth.get_block.return_value = MagicMock(timestamp=1_700_000_000)
    monkeypatch.setattr(cmd, "get_web3", lambda: web3)
    monkeypatch.setattr(cmd, "get_dbs", lambda: {})
    monkeypatch.setattr(cmd, "CREDIT_DIST_FLOOR_HEIGHT", 0)
    monkeypatch.setattr(
        cmd, "get_latest_successful_credit_distribution",
        AsyncMock(return_value=(0, None)),
    )

    # Witness check on (credit_revenue=True) — return True so we proceed.
    async def _witness_ok(*a, **kw):
        return True
    monkeypatch.setattr(
        "aleph_nodestatus.credit_distribution.has_expense_witness_after",
        _witness_ok,
    )

    # Snapshots: one entry so `snapshots[-1][2]` (close-of-window
    # resource_nodes) is a real dict the slashing pass can read.
    async def _fake_snaps(*a, **kw):
        return [(1_700_000_000.0, {}, {"crn1": {"inactive_since": 1}})]
    monkeypatch.setattr(cd, "fetch_node_snapshots", _fake_snaps)

    # compute_rewards: returns the stream shape the orchestrator unpacks.
    async def _fake_rewards(*a, **kw):
        return {
            "credit_revenue": ({}, cmd.zero_totals()),
            "holder_tier":    ({}, cmd.zero_totals()),
            "detailed": {"credit_revenue": {}, "holder_tier": {}},
        }
    monkeypatch.setattr(cmd, "compute_rewards", _fake_rewards)

    # merge_rewards: inject the known final_rewards; by_source / detailed empty.
    monkeypatch.setattr(
        cmd, "merge_rewards",
        lambda sources, details=None, dust_threshold=0.01: (
            dict(FINAL_REWARDS), {}, {},
        ),
    )

    captured = AsyncMock()
    monkeypatch.setattr(cmd, "create_distribution_tx_post", captured)
    return captured


def _posted_distribution(captured):
    assert captured.await_count == 1, "distribution post was not published"
    args, kwargs = captured.await_args
    return args[0]


@pytest.mark.asyncio
async def test_slashing_applied_withholds_payout_and_publishes_meta(
    patch_pipeline, monkeypatch,
):
    """compute_slashing returns ({"0xCRN": 4.0}, [node]). The published post
    must keep rewards == final_rewards intact, expose that exact `slashed`
    dict and the meta node list, and flag slashing enabled."""
    monkeypatch.setattr(settings, "credit_dist_slash_enabled", True)

    slashed = {"0xCRN": 4.0}
    meta_nodes = [{
        "node_id": "crn1", "address": "0xCRN", "stream": "credit_revenue",
        "amount": 4.0, "inactive_since": 1,
    }]

    def _fake_compute_slashing(accumulator, close_resource_nodes, close_height,
                               **kw):
        return slashed, meta_nodes

    monkeypatch.setattr(slashing_mod, "compute_slashing", _fake_compute_slashing)

    await cmd.process_credit_distribution(
        start_height=1_000_000, end_height=2_000_000,
        act=False, dry_run=False, force=False,
        flags={"credit_revenue": True, "wage": False, "holder_tier": False,
               "transfer": False, "publish": True},
        safety_blocks=0, require_witness=False,
    )

    dist = _posted_distribution(patch_pipeline)
    # rewards intact (full calculation, published as-is)
    assert dist["rewards"] == FINAL_REWARDS
    # withheld slash surfaced verbatim
    assert dist["slashed"] == slashed
    assert dist["slashed_meta"]["nodes"] == meta_nodes
    assert dist["slashed_meta"]["enabled"] is True
    assert dist["slashed_meta"]["streams"] == list(
        slashing_mod.enabled_slash_streams()
    )


@pytest.mark.asyncio
async def test_slashing_flag_off_is_noop(patch_pipeline, monkeypatch):
    """With credit_dist_slash_enabled=False the published post must report
    slashed == {} and slashed_meta.enabled is False, and rewards stay intact
    (payout would equal rewards — no withholding)."""
    monkeypatch.setattr(settings, "credit_dist_slash_enabled", False)

    # compute_slashing must NOT be called on the flag-off path; make it loud.
    def _boom(*a, **kw):  # pragma: no cover
        raise AssertionError("compute_slashing called while slashing disabled")
    monkeypatch.setattr(slashing_mod, "compute_slashing", _boom)

    await cmd.process_credit_distribution(
        start_height=1_000_000, end_height=2_000_000,
        act=False, dry_run=False, force=False,
        flags={"credit_revenue": True, "wage": False, "holder_tier": False,
               "transfer": False, "publish": True},
        safety_blocks=0, require_witness=False,
    )

    dist = _posted_distribution(patch_pipeline)
    assert dist["rewards"] == FINAL_REWARDS
    assert dist["slashed"] == {}
    assert dist["slashed_meta"]["enabled"] is False
    assert dist["slashed_meta"]["streams"] == []


# ─────────────────── _payout_after_slash unit coverage ───────────────────


def test_payout_after_slash_subtracts_floors_and_drops_zeros():
    final = {"0xA": 10.0, "0xB": 5.0, "0xC": 2.0}
    slashed = {"0xA": 4.0, "0xB": 5.0, "0xC": 100.0}
    # A: 10-4=6 kept; B: 5-5=0 dropped; C: max(0, 2-100)=0 dropped.
    assert cmd._payout_after_slash(final, slashed) == {"0xA": 6.0}


def test_payout_after_slash_empty_slash_returns_rewards_unchanged():
    final = {"0xA": 10.0, "0xB": 5.0}
    assert cmd._payout_after_slash(final, {}) == final
