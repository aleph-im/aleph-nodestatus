"""Tests for the hardcoded credit-distribution floor block."""

import sys
import types

# Stub plyvel before importing commands (it pulls storage at import time).
if "plyvel" not in sys.modules:
    _plyvel_stub = types.ModuleType("plyvel")
    _plyvel_stub.DB = object
    sys.modules["plyvel"] = _plyvel_stub

from unittest.mock import AsyncMock, MagicMock  # noqa: E402

import pytest  # noqa: E402


@pytest.fixture
def patch_orchestrator_deps(monkeypatch):
    """Patch web3 / dbs and pin the floor constant to a small, predictable value
    so tests can compare against concrete block numbers without depending on
    the production constant's exact value."""
    import aleph_nodestatus.commands as cmd

    web3 = MagicMock()
    web3.eth.block_number = 100_000
    web3.eth.get_block.return_value = MagicMock(timestamp=1_700_000_000)
    monkeypatch.setattr(cmd, "get_web3", lambda: web3)
    monkeypatch.setattr(cmd, "get_dbs", lambda: {})
    monkeypatch.setattr(cmd, "CREDIT_DIST_FLOOR_HEIGHT", 50_000)
    return web3


@pytest.mark.asyncio
async def test_explicit_start_below_floor_aborts(
    patch_orchestrator_deps, monkeypatch, capsys,
):
    """--start-height N where N < floor must abort. act=False bypasses cadence;
    the floor guard is independent of cadence."""
    import aleph_nodestatus.commands as cmd

    monkeypatch.setattr(
        cmd, "get_latest_successful_credit_distribution",
        AsyncMock(return_value=(0, None)),
    )

    with pytest.raises(SystemExit) as exc:
        await cmd.process_credit_distribution(
            start_height=1_000, end_height=100_000,
            act=False, dry_run=False, force=False,
            flags={"extract": False, "credit_revenue": False, "wage": False,
                   "holder_tier": False, "transfer": False, "publish": False},
        )
    assert exc.value.code == 2
    out = capsys.readouterr().out
    assert "below floor" in out


@pytest.mark.asyncio
async def test_no_prior_distribution_starts_at_floor(
    patch_orchestrator_deps, monkeypatch, capsys,
):
    """First-run path: no --start-height, no prior dist → use the hardcoded
    floor as the default start_height instead of erroring out."""
    import aleph_nodestatus.commands as cmd

    monkeypatch.setattr(
        cmd, "get_latest_successful_credit_distribution",
        AsyncMock(return_value=(0, None)),
    )

    await cmd.process_credit_distribution(
        start_height=-1, end_height=100_000,
        act=False, dry_run=False, force=False,
        flags={"extract": False, "credit_revenue": False, "wage": False,
               "holder_tier": False, "transfer": False, "publish": False},
    )
    out = capsys.readouterr().out
    assert "starting at floor 50000" in out


@pytest.mark.asyncio
async def test_derived_start_below_floor_aborts(
    patch_orchestrator_deps, monkeypatch, capsys,
):
    """Prior distribution's last_end+1 is below the floor — refuse to proceed
    rather than silently re-running pre-launch expenses."""
    import aleph_nodestatus.commands as cmd

    monkeypatch.setattr(
        cmd, "get_latest_successful_credit_distribution",
        AsyncMock(return_value=(1_000, {"end_height": 1_000})),
    )

    with pytest.raises(SystemExit) as exc:
        await cmd.process_credit_distribution(
            start_height=-1, end_height=100_000,
            act=False, dry_run=False, force=False,
            flags={"extract": False, "credit_revenue": False, "wage": False,
                   "holder_tier": False, "transfer": False, "publish": False},
        )
    assert exc.value.code == 2
    out = capsys.readouterr().out
    assert "below floor" in out
