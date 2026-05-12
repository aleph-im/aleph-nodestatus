import sys
import types

# Stub out plyvel before commands.py imports the storage module.
# plyvel has a native dependency that fails to load on some local machines;
# these tests only need the Click command object and never touch leveldb.
if "plyvel" not in sys.modules:
    _plyvel_stub = types.ModuleType("plyvel")
    _plyvel_stub.DB = object  # placeholder, never instantiated in these tests
    sys.modules["plyvel"] = _plyvel_stub

from unittest.mock import AsyncMock, MagicMock  # noqa: E402

import pytest  # noqa: E402

from aleph_nodestatus.credit_distribution import should_skip_run  # noqa: E402


def test_should_skip_when_within_interval():
    assert should_skip_run(last_end_height=1000, current_height=1500,
                           min_interval_blocks=1000, force=False) is True


def test_should_not_skip_when_past_interval():
    assert should_skip_run(last_end_height=1000, current_height=3000,
                           min_interval_blocks=1000, force=False) is False


def test_force_overrides_skip():
    assert should_skip_run(last_end_height=1000, current_height=1500,
                           min_interval_blocks=1000, force=True) is False


def test_no_last_end_does_not_skip():
    assert should_skip_run(last_end_height=None, current_height=1500,
                           min_interval_blocks=1000, force=False) is False


@pytest.fixture
def patch_orchestrator_deps(monkeypatch):
    """Patch external deps so process_credit_distribution can be exercised in-process."""
    import aleph_nodestatus.commands as cmd

    web3_mock = MagicMock()
    web3_mock.eth.block_number = 2_000_000
    web3_mock.eth.get_block.return_value = MagicMock(timestamp=1_700_000_000)
    monkeypatch.setattr(cmd, "get_web3", lambda: web3_mock)
    monkeypatch.setattr(cmd, "get_dbs", lambda: {})
    return web3_mock


@pytest.mark.asyncio
async def test_explicit_start_height_inside_cadence_window_aborts(
    patch_orchestrator_deps, capsys, monkeypatch,
):
    """C2: --start-height + --act + recent last_end + no --force = cadence skip."""
    import aleph_nodestatus.commands as cmd

    last_end = 1_999_000  # within default min_interval_blocks of current 2_000_000
    monkeypatch.setattr(
        cmd, "get_latest_successful_credit_distribution",
        AsyncMock(return_value=(last_end, {"end_height": last_end})),
    )

    await cmd.process_credit_distribution(
        start_height=1_000_000, end_height=2_000_000,
        act=True, dry_run=False, force=False,
        flags={"extract": False, "credit_revenue": False, "wage": False,
               "holder_tier": False, "transfer": False, "publish": False},
    )
    out = capsys.readouterr().out
    assert "Cadence guard" in out
    assert "skipping" in out.lower()


@pytest.mark.asyncio
async def test_explicit_start_height_with_force_proceeds(
    patch_orchestrator_deps, capsys, monkeypatch,
):
    """C2: --force overrides the guard even with --start-height.

    Asserts the guard didn't print AND that the orchestrator reached the
    block-height resolution work (web3.eth.get_block) past the guard."""
    import aleph_nodestatus.commands as cmd

    last_end = 1_999_000
    monkeypatch.setattr(
        cmd, "get_latest_successful_credit_distribution",
        AsyncMock(return_value=(last_end, {"end_height": last_end})),
    )

    web3_mock = patch_orchestrator_deps
    await cmd.process_credit_distribution(
        start_height=1_000_000, end_height=2_000_000,
        act=True, dry_run=False, force=True,
        flags={"extract": False, "credit_revenue": False, "wage": False,
               "holder_tier": False, "transfer": False, "publish": False},
    )
    out = capsys.readouterr().out
    assert "Cadence guard" not in out
    # Past-the-guard verification: orchestrator must have called get_block
    # for both start_time and end_time resolution.
    assert web3_mock.eth.get_block.call_count >= 2


@pytest.mark.asyncio
async def test_calculation_mode_ignores_cadence(
    patch_orchestrator_deps, capsys, monkeypatch,
):
    """C2: cadence guard only fires under --act; calculation-only runs are
    unthrottled and don't even query for a prior distribution in this path."""
    import aleph_nodestatus.commands as cmd

    last_end = 1_999_000
    mock_fn = AsyncMock(return_value=(last_end, {"end_height": last_end}))
    monkeypatch.setattr(
        cmd, "get_latest_successful_credit_distribution", mock_fn,
    )

    await cmd.process_credit_distribution(
        start_height=1_000_000, end_height=2_000_000,
        act=False, dry_run=False, force=False,
        flags={"extract": False, "credit_revenue": False, "wage": False,
               "holder_tier": False, "transfer": False, "publish": False},
    )
    out = capsys.readouterr().out
    assert "Cadence guard" not in out
    # Explicit start_height + act=False: the function should not query for a
    # prior distribution at all (no cadence check, no cursor derivation).
    assert mock_fn.call_count == 0


@pytest.mark.asyncio
async def test_act_with_no_prior_distribution_and_default_start_height_errors(
    patch_orchestrator_deps, capsys, monkeypatch,
):
    """C2: --act, no --start-height, and no prior distribution must abort with
    the 'first run' error rather than silently deriving a wrong start_height.

    Covers the path where get_latest_successful_credit_distribution returns
    (0, None) under act=True and the memoization correctly short-circuits
    the second fetch (last_end is 0 after the act branch, not None)."""
    import aleph_nodestatus.commands as cmd

    mock_fn = AsyncMock(return_value=(0, None))
    monkeypatch.setattr(
        cmd, "get_latest_successful_credit_distribution", mock_fn,
    )

    await cmd.process_credit_distribution(
        start_height=-1, end_height=2_000_000,
        act=True, dry_run=False, force=False,
        flags={"extract": False, "credit_revenue": False, "wage": False,
               "holder_tier": False, "transfer": False, "publish": False},
    )
    out = capsys.readouterr().out
    assert "--start-height required for first run" in out
    # Memoization must have prevented a second fetch.
    assert mock_fn.call_count == 1
