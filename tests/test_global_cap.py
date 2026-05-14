"""H5: global per-run distribution cap. Aborts when rewards exceed
credit_dist_max_total_aleph; --force-cap bypasses."""

import sys
import types

if "plyvel" not in sys.modules:
    _plyvel_stub = types.ModuleType("plyvel")
    _plyvel_stub.DB = object
    sys.modules["plyvel"] = _plyvel_stub

from unittest.mock import AsyncMock, MagicMock

import pytest

import aleph_nodestatus.commands as cmd
from aleph_nodestatus.settings import settings


def _patch_orchestrator(monkeypatch, final_rewards):
    """Stub deps so process_credit_distribution runs end-to-end with given rewards."""
    web3_mock = MagicMock()
    web3_mock.eth.block_number = 2_000_000
    web3_mock.eth.get_block.return_value = MagicMock(timestamp=1_700_000_000)
    monkeypatch.setattr(cmd, "get_web3", lambda: web3_mock)
    monkeypatch.setattr(cmd, "get_dbs", lambda: {})
    monkeypatch.setattr(
        cmd, "get_latest_successful_credit_distribution",
        AsyncMock(return_value=(0, None)),
    )
    monkeypatch.setattr(
        cmd, "merge_rewards",
        lambda sources, details=None, dust_threshold=0.01: (
            final_rewards,
            {name: dict(d) for name, d in sources.items()},
            {},
        ),
    )
    return web3_mock


@pytest.mark.asyncio
async def test_run_above_cap_aborts(monkeypatch, capsys):
    """H5: owed > max_total_aleph with no --force-cap must sys.exit(1)."""
    huge = {f"0x{i:040x}": settings.credit_dist_max_total_aleph + 1
            for i in range(1)}
    _patch_orchestrator(monkeypatch, huge)

    capsys.readouterr()   # drain any output produced by the orchestrator
                          # patch setup so the post-raise assertion below
                          # only inspects this run's stderr/stdout
    with pytest.raises(SystemExit) as exc:
        await cmd.process_credit_distribution(
            start_height=1_000_000, end_height=2_000_000,
            act=True, dry_run=False, force=False, force_cap=False,
            flags={"extract": False, "credit_revenue": True, "wage": False,
                   "holder_tier": False, "transfer": True, "publish": False},
        )
    assert exc.value.code == 1
    out = capsys.readouterr().out
    assert "max_total cap" in out  # disambiguate from the balance-check ABORT


@pytest.mark.asyncio
async def test_run_above_cap_with_force_cap_proceeds(monkeypatch, capsys):
    """H5: --force-cap bypasses the cap (cadence guard still independent)."""
    huge = {f"0x{i:040x}": settings.credit_dist_max_total_aleph + 1
            for i in range(1)}
    _patch_orchestrator(monkeypatch, huge)
    # Mock everything past the cap check so the test doesn't actually transfer.
    monkeypatch.setattr(cmd, "get_eth_account", lambda: MagicMock(address="0x" + "a"*40))
    monkeypatch.setattr(cmd, "get_token_contract", lambda w3: MagicMock(
        functions=MagicMock(balanceOf=lambda a: MagicMock(call=lambda: 10**30)),
    ))
    monkeypatch.setattr(cmd, "transfer_tokens", AsyncMock())
    monkeypatch.setattr(cmd, "create_distribution_tx_post", AsyncMock())

    await cmd.process_credit_distribution(
        start_height=1_000_000, end_height=2_000_000,
        act=True, dry_run=False, force=False, force_cap=True,
        flags={"extract": False, "credit_revenue": True, "wage": False,
               "holder_tier": False, "transfer": True, "publish": False},
    )
    assert "max_total cap" not in capsys.readouterr().out


@pytest.mark.asyncio
async def test_calculation_mode_ignores_cap(monkeypatch, capsys):
    """H5: cap only applies under --act + transfer; calc-only runs are not affected."""
    huge = {f"0x{i:040x}": settings.credit_dist_max_total_aleph + 1
            for i in range(1)}
    _patch_orchestrator(monkeypatch, huge)
    monkeypatch.setattr(cmd, "create_distribution_tx_post", AsyncMock())

    await cmd.process_credit_distribution(
        start_height=1_000_000, end_height=2_000_000,
        act=False, dry_run=False, force=False, force_cap=False,
        flags={"extract": False, "credit_revenue": True, "wage": False,
               "holder_tier": False, "transfer": False, "publish": False},
    )
    assert "max_total cap" not in capsys.readouterr().out


def test_cli_exposes_force_cap_flag():
    from click.testing import CliRunner
    from aleph_nodestatus.commands import distribute_credits

    runner = CliRunner()
    result = runner.invoke(distribute_credits, ["--help"])
    assert "--force-cap" in result.output
