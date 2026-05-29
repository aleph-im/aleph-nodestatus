"""Click CLI tests for nodestatus-extract-credits."""

import sys
import types

if "plyvel" not in sys.modules:
    plyvel_stub = types.ModuleType("plyvel")
    plyvel_stub.DB = object
    sys.modules["plyvel"] = plyvel_stub

from click.testing import CliRunner  # noqa: E402

from aleph_nodestatus.commands import extract_credits  # noqa: E402


def test_cli_help_lists_extract_flags():
    runner = CliRunner()
    result = runner.invoke(extract_credits, ["--help"])
    assert result.exit_code == 0
    for flag in ("--dry-run", "--act", "--no-transfer", "--slippage-bps"):
        assert flag in result.output


def test_cli_act_and_dry_run_mutually_exclusive():
    runner = CliRunner()
    result = runner.invoke(extract_credits, ["--act", "--dry-run"])
    assert result.exit_code != 0
    assert "mutually exclusive" in result.output.lower()


def test_cli_dry_run_invokes_orchestrator_with_transfer_false(monkeypatch):
    """--dry-run forces transfer=False in the orchestrator call."""
    import aleph_nodestatus.commands as cmd

    captured = {}
    async def fake_orch(**kwargs):
        captured.update(kwargs)
        return 0  # new orchestrator contract returns an exit code
    monkeypatch.setattr(cmd, "process_credit_extraction", fake_orch)

    runner = CliRunner()
    result = runner.invoke(extract_credits, ["--dry-run"])
    assert result.exit_code == 0, result.output
    assert captured["dry_run"] is True
    assert captured["transfer"] is False
    assert captured["act"] is False


def test_cli_no_testnet_flag():
    """Extract talks to Ethereum, not Aleph — --testnet is intentionally absent."""
    runner = CliRunner()
    result = runner.invoke(extract_credits, ["--testnet"])
    assert result.exit_code != 0  # unknown flag


def test_cli_help_lists_immediate_flag():
    runner = CliRunner()
    result = runner.invoke(extract_credits, ["--help"])
    assert result.exit_code == 0
    assert "--immediate" in result.output


def test_cli_immediate_forwarded_to_orchestrator(monkeypatch):
    import aleph_nodestatus.commands as cmd

    captured = {}
    async def fake_orch(**kwargs):
        captured.update(kwargs)
        return 0  # new orchestrator contract returns an exit code
    monkeypatch.setattr(cmd, "process_credit_extraction", fake_orch)

    runner = CliRunner()
    result = runner.invoke(extract_credits, ["--immediate", "--act"])
    assert result.exit_code == 0, result.output
    assert captured["immediate"] is True
    assert captured["act"] is True


def test_cli_dry_run_implies_immediate(monkeypatch):
    """--dry-run must forward immediate=True so the orchestrator skips
    the random sleep without needing an extra flag."""
    import aleph_nodestatus.commands as cmd

    captured = {}
    async def fake_orch(**kwargs):
        captured.update(kwargs)
        return 0  # new orchestrator contract returns an exit code
    monkeypatch.setattr(cmd, "process_credit_extraction", fake_orch)

    runner = CliRunner()
    result = runner.invoke(extract_credits, ["--dry-run"])
    assert result.exit_code == 0, result.output
    assert captured["dry_run"] is True
    assert captured["immediate"] is True
