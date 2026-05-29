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


# ---------------------------------------------------------------------------
# Per-token sizing flags: --token / --max-amount / --min-amount
# ---------------------------------------------------------------------------

def test_cli_help_lists_sizing_flags():
    runner = CliRunner()
    result = runner.invoke(extract_credits, ["--help"])
    assert result.exit_code == 0
    for flag in ("--token", "--max-amount", "--min-amount"):
        assert flag in result.output


def test_cli_token_filter_unknown_symbol_rejected():
    """--token DAI when DAI is not in settings.process_tokens errors out
    before reaching the orchestrator (no chain access required)."""
    runner = CliRunner()
    result = runner.invoke(extract_credits, ["--dry-run", "--token", "DAI"])
    assert result.exit_code != 0
    assert "DAI" in result.output


def test_cli_sizing_flags_forwarded(monkeypatch):
    """--token, --max-amount, --min-amount all reach the orchestrator
    parsed. CLI --min-amount overrides the setting default for that
    symbol; other symbols keep their setting defaults."""
    from decimal import Decimal
    import aleph_nodestatus.commands as cmd
    from aleph_nodestatus.settings import settings

    captured = {}
    async def fake_orch(**kwargs):
        captured.update(kwargs)
        return 0
    monkeypatch.setattr(cmd, "process_credit_extraction", fake_orch)

    runner = CliRunner()
    result = runner.invoke(extract_credits, [
        "--dry-run",
        "--token", "USDC",
        "--max-amount", "USDC=1000",
        "--min-amount", "USDC=50",  # override default 100 with 50
    ])
    assert result.exit_code == 0, result.output
    assert captured["tokens"] == ("USDC",)
    assert captured["max_amounts"] == {"USDC": Decimal("1000")}
    # USDC override applied; ETH/ALEPH inherit settings defaults.
    expected_min = {
        sym: Decimal(str(amt))
        for sym, amt in settings.extract_default_min_amounts.items()
    }
    expected_min["USDC"] = Decimal("50")
    assert captured["min_amounts"] == expected_min


def test_cli_max_amount_malformed_rejected():
    runner = CliRunner()
    result = runner.invoke(extract_credits, [
        "--dry-run", "--max-amount", "USDC1000",  # missing '='
    ])
    assert result.exit_code != 0
    assert "max-amount" in result.output.lower()


def test_cli_max_amount_unknown_symbol_rejected():
    runner = CliRunner()
    result = runner.invoke(extract_credits, [
        "--dry-run", "--max-amount", "DAI=1000",
    ])
    assert result.exit_code != 0
    assert "DAI" in result.output


def test_cli_max_amount_duplicate_rejected():
    """Repeating --max-amount USDC twice is rejected — operators who
    really want to override should pass one canonical value."""
    runner = CliRunner()
    result = runner.invoke(extract_credits, [
        "--dry-run",
        "--max-amount", "USDC=1000",
        "--max-amount", "USDC=2000",
    ])
    assert result.exit_code != 0
    assert "duplicate" in result.output.lower()


def test_cli_max_below_min_rejected():
    """When max < min for the same token, the cap would deterministically
    trigger the skip — surface that contradiction at the CLI."""
    runner = CliRunner()
    result = runner.invoke(extract_credits, [
        "--dry-run",
        "--max-amount", "USDC=50",
        "--min-amount", "USDC=100",
    ])
    assert result.exit_code != 0
    assert "below" in result.output.lower()


def test_cli_max_price_impact_flag_forwarded(monkeypatch):
    """--max-price-impact-bps reaches the orchestrator and overrides
    the setting default (0)."""
    import aleph_nodestatus.commands as cmd

    captured = {}
    async def fake_orch(**kwargs):
        captured.update(kwargs)
        return 0
    monkeypatch.setattr(cmd, "process_credit_extraction", fake_orch)

    runner = CliRunner()
    result = runner.invoke(extract_credits, [
        "--dry-run", "--max-price-impact-bps", "150",
    ])
    assert result.exit_code == 0, result.output
    assert captured["max_price_impact_bps"] == 150


def test_cli_max_price_impact_default_uses_setting(monkeypatch):
    """Without --max-price-impact-bps, the orchestrator gets the
    setting value (0 by default)."""
    import aleph_nodestatus.commands as cmd
    from aleph_nodestatus.settings import settings

    captured = {}
    async def fake_orch(**kwargs):
        captured.update(kwargs)
        return 0
    monkeypatch.setattr(cmd, "process_credit_extraction", fake_orch)

    runner = CliRunner()
    result = runner.invoke(extract_credits, ["--dry-run"])
    assert result.exit_code == 0, result.output
    assert captured["max_price_impact_bps"] == settings.extract_max_price_impact_bps


def test_cli_no_sizing_flags_uses_setting_defaults(monkeypatch):
    """Default invocation (no --token/--max/--min) forwards empty filter
    + empty max_amounts, but min_amounts comes pre-populated from
    settings.extract_default_min_amounts so the cron gets sensible
    per-token floors out of the box."""
    from decimal import Decimal
    import aleph_nodestatus.commands as cmd
    from aleph_nodestatus.settings import settings

    captured = {}
    async def fake_orch(**kwargs):
        captured.update(kwargs)
        return 0
    monkeypatch.setattr(cmd, "process_credit_extraction", fake_orch)

    runner = CliRunner()
    result = runner.invoke(extract_credits, ["--dry-run"])
    assert result.exit_code == 0, result.output
    assert captured["tokens"] == ()
    assert captured["max_amounts"] == {}
    expected_min = {
        sym: Decimal(str(amt))
        for sym, amt in settings.extract_default_min_amounts.items()
    }
    assert captured["min_amounts"] == expected_min


def test_cli_default_min_amounts_filtered_to_configured_tokens(monkeypatch):
    """A stale entry in extract_default_min_amounts (for a token no
    longer in process_tokens) is silently dropped — the CLI must not
    error out just because settings drifted."""
    from decimal import Decimal
    import aleph_nodestatus.commands as cmd
    from aleph_nodestatus.settings import settings

    monkeypatch.setattr(
        settings, "extract_default_min_amounts",
        {"USDC": "100", "DAI": "100"},  # DAI not in process_tokens
    )
    captured = {}
    async def fake_orch(**kwargs):
        captured.update(kwargs)
        return 0
    monkeypatch.setattr(cmd, "process_credit_extraction", fake_orch)

    runner = CliRunner()
    result = runner.invoke(extract_credits, ["--dry-run"])
    assert result.exit_code == 0, result.output
    assert captured["min_amounts"] == {"USDC": Decimal("100")}
