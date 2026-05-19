import sys
import types

# Stub out plyvel before commands.py imports the storage module.
# plyvel has a native dependency that fails to load on some local machines;
# the CLI tests only need the Click command object and never touch leveldb.
if "plyvel" not in sys.modules:
    plyvel_stub = types.ModuleType("plyvel")
    plyvel_stub.DB = object  # placeholder, never instantiated in these tests
    sys.modules["plyvel"] = plyvel_stub

from click.testing import CliRunner  # noqa: E402

from aleph_nodestatus.commands import distribute_credits  # noqa: E402


def test_cli_act_and_dry_run_mutually_exclusive():
    runner = CliRunner()
    result = runner.invoke(distribute_credits, ["--act", "--dry-run"])
    assert result.exit_code != 0
    assert "mutually exclusive" in result.output.lower() \
        or "cannot use" in result.output.lower()


def test_cli_act_and_testnet_mutually_exclusive():
    runner = CliRunner()
    result = runner.invoke(distribute_credits, ["--act", "--testnet"])
    assert result.exit_code != 0


def test_cli_help_lists_new_flags():
    runner = CliRunner()
    result = runner.invoke(distribute_credits, ["--help"])
    assert "--dry-run" in result.output
    assert "--no-wage" in result.output
    assert "--enable-holder-tier" in result.output
    assert "--no-transfer" in result.output
    assert "--no-publish" in result.output
    assert "--force" in result.output


def test_resolve_feature_flags_holder_tier_requires_credit_revenue(capsys):
    """holder_tier piggy-backs on credit_revenue's expense fetch; if
    credit_revenue is disabled, holder_tier must be forced off (and a
    warning emitted) rather than silently producing an empty stream."""
    from aleph_nodestatus.commands import _resolve_feature_flags

    flags = _resolve_feature_flags(
        no_credit_revenue=True, no_wage=False,
        enable_holder_tier=True, no_holder_tier=False,
        no_transfer=False, no_publish=False,
        act=False, dry_run=False,
    )
    out = capsys.readouterr().out

    assert flags["credit_revenue"] is False
    assert flags["holder_tier"] is False
    assert "WARNING" in out
    assert "holder_tier" in out
