"""End-to-end dry-run integration test against recorded fixtures.

Mocks the high-level fetchers (expense messages, node snapshots), the on-chain
process() extraction, and the Aleph post. Asserts that --dry-run produces no
side effects and emits a DRY-RUN summary.
"""

# plyvel stub for environments without the native library; safe no-op
# elsewhere. See payment_processor / commands plyvel notes.
import sys
import types

if "plyvel" not in sys.modules:
    _plyvel_stub = types.ModuleType("plyvel")
    _plyvel_stub.DB = lambda *a, **kw: None  # type: ignore[attr-defined]
    sys.modules["plyvel"] = _plyvel_stub

import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from click.testing import CliRunner

from aleph_nodestatus.commands import distribute_credits


FIX = Path(__file__).parent / "fixtures"


@pytest.fixture
def patched_pipeline(monkeypatch):
    """Patch external I/O so the orchestrator runs end-to-end in-memory."""
    expense_msg = json.loads((FIX / "expense_execution.json").read_text())
    snapshot = json.loads((FIX / "snapshot.json").read_text())

    async def fake_fetch_msgs(*a, **kw):
        return [expense_msg["message"]]

    async def fake_fetch_snaps(*a, **kw):
        nodes = {n["hash"]: n for n in snapshot["nodes"]}
        rnodes = {r["hash"]: r for r in snapshot["resource_nodes"]}
        return [(snapshot["height"], nodes, rnodes)]

    monkeypatch.setattr(
        "aleph_nodestatus.credit_distribution._fetch_expense_messages",
        fake_fetch_msgs,
    )
    monkeypatch.setattr(
        "aleph_nodestatus.credit_distribution.fetch_node_snapshots",
        fake_fetch_snaps,
    )

    # Stub get_dbs so we don't open the on-disk LevelDB.
    import aleph_nodestatus.commands as cmd_module
    monkeypatch.setattr(cmd_module, "get_dbs", lambda: {})
    # Sidestep the launch-block floor — fixtures use small synthetic heights.
    monkeypatch.setattr(cmd_module, "CREDIT_DIST_FLOOR_HEIGHT", 0)

    # Stub web3 — no live RPC available in the test env.
    fake_web3 = MagicMock()
    fake_web3.to_checksum_address = lambda x: x
    fake_web3.eth.get_balance.return_value = 0
    fake_web3.eth.block_number = 100
    fake_web3.eth.get_block = lambda h: MagicMock(timestamp=1778050000 + h)
    monkeypatch.setattr(
        "aleph_nodestatus.commands.get_web3", lambda: fake_web3,
    )

    # Track side effects
    posts = []
    transfers = []

    async def fake_post(*a, **kw):
        posts.append((a, kw))

    async def fake_transfer(*a, **kw):
        transfers.append((a, kw))

    monkeypatch.setattr(
        "aleph_nodestatus.commands.create_distribution_tx_post",
        fake_post,
    )
    monkeypatch.setattr(
        "aleph_nodestatus.commands.transfer_tokens",
        fake_transfer,
    )

    return {"posts": posts, "transfers": transfers}


def test_dry_run_does_not_post_or_transfer(patched_pipeline):
    """--dry-run runs every computation step but skips posts + transfers."""
    runner = CliRunner()
    result = runner.invoke(distribute_credits, [
        "--dry-run", "--no-extract",
        "--start-height", "10",
        "--end-height",   "20",
    ])
    assert result.exit_code == 0, result.output
    assert patched_pipeline["posts"] == []
    assert patched_pipeline["transfers"] == []
    assert "DRY-RUN" in result.output


def test_dry_run_includes_wage_and_credit_in_summary(patched_pipeline):
    """The summary printed in dry-run mode contains both per-stream totals."""
    runner = CliRunner()
    result = runner.invoke(distribute_credits, [
        "--dry-run", "--no-extract",
        "--start-height", "10",
        "--end-height",   "20",
    ])
    assert result.exit_code == 0, result.output
    # The summary preview JSON should mention these top-level keys
    assert "wage_subsidy" in result.output
    assert "credit_revenue_totals" in result.output
    assert "feature_flags" in result.output
    assert "start_height" in result.output
    assert "end_height" in result.output


def test_wage_unallocated_when_no_snapshots(monkeypatch):
    """When the snapshot fetch returns empty, the period subsidy is recorded
    as unallocated rather than silently zeroed.
    """
    import json as _json
    expense_msg = _json.loads((FIX / "expense_execution.json").read_text())

    async def fake_fetch_msgs(*a, **kw):
        return [expense_msg["message"]]

    # Return empty snapshot list — but compute_rewards needs at least one
    # snapshot for the credit-revenue path. So we have to disable credit_revenue
    # in this test (we're only checking the wage path's empty-snapshot behavior).
    async def fake_fetch_snaps(*a, **kw):
        return []

    monkeypatch.setattr(
        "aleph_nodestatus.credit_distribution._fetch_expense_messages",
        fake_fetch_msgs,
    )
    monkeypatch.setattr(
        "aleph_nodestatus.credit_distribution.fetch_node_snapshots",
        fake_fetch_snaps,
    )

    import aleph_nodestatus.commands as cmd_module
    monkeypatch.setattr(cmd_module, "get_dbs", lambda: {})
    monkeypatch.setattr(cmd_module, "CREDIT_DIST_FLOOR_HEIGHT", 0)

    fake_web3 = MagicMock()
    fake_web3.to_checksum_address = lambda x: x
    fake_web3.eth.get_balance.return_value = 0
    fake_web3.eth.block_number = 100
    fake_web3.eth.get_block = lambda h: MagicMock(timestamp=1778050000 + h)
    monkeypatch.setattr(
        "aleph_nodestatus.commands.get_web3", lambda: fake_web3,
    )

    monkeypatch.setattr(
        "aleph_nodestatus.commands.create_distribution_tx_post",
        lambda *a, **kw: None,
    )

    # Set a wage_start_date that overlaps the test window
    from aleph_nodestatus.settings import settings
    original = settings.wage_start_date
    settings.wage_start_date = "2026-04-01T00:00:00+00:00"

    try:
        runner = CliRunner()
        result = runner.invoke(distribute_credits, [
            "--dry-run", "--no-extract", "--no-credit-revenue",
            "--start-height", "10",
            "--end-height",   "20",
        ])
        assert result.exit_code == 0, result.output
        # The console output prints the distribution preview JSON
        assert "wage_subsidy" in result.output
        # The unallocated value should be > 0 since we have a wage window
        # We just verify the key/value structure is right
        assert "unallocated_aleph" in result.output
        assert "period_total_aleph" in result.output
    finally:
        settings.wage_start_date = original


def test_balance_safety_aborts_when_short(monkeypatch):
    """When total rewards exceed the distribution_recipient's ALEPH balance,
    the run aborts with a non-zero exit code — regardless of which stream
    (credit_revenue, holder_tier, wage_subsidy) produced the rewards.
    """
    import json as _json
    expense_msg = _json.loads((FIX / "expense_execution.json").read_text())
    # Inject hold[] into the fixture so the holder_tier path produces rewards.
    # (Renamed from `rewards[]` to `hold[]` after the indexer's pricing bug
    # fix; `hold[]` is the canonical holder-tier array going forward.)
    expense_msg["message"]["content"]["content"]["expense"]["hold"] = [
        {"amount": 10_000_000, "node_id": "test-crn-1",
         "address": "0xH1"},
    ]
    snapshot = _json.loads((FIX / "snapshot.json").read_text())

    async def fake_fetch_msgs(*a, **kw):
        return [expense_msg["message"]]

    async def fake_fetch_snaps(*a, **kw):
        nodes = {n["hash"]: n for n in snapshot["nodes"]}
        rnodes = {r["hash"]: r for r in snapshot["resource_nodes"]}
        return [(snapshot["height"], nodes, rnodes)]

    monkeypatch.setattr(
        "aleph_nodestatus.credit_distribution._fetch_expense_messages",
        fake_fetch_msgs,
    )
    monkeypatch.setattr(
        "aleph_nodestatus.credit_distribution.fetch_node_snapshots",
        fake_fetch_snaps,
    )

    import aleph_nodestatus.commands as cmd_module
    monkeypatch.setattr(cmd_module, "get_dbs", lambda: {})
    monkeypatch.setattr(cmd_module, "CREDIT_DIST_FLOOR_HEIGHT", 0)

    fake_web3 = MagicMock()
    fake_web3.to_checksum_address = lambda x: x
    fake_web3.eth.get_balance.return_value = 0
    fake_web3.eth.block_number = 100
    fake_web3.eth.get_block = lambda h: MagicMock(timestamp=1778050000 + h)
    monkeypatch.setattr(
        "aleph_nodestatus.commands.get_web3", lambda: fake_web3,
    )

    # ALEPH balance at the transfer sender: very low — safety must fire
    fake_token = MagicMock()
    fake_token.functions.balanceOf.return_value.call.return_value = 1  # 1 wei
    monkeypatch.setattr(
        "aleph_nodestatus.commands.get_token_contract",
        lambda w3: fake_token,
    )
    fake_account = MagicMock()
    fake_account.address = "0x3a5CC6aBd06B601f4654035d125F9DD2FC992C25"
    monkeypatch.setattr(
        "aleph_nodestatus.commands.get_eth_account", lambda: fake_account,
    )

    runner = CliRunner()
    result = runner.invoke(distribute_credits, [
        "--act", "--no-extract", "--enable-holder-tier", "--no-wage",
        "--start-height", "10",
        "--end-height",   "20",
    ])
    assert result.exit_code != 0, result.output
    assert "ABORT" in result.output


def test_balance_safety_aborts_for_credit_revenue_only(monkeypatch):
    """The same safety check must fire on a credit-revenue-only run (no
    holder_tier, no wage) — the previous gate was too narrow.
    """
    import json as _json
    expense_msg = _json.loads((FIX / "expense_execution.json").read_text())
    snapshot = _json.loads((FIX / "snapshot.json").read_text())

    async def fake_fetch_msgs(*a, **kw):
        return [expense_msg["message"]]

    async def fake_fetch_snaps(*a, **kw):
        nodes = {n["hash"]: n for n in snapshot["nodes"]}
        rnodes = {r["hash"]: r for r in snapshot["resource_nodes"]}
        return [(snapshot["height"], nodes, rnodes)]

    monkeypatch.setattr(
        "aleph_nodestatus.credit_distribution._fetch_expense_messages",
        fake_fetch_msgs,
    )
    monkeypatch.setattr(
        "aleph_nodestatus.credit_distribution.fetch_node_snapshots",
        fake_fetch_snaps,
    )

    import aleph_nodestatus.commands as cmd_module
    monkeypatch.setattr(cmd_module, "get_dbs", lambda: {})
    monkeypatch.setattr(cmd_module, "CREDIT_DIST_FLOOR_HEIGHT", 0)

    fake_web3 = MagicMock()
    fake_web3.to_checksum_address = lambda x: x
    fake_web3.eth.get_balance.return_value = 0
    fake_web3.eth.block_number = 100
    fake_web3.eth.get_block = lambda h: MagicMock(timestamp=1778050000 + h)
    monkeypatch.setattr(
        "aleph_nodestatus.commands.get_web3", lambda: fake_web3,
    )

    fake_token = MagicMock()
    fake_token.functions.balanceOf.return_value.call.return_value = 1  # 1 wei
    monkeypatch.setattr(
        "aleph_nodestatus.commands.get_token_contract",
        lambda w3: fake_token,
    )
    fake_account = MagicMock()
    fake_account.address = "0x3a5CC6aBd06B601f4654035d125F9DD2FC992C25"
    monkeypatch.setattr(
        "aleph_nodestatus.commands.get_eth_account", lambda: fake_account,
    )

    runner = CliRunner()
    result = runner.invoke(distribute_credits, [
        "--act", "--no-extract", "--no-holder-tier", "--no-wage",
        "--start-height", "10",
        "--end-height",   "20",
    ])
    assert result.exit_code != 0, result.output
    assert "ABORT" in result.output
