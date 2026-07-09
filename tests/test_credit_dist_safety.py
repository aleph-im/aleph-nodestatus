"""Tests for the right-edge finality safety mechanism (safety_blocks +
expense witness) added on top of credit-distribution-v2.

Two pieces under test:

  1. `process_credit_distribution`'s safety_blocks cap, which trims
     `end_height` down to `current_block - safety_blocks` so we never
     compute a period whose right edge sits past the publisher's known
     output frontier.

  2. `credit_distribution.has_expense_witness_after`, the existence
     check that backs the cap: an `aleph_credit_expense` post with
     `expense.end_date > end_time` proves the publisher has progressed
     past the period under calculation. Missing witness either aborts
     (default) or warns (--no-witness-required / require_witness=False).
"""

import sys
import types

# plyvel stub — mirrors test_cadence_guard / test_global_cap. The actual
# native library isn't needed in these tests; commands.py just imports
# storage at module load time.
if "plyvel" not in sys.modules:
    _plyvel_stub = types.ModuleType("plyvel")
    _plyvel_stub.DB = object
    sys.modules["plyvel"] = _plyvel_stub

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

import aleph_nodestatus.commands as cmd
from aleph_nodestatus import credit_distribution as cd


# ──────────────────────── safety_blocks cap ────────────────────────


@pytest.fixture
def patch_orchestrator(monkeypatch):
    """Stub web3 + DB + last-distribution lookup so process_credit_distribution
    reaches the cap branch without doing real network/disk I/O. Returns the
    web3 mock so individual tests can tweak block_number."""
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
    # Bypass merge_rewards so we don't depend on inner pipeline details —
    # the cap fires before merge_rewards is called.
    monkeypatch.setattr(
        cmd, "merge_rewards",
        lambda sources, details=None, dust_threshold=0.01: (
            {}, {name: dict(d) for name, d in sources.items()}, {},
        ),
    )
    return web3


@pytest.mark.asyncio
async def test_safety_blocks_caps_end_height_when_block_number_is_high(
    patch_orchestrator, capsys,
):
    """block_number=2M, end_height=2M, safety=500 → end_height capped to
    1_999_500. The cap echo message names the original and new heights."""
    await cmd.process_credit_distribution(
        start_height=1_000_000, end_height=2_000_000,
        act=False, dry_run=False, force=False,
        flags={"credit_revenue": False, "wage": False,
               "holder_tier": False, "transfer": False, "publish": False},
        safety_blocks=500,
        require_witness=False,
    )
    out = capsys.readouterr().out
    assert "Capping end_height 2000000 -> 1999500" in out
    assert "safety_blocks=500" in out


@pytest.mark.asyncio
async def test_safety_blocks_does_not_cap_when_end_height_already_below_cap(
    patch_orchestrator, capsys,
):
    """The caller passed `end_height=1_500_000` deliberately. current_block
    (2M) - safety (500) = 1_999_500 — already above the caller's choice,
    so the cap is a no-op. No "Capping" line printed."""
    await cmd.process_credit_distribution(
        start_height=1_000_000, end_height=1_500_000,
        act=False, dry_run=False, force=False,
        flags={"credit_revenue": False, "wage": False,
               "holder_tier": False, "transfer": False, "publish": False},
        safety_blocks=500,
        require_witness=False,
    )
    out = capsys.readouterr().out
    assert "Capping end_height" not in out


@pytest.mark.asyncio
async def test_safety_blocks_uses_setting_default_when_kwarg_omitted(
    patch_orchestrator, capsys, monkeypatch,
):
    """When no `safety_blocks=` kwarg is passed, settings.credit_dist_safety_blocks
    is used. Mutate it to a sentinel and verify the cap echo names it."""
    from aleph_nodestatus.settings import settings as global_settings

    original = global_settings.credit_dist_safety_blocks
    global_settings.credit_dist_safety_blocks = 1234
    try:
        await cmd.process_credit_distribution(
            start_height=1_000_000, end_height=2_000_000,
            act=False, dry_run=False, force=False,
            flags={"credit_revenue": False, "wage": False,
                   "holder_tier": False, "transfer": False, "publish": False},
            require_witness=False,
        )
        out = capsys.readouterr().out
        assert "Capping end_height 2000000 -> 1998766" in out  # 2M - 1234
        assert "safety_blocks=1234" in out
    finally:
        global_settings.credit_dist_safety_blocks = original


# ──────────────── has_expense_witness_after helper ────────────────


def _post_with_end_date(end_date_ms, *, item_hash="h1", confirmed=True):
    """Build the post-dict shape `_iter_posts_dedup` yields downstream."""
    confs = [{"chain": "ETH", "height": 100}] if confirmed else []
    return {
        "item_hash": item_hash,
        "original_item_hash": item_hash,
        "confirmations": confs,
        "time": (end_date_ms / 1000.0) - 60,
        "content": {
            "expense": {"end_date": end_date_ms},
            "tags": ["credit_expense", "type_execution"],
        },
    }


def _run(coro):
    return asyncio.run(coro)


def test_has_expense_witness_after_returns_true_when_witness_exists(monkeypatch):
    """end_time≈1.778e9s (a real 2026 timestamp). A confirmed expense whose
    end_date is 500s after end_time is a valid witness → True.

    Realistic timestamps are required because `_normalize_ts_to_seconds`
    uses magnitude detection: anything > 1e12 is treated as ms; smaller
    values stay as seconds. Using `end_date_ms = 1_500_000` would be
    interpreted as 1.5M seconds (year 1970), not the intended ms value.
    """
    end_time = 1_778_059_834.0  # s — within the 2026 credit pipeline
    witness_post = _post_with_end_date(
        end_date_ms=int((end_time + 500) * 1000),  # 500s after end_time
    )

    async def fake_iter(client, post_filter, last_end_height=None):
        for p in [witness_post]:
            yield p

    class _Client:
        async def __aenter__(self): return self
        async def __aexit__(self, *a): return False

    monkeypatch.setattr(cd, "_aleph_client", lambda api: _Client())
    monkeypatch.setattr(cd, "_iter_posts_dedup", fake_iter)

    assert _run(cd.has_expense_witness_after(
        api_server="http://x", end_time=end_time, sender="0xS",
    )) is True


def test_has_expense_witness_after_returns_false_when_no_post(monkeypatch):
    """No posts in the forward window → no witness → False."""
    async def fake_iter(client, post_filter, last_end_height=None):
        return
        yield  # pragma: no cover  (make this an async generator)

    class _Client:
        async def __aenter__(self): return self
        async def __aexit__(self, *a): return False

    monkeypatch.setattr(cd, "_aleph_client", lambda api: _Client())
    monkeypatch.setattr(cd, "_iter_posts_dedup", fake_iter)

    assert _run(cd.has_expense_witness_after(
        api_server="http://x", end_time=1000.0, sender="0xS",
    )) is False


def test_has_expense_witness_after_returns_false_when_end_date_not_after(
    monkeypatch,
):
    """A confirmed post whose `end_date` is exactly equal to (or before)
    `end_time` does NOT count as a witness — the publisher must have
    visibly moved past the period."""
    end_time = 1_778_059_834.0
    # Equal: end_date == end_time → not a witness.
    equal_post = _post_with_end_date(end_date_ms=int(end_time * 1000))

    async def fake_iter(client, post_filter, last_end_height=None):
        for p in [equal_post]:
            yield p

    class _Client:
        async def __aenter__(self): return self
        async def __aexit__(self, *a): return False

    monkeypatch.setattr(cd, "_aleph_client", lambda api: _Client())
    monkeypatch.setattr(cd, "_iter_posts_dedup", fake_iter)

    assert _run(cd.has_expense_witness_after(
        api_server="http://x", end_time=end_time, sender="0xS",
    )) is False


# ───────────────── witness gate inside the orchestrator ─────────────────


@pytest.mark.asyncio
async def test_missing_witness_aborts_when_require_witness_true(
    patch_orchestrator, capsys, monkeypatch,
):
    """credit_revenue is on, witness returns False, require_witness=True →
    sys.exit(2) with an ABORT message naming `end_time`."""
    async def _no_witness(*a, **kw):
        return False
    monkeypatch.setattr(
        "aleph_nodestatus.credit_distribution.has_expense_witness_after",
        _no_witness,
    )

    with pytest.raises(SystemExit) as exc:
        await cmd.process_credit_distribution(
            start_height=1_000_000, end_height=2_000_000,
            act=False, dry_run=False, force=False,
            flags={"credit_revenue": True, "wage": False,
                   "holder_tier": False, "transfer": False, "publish": False},
            safety_blocks=0,
            require_witness=True,
        )
    assert exc.value.code == 2
    out = capsys.readouterr().out
    assert "ABORT" in out
    assert "expense witness" in out


@pytest.mark.asyncio
async def test_missing_witness_only_warns_when_require_witness_false(
    patch_orchestrator, capsys, monkeypatch, caplog,
):
    """require_witness=False → missing witness is logged (warning) but the
    orchestrator proceeds. Use stubbed downstream fetchers so the run
    completes without a real network call."""
    import logging

    async def _no_witness(*a, **kw):
        return False

    async def _fake_fetch_msgs(*a, **kw):
        return []

    async def _fake_fetch_snaps(*a, **kw):
        return []

    monkeypatch.setattr(
        "aleph_nodestatus.credit_distribution.has_expense_witness_after",
        _no_witness,
    )
    monkeypatch.setattr(
        "aleph_nodestatus.credit_distribution._fetch_expense_messages",
        _fake_fetch_msgs,
    )
    monkeypatch.setattr(
        "aleph_nodestatus.credit_distribution.fetch_node_snapshots",
        _fake_fetch_snaps,
    )

    with caplog.at_level(logging.WARNING, logger="aleph_nodestatus.commands"):
        await cmd.process_credit_distribution(
            start_height=1_000_000, end_height=2_000_000,
            act=False, dry_run=False, force=False,
            flags={"credit_revenue": True, "wage": False,
                   "holder_tier": False, "transfer": False, "publish": False},
            safety_blocks=0,
            require_witness=False,
        )
    # The warning must have been emitted via the logger (downgrade path),
    # and no ABORT message should appear in stdout.
    out = capsys.readouterr().out
    assert "ABORT" not in out
    assert any(
        "expense witness" in rec.message for rec in caplog.records
    ), [rec.message for rec in caplog.records]


@pytest.mark.asyncio
async def test_witness_check_skipped_when_credit_revenue_disabled(
    patch_orchestrator, monkeypatch,
):
    """If credit_revenue is off, the witness check is a no-op — the
    helper must NOT be called. This is what lets operators run
    --no-credit-revenue without a live publisher."""
    calls = []

    async def _spy_witness(*a, **kw):
        calls.append((a, kw))
        return False  # would abort if called

    monkeypatch.setattr(
        "aleph_nodestatus.credit_distribution.has_expense_witness_after",
        _spy_witness,
    )

    await cmd.process_credit_distribution(
        start_height=1_000_000, end_height=2_000_000,
        act=False, dry_run=False, force=False,
        flags={"credit_revenue": False, "wage": False,
               "holder_tier": False, "transfer": False, "publish": False},
        safety_blocks=0,
        require_witness=True,
    )
    assert calls == []
