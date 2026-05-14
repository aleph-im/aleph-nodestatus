"""Verify extract_aleph runs on a worker thread so the event loop stays free."""

import sys
import types

if "plyvel" not in sys.modules:
    _plyvel_stub = types.ModuleType("plyvel")
    _plyvel_stub.DB = object
    sys.modules["plyvel"] = _plyvel_stub

import asyncio
import time
from unittest.mock import AsyncMock, MagicMock

import pytest

import aleph_nodestatus.commands as cmd


@pytest.mark.asyncio
async def test_extract_does_not_block_event_loop(monkeypatch):
    """A blocking extract_aleph must not stall a concurrent asyncio task."""
    BLOCK_SECONDS = 1.5

    def fake_extract_aleph(*args, **kwargs):
        time.sleep(BLOCK_SECONDS)  # simulates wait_for_transaction_receipt
        return {"tokens": [], "errors": []}

    monkeypatch.setattr(cmd, "extract_aleph", fake_extract_aleph)

    # Mock everything else process_credit_distribution touches.
    web3_mock = MagicMock()
    web3_mock.eth.block_number = 2_000_000
    web3_mock.eth.get_block.return_value = MagicMock(timestamp=1_700_000_000)
    monkeypatch.setattr(cmd, "get_web3", lambda: web3_mock)
    monkeypatch.setattr(cmd, "get_dbs", lambda: {})
    # Sidestep the launch-block floor — this test uses fictitious block numbers.
    monkeypatch.setattr(cmd, "CREDIT_DIST_FLOOR_HEIGHT", 0)
    monkeypatch.setattr(cmd, "get_processor_contract", lambda w3: MagicMock())
    monkeypatch.setattr(cmd, "get_quoter_contract", lambda w3: MagicMock())
    monkeypatch.setattr(cmd, "get_v2_router_contract", lambda w3: MagicMock())
    monkeypatch.setattr(cmd, "get_v4_quoter_contract", lambda w3: MagicMock())
    monkeypatch.setattr(
        cmd, "get_latest_successful_credit_distribution",
        AsyncMock(return_value=(0, None)),
    )

    # Track whether a concurrent task gets scheduled while extract is running.
    concurrent_done_at = [None]

    async def concurrent_pinger():
        await asyncio.sleep(0.05)
        concurrent_done_at[0] = time.monotonic()

    started = time.monotonic()

    async def run_orchestrator():
        await cmd.process_credit_distribution(
            start_height=1_000_000, end_height=2_000_000,
            act=False, dry_run=True, force=False,
            flags={"extract": True, "credit_revenue": False, "wage": False,
                   "holder_tier": False, "transfer": False, "publish": False},
        )

    await asyncio.gather(run_orchestrator(), concurrent_pinger())

    elapsed_to_ping = concurrent_done_at[0] - started
    # If extract blocks the loop, the ping waits ~BLOCK_SECONDS to run.
    # With to_thread it should run within ~100ms of being scheduled.
    assert elapsed_to_ping < 0.5, (
        f"Concurrent task completed only after {elapsed_to_ping:.2f}s "
        f"(threshold 0.5s) — extract_aleph is blocking the event loop."
    )
