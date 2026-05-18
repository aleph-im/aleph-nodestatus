"""Verify extract_aleph is offloaded so the asyncio loop stays free."""

import sys
import types

if "plyvel" not in sys.modules:
    _plyvel_stub = types.ModuleType("plyvel")
    _plyvel_stub.DB = object
    sys.modules["plyvel"] = _plyvel_stub

import asyncio
import time
from unittest.mock import MagicMock

import pytest


@pytest.mark.asyncio
async def test_extract_does_not_block_event_loop(monkeypatch):
    """A blocking extract_aleph must not stall a concurrent asyncio task."""
    import aleph_nodestatus.credit_extraction as ce

    BLOCK_SECONDS = 1.5

    def fake_extract_aleph(*args, **kwargs):
        time.sleep(BLOCK_SECONDS)
        return {"tokens": [], "errors": []}
    monkeypatch.setattr(ce, "extract_aleph", fake_extract_aleph)

    fake_web3 = MagicMock()
    fake_web3.eth.get_balance.return_value = 10 ** 20
    fake_web3.to_wei = lambda n, unit: int(n * 1e9)
    monkeypatch.setattr(ce, "get_web3", lambda: fake_web3)
    monkeypatch.setattr(ce, "get_processor_contract", lambda w3: MagicMock())
    monkeypatch.setattr(ce, "get_quoter_contract",    lambda w3: MagicMock())
    monkeypatch.setattr(ce, "get_v2_router_contract", lambda w3: MagicMock())
    monkeypatch.setattr(ce, "get_v4_quoter_contract", lambda w3: MagicMock())

    concurrent_done_at = [None]

    async def concurrent_pinger():
        await asyncio.sleep(0.05)
        concurrent_done_at[0] = time.monotonic()

    started = time.monotonic()

    async def run_orchestrator():
        await ce.process_credit_extraction(
            act=False, dry_run=True, transfer=False,
        )

    await asyncio.gather(run_orchestrator(), concurrent_pinger())

    elapsed_to_ping = concurrent_done_at[0] - started
    assert elapsed_to_ping < 0.5, (
        f"Concurrent task completed only after {elapsed_to_ping:.2f}s "
        f"(threshold 0.5s) — extract_aleph is blocking the event loop."
    )
