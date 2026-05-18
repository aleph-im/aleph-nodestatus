"""Unit tests for the credit_extraction orchestrator."""

import sys
import types

# Same plyvel stub used in other CLI-adjacent tests.
if "plyvel" not in sys.modules:
    _plyvel_stub = types.ModuleType("plyvel")
    _plyvel_stub.DB = object
    sys.modules["plyvel"] = _plyvel_stub

from unittest.mock import MagicMock

import pytest


@pytest.mark.asyncio
async def test_process_credit_extraction_calls_extract_aleph(monkeypatch):
    """Calculation-only run: invokes extract_aleph via to_thread and returns
    its dict unchanged."""
    import aleph_nodestatus.credit_extraction as ce

    fake_web3 = MagicMock()
    fake_web3.eth.get_balance.return_value = 10 ** 18  # plenty of ETH
    monkeypatch.setattr(ce, "get_web3", lambda: fake_web3)
    monkeypatch.setattr(ce, "get_processor_contract", lambda w3: MagicMock())
    monkeypatch.setattr(ce, "get_quoter_contract",    lambda w3: MagicMock())
    monkeypatch.setattr(ce, "get_v2_router_contract", lambda w3: MagicMock())
    monkeypatch.setattr(ce, "get_v4_quoter_contract", lambda w3: MagicMock())

    captured = {}
    def fake_extract_aleph(w3, processor, quoters, **kwargs):
        captured.update(kwargs)
        return {"tokens": [{"symbol": "ALEPH"}], "errors": []}
    monkeypatch.setattr(ce, "extract_aleph", fake_extract_aleph)

    result = await ce.process_credit_extraction(
        act=False, dry_run=False, transfer=False,
    )

    assert result == {"tokens": [{"symbol": "ALEPH"}], "errors": []}
    # calculation-only path: dry_run forwarded as True (because transfer=False)
    assert captured["dry_run"] is True
    assert captured["transfer_enabled"] is False
