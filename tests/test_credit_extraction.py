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


@pytest.mark.asyncio
async def test_admin_pkey_fallback_to_ethereum_pkey(monkeypatch, caplog):
    """Empty payment_processor_admin_pkey falls back to ethereum_pkey with a
    WARNING log."""
    import logging
    import aleph_nodestatus.credit_extraction as ce
    from aleph_nodestatus.settings import settings as s

    monkeypatch.setattr(s, "payment_processor_admin_pkey", "")
    # 32-byte valid hex key (eth_account expects 0x + 64 hex chars).
    monkeypatch.setattr(s, "ethereum_pkey", "0x" + "11" * 32)

    fake_web3 = MagicMock()
    fake_web3.eth.get_balance.return_value = 10 ** 20
    fake_web3.to_wei = lambda n, unit: int(n * 1e9)
    monkeypatch.setattr(ce, "get_web3", lambda: fake_web3)
    monkeypatch.setattr(ce, "get_processor_contract", lambda w3: MagicMock())
    monkeypatch.setattr(ce, "get_quoter_contract",    lambda w3: MagicMock())
    monkeypatch.setattr(ce, "get_v2_router_contract", lambda w3: MagicMock())
    monkeypatch.setattr(ce, "get_v4_quoter_contract", lambda w3: MagicMock())

    captured = {}
    def fake_extract_aleph(w3, processor, quoters, **kwargs):
        captured.update(kwargs)
        return {"tokens": [], "errors": []}
    monkeypatch.setattr(ce, "extract_aleph", fake_extract_aleph)

    with caplog.at_level(logging.WARNING, logger="aleph_nodestatus.credit_extraction"):
        await ce.process_credit_extraction(act=True, dry_run=False, transfer=True)

    assert captured["account"] is not None
    assert captured["from_address"] == captured["account"].address
    assert any("payment_processor_admin_pkey not set" in r.message
               for r in caplog.records)


@pytest.mark.asyncio
async def test_admin_pkey_set_no_warning(monkeypatch, caplog):
    """When payment_processor_admin_pkey is set, no fallback warning is emitted."""
    import logging
    import aleph_nodestatus.credit_extraction as ce
    from aleph_nodestatus.settings import settings as s

    monkeypatch.setattr(s, "payment_processor_admin_pkey", "0x" + "22" * 32)
    monkeypatch.setattr(s, "ethereum_pkey", "0x" + "33" * 32)

    fake_web3 = MagicMock()
    fake_web3.eth.get_balance.return_value = 10 ** 20
    fake_web3.to_wei = lambda n, unit: int(n * 1e9)
    monkeypatch.setattr(ce, "get_web3", lambda: fake_web3)
    monkeypatch.setattr(ce, "get_processor_contract", lambda w3: MagicMock())
    monkeypatch.setattr(ce, "get_quoter_contract",    lambda w3: MagicMock())
    monkeypatch.setattr(ce, "get_v2_router_contract", lambda w3: MagicMock())
    monkeypatch.setattr(ce, "get_v4_quoter_contract", lambda w3: MagicMock())

    monkeypatch.setattr(ce, "extract_aleph",
                        lambda *a, **kw: {"tokens": [], "errors": []})

    with caplog.at_level(logging.WARNING, logger="aleph_nodestatus.credit_extraction"):
        await ce.process_credit_extraction(act=True, dry_run=False, transfer=True)

    assert not any("payment_processor_admin_pkey not set" in r.message
                   for r in caplog.records)
