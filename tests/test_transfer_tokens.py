"""transfer_tokens balance pre-check: a batch that exactly equals the
available balance is spendable and must not be rejected; only a batch
strictly above the balance fails."""
import asyncio
from unittest.mock import MagicMock

import aleph_nodestatus.ethereum as ethereum
from aleph_nodestatus.ethereum import DECIMALS, transfer_tokens


def _run_transfer(monkeypatch, *, balance_aleph, targets):
    contract = MagicMock()
    contract.functions.balanceOf.return_value.call.return_value = int(
        balance_aleph * DECIMALS
    )
    w3 = MagicMock()
    w3.eth.get_transaction_count.return_value = 7
    w3.eth.send_raw_transaction.return_value.hex.return_value = "0xabc"
    account = MagicMock()
    account.address = "0xSENDER"

    monkeypatch.setattr(ethereum, "get_web3", lambda: w3)
    monkeypatch.setattr(ethereum, "get_token_contract", lambda _w3: contract)
    monkeypatch.setattr(ethereum, "get_eth_account", lambda: account)
    monkeypatch.setattr(ethereum, "get_gas_info", lambda _w3: (10, 1))
    monkeypatch.setattr(ethereum, "NONCE", None)

    metadata = asyncio.run(transfer_tokens(targets, metadata={}))
    return metadata["targets"][0]


def test_transfer_equal_to_balance_is_allowed(monkeypatch):
    result = _run_transfer(
        monkeypatch, balance_aleph=100.0, targets={"0xA": 60.0, "0xB": 40.0},
    )
    assert result["success"] is True
    assert result["tx"] == "0xabc"


def test_transfer_above_balance_is_rejected(monkeypatch):
    result = _run_transfer(
        monkeypatch, balance_aleph=100.0, targets={"0xA": 60.0, "0xB": 40.5},
    )
    assert result["success"] is False
    assert result["tx"] is None
