import pytest
from unittest.mock import MagicMock

from aleph_nodestatus.payment_processor import (
    apply_slippage,
    quote_amount_out,
)


def test_apply_slippage_bps_200():
    assert apply_slippage(1_000_000, 200) == 980_000


def test_apply_slippage_zero():
    assert apply_slippage(1_000_000, 0) == 1_000_000


def test_apply_slippage_max_10000_rejects():
    with pytest.raises(ValueError):
        apply_slippage(1_000_000, 10_000)


def test_quote_amount_out_v3_calls_quoter():
    quoter = MagicMock()
    quoter.functions.quoteExactInput.return_value.call.return_value = (
        9_999_999, [0], [0], 0
    )
    swap_config = {"v": 3, "v3": b"\x01\x02"}
    out = quote_amount_out(quoter, swap_config, amount_in=1_000_000)
    assert out == 9_999_999
    quoter.functions.quoteExactInput.assert_called_once_with(b"\x01\x02", 1_000_000)


def test_quote_amount_out_v2_uses_v2_path(monkeypatch):
    quoter = MagicMock()
    swap_config = {"v": 2, "v2": ["0xA", "0xB"]}
    with pytest.raises(NotImplementedError, match="V2"):
        quote_amount_out(quoter, swap_config, amount_in=1_000_000)


def test_quote_amount_out_v4_raises():
    quoter = MagicMock()
    swap_config = {"v": 4, "v4": []}
    with pytest.raises(NotImplementedError, match="V4"):
        quote_amount_out(quoter, swap_config, amount_in=1_000_000)


from aleph_nodestatus.payment_processor import simulate_process


def test_simulate_process_success_returns_no_error(monkeypatch):
    w3 = MagicMock()
    w3.eth.call.return_value = b""
    processor = MagicMock()
    processor.encodeABI.return_value = b"\xab\xcd"

    err = simulate_process(
        w3, processor,
        from_address="0xC870B0Ca4B3d65f33E2a3c732ab3cD2aE555b14E",
        token="0xUSDC", amount_in=1_000_000, min_out=999_000, ttl=1800,
    )
    assert err is None
    w3.eth.call.assert_called_once()


def test_simulate_process_revert_returns_message(monkeypatch):
    class ContractLogicError(Exception):
        pass
    w3 = MagicMock()
    w3.eth.call.side_effect = ContractLogicError("InsufficientOutput()")
    processor = MagicMock()
    processor.encodeABI.return_value = b"\xab\xcd"

    err = simulate_process(
        w3, processor,
        from_address="0xC870B0Ca4B3d65f33E2a3c732ab3cD2aE555b14E",
        token="0xUSDC", amount_in=1_000_000, min_out=999_000_000, ttl=1800,
        contract_logic_error_cls=ContractLogicError,
    )
    assert "InsufficientOutput" in err


from aleph_nodestatus.payment_processor import execute_process


def test_execute_process_signs_and_sends(monkeypatch):
    w3 = MagicMock()
    w3.eth.get_transaction_count.return_value = 7
    w3.to_wei.return_value = 1_000_000_000
    w3.eth.get_block.return_value.baseFeePerGas = 5_000_000_000

    fake_built = {"chainId": 1, "nonce": 7}
    processor = MagicMock()
    processor.functions.process.return_value.build_transaction.return_value = fake_built

    acct = MagicMock()
    acct.address = "0xC870B0Ca4B3d65f33E2a3c732ab3cD2aE555b14E"
    signed = MagicMock()
    signed.rawTransaction = b"\xde\xad"
    acct.sign_transaction.return_value = signed

    w3.eth.send_raw_transaction.return_value.hex.return_value = "0xfeedbeef"
    w3.eth.wait_for_transaction_receipt.return_value = {
        "status": 1, "transactionHash": MagicMock(hex=lambda: "0xfeedbeef"),
    }

    result = execute_process(
        w3, processor, account=acct,
        token="0xUSDC", amount_in=1_000_000, min_out=999_000, ttl=1800,
    )

    assert result["tx_hash"] == "0xfeedbeef"
    assert result["status"] == 1
    processor.functions.process.assert_called_once_with(
        "0xUSDC", 1_000_000, 999_000, 1800
    )
