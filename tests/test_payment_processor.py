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


from aleph_nodestatus.payment_processor import extract_aleph


def _mk_swap_config(v, t="0x000000000000000000000000000000000000abCd"):
    # Match the on-chain SwapConfig struct shape: (v, t, v2, v3, v4)
    return (v, t, [], b"\xde\xad", [])


def test_extract_aleph_dry_run_does_not_broadcast(monkeypatch):
    w3 = MagicMock()
    processor = MagicMock()
    quoter = MagicMock()

    processor.functions.getSwapConfig.return_value.call.return_value = _mk_swap_config(3)
    quoter.functions.quoteExactInput.return_value.call.return_value = (
        10_000, [0], [0], 0
    )

    erc20_mock = MagicMock()
    erc20_mock.functions.balanceOf.return_value.call.return_value = 1_000_000
    monkeypatch.setattr(
        "aleph_nodestatus.payment_processor._erc20_contract",
        lambda w3, addr: erc20_mock,
    )

    monkeypatch.setattr(
        "aleph_nodestatus.payment_processor.simulate_process",
        lambda *a, **kw: None,
    )

    execute_called = []
    monkeypatch.setattr(
        "aleph_nodestatus.payment_processor.execute_process",
        lambda *a, **kw: execute_called.append(1),
    )

    result = extract_aleph(
        w3, processor, quoter, account=None,
        from_address="0xC870B0Ca4B3d65f33E2a3c732ab3cD2aE555b14E",
        dry_run=True,
    )

    assert execute_called == []
    assert len(result["tokens"]) == 3
    for entry in result["tokens"]:
        assert entry["simulated_only"] is True


def test_extract_aleph_slippage_bps_override(monkeypatch):
    """Per-run slippage_bps override is passed to apply_slippage, not mutated."""
    w3 = MagicMock()
    processor = MagicMock()
    quoter = MagicMock()
    processor.functions.getSwapConfig.return_value.call.return_value = _mk_swap_config(3)
    quoter.functions.quoteExactInput.return_value.call.return_value = (
        10_000_000, [0], [0], 0
    )
    erc20_mock = MagicMock()
    erc20_mock.functions.balanceOf.return_value.call.return_value = 1_000_000
    monkeypatch.setattr(
        "aleph_nodestatus.payment_processor._erc20_contract",
        lambda w3, addr: erc20_mock,
    )
    monkeypatch.setattr(
        "aleph_nodestatus.payment_processor.simulate_process",
        lambda *a, **kw: None,
    )

    # Snapshot original value
    from aleph_nodestatus.settings import settings
    original = settings.process_slippage_bps

    result = extract_aleph(
        w3, processor, quoter, account=None,
        from_address="0xC870B0Ca4B3d65f33E2a3c732ab3cD2aE555b14E",
        dry_run=True,
        slippage_bps=500,   # 5% — different from default
    )

    # Side-effect free: settings unchanged
    assert settings.process_slippage_bps == original
    # USDC entry uses 5% slippage: min_out = 10_000_000 * 9500 / 10000 = 9_500_000
    usdc_entry = next(e for e in result["tokens"] if e["symbol"] == "USDC")
    assert int(usdc_entry["min_out"]) == 9_500_000


def test_extract_aleph_quote_failure_appends_once(monkeypatch):
    """A quote failure produces exactly one entry in tokens and one in errors
    (not duplicates across both early and final append sites)."""
    w3 = MagicMock()
    processor = MagicMock()
    quoter = MagicMock()
    processor.functions.getSwapConfig.return_value.call.return_value = _mk_swap_config(3)

    # quote raises on every call
    quoter.functions.quoteExactInput.return_value.call.side_effect = \
        RuntimeError("quoter unavailable")

    erc20_mock = MagicMock()
    erc20_mock.functions.balanceOf.return_value.call.return_value = 1_000_000
    monkeypatch.setattr(
        "aleph_nodestatus.payment_processor._erc20_contract",
        lambda w3, addr: erc20_mock,
    )
    monkeypatch.setattr(
        "aleph_nodestatus.payment_processor.simulate_process",
        lambda *a, **kw: None,
    )

    result = extract_aleph(
        w3, processor, quoter, account=None,
        from_address="0xC870B0Ca4B3d65f33E2a3c732ab3cD2aE555b14E",
        dry_run=True,
    )

    # 3 configured tokens; the ALEPH entry skips the quote, the other two fail
    assert len(result["tokens"]) == 3
    failing = [e for e in result["tokens"] if e["error"]]
    assert len(failing) == 2
    for e in failing:
        assert e["error"].startswith("quote_failed:")
    assert len(result["errors"]) == 2
    # Same entry objects on both sides (no duplicates)
    assert {id(e) for e in result["errors"]} <= {id(e) for e in result["tokens"]}


def test_extract_aleph_zero_balance_skipped(monkeypatch):
    w3 = MagicMock()
    processor = MagicMock()
    quoter = MagicMock()
    processor.functions.getSwapConfig.return_value.call.return_value = _mk_swap_config(3)
    erc20_mock = MagicMock()
    erc20_mock.functions.balanceOf.return_value.call.return_value = 0
    w3.eth.get_balance.return_value = 0
    monkeypatch.setattr(
        "aleph_nodestatus.payment_processor._erc20_contract",
        lambda w3, addr: erc20_mock,
    )
    monkeypatch.setattr(
        "aleph_nodestatus.payment_processor.simulate_process",
        lambda *a, **kw: None,
    )

    result = extract_aleph(
        w3, processor, quoter, account=None,
        from_address="0xC870B0Ca4B3d65f33E2a3c732ab3cD2aE555b14E",
        dry_run=True,
    )
    for entry in result["tokens"]:
        assert entry["skipped_reason"] == "zero_balance"
