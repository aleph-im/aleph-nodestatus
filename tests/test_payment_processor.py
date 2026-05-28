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
    v3_quoter = MagicMock()
    v3_quoter.functions.quoteExactInput.return_value.call.return_value = (
        9_999_999, [0], [0], 0
    )
    quoters = {"v2": None, "v3": v3_quoter, "v4": None}
    swap_config = {"v": 3, "v3": b"\x01\x02"}
    out = quote_amount_out(quoters, swap_config, amount_in=1_000_000)
    assert out == 9_999_999
    v3_quoter.functions.quoteExactInput.assert_called_once_with(
        b"\x01\x02", 1_000_000,
    )


def test_quote_amount_out_v2_uses_v2_router():
    """V2 path delegates to the router's `getAmountsOut` and returns the
    last hop's output."""
    v2_router = MagicMock()
    v2_router.functions.getAmountsOut.return_value.call.return_value = [
        1_000_000, 500_000, 2_500,
    ]
    quoters = {"v2": v2_router, "v3": None, "v4": None}
    path = ["0x" + "a" * 40, "0x" + "b" * 40, "0x" + "c" * 40]
    swap_config = {"v": 2, "v2": path}
    out = quote_amount_out(quoters, swap_config, amount_in=1_000_000)
    assert out == 2_500
    v2_router.functions.getAmountsOut.assert_called_once_with(1_000_000, path)


def test_quote_amount_out_v4_uses_v4_quoter():
    """V4 path calls `quoteExactInput` with the (token_in, path, amount_in)
    tuple required by Uniswap V4."""
    v4_quoter = MagicMock()
    v4_quoter.functions.quoteExactInput.return_value.call.return_value = (
        9_000_000, 0,
    )
    quoters = {"v2": None, "v3": None, "v4": v4_quoter}
    token_in = "0xC870B0Ca4B3d65f33E2a3c732ab3cD2aE555b14E"
    swap_config = {"v": 4, "v4": []}
    out = quote_amount_out(
        quoters, swap_config, amount_in=1_000_000, token_in=token_in,
    )
    assert out == 9_000_000
    v4_quoter.functions.quoteExactInput.assert_called_once_with(
        (token_in, [], 1_000_000),
    )


def test_quote_amount_out_missing_quoter_raises():
    """When the configured swap version has no matching quoter instance,
    quote_amount_out raises a clear ValueError rather than silently
    returning a MagicMock."""
    quoters = {"v2": None, "v3": None, "v4": None}
    with pytest.raises(ValueError, match="V3"):
        quote_amount_out(quoters, {"v": 3, "v3": b""}, amount_in=1)


from aleph_nodestatus.payment_processor import simulate_process


def test_simulate_process_success_returns_no_error(monkeypatch):
    w3 = MagicMock()
    w3.eth.call.return_value = b""
    processor = MagicMock()
    processor.encode_abi.return_value = "0xabcd"

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
    processor.encode_abi.return_value = "0xabcd"

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
    processor.functions.process.return_value.estimate_gas.return_value = 200_000
    processor.functions.process.return_value.build_transaction.return_value = fake_built

    acct = MagicMock()
    acct.address = "0xC870B0Ca4B3d65f33E2a3c732ab3cD2aE555b14E"
    signed = MagicMock()
    signed.raw_transaction = b"\xde\xad"
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
    build_tx_call_args = processor.functions.process.return_value.build_transaction.call_args
    assert build_tx_call_args[0][0]["gas"] == 250_000  # 200_000 * 5 // 4


from aleph_nodestatus.payment_processor import extract_aleph


def _mk_swap_config(v, t="0x000000000000000000000000000000000000abCd"):
    # Match the on-chain SwapConfig struct shape: (v, t, v2, v3, v4)
    return (v, t, [], b"\xde\xad", [])


def test_extract_aleph_dry_run_does_not_broadcast(monkeypatch):
    w3 = MagicMock()
    w3.to_checksum_address = lambda x: x   # identity, so the ALEPH-token
                                           # `token_lc == aleph_address.lower()`
                                           # comparison resolves correctly
                                           # under the mocked w3.
    processor = _mk_extract_processor(is_stable=False)
    quoters   = _mk_quoters_v3(call_return_value=(10_000, [0], [0], 0))

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
        w3, processor, quoters, account=None,
        from_address="0xC870B0Ca4B3d65f33E2a3c732ab3cD2aE555b14E",
        dry_run=True,
    )

    assert execute_called == []
    assert len(result["tokens"]) == 3
    for entry in result["tokens"]:
        assert entry["simulated_only"] is True


def _mk_quoters_v3(call_return_value=None, call_side_effect=None):
    """Build the {v2, v3, v4} dict that extract_aleph expects, with the V3
    quoter configured. V2/V4 entries are None — fine for ALEPH-tracked
    tokens that all use V3 paths."""
    v3 = MagicMock()
    if call_side_effect is not None:
        v3.functions.quoteExactInput.return_value.call.side_effect = (
            call_side_effect
        )
    else:
        v3.functions.quoteExactInput.return_value.call.return_value = (
            call_return_value
        )
    return {"v2": None, "v3": v3, "v4": None}


def _mk_extract_processor(is_stable=False, dev_pct=5, swap_config_version=3):
    """Build a MagicMock processor pre-wired with the three view calls
    that extract_aleph makes: getSwapConfig, developersPercentage,
    isStableToken. Defaults exercise the non-stable path (dev cut off);
    pass `is_stable=True` to drive the production-style stable-token
    branch with an explicit dev_pct."""
    proc = MagicMock()
    proc.functions.getSwapConfig.return_value.call.return_value = (
        _mk_swap_config(swap_config_version)
    )
    proc.functions.developersPercentage.return_value.call.return_value = dev_pct
    proc.functions.isStableToken.return_value.call.return_value = is_stable
    return proc


def test_extract_aleph_slippage_bps_override(monkeypatch):
    """Per-run slippage_bps override is passed to apply_slippage, not mutated.
    Also drives the stable-token dev-cut path with realistic values
    (dev_pct=5, isStableToken=True) so the quote happens on the post-deduction
    amount rather than the full balance."""
    w3 = MagicMock()
    w3.to_checksum_address = lambda x: x
    processor = _mk_extract_processor(is_stable=True, dev_pct=5)
    quoters   = _mk_quoters_v3(call_return_value=(10_000_000, [0], [0], 0))
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
        w3, processor, quoters, account=None,
        from_address="0xC870B0Ca4B3d65f33E2a3c732ab3cD2aE555b14E",
        dry_run=True,
        slippage_bps=500,   # 5% — different from default
    )

    # Side-effect free: settings unchanged
    assert settings.process_slippage_bps == original
    usdc_entry = next(e for e in result["tokens"] if e["symbol"] == "USDC")
    # The contract deducts 5% of `amount_in` BEFORE swapping; we should
    # have quoted on the post-deduction amount (1_000_000 × 95 / 100).
    assert usdc_entry["amount_in"]      == "1000000"
    assert usdc_entry["swap_amount_in"] == "950000"
    # min_out = expected_out × (1 - 5%) = 10_000_000 × 0.95 = 9_500_000.
    # (The mock quoter returns 10M regardless of input, so this asserts the
    # slippage math, not the quote math.)
    assert int(usdc_entry["min_out"]) == 9_500_000


def test_extract_aleph_quote_failure_appends_once(monkeypatch):
    """A quote failure produces exactly one entry in tokens and one in errors
    (not duplicates across both early and final append sites)."""
    w3 = MagicMock()
    w3.to_checksum_address = lambda x: x
    processor = _mk_extract_processor(is_stable=False)
    quoters = _mk_quoters_v3(
        call_side_effect=RuntimeError("quoter unavailable"),
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

    result = extract_aleph(
        w3, processor, quoters, account=None,
        from_address="0xC870B0Ca4B3d65f33E2a3c732ab3cD2aE555b14E",
        dry_run=True,
    )

    # 3 configured tokens; the ALEPH entry skips the quote, the other two fail
    assert len(result["tokens"]) == 3
    failing = [e for e in result["tokens"] if e["error"]]
    assert len(failing) == 2
    for e in failing:
        assert e["error"].startswith("quote_failed:")
    # One errors entry per failing token entry — no duplicates.
    assert len(result["errors"]) == len(failing)


def test_extract_aleph_skips_token_when_oracle_says_not_ok(monkeypatch):
    """When pool_oracle.check_swap_price_deviation returns ok=False, the
    token gets skipped_reason set and the swap is NOT attempted (no
    quote_amount_out, no simulate_process)."""
    from aleph_nodestatus.payment_processor import extract_aleph
    import aleph_nodestatus.payment_processor as pp
    from aleph_nodestatus.pool_oracle import OracleResult

    w3 = MagicMock()
    w3.to_checksum_address = lambda x: x
    processor = _mk_extract_processor(is_stable=False)
    quoters = _mk_quoters_v3(call_return_value=(10_000, [0], [0], 0))

    erc20_mock = MagicMock()
    erc20_mock.functions.balanceOf.return_value.call.return_value = 1_000_000
    monkeypatch.setattr(pp, "_erc20_contract", lambda w3, addr: erc20_mock)

    quote_calls = []
    simulate_calls = []
    monkeypatch.setattr(pp, "quote_amount_out",
                        lambda *a, **kw: quote_calls.append(1) or 10_000)
    monkeypatch.setattr(pp, "simulate_process",
                        lambda *a, **kw: simulate_calls.append(1) or None)

    monkeypatch.setattr(
        pp, "check_swap_price_deviation",
        lambda w3, cfg, token_in: OracleResult(
            ok=False, reason="price_deviation",
            deviation_bps=350, spot_price=1.04, ref_price=1.00,
        ),
    )

    result = extract_aleph(
        w3, processor, quoters, account=None,
        from_address="0xC870B0Ca4B3d65f33E2a3c732ab3cD2aE555b14E",
        dry_run=True,
    )

    # ALEPH has no swap → still simulated. USDC and ETH have swaps → both
    # should be flagged as price_deviation, no quote/simulate called.
    skipped = [e for e in result["tokens"]
               if e.get("skipped_reason") == "price_deviation"]
    assert len(skipped) == 2, [e for e in result["tokens"]]
    for e in skipped:
        assert e["oracle"]["deviation_bps"] == 350
        assert e["oracle"]["spot_price"] == 1.04
        assert e["oracle"]["ref_price"] == 1.00
    assert quote_calls == []     # never quoted the deviating tokens


def test_extract_aleph_zero_balance_skipped(monkeypatch):
    w3 = MagicMock()
    w3.to_checksum_address = lambda x: x
    processor = _mk_extract_processor(is_stable=False)
    quoters   = _mk_quoters_v3(call_return_value=(10_000, [0], [0], 0))
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
        w3, processor, quoters, account=None,
        from_address="0xC870B0Ca4B3d65f33E2a3c732ab3cD2aE555b14E",
        dry_run=True,
    )
    for entry in result["tokens"]:
        assert entry["skipped_reason"] == "zero_balance"


# ---------------------------------------------------------------------------
# Real-Contract regression tests for simulate_process
#
# The MagicMock-based tests above auto-vivify ANY attribute name on the
# processor mock, so they silently survived the web3.py v6 → v7
# `encodeABI` → `encode_abi` rename that broke production. The tests
# below build a real `Contract` from the vendored AlephPaymentProcessor
# ABI; any future API rename trips an AttributeError instead of passing
# CI and exploding at runtime.
# ---------------------------------------------------------------------------

from web3 import Web3

from aleph_nodestatus.payment_processor import _load_abi
from aleph_nodestatus.settings import settings as _pp_settings


def _real_processor_contract():
    """A real (offline) Contract bound to the configured processor address
    and the vendored AlephPaymentProcessor ABI. No RPC required for
    encode_abi / decode_function_input."""
    w3 = Web3()
    contract = w3.eth.contract(
        address=w3.to_checksum_address(_pp_settings.payment_processor_address),
        abi=_load_abi("AlephPaymentProcessor"),
    )
    return w3, contract


def test_simulate_process_uses_real_v7_contract_api():
    """Regression: the actual web3.py v7 Contract.encode_abi method exists,
    is called with the correct kwargs, and produces the ABI-encoded
    process() payload that gets handed to eth_call.

    If web3.py ever renames encode_abi again, this test fails loudly
    at import/call time, instead of waiting for the docker cron to
    crash."""
    w3, contract = _real_processor_contract()
    captured: dict = {}

    def fake_call(tx, *args, **kwargs):
        captured.update(tx)
        return b""
    w3.eth.call = fake_call

    token = Web3.to_checksum_address("0x" + "aa" * 20)
    from_addr = "0xC870B0Ca4B3d65f33E2a3c732ab3cD2aE555b14E"
    err = simulate_process(
        w3, contract,
        from_address=from_addr,
        token=token,
        amount_in=1_000_000, min_out=950_000, ttl=1800,
    )
    assert err is None
    assert captured["from"] == from_addr
    assert captured["to"] == w3.to_checksum_address(
        _pp_settings.payment_processor_address,
    )
    # 4-byte selector + 4 × 32-byte words, hex-encoded with "0x"
    assert isinstance(captured["data"], str) and len(captured["data"]) == 2 + 8 + 4 * 64
    expected_selector = Web3.keccak(
        text="process(address,uint128,uint128,uint48)",
    ).hex()[:8]
    assert captured["data"][2:10].lower() == expected_selector.lower()

    fn, decoded = contract.decode_function_input(captured["data"])
    assert fn.fn_name == "process"
    assert decoded["_token"].lower() == token.lower()
    assert decoded["_amountIn"] == 1_000_000
    assert decoded["_amountOutMinimum"] == 950_000
    assert decoded["_ttl"] == 1800


def test_simulate_process_revert_on_real_contract_surfaces_message():
    """Regression: a ContractLogicError from eth_call (the realistic revert
    surface) is caught and returned as an error string — using a real
    Contract, not a MagicMock that would mask the encoding path."""
    from web3.exceptions import ContractLogicError

    w3, contract = _real_processor_contract()

    def fake_call(tx, *args, **kwargs):
        raise ContractLogicError("InsufficientOutput()")
    w3.eth.call = fake_call

    err = simulate_process(
        w3, contract,
        from_address="0xC870B0Ca4B3d65f33E2a3c732ab3cD2aE555b14E",
        token=Web3.to_checksum_address("0x" + "aa" * 20),
        amount_in=1_000_000, min_out=10**18, ttl=1800,
    )
    assert err is not None
    assert "InsufficientOutput" in err


def test_simulate_process_calls_v7_encode_abi_kwargs():
    """Lock the v7 encode_abi keyword surface (`abi_element_identifier`,
    `args`) so a future rename — or an accidental revert to the legacy
    `fn_name=` kwarg — is caught at unit-test time, not at runtime."""
    w3 = MagicMock()
    w3.eth.call.return_value = b""
    processor = MagicMock()
    processor.encode_abi.return_value = "0x" + "00" * 4

    simulate_process(
        w3, processor,
        from_address="0xC870B0Ca4B3d65f33E2a3c732ab3cD2aE555b14E",
        token="0xUSDC", amount_in=1_000_000, min_out=999_000, ttl=1800,
    )

    processor.encode_abi.assert_called_once_with(
        abi_element_identifier="process",
        args=["0xUSDC", 1_000_000, 999_000, 1800],
    )
    # The legacy camelCase entry point must NOT be invoked.
    assert not processor.encodeABI.called
