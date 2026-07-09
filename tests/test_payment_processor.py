import pytest
from unittest.mock import MagicMock

from aleph_nodestatus.payment_processor import (
    apply_slippage,
    quote_amount_out,
)


@pytest.fixture(autouse=True)
def _stub_sizing_pass(monkeypatch):
    """Neutralize the unified sizing loop by default so the extract_aleph
    swap-path tests don't make live HTTP calls or require V4 pool state.

    Stubs pool_spot_rate, fair_aleph_rate, and bisect_swap_amount so the
    swap path always proceeds with the full effective_amount (no resize,
    no skip). A test that wants the loop to reject overrides individual
    stubs, which shadow this fixture's patches for that test."""
    import aleph_nodestatus.payment_processor as pp
    monkeypatch.setattr(pp, "pool_spot_rate", lambda *a, **k: 80.0)
    monkeypatch.setattr(pp, "fair_aleph_rate", lambda *a, **k: 80.0)
    monkeypatch.setattr(
        pp, "bisect_swap_amount",
        lambda *a, **k: {
            "settled_amount_in": k["upper_amount_in"],
            "iterations": [],
            "binding": None,
        },
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

    # `build_transaction` in real web3 always includes "to" (the contract
    # address) on top of whatever overrides the caller supplied — keep
    # the fixture consistent with that so audit-path consumers don't
    # KeyError on the returned tx_info["built_tx"]["to"].
    fake_built = {
        "chainId": 1, "nonce": 7,
        "to": "0x6b55F32Ea969910838defd03746Ced5E2AE8cB8B",
    }
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
    w3.eth.get_balance.return_value = 1_000_000  # ETH balance as int
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
    w3.eth.get_balance.return_value = 1_000_000  # ETH balance as int
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


def test_extract_aleph_unsupported_swap_config_is_explicit(monkeypatch, caplog):
    """A v2/v3 swap config makes pool_spot_rate raise NotImplementedError.
    That is a config/code mismatch, not a transient read failure: it must
    surface as an explicit ERROR with its own skipped_reason, not hide
    behind the generic spot_read_failed warning."""
    import logging

    import aleph_nodestatus.payment_processor as pp_mod

    w3 = MagicMock()
    w3.to_checksum_address = lambda x: x
    w3.eth.get_balance.return_value = 1_000_000
    processor = _mk_extract_processor(is_stable=False, swap_config_version=3)
    quoters = _mk_quoters_v3(call_return_value=(10_000, [0], [0], 0))
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
    # Shadow the autouse stub with the REAL pool_spot_rate so the v3
    # config actually raises NotImplementedError.
    monkeypatch.setattr(pp_mod, "pool_spot_rate", pool_spot_rate)

    with caplog.at_level(logging.ERROR):
        result = extract_aleph(
            w3, processor, quoters, account=None,
            from_address="0xC870B0Ca4B3d65f33E2a3c732ab3cD2aE555b14E",
            dry_run=True,
        )

    usdc = next(e for e in result["tokens"] if e["symbol"] == "USDC")
    assert usdc["skipped_reason"] == "unsupported_swap_config"
    assert usdc["swap_config_version"] == 3
    assert "unsupported" in caplog.text.lower()


def test_extract_aleph_quote_failure_appends_once(monkeypatch):
    """A quote failure produces exactly one entry in tokens and one in errors
    (not duplicates across both early and final append sites)."""
    w3 = MagicMock()
    w3.to_checksum_address = lambda x: x
    w3.eth.get_balance.return_value = 1_000_000  # ETH balance as int
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


def test_extract_aleph_skips_token_when_bisect_returns_price_deviation(monkeypatch):
    """When bisect_swap_amount returns settled=0 with binding='price_deviation',
    the token gets skipped_reason set and simulate_process is NOT reached."""
    from aleph_nodestatus.payment_processor import extract_aleph
    import aleph_nodestatus.payment_processor as pp

    w3 = MagicMock()
    w3.to_checksum_address = lambda x: x
    processor = _mk_extract_processor(is_stable=False)
    quoters = _mk_quoters_v3(call_return_value=(10_000, [0], [0], 0))

    erc20_mock = MagicMock()
    erc20_mock.functions.balanceOf.return_value.call.return_value = 1_000_000
    erc20_mock.functions.decimals.return_value.call.return_value = 6
    monkeypatch.setattr(pp, "_erc20_contract", lambda w3, addr: erc20_mock)

    simulate_calls = []
    monkeypatch.setattr(pp, "simulate_process",
                        lambda *a, **kw: simulate_calls.append(1) or None)

    monkeypatch.setattr(
        pp, "bisect_swap_amount",
        lambda *a, **k: {
            "settled_amount_in": 0,
            "iterations": [{"amount_in": k["upper_amount_in"], "fail": "price_deviation"}],
            "binding": "price_deviation",
        },
    )

    result = extract_aleph(
        w3, processor, quoters, account=None,
        from_address="0xC870B0Ca4B3d65f33E2a3c732ab3cD2aE555b14E",
        dry_run=True,
    )

    # USDC and ETH swap tokens should be skipped with price_deviation.
    skipped = [e for e in result["tokens"]
               if e.get("skipped_reason") == "price_deviation"]
    assert len(skipped) == 2, [e for e in result["tokens"]]
    # Neither deviating token reached simulate_process.
    # ALEPH (passthrough, no swap) still calls simulate once.
    assert len(simulate_calls) == 1


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
# Per-token sizing knobs: token_filter, max_amounts, min_amounts
# ---------------------------------------------------------------------------

def _mk_erc20_mock(balance: int, decimals: int = 6):
    """ERC20 mock used by the cap/min tests. Real USDC has 6 decimals;
    the human-units → wei conversion in `extract_aleph` reads decimals
    from this mock for any token that has a cap or floor configured."""
    m = MagicMock()
    m.functions.balanceOf.return_value.call.return_value = balance
    m.functions.decimals.return_value.call.return_value = decimals
    return m


def test_extract_aleph_token_filter_restricts_to_subset(monkeypatch):
    """Passing token_filter={'USDC'} short-circuits the other configured
    tokens BEFORE the balance read — they don't even appear in the
    result entries."""
    w3 = MagicMock()
    w3.to_checksum_address = lambda x: x
    processor = _mk_extract_processor(is_stable=False)
    quoters   = _mk_quoters_v3(call_return_value=(10_000, [0], [0], 0))

    erc20_mock = _mk_erc20_mock(balance=1_000_000)
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
        token_filter={"USDC"},
    )

    # ETH and ALEPH should be entirely absent — not "skipped".
    assert [e["symbol"] for e in result["tokens"]] == ["USDC"]


def test_extract_aleph_max_amount_caps_swap_input(monkeypatch):
    """max_amounts caps the amount_in passed to simulate/execute below
    the full contract balance. USDC has 6 decimals; cap of 500 USDC
    becomes 500_000_000 wei. simulate_process sees the capped value."""
    from decimal import Decimal

    w3 = MagicMock()
    w3.to_checksum_address = lambda x: x
    processor = _mk_extract_processor(is_stable=False)
    quoters   = _mk_quoters_v3(call_return_value=(10_000, [0], [0], 0))

    # Balance = 1000 USDC = 1_000_000_000 wei (6 dp)
    erc20_mock = _mk_erc20_mock(balance=1_000_000_000, decimals=6)
    monkeypatch.setattr(
        "aleph_nodestatus.payment_processor._erc20_contract",
        lambda w3, addr: erc20_mock,
    )
    simulate_kwargs = []
    monkeypatch.setattr(
        "aleph_nodestatus.payment_processor.simulate_process",
        lambda *a, **kw: simulate_kwargs.append(kw) or None,
    )

    result = extract_aleph(
        w3, processor, quoters, account=None,
        from_address="0xC870B0Ca4B3d65f33E2a3c732ab3cD2aE555b14E",
        dry_run=True,
        token_filter={"USDC"},
        max_amounts={"USDC": Decimal("500")},
    )

    usdc = result["tokens"][0]
    assert usdc["balance"]   == "1000000000"   # full on-chain balance
    assert usdc["amount_in"] == "500000000"    # capped to 500 USDC
    assert usdc.get("capped_from_balance") is True
    # simulate_process saw the capped amount, not the original balance.
    assert simulate_kwargs[0]["amount_in"] == 500_000_000


def test_extract_aleph_max_amount_above_balance_is_noop(monkeypatch):
    """If the configured cap exceeds the on-chain balance, the cap has
    no effect — amount_in stays at balance and `capped_from_balance` is
    not set. Prevents a confusing "(capped)" label in the summary when
    nothing was actually capped."""
    from decimal import Decimal

    w3 = MagicMock()
    w3.to_checksum_address = lambda x: x
    processor = _mk_extract_processor(is_stable=False)
    quoters   = _mk_quoters_v3(call_return_value=(10_000, [0], [0], 0))

    erc20_mock = _mk_erc20_mock(balance=100_000_000, decimals=6)  # 100 USDC
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
        token_filter={"USDC"},
        max_amounts={"USDC": Decimal("500")},  # > balance
    )

    usdc = result["tokens"][0]
    assert usdc["balance"]   == "100000000"
    assert usdc["amount_in"] == "100000000"
    assert "capped_from_balance" not in usdc


def test_extract_aleph_min_amount_skips_below_threshold(monkeypatch):
    """When balance is below the min_amounts floor, the token is skipped
    with reason 'below_min_amount'. No quote, simulate, or execute is
    invoked — the operator wants to wait until the balance accrues."""
    from decimal import Decimal

    w3 = MagicMock()
    w3.to_checksum_address = lambda x: x
    processor = _mk_extract_processor(is_stable=False)
    quoters   = _mk_quoters_v3(call_return_value=(10_000, [0], [0], 0))

    erc20_mock = _mk_erc20_mock(balance=50_000_000, decimals=6)  # 50 USDC
    monkeypatch.setattr(
        "aleph_nodestatus.payment_processor._erc20_contract",
        lambda w3, addr: erc20_mock,
    )
    quote_calls = []
    simulate_calls = []
    monkeypatch.setattr(
        "aleph_nodestatus.payment_processor.quote_amount_out",
        lambda *a, **kw: quote_calls.append(1) or 1,
    )
    monkeypatch.setattr(
        "aleph_nodestatus.payment_processor.simulate_process",
        lambda *a, **kw: simulate_calls.append(1) or None,
    )

    result = extract_aleph(
        w3, processor, quoters, account=None,
        from_address="0xC870B0Ca4B3d65f33E2a3c732ab3cD2aE555b14E",
        dry_run=True,
        token_filter={"USDC"},
        min_amounts={"USDC": Decimal("100")},
    )

    usdc = result["tokens"][0]
    assert usdc["skipped_reason"] == "below_min_amount"
    assert usdc["min_amount_wei"] == "100000000"
    assert quote_calls    == []
    assert simulate_calls == []


def test_extract_aleph_max_below_min_skips(monkeypatch):
    """Cap-below-min path: balance=1000 USDC, cap=50 USDC, min=100 USDC.
    Cap reduces to 50 USDC; that's below min; skipped. Catches the case
    where the operator capped tightly but the floor still protects gas."""
    from decimal import Decimal

    w3 = MagicMock()
    w3.to_checksum_address = lambda x: x
    processor = _mk_extract_processor(is_stable=False)
    quoters   = _mk_quoters_v3(call_return_value=(10_000, [0], [0], 0))

    erc20_mock = _mk_erc20_mock(balance=1_000_000_000, decimals=6)  # 1000 USDC
    monkeypatch.setattr(
        "aleph_nodestatus.payment_processor._erc20_contract",
        lambda w3, addr: erc20_mock,
    )
    simulate_calls = []
    monkeypatch.setattr(
        "aleph_nodestatus.payment_processor.simulate_process",
        lambda *a, **kw: simulate_calls.append(1) or None,
    )

    result = extract_aleph(
        w3, processor, quoters, account=None,
        from_address="0xC870B0Ca4B3d65f33E2a3c732ab3cD2aE555b14E",
        dry_run=True,
        token_filter={"USDC"},
        max_amounts={"USDC": Decimal("50")},
        min_amounts={"USDC": Decimal("100")},
    )

    usdc = result["tokens"][0]
    assert usdc["skipped_reason"] == "below_min_amount"
    # The cap brought us to 50 USDC before the floor check rejected it.
    assert usdc["amount_in"] == "50000000"
    assert simulate_calls == []


def test_extract_aleph_stable_dev_pct_uses_capped_amount(monkeypatch):
    """For stable tokens the contract trims dev_pct off the input BEFORE
    swapping. With cap=500 USDC and dev_pct=5, the quote must run on
    500 × 95/100 = 475 USDC — proving the cap propagates through the
    stable adjustment, not just to simulate/execute."""
    from decimal import Decimal

    w3 = MagicMock()
    w3.to_checksum_address = lambda x: x
    processor = _mk_extract_processor(is_stable=True, dev_pct=5)
    quoters   = _mk_quoters_v3(call_return_value=(10_000_000, [0], [0], 0))

    erc20_mock = _mk_erc20_mock(balance=1_000_000_000, decimals=6)
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
        token_filter={"USDC"},
        max_amounts={"USDC": Decimal("500")},
    )

    usdc = result["tokens"][0]
    assert usdc["amount_in"]      == "500000000"
    # 500_000_000 × 95 / 100 = 475_000_000
    assert usdc["swap_amount_in"] == "475000000"


# ---------------------------------------------------------------------------
# Auto-sizing integration: extract_aleph end-to-end
# ---------------------------------------------------------------------------

def test_extract_aleph_sizing_loop_always_runs_for_swap_tokens(monkeypatch):
    """The unified sizing loop runs for every non-ALEPH token regardless of
    max_price_impact_bps. The sizing loop (bisect_swap_amount) always runs for
    swap tokens; the pass-through stub (from _stub_sizing_pass) leaves
    amount_in unchanged."""
    import aleph_nodestatus.payment_processor as pp

    w3 = MagicMock()
    w3.to_checksum_address = lambda x: x
    w3.eth.get_balance.return_value = 1_000_000  # ETH balance as int
    processor = _mk_extract_processor(is_stable=False)
    quoters   = _mk_quoters_v3(call_return_value=(10_000, [0], [0], 0))

    erc20_mock = _mk_erc20_mock(balance=1_000_000)
    monkeypatch.setattr(
        "aleph_nodestatus.payment_processor._erc20_contract",
        lambda w3, addr: erc20_mock,
    )
    monkeypatch.setattr(
        "aleph_nodestatus.payment_processor.simulate_process",
        lambda *a, **kw: None,
    )

    bisect_new_calls = []
    orig_stub = pp.bisect_swap_amount  # the _stub_sizing_pass lambda

    def counting_bisect(*a, **k):
        bisect_new_calls.append(1)
        return orig_stub(*a, **k)

    monkeypatch.setattr(pp, "bisect_swap_amount", counting_bisect)

    result = extract_aleph(
        w3, processor, quoters, account=None,
        from_address="0xC870B0Ca4B3d65f33E2a3c732ab3cD2aE555b14E",
        dry_run=True,
        max_price_impact_bps=0,
    )
    # bisect_swap_amount is called for every non-ALEPH token (USDC + ETH = 2)
    assert len(bisect_new_calls) == 2
    # bisect_swap_amount_for_impact is NOT in the call path at all
    assert not hasattr(pp, "_bisect_for_impact_called"), "old path must not run"
    # amounts stayed at balance (stub returns upper_amount_in unchanged)
    for entry in result["tokens"]:
        if entry["symbol"] != "ALEPH":
            assert entry.get("auto_sized") is None


def test_extract_aleph_auto_sizing_shrinks_amount_in(monkeypatch):
    """With bisect_swap_amount returning a reduced settled amount, amount_in
    is shrunk and auto_sized is set, and the trace is stored in price_size_search."""
    import aleph_nodestatus.payment_processor as pp
    from decimal import Decimal

    w3 = MagicMock()
    w3.to_checksum_address = lambda x: x
    processor = _mk_extract_processor(is_stable=False)
    quoters   = _mk_quoters_v3(call_return_value=(10_000, [0], [0], 0))

    erc20_mock = _mk_erc20_mock(balance=50_000_000, decimals=6)  # 50 USDC
    monkeypatch.setattr(
        "aleph_nodestatus.payment_processor._erc20_contract",
        lambda w3, addr: erc20_mock,
    )
    simulate_kwargs = []
    monkeypatch.setattr(
        "aleph_nodestatus.payment_processor.simulate_process",
        lambda *a, **kw: simulate_kwargs.append(kw) or None,
    )

    # Stub bisect_swap_amount to return a reduced settled amount
    reduced = 10_000_000  # 10 USDC
    monkeypatch.setattr(
        pp, "bisect_swap_amount",
        lambda *a, **k: {
            "settled_amount_in": reduced,
            "iterations": [{"amount_in": k["upper_amount_in"], "fail": "price_impact_too_high"},
                           {"amount_in": reduced, "fail": None}],
            "binding": None,
        },
    )

    result = extract_aleph(
        w3, processor, quoters, account=None,
        from_address="0xC870B0Ca4B3d65f33E2a3c732ab3cD2aE555b14E",
        dry_run=True,
        token_filter={"USDC"},
    )

    usdc = result["tokens"][0]
    assert usdc["auto_sized"] is True
    assert int(usdc["amount_in"]) == reduced
    # simulate_process saw the auto-sized amount, not the full balance.
    assert simulate_kwargs[0]["amount_in"] == reduced
    # Trace recorded on the entry under the new key.
    assert usdc["price_size_search"]["settled_amount_in"] == reduced
    assert len(usdc["price_size_search"]["iterations"]) == 2


def test_extract_aleph_auto_sizing_skips_when_pool_too_shallow(monkeypatch):
    """bisect_swap_amount returning settled=0 with binding='price_impact_too_high'
    causes the token to be skipped with that reason."""
    from decimal import Decimal
    import aleph_nodestatus.payment_processor as pp

    w3 = MagicMock()
    w3.to_checksum_address = lambda x: x
    processor = _mk_extract_processor(is_stable=False)
    quoters   = _mk_quoters_v3(call_return_value=(10_000, [0], [0], 0))

    erc20_mock = _mk_erc20_mock(balance=500_000_000, decimals=6)
    monkeypatch.setattr(
        "aleph_nodestatus.payment_processor._erc20_contract",
        lambda w3, addr: erc20_mock,
    )
    simulate_calls = []
    monkeypatch.setattr(
        "aleph_nodestatus.payment_processor.simulate_process",
        lambda *a, **kw: simulate_calls.append(1) or None,
    )

    monkeypatch.setattr(
        pp, "bisect_swap_amount",
        lambda *a, **k: {
            "settled_amount_in": 0,
            "iterations": [{"amount_in": k["upper_amount_in"], "fail": "price_impact_too_high"}],
            "binding": "price_impact_too_high",
        },
    )

    result = extract_aleph(
        w3, processor, quoters, account=None,
        from_address="0xC870B0Ca4B3d65f33E2a3c732ab3cD2aE555b14E",
        dry_run=True,
        token_filter={"USDC"},
        min_amounts={"USDC": Decimal("10")},  # 10 USDC floor
    )

    usdc = result["tokens"][0]
    assert usdc["skipped_reason"] == "price_impact_too_high"
    assert simulate_calls == []


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


# ---------------------------------------------------------------------------
# audit_process_tx — fork-mode reconciliation
#
# Builds a synthetic receipt whose `logs` contain a real
# TokenPaymentsProcessed event for the audited token. The audit helper
# must decode it, populate entry["audit"], and either pass or hard-fail
# based on the realized-vs-expected delta against `reconcile_bps`.
# ---------------------------------------------------------------------------

from aleph_nodestatus.payment_processor import (
    audit_process_tx, _TOKEN_PAYMENTS_PROCESSED_EVENT_ABI,
)


def _mk_token_payments_log(
    w3, *, token: str, sender: str,
    amount: int, swap_amount: int, aleph_received: int,
    burned: int = 0,
    to_distribution: int = None,
    to_developers: int = 0,
    swap_version: int = 4,
    is_stable: bool = True,
):
    """Build a TokenPaymentsProcessed log dict in the exact shape
    `eth_getTransactionReceipt` returns — `topics` is a list of bytes
    and `data` is the ABI-encoded non-indexed payload."""
    if to_distribution is None:
        to_distribution = aleph_received  # default: nothing held back
    topic0 = w3.keccak(text=(
        "TokenPaymentsProcessed(address,address,uint256,uint256,uint256,"
        "uint256,uint256,uint256,uint8,bool)"
    ))
    # Indexed addresses are left-padded to 32 bytes.
    pad_addr = lambda a: bytes(12) + bytes.fromhex(a[2:].rjust(40, "0"))
    topics = [topic0, pad_addr(token), pad_addr(sender)]
    data = w3.codec.encode(
        ["uint256", "uint256", "uint256", "uint256", "uint256",
         "uint256", "uint8", "bool"],
        [amount, swap_amount, aleph_received, burned,
         to_distribution, to_developers, swap_version, is_stable],
    )
    return {
        "address":          "0x6b55F32Ea969910838defd03746Ced5E2AE8cB8B",
        "topics":           topics,
        "data":             data,
        "blockNumber":      19_482_103,
        "transactionIndex": 0,
        "logIndex":         0,
        "transactionHash":  b"\xab" * 32,
        "blockHash":        b"\xcd" * 32,
        "removed":          False,
    }


def _mk_audit_tx_info(receipt_logs, *, tx_hash="0xabc", gas_used=234_812):
    """Mimic execute_process's return shape with a synthetic receipt."""
    return {
        "tx_hash":  tx_hash,
        "status":   1,
        "receipt":  {
            "status":      1,
            "blockNumber": 19_482_103,
            "gasUsed":     gas_used,
            "logs":        receipt_logs,
        },
        "built_tx": {
            "from":    "0xC870B0Ca4B3d65f33E2a3c732ab3cD2aE555b14E",
            "to":      "0x6b55F32Ea969910838defd03746Ced5E2AE8cB8B",
            "gas":     250_000, "nonce": 42, "chainId": 1,
            "fn":      "process",
            "args":    {"token": "0xUSDC", "amountIn": 1, "minOut": 1, "ttl": 1800},
        },
    }


def _mk_real_w3():
    """A Web3 with no provider — only for codec/keccak helpers."""
    from web3 import Web3
    return Web3()


def test_audit_process_tx_clean_match_returns_true(capsys):
    """Realized output exactly equal to expected_out → reconciliation
    passes, entry["audit"] populated, no fail flag."""
    w3 = _mk_real_w3()
    token = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"  # USDC
    log = _mk_token_payments_log(
        w3, token=token, sender="0xC870B0Ca4B3d65f33E2a3c732ab3cD2aE555b14E",
        amount=582_330_000, swap_amount=553_213_500,
        aleph_received=27_827_564_813_933_937_875_752,
        to_distribution=27_827_564_813_933_937_875_752,
        to_developers=29_116_500,
    )
    entry = {"symbol": "USDC", "token": token}
    # Stub out the decimals lookup — `_token_decimals` would otherwise try
    # an on-chain call against a real ERC20 we don't have.
    import aleph_nodestatus.payment_processor as pp
    orig = pp._token_decimals
    pp._token_decimals = lambda w3, addr: 6 if addr == token else 18

    try:
        ok = audit_process_tx(
            w3, processor=None,
            tx_info=_mk_audit_tx_info([log]),
            entry=entry, token=token,
            expected_out=27_827_564_813_933_937_875_752,
            min_out=27_549_289_165_794_598_496_994,
            reconcile_bps=200,
        )
    finally:
        pp._token_decimals = orig

    assert ok is True
    assert entry["audit"]["reconcile_failed"] is False
    assert entry["audit"]["delta_expected_bps"] == 0
    assert entry["audit"]["aleph_received"] == "27827564813933937875752"
    out = capsys.readouterr().out
    assert "=== Extract tx audit: USDC === PASS" in out
    assert "TokenPaymentsProcessed" in out
    assert "reconciliation" in out


def test_audit_process_tx_delta_above_threshold_fails(capsys):
    """Realized 5% below expected with reconcile_bps=200 (2%) → fail flag
    set, function returns False, audit block prints FAIL."""
    w3 = _mk_real_w3()
    token = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
    expected = 27_827_564_813_933_937_875_752
    realized = expected * 95 // 100  # 5% below expected, well past 2%

    log = _mk_token_payments_log(
        w3, token=token, sender="0xC870B0Ca4B3d65f33E2a3c732ab3cD2aE555b14E",
        amount=582_330_000, swap_amount=553_213_500,
        aleph_received=realized,
        to_developers=29_116_500,
    )
    entry = {"symbol": "USDC", "token": token}
    import aleph_nodestatus.payment_processor as pp
    orig = pp._token_decimals
    pp._token_decimals = lambda w3, addr: 6 if addr == token else 18

    try:
        ok = audit_process_tx(
            w3, processor=None,
            tx_info=_mk_audit_tx_info([log]),
            entry=entry, token=token,
            expected_out=expected,
            min_out=expected * 99 // 100,
            reconcile_bps=200,
        )
    finally:
        pp._token_decimals = orig

    assert ok is False
    assert entry["audit"]["reconcile_failed"] is True
    # 5% below expected → -500 bps ± 1 due to integer floor.
    assert entry["audit"]["delta_expected_bps"] in (-501, -500)
    out = capsys.readouterr().out
    assert "=== Extract tx audit: USDC === FAIL" in out


def test_audit_process_tx_missing_event_marks_failure(capsys):
    """Receipt has no TokenPaymentsProcessed log for this token → audit
    records reason, marks failure, returns False."""
    w3 = _mk_real_w3()
    token = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
    entry = {"symbol": "USDC", "token": token}

    ok = audit_process_tx(
        w3, processor=None,
        tx_info=_mk_audit_tx_info([]),  # empty logs
        entry=entry, token=token,
        expected_out=27_827_564_813_933_937_875_752,
        min_out=27_549_289_165_794_598_496_994,
        reconcile_bps=200,
    )
    assert ok is False
    assert entry["audit"]["reconcile_failed"] is True
    assert "no TokenPaymentsProcessed" in entry["audit"]["reason"]
    out = capsys.readouterr().out
    assert "=== Extract tx audit: USDC === FAIL" in out


def test_audit_process_tx_filters_other_token_events(capsys):
    """Receipt has TokenPaymentsProcessed for a DIFFERENT token (ETH) but
    not for the audited one (USDC) → still treated as missing for USDC."""
    w3 = _mk_real_w3()
    usdc  = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
    other = "0x0000000000000000000000000000000000000000"  # ETH sentinel
    log = _mk_token_payments_log(
        w3, token=other, sender="0xC870B0Ca4B3d65f33E2a3c732ab3cD2aE555b14E",
        amount=1, swap_amount=1, aleph_received=1,
    )
    entry = {"symbol": "USDC", "token": usdc}

    ok = audit_process_tx(
        w3, processor=None,
        tx_info=_mk_audit_tx_info([log]),
        entry=entry, token=usdc,
        expected_out=1, min_out=1, reconcile_bps=200,
    )
    assert ok is False
    assert entry["audit"]["reconcile_failed"] is True


def test_audit_process_tx_aleph_passthrough_matches(capsys):
    """ALEPH passthrough: expected_out == balance, realized == balance
    (no swap). The reconciliation must accept this as a clean match
    even though swapAmount is 0."""
    w3 = _mk_real_w3()
    aleph = "0x27702a26126e0B3702af63Ee09aC4d1A084EF628"
    balance = 1_573_493 * 10**18
    log = _mk_token_payments_log(
        w3, token=aleph, sender="0xC870B0Ca4B3d65f33E2a3c732ab3cD2aE555b14E",
        amount=balance, swap_amount=0, aleph_received=balance,
        to_distribution=balance, to_developers=0,
        swap_version=0, is_stable=False,
    )
    entry = {"symbol": "ALEPH", "token": aleph}
    import aleph_nodestatus.payment_processor as pp
    orig = pp._token_decimals
    pp._token_decimals = lambda w3, addr: 18

    try:
        ok = audit_process_tx(
            w3, processor=None,
            tx_info=_mk_audit_tx_info([log]),
            entry=entry, token=aleph,
            expected_out=balance, min_out=0, reconcile_bps=200,
        )
    finally:
        pp._token_decimals = orig

    assert ok is True
    assert entry["audit"]["reconcile_failed"] is False
    assert entry["audit"]["aleph_received"] == str(balance)
    assert entry["audit"]["delta_expected_bps"] == 0
    # min_out was 0 → delta_min_bps is 0 by convention (we don't divide).
    assert entry["audit"]["delta_min_bps"] == 0


from aleph_nodestatus.payment_processor import pool_fee_bps


def test_pool_fee_bps_v4_single_hop():
    cfg = {"v": 4, "t": "0xUSDC",
           "v4": [["0xALEPH", 10000, 200, "0x0", b""]], "v3": b"", "v2": []}
    assert pool_fee_bps(cfg) == 100


def test_pool_fee_bps_v2_flat_30_per_hop():
    cfg = {"v": 2, "t": "0xA", "v2": ["0xA", "0xB"], "v3": b"", "v4": []}
    assert pool_fee_bps(cfg) == 30  # one hop


def test_pool_fee_bps_v3_decodes_and_sums():
    # path: token(20) + fee(3, 3000) + token(20) + fee(3, 500) + token(20)
    path = (b"\x11" * 20 + (3000).to_bytes(3, "big")
            + b"\x22" * 20 + (500).to_bytes(3, "big") + b"\x33" * 20)
    cfg = {"v": 3, "t": "0xA", "v3": path, "v2": [], "v4": []}
    assert pool_fee_bps(cfg) == 35  # 30 + 5


from aleph_nodestatus.payment_processor import pool_spot_rate, _v4_pool_id


def test_v4_pool_id_matches_known_fixture():
    # USDC/ALEPH 1% pool at block 25372912
    usdc = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
    aleph = "0x27702a26126e0B3702af63Ee09aC4d1A084EF628"
    c0, c1 = sorted([usdc.lower(), aleph.lower()])
    pid = _v4_pool_id(c0, c1, 10000, 200,
                      "0x0000000000000000000000000000000000000000")
    assert pid.hex() == (
        "8ee28047ee72104999ce30d35f92e1757a7a94a5ac2bc200f4c2da1eabfe6429"
    )


def test_pool_spot_rate_usdc_in_aleph_out(monkeypatch):
    import aleph_nodestatus.payment_processor as pp
    usdc = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
    aleph = "0x27702a26126e0B3702af63Ee09aC4d1A084EF628"
    cfg = {"v": 4, "t": usdc,
           "v4": [[aleph, 10000, 200,
                   "0x0000000000000000000000000000000000000000", b""]]}
    sqrtp = 8801253120988487230264  # slot0[0] at block 25372912
    sv = MagicMock()
    sv.functions.getSlot0.return_value.call.return_value = [sqrtp, 0, 0, 10000]
    w3 = MagicMock()
    w3.to_checksum_address.side_effect = lambda a: a
    w3.eth.contract.return_value = sv
    rate = pool_spot_rate(w3, cfg, usdc)
    # currency0=ALEPH, currency1=USDC; raw = USDC_wei/ALEPH_wei;
    # USDC->ALEPH wants ALEPH_wei/USDC_wei = 1/raw ~ 8.1e13
    raw = (sqrtp / 2 ** 96) ** 2
    assert abs(rate - (1.0 / raw)) / rate < 1e-9


def _spot_rate_with_slot0(sqrtp, cfg, token_in):
    sv = MagicMock()
    sv.functions.getSlot0.return_value.call.return_value = [sqrtp, 0, 0, 10000]
    w3 = MagicMock()
    w3.to_checksum_address.side_effect = lambda a: a
    w3.eth.contract.return_value = sv
    return pool_spot_rate(w3, cfg, token_in)


def test_pool_spot_rate_is_exact_for_extreme_prices():
    # sqrtPriceX96 with more significant bits than a float mantissa (53)
    # can hold: a float-based rate would mis-size the integer spot that
    # the fee-free impact check compares against.
    token_in = "0x1111111111111111111111111111111111111111"
    aleph = "0x27702a26126e0B3702af63Ee09aC4d1A084EF628"
    cfg = {"v": 4, "t": token_in,
           "v4": [[aleph, 10000, 200,
                   "0x0000000000000000000000000000000000000000", b""]]}
    sqrtp = (10 ** 7 << 96) + 12345678901234567
    # token_in sorts below ALEPH -> currency0=token_in -> rate = raw
    rate = _spot_rate_with_slot0(sqrtp, cfg, token_in)
    swap = 10 ** 18
    assert int(swap * rate) == swap * sqrtp ** 2 // 2 ** 192


def test_pool_spot_rate_inverted_orientation_is_exact():
    # Same requirement for the 1/raw orientation (ALEPH is currency0),
    # using the real USDC/ALEPH slot0 fixture from block 25372912.
    usdc = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
    aleph = "0x27702a26126e0B3702af63Ee09aC4d1A084EF628"
    cfg = {"v": 4, "t": usdc,
           "v4": [[aleph, 10000, 200,
                   "0x0000000000000000000000000000000000000000", b""]]}
    sqrtp = 8801253120988487230264
    rate = _spot_rate_with_slot0(sqrtp, cfg, usdc)
    swap = 10 ** 18
    assert int(swap * rate) == swap * 2 ** 192 // sqrtp ** 2


import aleph_nodestatus.payment_processor as pp
from aleph_nodestatus.payment_processor import bisect_swap_amount


def _make_quoters_linear(monkeypatch, *, mid_rate, impact_per_wei):
    """Patch quote_amount_out so out = swap*mid_rate*(1 - impact_per_wei*swap),
    i.e. impact grows with size. Fee is applied separately by the loop."""
    def fake(quoters, swap_config, amount_in, token_in=None):
        eff = mid_rate * (1 - impact_per_wei * amount_in)
        return int(amount_in * eff)
    monkeypatch.setattr(pp, "quote_amount_out", fake)


def test_bisect_shrinks_until_both_checks_pass(monkeypatch):
    # mid_rate 80 ALEPH/USDC-unit; fair slightly above mid (favorable source
    # gap absent); fee 100 bps; impact grows ~ with size.
    _make_quoters_linear(monkeypatch, mid_rate=80.0, impact_per_wei=1e-8)
    res = bisect_swap_amount(
        {}, {"v": 4, "t": "0xUSDC", "v4": [["0xALEPH", 10000, 200, "0x0", b""]]},
        token_in="0xUSDC",
        upper_amount_in=100_000_000,
        min_amount_in=1_000_000,
        fair_rate=80.0,            # ALEPH-wei per input-wei (mid)
        spot_rate=80.0,
        pool_fee_bps=100,
        max_deviation_bps=200,
        max_impact_bps=200,
    )
    assert 0 < res["settled_amount_in"] <= 100_000_000
    chosen = next(
        it for it in res["iterations"]
        if it["amount_in"] == res["settled_amount_in"]
    )
    assert chosen["fail"] is None
    assert chosen["impact_bps"] <= 200 and chosen["dev_api_bps"] <= 200


def test_bisect_skips_when_min_fails(monkeypatch):
    # Even the smallest amount has huge impact -> nothing fits.
    _make_quoters_linear(monkeypatch, mid_rate=80.0, impact_per_wei=1e-6)
    res = bisect_swap_amount(
        {}, {"v": 4, "t": "0xUSDC", "v4": [["0xALEPH", 10000, 200, "0x0", b""]]},
        token_in="0xUSDC",
        upper_amount_in=100_000_000,
        min_amount_in=1_000_000,
        fair_rate=80.0, spot_rate=80.0, pool_fee_bps=100,
        max_deviation_bps=200, max_impact_bps=200,
    )
    assert res["settled_amount_in"] == 0
    assert res["binding"] == "price_impact_too_high"


def test_bisect_binding_price_deviation(monkeypatch):
    # Pool depth is fine (spot==out_adj so impact~0) but fair is far above
    # out_adj -> dev_api binds.
    _make_quoters_linear(monkeypatch, mid_rate=80.0, impact_per_wei=0.0)
    res = bisect_swap_amount(
        {}, {"v": 4, "t": "0xUSDC", "v4": [["0xALEPH", 10000, 200, "0x0", b""]]},
        token_in="0xUSDC",
        upper_amount_in=100_000_000, min_amount_in=1_000_000,
        fair_rate=120.0,           # fair >> out_adj -> big deviation
        spot_rate=80.0 / (1 - 0.01),  # spot == out_adj -> impact 0
        pool_fee_bps=100,
        max_deviation_bps=200, max_impact_bps=200,
    )
    assert res["settled_amount_in"] == 0
    assert res["binding"] == "price_deviation"


def test_extract_aleph_aborts_when_credit_api_down(monkeypatch):
    import aleph_nodestatus.payment_processor as pp
    from aleph_nodestatus.price_oracle import CreditApiUnavailable

    w3 = MagicMock(); w3.to_checksum_address.side_effect = lambda a: a
    processor = MagicMock()
    processor.functions.developersPercentage.return_value.call.return_value = 5
    processor.functions.isStableToken.return_value.call.return_value = True
    processor.functions.getSwapConfig.return_value.call.return_value = (
        4, "0xUSDC", [], b"", [["0xALEPH", 10000, 200,
                                "0x0000000000000000000000000000000000000000", b""]])

    monkeypatch.setattr(pp, "_balance_of", lambda *a, **k: 500_000_000)
    monkeypatch.setattr(pp, "_human_to_wei", lambda w3, t, h: int(h) * 10 ** 6)
    monkeypatch.setattr(pp, "_token_decimals", lambda w3, t: 6)
    monkeypatch.setattr(pp, "pool_spot_rate", lambda *a, **k: 80.0)

    def boom(symbol):
        raise CreditApiUnavailable("down")
    monkeypatch.setattr(pp, "fair_aleph_rate", boom)

    from aleph_nodestatus.settings import settings as s
    monkeypatch.setattr(s, "process_tokens", [("USDC", "0xUSDC")])

    with pytest.raises(CreditApiUnavailable):
        pp.extract_aleph(w3, processor, {}, account=MagicMock(),
                         from_address="0xADMIN", dry_run=True,
                         transfer_enabled=False)
