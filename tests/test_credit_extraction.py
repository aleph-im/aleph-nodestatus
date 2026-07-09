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
        return {
            "tokens": [{
                "symbol": "ALEPH", "token": "0xB", "amount_in": "0",
                "swap_amount_in": None, "min_out": None, "expected_out": None,
                "tx_hash": None, "simulated_only": False,
                "skipped_reason": "zero_balance", "error": None,
            }],
            "errors": [],
        }
    monkeypatch.setattr(ce, "extract_aleph", fake_extract_aleph)

    result = await ce.process_credit_extraction(
        act=False, dry_run=False, transfer=False, immediate=True,
    )

    # process_credit_extraction now returns an exit code (int), not the
    # raw extract_block dict — the dict is consumed via _print_summary
    # and the orchestrator surfaces only the exit-code contract the CLI
    # needs. Calc-only path with no errors → 0.
    assert result == 0
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
        await ce.process_credit_extraction(
            act=True, dry_run=False, transfer=True, immediate=True,
        )

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
        await ce.process_credit_extraction(
            act=True, dry_run=False, transfer=True, immediate=True,
        )

    assert not any("payment_processor_admin_pkey not set" in r.message
                   for r in caplog.records)


@pytest.mark.asyncio
async def test_eth_preflight_warns_on_low_balance(monkeypatch, caplog):
    """Admin ETH balance below the gas headroom triggers a WARNING."""
    import logging
    import aleph_nodestatus.credit_extraction as ce
    from aleph_nodestatus.settings import settings as s

    monkeypatch.setattr(s, "payment_processor_admin_pkey", "0x" + "22" * 32)

    fake_web3 = MagicMock()
    fake_web3.eth.get_balance.return_value = 1  # 1 wei — way under headroom
    fake_web3.to_wei = lambda n, unit: int(n * 1e9)
    monkeypatch.setattr(ce, "get_web3", lambda: fake_web3)
    monkeypatch.setattr(ce, "get_processor_contract", lambda w3: MagicMock())
    monkeypatch.setattr(ce, "get_quoter_contract",    lambda w3: MagicMock())
    monkeypatch.setattr(ce, "get_v2_router_contract", lambda w3: MagicMock())
    monkeypatch.setattr(ce, "get_v4_quoter_contract", lambda w3: MagicMock())
    monkeypatch.setattr(ce, "extract_aleph",
                        lambda *a, **kw: {"tokens": [], "errors": []})

    with caplog.at_level(logging.WARNING, logger="aleph_nodestatus.credit_extraction"):
        await ce.process_credit_extraction(
            act=True, dry_run=False, transfer=True, immediate=True,
        )

    assert any("recommended" in r.message and "ETH" in r.message
               for r in caplog.records)


@pytest.mark.asyncio
async def test_eth_preflight_skipped_in_dry_run(monkeypatch, caplog):
    """Dry-run / calculation-only paths do not read get_balance, so no
    preflight warning is emitted even when balance is 0."""
    import logging
    import aleph_nodestatus.credit_extraction as ce

    fake_web3 = MagicMock()
    fake_web3.eth.get_balance.return_value = 0
    fake_web3.to_wei = lambda n, unit: int(n * 1e9)
    monkeypatch.setattr(ce, "get_web3", lambda: fake_web3)
    monkeypatch.setattr(ce, "get_processor_contract", lambda w3: MagicMock())
    monkeypatch.setattr(ce, "get_quoter_contract",    lambda w3: MagicMock())
    monkeypatch.setattr(ce, "get_v2_router_contract", lambda w3: MagicMock())
    monkeypatch.setattr(ce, "get_v4_quoter_contract", lambda w3: MagicMock())
    monkeypatch.setattr(ce, "extract_aleph",
                        lambda *a, **kw: {"tokens": [], "errors": []})

    with caplog.at_level(logging.WARNING, logger="aleph_nodestatus.credit_extraction"):
        await ce.process_credit_extraction(act=False, dry_run=True, transfer=False)

    assert not any("recommended" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_stdout_summary_per_token(monkeypatch, capsys):
    """Each token entry produces one summary line; final tally reports errors."""
    import aleph_nodestatus.credit_extraction as ce

    fake_web3 = MagicMock()
    fake_web3.eth.get_balance.return_value = 10 ** 20
    fake_web3.to_wei = lambda n, unit: int(n * 1e9)
    monkeypatch.setattr(ce, "get_web3", lambda: fake_web3)
    monkeypatch.setattr(ce, "get_processor_contract", lambda w3: MagicMock())
    monkeypatch.setattr(ce, "get_quoter_contract",    lambda w3: MagicMock())
    monkeypatch.setattr(ce, "get_v2_router_contract", lambda w3: MagicMock())
    monkeypatch.setattr(ce, "get_v4_quoter_contract", lambda w3: MagicMock())

    def fake_extract_aleph(*a, **kw):
        return {
            "tokens": [
                {"symbol": "USDC", "token": "0xA", "amount_in": "1000",
                 "swap_amount_in": "950", "min_out": "9000",
                 "expected_out": "9500", "tx_hash": "0xdeadbeef",
                 "simulated_only": False, "skipped_reason": None, "error": None},
                {"symbol": "ETH",  "token": "0x0", "amount_in": "0",
                 "swap_amount_in": None, "min_out": None, "expected_out": None,
                 "tx_hash": None, "simulated_only": False,
                 "skipped_reason": "zero_balance", "error": None},
                {"symbol": "ALEPH","token": "0xB", "amount_in": "500",
                 "swap_amount_in": None, "min_out": None, "expected_out": None,
                 "tx_hash": None, "simulated_only": False,
                 "skipped_reason": None, "error": "tx_failed: boom"},
                {"symbol": "DAI",  "token": "0xC", "amount_in": "2000",
                 "swap_amount_in": "1900", "min_out": "1800",
                 "expected_out": "1850", "tx_hash": None,
                 "simulated_only": True,
                 "skipped_reason": None, "error": None},
            ],
            "errors": [{"symbol": "ALEPH"}],
        }
    monkeypatch.setattr(ce, "extract_aleph", fake_extract_aleph)

    await ce.process_credit_extraction(act=False, dry_run=True, transfer=False)

    out = capsys.readouterr().out
    assert "USDC" in out and "0xdeadbeef" in out
    assert "ETH"  in out and "zero_balance" in out
    assert "ALEPH" in out and "tx_failed" in out
    assert "DAI" in out and "simulated_only" in out and "1800" in out
    assert "1 error" in out


@pytest.mark.asyncio
async def test_random_delay_sleeps_by_default(monkeypatch):
    """For act=True, dry_run=False, transfer=True, immediate=False:
    a uniformly chosen delay is slept before extract_aleph runs."""
    import aleph_nodestatus.credit_extraction as ce
    from aleph_nodestatus.settings import settings as s

    monkeypatch.setattr(s, "payment_processor_admin_pkey", "0x" + "22" * 32)
    monkeypatch.setattr(s, "extract_random_delay_max_seconds", 60)

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

    captured = {"delay": None}
    async def fake_sleep(d):
        captured["delay"] = d
    monkeypatch.setattr(ce.asyncio, "sleep", fake_sleep)
    monkeypatch.setattr(ce.random, "randint", lambda lo, hi: 42)

    await ce.process_credit_extraction(
        act=True, dry_run=False, transfer=True, immediate=False,
    )

    assert captured["delay"] == 42


@pytest.mark.asyncio
async def test_random_delay_skipped_when_immediate(monkeypatch):
    """immediate=True bypasses the sleep regardless of other flags."""
    import aleph_nodestatus.credit_extraction as ce
    from aleph_nodestatus.settings import settings as s

    monkeypatch.setattr(s, "payment_processor_admin_pkey", "0x" + "22" * 32)
    monkeypatch.setattr(s, "extract_random_delay_max_seconds", 3540)

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

    sleep_calls = []
    async def fake_sleep(d):
        sleep_calls.append(d)
    monkeypatch.setattr(ce.asyncio, "sleep", fake_sleep)

    await ce.process_credit_extraction(
        act=True, dry_run=False, transfer=True, immediate=True,
    )

    assert sleep_calls == []


@pytest.mark.asyncio
async def test_random_delay_skipped_when_dry_run(monkeypatch):
    """dry_run=True bypasses the sleep regardless of immediate."""
    import aleph_nodestatus.credit_extraction as ce
    from aleph_nodestatus.settings import settings as s

    monkeypatch.setattr(s, "extract_random_delay_max_seconds", 3540)

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

    sleep_calls = []
    async def fake_sleep(d):
        sleep_calls.append(d)
    monkeypatch.setattr(ce.asyncio, "sleep", fake_sleep)

    await ce.process_credit_extraction(
        act=False, dry_run=True, transfer=False, immediate=False,
    )

    assert sleep_calls == []


@pytest.mark.asyncio
async def test_random_delay_skipped_when_max_is_zero(monkeypatch):
    """extract_random_delay_max_seconds=0 disables jitter entirely."""
    import aleph_nodestatus.credit_extraction as ce
    from aleph_nodestatus.settings import settings as s

    monkeypatch.setattr(s, "payment_processor_admin_pkey", "0x" + "22" * 32)
    monkeypatch.setattr(s, "extract_random_delay_max_seconds", 0)

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

    sleep_calls = []
    async def fake_sleep(d):
        sleep_calls.append(d)
    monkeypatch.setattr(ce.asyncio, "sleep", fake_sleep)

    await ce.process_credit_extraction(
        act=True, dry_run=False, transfer=True, immediate=False,
    )

    assert sleep_calls == []


@pytest.mark.asyncio
async def test_stdout_summary_renders_price_deviation(monkeypatch, capsys):
    """A token with skipped_reason='price_deviation' shows dev/impact bps
    from the smallest-amount iteration of price_size_search."""
    import aleph_nodestatus.credit_extraction as ce

    fake_web3 = MagicMock()
    fake_web3.eth.get_balance.return_value = 10 ** 20
    fake_web3.to_wei = lambda n, unit: int(n * 1e9)
    monkeypatch.setattr(ce, "get_web3", lambda: fake_web3)
    monkeypatch.setattr(ce, "get_processor_contract", lambda w3: MagicMock())
    monkeypatch.setattr(ce, "get_quoter_contract",    lambda w3: MagicMock())
    monkeypatch.setattr(ce, "get_v2_router_contract", lambda w3: MagicMock())
    monkeypatch.setattr(ce, "get_v4_quoter_contract", lambda w3: MagicMock())

    monkeypatch.setattr(ce, "extract_aleph", lambda *a, **kw: {
        "tokens": [
            {"symbol": "USDC", "token": "0xA", "amount_in": "1000",
             "swap_amount_in": None, "min_out": None, "expected_out": None,
             "tx_hash": None, "simulated_only": False,
             "skipped_reason": "price_deviation", "error": None,
             "price_size_search": {"settled_amount_in": 0, "binding": "price_deviation",
                 "iterations": [{"amount_in": 1000, "dev_api_bps": 350,
                                 "impact_bps": 12, "fail": "price_deviation"}]}},
        ],
        "errors": [],
    })

    await ce.process_credit_extraction(
        act=False, dry_run=True, transfer=False,
    )

    out = capsys.readouterr().out
    assert "USDC" in out and "price_deviation" in out
    assert "350" in out  # dev_api_bps shown


# ---------------------------------------------------------------------------
# Pkey validation — catches the three operator mistakes that previously
# surfaced as `binascii.Error: Non-hexadecimal digit found` from inside
# `Account.from_key(HexBytes(...))` with no useful context.
# ---------------------------------------------------------------------------

from aleph_nodestatus.credit_extraction import _validate_pkey


def test_validate_pkey_accepts_64_hex_chars_with_prefix():
    assert _validate_pkey("0x" + "ab" * 32, source="ethereum_pkey") is None


def test_validate_pkey_accepts_64_hex_chars_without_prefix():
    assert _validate_pkey("ab" * 32, source="ethereum_pkey") is None


def test_validate_pkey_empty_returns_clear_error():
    err = _validate_pkey("", source="payment_processor_admin_pkey")
    assert err is not None
    assert "empty" in err.lower()
    assert "payment_processor_admin_pkey" in err


def test_validate_pkey_none_returns_clear_error():
    err = _validate_pkey(None, source="ethereum_pkey")
    assert err is not None
    assert "empty" in err.lower()


def test_validate_pkey_template_placeholder_caught(capsys):
    """`.env` left with the example template (`<processor_admin_private_key>`)
    must produce a clear error pointing at the source file, not a
    binascii traceback. This is the foot-gun the user actually hit."""
    err = _validate_pkey(
        "<processor_admin_private_key>",
        source="payment_processor_admin_pkey",
    )
    assert err is not None
    msg = err.lower()
    assert ".env.extract" in msg or ".env.dist" in msg
    # Either the length check or the hex-char check would catch it;
    # we don't pin which, but both must produce something actionable.
    assert "placeholder" in msg or "non-hex" in msg or "wrong length" in msg


def test_validate_pkey_wrong_length_reports_actual_length():
    err = _validate_pkey("0xabcd", source="ethereum_pkey")
    assert err is not None
    assert "len=4" in err  # body length after stripping 0x
    assert "64" in err


def test_validate_pkey_invalid_hex_char_caught():
    """A single non-hex character (e.g. an accidental `g` or stray
    quote) must be flagged with the bad characters echoed back so
    the operator can find them in the env file."""
    bad = "0x" + "g" + "a" * 63
    err = _validate_pkey(bad, source="ethereum_pkey")
    assert err is not None
    assert "non-hex" in err.lower()
    assert "'g'" in err  # the bad chars are echoed


def test_validate_pkey_fork_mode_intro_mentions_fork():
    err = _validate_pkey("", source="ethereum_pkey",
                         fork_rpc="http://localhost:8545")
    assert err is not None
    assert "fork" in err.lower()

