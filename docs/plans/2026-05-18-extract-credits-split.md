# Split credit extraction into its own CLI — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Spec:** `docs/specs/2026-05-18-extract-credits-split.md`

**Goal:** Replace the embedded `extract` step inside `nodestatus-distribute-credits` with a standalone `nodestatus-extract-credits` CLI so the two operations can run on independent cadences.

**Architecture:** Add a new orchestrator module `credit_extraction.py` that wraps the existing `payment_processor.extract_aleph` primitive (admin-pkey load, ETH preflight, async offload, stdout summary). Expose it via a Click command `extract_credits` and a new console script. Strip the corresponding logic, flag, payload field, and setting out of the distribute pipeline. The extract command does **not** publish anything to Aleph — on-chain `process()` tx hashes are the durable audit trail.

**Tech Stack:** Python 3, Click (CLI), web3.py (Ethereum), eth-account, pytest + pytest-asyncio (tests), `asyncio.to_thread` for offloading the sync web3 calls.

**File map:**

- **Create** `src/aleph_nodestatus/credit_extraction.py` — orchestrator module (admin resolution, ETH preflight, `extract_aleph` invocation, stdout summary).
- **Create** `tests/test_credit_extraction.py` — unit tests for the new orchestrator.
- **Create** `tests/test_extract_credits_cli.py` — Click CLI tests for the new command.
- **Modify** `src/aleph_nodestatus/commands.py` — register the new command, strip the extract step + `--no-extract` flag + `extract` payload field from `distribute_credits`, drop now-unused imports.
- **Modify** `src/aleph_nodestatus/settings.py` — remove `credit_dist_extract_enabled`.
- **Modify** `setup.cfg` — add the `nodestatus-extract-credits` console script.
- **Modify** `tests/test_async_extract.py` — retarget at the new orchestrator (the distribute orchestrator no longer calls `extract_aleph`).
- **Modify** `tests/test_distribute_credits_cli.py` — drop `--no-extract` assertions / kwargs.
- **Modify** `tests/test_credit_pipeline.py` — drop `--no-extract` from CLI invocations.
- **Modify** `tests/test_settings_new.py` — drop the `credit_dist_extract_enabled` assertion.

**Conventions:**
- Existing test files start with a `plyvel` stub (the storage module has a native dep that fails to import in some envs). New test files that import from `commands.py` or `storage.py` must do the same — see `tests/test_distribute_credits_cli.py:1-10`.
- All Click commands in this repo use lowercase `-v/--verbose` count flags and `--act` / `--dry-run` semantics with explicit mutual-exclusivity checks that `sys.exit(2)`.
- Pytest invocations: `pytest tests/<file>.py -v` (the repo uses pytest config from `setup.cfg`).
- Repo working dir: `/Users/angelmanzano/Projects/aleph/back/aleph-nodestatus`. All paths below are relative to that.
- Branch: `feat/credit-distribution-v2`. Commit directly here.

---

## Task 1: Create `credit_extraction.py` skeleton and happy-path test

Build the smallest version of `process_credit_extraction` that calls `extract_aleph` through `asyncio.to_thread` and returns whatever it produced. No fallback, no preflight, no summary yet — those land in later tasks.

**Files:**
- Create: `src/aleph_nodestatus/credit_extraction.py`
- Test:   `tests/test_credit_extraction.py`

- [ ] **Step 1: Write the failing happy-path test**

Create `tests/test_credit_extraction.py`:

```python
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_credit_extraction.py -v`

Expected: FAIL with `ModuleNotFoundError: No module named 'aleph_nodestatus.credit_extraction'`.

- [ ] **Step 3: Create the skeleton module**

Create `src/aleph_nodestatus/credit_extraction.py`:

```python
"""Orchestration for the credit-extraction CLI.

Wraps payment_processor.extract_aleph with the operational glue that
used to live inside process_credit_distribution: admin-account
resolution, ETH preflight, async offload of the sync web3 calls, and a
human-readable stdout summary.

No Aleph audit post is written. The on-chain process() transactions
are the canonical record of every extraction; ops review happens via
stdout / log aggregation.
"""

import asyncio
import logging
from typing import Optional

from .ethereum import get_web3
from .payment_processor import (
    extract_aleph,
    get_processor_contract,
    get_quoter_contract,
    get_v2_router_contract,
    get_v4_quoter_contract,
)
from .settings import settings

LOGGER = logging.getLogger(__name__)


async def process_credit_extraction(
    *, act: bool, dry_run: bool, transfer: bool,
    slippage_bps: Optional[int] = None,
) -> dict:
    """Run one extract pass. Returns the dict produced by extract_aleph."""
    web3 = get_web3()

    processor = get_processor_contract(web3)
    quoters = {
        "v2": get_v2_router_contract(web3),
        "v3": get_quoter_contract(web3),
        "v4": get_v4_quoter_contract(web3),
    }

    extract_block = await asyncio.to_thread(
        extract_aleph,
        web3, processor, quoters,
        account=None,
        from_address=settings.payment_processor_admin_address,
        dry_run=dry_run or not transfer,
        transfer_enabled=transfer,
        slippage_bps=(
            slippage_bps if slippage_bps is not None
            else settings.process_slippage_bps
        ),
    )
    return extract_block
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_credit_extraction.py -v`

Expected: PASS (1 test).

- [ ] **Step 5: Commit**

```bash
git add src/aleph_nodestatus/credit_extraction.py tests/test_credit_extraction.py
git commit -m "$(cat <<'EOF'
feat(extract): add credit_extraction orchestrator skeleton

Introduces process_credit_extraction wrapping extract_aleph via
asyncio.to_thread. Calculation-only path verified.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 2: Admin pkey resolution with fallback warning

Load the admin private key from `payment_processor_admin_pkey` for real (--act + transfer) runs, falling back to `ethereum_pkey` with a WARNING log. Calculation-only / dry-run paths don't need the key.

**Files:**
- Modify: `src/aleph_nodestatus/credit_extraction.py`
- Test:   `tests/test_credit_extraction.py`

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_credit_extraction.py`:

```python
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_credit_extraction.py::test_admin_pkey_fallback_to_ethereum_pkey tests/test_credit_extraction.py::test_admin_pkey_set_no_warning -v`

Expected: both FAIL — the implementation does not yet load an Account.

- [ ] **Step 3: Add admin resolution to the orchestrator**

Edit `src/aleph_nodestatus/credit_extraction.py`. Add at the top of the imports:

```python
from eth_account import Account
from hexbytes import HexBytes
```

Replace the body of `process_credit_extraction` (everything between the docstring and the `return extract_block`) with:

```python
    web3 = get_web3()

    admin_account = None
    admin_address = settings.payment_processor_admin_address
    if act and not dry_run and transfer:
        pk = settings.payment_processor_admin_pkey or settings.ethereum_pkey
        if not settings.payment_processor_admin_pkey:
            LOGGER.warning(
                "payment_processor_admin_pkey not set; falling back to "
                "ethereum_pkey"
            )
        admin_account = Account.from_key(HexBytes(pk))
        admin_address = admin_account.address

    processor = get_processor_contract(web3)
    quoters = {
        "v2": get_v2_router_contract(web3),
        "v3": get_quoter_contract(web3),
        "v4": get_v4_quoter_contract(web3),
    }

    extract_block = await asyncio.to_thread(
        extract_aleph,
        web3, processor, quoters,
        account=admin_account,
        from_address=admin_address,
        dry_run=dry_run or not transfer,
        transfer_enabled=transfer,
        slippage_bps=(
            slippage_bps if slippage_bps is not None
            else settings.process_slippage_bps
        ),
    )
```

- [ ] **Step 4: Run all tests in the new file**

Run: `pytest tests/test_credit_extraction.py -v`

Expected: all 3 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/aleph_nodestatus/credit_extraction.py tests/test_credit_extraction.py
git commit -m "$(cat <<'EOF'
feat(extract): load admin pkey with ethereum_pkey fallback warning

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 3: ETH preflight balance check

Mirror the gas-headroom warning that today lives in `commands.py:376-391`. Fires only when the orchestrator will broadcast (`act and not dry_run and transfer`).

**Files:**
- Modify: `src/aleph_nodestatus/credit_extraction.py`
- Test:   `tests/test_credit_extraction.py`

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_credit_extraction.py`:

```python
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
        await ce.process_credit_extraction(act=True, dry_run=False, transfer=True)

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
```

- [ ] **Step 2: Run tests to verify they fail / pass status**

Run: `pytest tests/test_credit_extraction.py::test_eth_preflight_warns_on_low_balance tests/test_credit_extraction.py::test_eth_preflight_skipped_in_dry_run -v`

Expected: the first FAILS (no preflight implemented yet); the second PASSES (no preflight = no warning).

- [ ] **Step 3: Add the preflight inside the act+transfer branch**

In `src/aleph_nodestatus/credit_extraction.py`, inside the `if act and not dry_run and transfer:` block, after the `admin_address = admin_account.address` line, append:

```python
        admin_eth_wei = web3.eth.get_balance(admin_address)
        gas_headroom = (
            settings.process_gas_ceiling
            * int(web3.to_wei(50, "gwei"))
            * len(settings.process_tokens)
        )
        if admin_eth_wei < gas_headroom:
            LOGGER.warning(
                "Admin %s has %.4f ETH; recommended >= %.4f ETH to cover "
                "~%d process() transactions at 50 gwei. Extract may fail "
                "partway if gas runs out.",
                admin_address,
                admin_eth_wei / 1e18,
                gas_headroom / 1e18,
                len(settings.process_tokens),
            )
```

- [ ] **Step 4: Run tests to verify all pass**

Run: `pytest tests/test_credit_extraction.py -v`

Expected: all 5 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/aleph_nodestatus/credit_extraction.py tests/test_credit_extraction.py
git commit -m "$(cat <<'EOF'
feat(extract): warn when admin ETH balance is below gas headroom

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 4: Stdout summary

Print a per-token outcome summary at the end of every run (success, simulated, skipped, error). This is the human-facing replacement for the (now-absent) Aleph audit post.

**Files:**
- Modify: `src/aleph_nodestatus/credit_extraction.py`
- Test:   `tests/test_credit_extraction.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_credit_extraction.py`:

```python
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
            ],
            "errors": [{"symbol": "ALEPH"}],
        }
    monkeypatch.setattr(ce, "extract_aleph", fake_extract_aleph)

    await ce.process_credit_extraction(act=False, dry_run=True, transfer=False)

    out = capsys.readouterr().out
    assert "USDC" in out and "0xdeadbeef" in out
    assert "ETH"  in out and "zero_balance" in out
    assert "ALEPH" in out and "tx_failed" in out
    assert "1 error" in out
```

- [ ] **Step 2: Run test to verify failure**

Run: `pytest tests/test_credit_extraction.py::test_stdout_summary_per_token -v`

Expected: FAIL — no summary is printed yet.

- [ ] **Step 3: Add the summary helper and call it**

In `src/aleph_nodestatus/credit_extraction.py`, add this `import` at the top with the others:

```python
import click
```

Append at module bottom:

```python
def _print_summary(extract_block: dict) -> None:
    click.echo("=== Extract summary ===")
    for entry in extract_block.get("tokens", []):
        line = f"  {entry['symbol']:6} balance={entry['amount_in']}"
        if entry.get("skipped_reason"):
            line += f" skipped={entry['skipped_reason']}"
        elif entry.get("error"):
            line += f" ERROR={entry['error']}"
        elif entry.get("simulated_only"):
            line += f" simulated_only min_out={entry['min_out']}"
        else:
            line += f" tx_hash={entry['tx_hash']}"
        click.echo(line)
    n_err = len(extract_block.get("errors", []))
    click.echo(f"  {n_err} error(s)" if n_err else "  no errors")
```

Then inside `process_credit_extraction`, change the final two lines from:

```python
    extract_block = await asyncio.to_thread(
        ...
    )
```

to:

```python
    extract_block = await asyncio.to_thread(
        ...
    )

    _print_summary(extract_block)
    return extract_block
```

(Leave the `await asyncio.to_thread(...)` invocation itself unchanged — only the trailing two lines are new.)

- [ ] **Step 4: Run tests**

Run: `pytest tests/test_credit_extraction.py -v`

Expected: all 6 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/aleph_nodestatus/credit_extraction.py tests/test_credit_extraction.py
git commit -m "$(cat <<'EOF'
feat(extract): print per-token stdout summary at end of run

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 5: Add `extract_credits` Click command + entry point

Wire the orchestrator behind a Click command in `commands.py`, register the console script in `setup.cfg`, and cover the CLI surface with tests.

**Files:**
- Modify: `src/aleph_nodestatus/commands.py`
- Modify: `setup.cfg`
- Create: `tests/test_extract_credits_cli.py`

- [ ] **Step 1: Write the failing CLI tests**

Create `tests/test_extract_credits_cli.py`:

```python
"""Click CLI tests for nodestatus-extract-credits."""

import sys
import types

if "plyvel" not in sys.modules:
    plyvel_stub = types.ModuleType("plyvel")
    plyvel_stub.DB = object
    sys.modules["plyvel"] = plyvel_stub

from unittest.mock import AsyncMock  # noqa: E402

from click.testing import CliRunner  # noqa: E402

from aleph_nodestatus.commands import extract_credits  # noqa: E402


def test_cli_help_lists_extract_flags():
    runner = CliRunner()
    result = runner.invoke(extract_credits, ["--help"])
    assert result.exit_code == 0
    for flag in ("--dry-run", "--act", "--no-transfer", "--slippage-bps"):
        assert flag in result.output


def test_cli_act_and_dry_run_mutually_exclusive():
    runner = CliRunner()
    result = runner.invoke(extract_credits, ["--act", "--dry-run"])
    assert result.exit_code != 0
    assert "mutually exclusive" in result.output.lower()


def test_cli_dry_run_invokes_orchestrator_with_transfer_false(monkeypatch):
    """--dry-run forces transfer=False in the orchestrator call."""
    import aleph_nodestatus.commands as cmd

    captured = {}
    async def fake_orch(**kwargs):
        captured.update(kwargs)
        return {"tokens": [], "errors": []}
    monkeypatch.setattr(cmd, "process_credit_extraction", fake_orch)

    runner = CliRunner()
    result = runner.invoke(extract_credits, ["--dry-run"])
    assert result.exit_code == 0, result.output
    assert captured["dry_run"] is True
    assert captured["transfer"] is False
    assert captured["act"] is False


def test_cli_no_testnet_flag():
    """Extract talks to Ethereum, not Aleph — --testnet is intentionally absent."""
    runner = CliRunner()
    result = runner.invoke(extract_credits, ["--testnet"])
    assert result.exit_code != 0  # unknown flag
```

- [ ] **Step 2: Run tests to verify failure**

Run: `pytest tests/test_extract_credits_cli.py -v`

Expected: import fails — `extract_credits` does not exist yet on `commands.py`.

- [ ] **Step 3: Add the Click command to `commands.py`**

Edit `src/aleph_nodestatus/commands.py`. Add this import alongside the other module-level imports (e.g. right after `from .credit_distribution import (...)`):

```python
from .credit_extraction import process_credit_extraction
```

Append a new command function at the end of the file, just before the `def run()` block at the bottom:

```python
@click.command()
@click.option("-v", "--verbose", count=True)
@click.option("-a", "--act", help="Broadcast process() txs", is_flag=True)
@click.option("--dry-run", "dry_run", is_flag=True,
              help="No broadcast; only eth_call simulate")
@click.option("--no-transfer", "no_transfer", is_flag=True,
              help="Compute + simulate, don't broadcast")
@click.option("--slippage-bps", "slippage_bps", default=None, type=int,
              help="Override per-run slippage tolerance")
def extract_credits(verbose, act, dry_run, no_transfer, slippage_bps):
    """Extract ALEPH from the AlephPaymentProcessor (no Aleph publish)."""
    setup_logging(verbose)

    if act and dry_run:
        click.echo("ERROR: --act and --dry-run are mutually exclusive")
        sys.exit(2)

    transfer = not no_transfer
    if dry_run or not act:
        transfer = False

    mode = (
        "DRY-RUN"        if dry_run else
        "LIVE EXTRACT"   if act else
        "CALCULATION ONLY"
    )
    click.echo(f"Mode: {mode}")

    asyncio.run(process_credit_extraction(
        act=act, dry_run=dry_run, transfer=transfer,
        slippage_bps=slippage_bps,
    ))
```

- [ ] **Step 4: Register the console script**

Edit `setup.cfg`. In the `[options.entry_points]` block, inside `console_scripts =`, add a line after `nodestatus-distribute-credits = ...`:

```
    nodestatus-extract-credits = aleph_nodestatus.commands:extract_credits
```

The final block should look like:

```
console_scripts =
    nodestatus = aleph_nodestatus.commands:run
    nodestatus-distribute = aleph_nodestatus.commands:distribute
    nodestatus-distribute-credits = aleph_nodestatus.commands:distribute_credits
    nodestatus-extract-credits = aleph_nodestatus.commands:extract_credits
    monitor-balances = aleph_nodestatus.commands:monitor_erc20
    monitor-sablier = aleph_nodestatus.commands:monitor_sablier
    monitor-solana = aleph_nodestatus.commands:monitor_solana
    monitor-indexer = aleph_nodestatus.commands:monitor_indexer
    reset-balances = aleph_nodestatus.commands:reset_balances
```

- [ ] **Step 5: Run all tests**

Run: `pytest tests/test_extract_credits_cli.py tests/test_credit_extraction.py -v`

Expected: all PASS.

- [ ] **Step 6: Commit**

```bash
git add src/aleph_nodestatus/commands.py setup.cfg tests/test_extract_credits_cli.py
git commit -m "$(cat <<'EOF'
feat(extract): add nodestatus-extract-credits CLI command

Wires process_credit_extraction behind a Click command and a new
console script.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 6: Retarget `test_async_extract.py` at the new orchestrator

`tests/test_async_extract.py` today monkeypatches `extract_aleph` on the `commands` module and runs `process_credit_distribution` to assert the async offload. Once Task 7 removes the extract call from `process_credit_distribution`, this test no longer covers anything meaningful. Retarget it now (before that removal lands) so the assertion shifts to `process_credit_extraction`.

**Files:**
- Modify: `tests/test_async_extract.py`

- [ ] **Step 1: Rewrite the test**

Open `tests/test_async_extract.py` and replace its full contents with:

```python
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
```

- [ ] **Step 2: Run the retargeted test**

Run: `pytest tests/test_async_extract.py -v`

Expected: PASS.

- [ ] **Step 3: Run the full test suite to confirm no collateral damage**

Run: `pytest tests/ -v`

Expected: all PASS (the distribute pipeline still calls `extract_aleph` internally — those tests still go green; only `test_async_extract.py` shifted target).

- [ ] **Step 4: Commit**

```bash
git add tests/test_async_extract.py
git commit -m "$(cat <<'EOF'
test(extract): retarget event-loop test at process_credit_extraction

The blocking-offload behaviour now lives in the new orchestrator; the
distribute pipeline will lose its extract step in the next commit.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 7: Strip extract from `process_credit_distribution`

Remove the admin-account resolution, the ETH preflight, the extract step itself, and the `extract` field from the published distribution payload. The orchestrator becomes pure reward calculation + transfer + post.

**Files:**
- Modify: `src/aleph_nodestatus/commands.py`

- [ ] **Step 1: Remove the admin/preflight block**

In `src/aleph_nodestatus/commands.py`, delete the entire block from line 359 through 391 inclusive (currently begins with `admin_address = settings.payment_processor_admin_address` and ends with the closing `)` of the `LOGGER.warning(...)` call).

After the deletion the function should flow directly from the `if end_height <= start_height:` ValueError guard (and its `start_time`/`end_time` reads) into the Step 1 comment block.

- [ ] **Step 2: Remove the Step 1 extract block**

Delete lines 393 through 424 inclusive — from the comment `# Step ordering note: extract runs BEFORE the balance check.` through the closing `)` of the `extract_block = await asyncio.to_thread(...)` call. Also delete the surrounding blank line directly above and below as needed so the file flows naturally into `# === Step 2: credit_revenue + holder_tier rewards ===`.

- [ ] **Step 3: Remove the `extract` key from the distribution payload**

Around line 594, delete the line:

```python
        "extract": extract_block,
```

- [ ] **Step 4: Run the existing distribute test suite to confirm extract no longer participates**

Run: `pytest tests/test_async_extract.py tests/test_credit_pipeline.py -v`

Expected: PASS. (`test_credit_pipeline.py` still passes `--no-extract` on the CLI; the flag is still parsed in `_resolve_feature_flags` and just becomes inert. That cleanup lands in Task 8.)

- [ ] **Step 5: Commit**

```bash
git add src/aleph_nodestatus/commands.py
git commit -m "$(cat <<'EOF'
refactor(distribute): drop extract step from process_credit_distribution

Extract now lives in nodestatus-extract-credits. The distribute
pipeline no longer touches the processor, the admin pkey, or the
extract field of the published post.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 8: Remove `--no-extract` flag and the `extract` feature-flag branch

The flag and `_resolve_feature_flags` branch are now dead — no caller reads `flags["extract"]`. Remove both, plus the test references that still pass `--no-extract` / `no_extract=False`.

**Files:**
- Modify: `src/aleph_nodestatus/commands.py`
- Modify: `tests/test_distribute_credits_cli.py`
- Modify: `tests/test_credit_pipeline.py`

- [ ] **Step 1: Update the failing CLI test first (TDD)**

In `tests/test_distribute_credits_cli.py`:

**a.** In `test_cli_help_lists_new_flags`, delete the line:

```python
    assert "--no-extract" in result.output
```

**b.** In `test_resolve_feature_flags_holder_tier_requires_credit_revenue`, change the `_resolve_feature_flags` call from:

```python
    flags = _resolve_feature_flags(
        no_extract=False, no_credit_revenue=True, no_wage=False,
        enable_holder_tier=True, no_holder_tier=False,
        no_transfer=False, no_publish=False,
        act=False, dry_run=False,
    )
```

to:

```python
    flags = _resolve_feature_flags(
        no_credit_revenue=True, no_wage=False,
        enable_holder_tier=True, no_holder_tier=False,
        no_transfer=False, no_publish=False,
        act=False, dry_run=False,
    )
```

- [ ] **Step 2: Update `test_credit_pipeline.py`**

Open `tests/test_credit_pipeline.py` and remove every `"--no-extract"` token from the four CLI invocations:

- Line 96 — remove `"--no-extract"` from the args list.
- Line 110 — remove `"--no-extract"`.
- Line 174 — remove `"--no-extract"`.
- Line 250 — remove `"--no-extract"`.
- Line 310 — remove `"--no-extract"`.

After each removal the surrounding flag list should still be well-formed (no stray commas).

- [ ] **Step 3: Run the updated tests — expect failure on the unchanged CLI**

Run: `pytest tests/test_distribute_credits_cli.py tests/test_credit_pipeline.py -v`

Expected: at least one FAIL — Click still parses `--no-extract` and passes `no_extract=` to the function, so `_resolve_feature_flags` now receives an unexpected kwarg from the wrapping `distribute_credits` call. (Tests in test_credit_pipeline pass — they no longer use the flag.)

- [ ] **Step 4: Remove `--no-extract` from `distribute_credits` and `_resolve_feature_flags`**

In `src/aleph_nodestatus/commands.py`:

**a.** Delete the Click option decorator (around line 663):

```python
@click.option("--no-extract", "no_extract", is_flag=True,
              help="Skip process() calls")
```

**b.** In the `def distribute_credits(...)` signature (around line 687), remove `no_extract,` from the parameter list.

**c.** In the body of `distribute_credits` where it calls `_resolve_feature_flags` (around line 709), change:

```python
    flags = _resolve_feature_flags(
        no_extract=no_extract, no_credit_revenue=no_credit_revenue,
        no_wage=no_wage, enable_holder_tier=enable_holder_tier,
        ...
    )
```

to:

```python
    flags = _resolve_feature_flags(
        no_credit_revenue=no_credit_revenue,
        no_wage=no_wage, enable_holder_tier=enable_holder_tier,
        ...
    )
```

**d.** In `def _resolve_feature_flags(...)` (around line 734), update the signature from:

```python
def _resolve_feature_flags(*, no_extract, no_credit_revenue, no_wage,
                           enable_holder_tier, no_holder_tier,
                           no_transfer, no_publish, act, dry_run):
```

to:

```python
def _resolve_feature_flags(*, no_credit_revenue, no_wage,
                           enable_holder_tier, no_holder_tier,
                           no_transfer, no_publish, act, dry_run):
```

**e.** In the body of `_resolve_feature_flags`, delete the line:

```python
        "extract":        settings.credit_dist_extract_enabled,
```

from the `f = { ... }` dict, and delete:

```python
    if no_extract:        f["extract"] = False
```

- [ ] **Step 5: Run all affected tests**

Run: `pytest tests/test_distribute_credits_cli.py tests/test_credit_pipeline.py tests/test_async_extract.py -v`

Expected: all PASS.

- [ ] **Step 6: Commit**

```bash
git add src/aleph_nodestatus/commands.py tests/test_distribute_credits_cli.py tests/test_credit_pipeline.py
git commit -m "$(cat <<'EOF'
refactor(distribute): drop --no-extract flag and extract feature branch

The flag was inert after the extract step moved out of
process_credit_distribution. Removes the CLI option, the parameter
threading, and the extract entry in _resolve_feature_flags.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 9: Remove `credit_dist_extract_enabled` setting

With the feature branch gone, the env var is dead. Remove the setting and its test.

**Files:**
- Modify: `src/aleph_nodestatus/settings.py`
- Modify: `tests/test_settings_new.py`

- [ ] **Step 1: Update the failing settings test first**

In `tests/test_settings_new.py`, find `test_feature_flags_defaults` (around line 30) and delete the line:

```python
    assert settings.credit_dist_extract_enabled is True
```

- [ ] **Step 2: Run the settings test — expect to pass either way**

Run: `pytest tests/test_settings_new.py::test_feature_flags_defaults -v`

Expected: PASS (the assertion just got softer; the field still exists in settings.py for now).

- [ ] **Step 3: Remove the setting from `settings.py`**

In `src/aleph_nodestatus/settings.py`, delete the line:

```python
    credit_dist_extract_enabled: bool        = True
```

- [ ] **Step 4: Run all tests**

Run: `pytest tests/ -v`

Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add src/aleph_nodestatus/settings.py tests/test_settings_new.py
git commit -m "$(cat <<'EOF'
chore(settings): drop credit_dist_extract_enabled

The flag had no consumer left after the extract step moved out of the
distribute pipeline.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 10: Clean up unused imports

After the extract step is gone, several imports in `commands.py` are no longer referenced. Removing them surfaces any subtle leftover usage.

**Files:**
- Modify: `src/aleph_nodestatus/commands.py`

- [ ] **Step 1: Inspect and remove dead imports**

In `src/aleph_nodestatus/commands.py`, remove these imports if no longer referenced anywhere in the file:

- `from eth_account import Account`
- `from hexbytes import HexBytes`
- The block `from .payment_processor import (extract_aleph, get_processor_contract, get_quoter_contract, get_v2_router_contract, get_v4_quoter_contract,)` — remove the whole multi-line import.

Confirm each removal with a quick grep before deleting:

```bash
grep -n "Account\b" src/aleph_nodestatus/commands.py
grep -n "HexBytes" src/aleph_nodestatus/commands.py
grep -n "extract_aleph\|get_processor_contract\|get_quoter_contract\|get_v2_router_contract\|get_v4_quoter_contract" src/aleph_nodestatus/commands.py
```

Expected: each command returns either no matches (then it is safe to drop the import) or matches only in comments / docstrings. If any production code still references a name, leave that single import.

- [ ] **Step 2: Run the full test suite to catch import-related breakage**

Run: `pytest tests/ -v`

Expected: all PASS.

- [ ] **Step 3: Commit**

```bash
git add src/aleph_nodestatus/commands.py
git commit -m "$(cat <<'EOF'
chore(distribute): drop unused imports after extract removal

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Final check

- [ ] **Step 1: Confirm the full suite is green**

Run: `pytest tests/ -v`

Expected: all green.

- [ ] **Step 2: Confirm the new console script is installable**

Run: `python -m pip install -e . && nodestatus-extract-credits --help`

Expected: the help output renders the four flags from Task 5.

- [ ] **Step 3: Smoke-check the help on the trimmed distribute command**

Run: `nodestatus-distribute-credits --help`

Expected: no `--no-extract` in the output; everything else looks unchanged.
