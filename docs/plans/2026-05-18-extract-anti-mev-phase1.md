# Anti-MEV Phase 1 — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Spec:** `docs/specs/2026-05-18-extract-anti-mev-phase1.md` (unstaged — to be committed together with the first implementing commit, per user policy)

**Goal:** Ship two anti-MEV protections for `nodestatus-extract-credits`: (1) a uniformly random pre-extract delay (anti-tracking) and (2) a per-hop pool-spot vs Chainlink-USD-ratio deviation guard at 200 bps (anti-pre-manipulation).

**Architecture:** Random delay lives in `process_credit_extraction` (Python CLI, bypassable via new `--immediate` flag; `--dry-run` implies it). Pool oracle lives in a new module `pool_oracle.py` with a `check_swap_price_deviation(w3, swap_config, token_in) → OracleResult` interface that dispatches by path version (V2/V3/V4). Integration point in `payment_processor.extract_aleph` is between `getSwapConfig` and `quote_amount_out`. New ABIs (V3 pool, V3 factory, V4 StateView, Chainlink aggregator) added under `src/aleph_nodestatus/abi/`.

**Tech Stack:** Python 3, web3.py (sync calls offloaded via `asyncio.to_thread`), `eth_abi` / `eth_utils` for V4 poolId keccak, Click (CLI), pytest + pytest-asyncio.

**Repository / branch:** `/Users/angelmanzano/Projects/aleph/back/aleph-nodestatus` on `feat/credit-distribution-v2`. Commits approved.

---

## File Map

**Create:**
- `src/aleph_nodestatus/pool_oracle.py` — `OracleResult` dataclass + `check_swap_price_deviation` orchestrator + per-version helpers (`_v2_spot`, `_v3_spot`, `_v4_spot`), Chainlink reader, path decoders.
- `src/aleph_nodestatus/abi/IUniswapV3Pool.json` — `slot0` only.
- `src/aleph_nodestatus/abi/IUniswapV3Factory.json` — `getPool(address,address,uint24)` only.
- `src/aleph_nodestatus/abi/IUniswapV4StateView.json` — `getSlot0(bytes32)` only.
- `src/aleph_nodestatus/abi/ChainlinkAggregator.json` — `latestRoundData()`, `decimals()`.
- `tests/test_pool_oracle.py` — unit tests for the oracle module.

**Modify:**
- `src/aleph_nodestatus/settings.py` — five new settings (delay, deviation bps, chainlink staleness, two contract addrs, feed dict).
- `src/aleph_nodestatus/credit_extraction.py` — add `immediate` kwarg + random sleep call before `extract_aleph`.
- `src/aleph_nodestatus/commands.py` — add `--immediate` Click option on `extract_credits`, forward to orchestrator.
- `src/aleph_nodestatus/payment_processor.py` — call `check_swap_price_deviation` between `_swap_config_to_dict` (line 321) and `quote_amount_out` (line 322); short-circuit if `not ok`.
- `tests/test_credit_extraction.py` — random delay tests + new summary rendering test.
- `tests/test_extract_credits_cli.py` — `--immediate` flag test.
- `tests/test_payment_processor.py` — extract_aleph oracle integration tests.
- `tests/test_settings_new.py` — defaults assertion for new settings.
- `tests/test_abi_files.py` — new ABIs listed.
- `docker/.env.extract.example` — optional overrides block.

**Spec + plan files (already on disk, untracked):**
- `docs/specs/2026-05-18-extract-anti-mev-phase1.md`
- `docs/plans/2026-05-18-extract-anti-mev-phase1.md` (this file)

Both get committed together with **Task 1's** implementation commit (per user policy: specs/plans land with the first implementing commit, never standalone).

---

## Conventions

- Existing test files start with a `plyvel` stub. Any new test file that imports from `commands.py` or `storage.py` must do the same. See `tests/test_distribute_credits_cli.py:1-10` for the canonical shape.
- Pytest invocation: `.venv/bin/python -m pytest tests/<file>.py -v` (the venv is at `.venv/` per the prior task work).
- After Phase-1 commits land, the full suite plus the existing pre-existing collection errors (`test_scores.py`, `test_skeleton.py`) remain unchanged — those are ignored via `--ignore`.
- Per user policy: spec and plan files in `docs/specs/` and `docs/plans/` do NOT get committed as standalone documentation commits. They land with implementation.

---

## Task 1: Settings additions + random delay in orchestrator + `--immediate` CLI flag

Self-contained PR-able chunk: introduces both the random-delay mechanism and all five new settings (the deviation/chainlink/contract ones won't be consumed until later tasks, but landing them now keeps the settings PR-able as a unit and lets later tasks read them without needing to add them as side effects).

**Files:**
- Modify: `src/aleph_nodestatus/settings.py`
- Modify: `src/aleph_nodestatus/credit_extraction.py`
- Modify: `src/aleph_nodestatus/commands.py`
- Modify: `tests/test_credit_extraction.py`
- Modify: `tests/test_extract_credits_cli.py`
- Modify: `tests/test_settings_new.py`

- [ ] **Step 1.1: Write the failing settings-defaults test**

In `tests/test_settings_new.py`, append a new test:

```python
def test_anti_mev_defaults():
    assert settings.extract_random_delay_max_seconds == 3540
    assert settings.extract_max_deviation_bps == 200
    assert settings.chainlink_max_age_seconds == 3600
    assert settings.uniswap_v3_factory_address.lower() == \
        "0x1f98431c8ad98523631ae4a59f267346ea31f984"
    assert settings.uniswap_v4_state_view_address.lower() == \
        "0x7ffe42c4a5deea5b0fec41c94c136cf115597227"
    # ETH sentinel and WETH both map to the ETH/USD feed.
    eth_usd = "0x5f4eC3Df9cbd43714FE2740f5E3616155c5b8419".lower()
    feeds = {k.lower(): v.lower() for k, v in
             settings.chainlink_usd_feeds.items()}
    assert feeds["0x0000000000000000000000000000000000000000"] == eth_usd
    assert feeds["0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"] == eth_usd
    assert feeds["0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"] == \
        "0x8fffffd4afb6115b954bd326cbe7b4ba576818f6".lower()
```

- [ ] **Step 1.2: Run the test — expect failure**

Run: `.venv/bin/python -m pytest tests/test_settings_new.py::test_anti_mev_defaults -v`

Expected: FAIL with `AttributeError: 'Settings' object has no attribute 'extract_random_delay_max_seconds'`.

- [ ] **Step 1.3: Add the new settings**

In `src/aleph_nodestatus/settings.py`, after the existing feature-flag block (`credit_dist_*_enabled`) and before the `class Config:` line, add:

```python
    # === Anti-MEV (Phase 1) ===
    extract_random_delay_max_seconds: int = 3540
    extract_max_deviation_bps: int        = 200
    chainlink_max_age_seconds: int        = 3600

    uniswap_v3_factory_address: str       = "0x1F98431c8aD98523631AE4a59f267346ea31F984"
    uniswap_v4_state_view_address: str    = "0x7fFE42C4a5DEeA5b0feC41C94C136Cf115597227"

    # Chainlink USD feed addresses keyed by lowercase token address.
    # ETH sentinel (0x000…000) and WETH (0xC02a…6cc2) both map to the
    # ETH/USD feed because process_tokens refers to ETH via the sentinel
    # while the on-chain swap path encodes WETH.
    chainlink_usd_feeds: Dict[str, str] = {
        "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48": "0x8fFfFfd4AfB6115b954Bd326cbe7B4BA576818f6",
        "0x0000000000000000000000000000000000000000": "0x5f4eC3Df9cbd43714FE2740f5E3616155c5b8419",
        "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2": "0x5f4eC3Df9cbd43714FE2740f5E3616155c5b8419",
    }
```

If `Dict` isn't already imported at the top of `settings.py`, add `from typing import Dict`.

- [ ] **Step 1.4: Run settings test — expect PASS**

Run: `.venv/bin/python -m pytest tests/test_settings_new.py::test_anti_mev_defaults -v`

Expected: PASS.

- [ ] **Step 1.5: Write the failing random-delay tests**

In `tests/test_credit_extraction.py`, append four new tests:

```python
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
```

- [ ] **Step 1.6: Run the new tests — expect failure**

Run: `.venv/bin/python -m pytest tests/test_credit_extraction.py -k random_delay -v`

Expected: ALL FAIL with `TypeError: process_credit_extraction() got an unexpected keyword argument 'immediate'`.

- [ ] **Step 1.7: Add random delay to `process_credit_extraction`**

Edit `src/aleph_nodestatus/credit_extraction.py`.

Add to the imports at the top of the file (after the existing standard-lib imports):

```python
import random
```

Update the function signature from:

```python
async def process_credit_extraction(
    *, act: bool, dry_run: bool, transfer: bool,
    slippage_bps: Optional[int] = None,
) -> dict:
```

to:

```python
async def process_credit_extraction(
    *, act: bool, dry_run: bool, transfer: bool,
    immediate: bool = False,
    slippage_bps: Optional[int] = None,
) -> dict:
```

After the admin-account block but BEFORE the `processor = get_processor_contract(web3)` line, insert:

```python
    # Anti-MEV random delay: hourly cron is predictable; this jitter
    # uniformly spreads the actual execution over [0, max] seconds so a
    # bot watching the schedule can't pre-position a sandwich for a
    # specific block. Bypass via --immediate (manual retries) or
    # --dry-run (no on-chain tx, no need to obscure timing).
    if (not dry_run
        and not immediate
        and settings.extract_random_delay_max_seconds > 0):
        delay = random.randint(0, settings.extract_random_delay_max_seconds)
        click.echo(f"Sleeping {delay}s before extract (anti-MEV jitter)…")
        await asyncio.sleep(delay)
```

- [ ] **Step 1.8: Run random-delay tests — expect PASS**

Run: `.venv/bin/python -m pytest tests/test_credit_extraction.py -k random_delay -v`

Expected: 4 PASS.

- [ ] **Step 1.9: Write the failing CLI tests**

In `tests/test_extract_credits_cli.py`, append:

```python
def test_cli_help_lists_immediate_flag():
    runner = CliRunner()
    result = runner.invoke(extract_credits, ["--help"])
    assert result.exit_code == 0
    assert "--immediate" in result.output


def test_cli_immediate_forwarded_to_orchestrator(monkeypatch):
    import aleph_nodestatus.commands as cmd

    captured = {}
    async def fake_orch(**kwargs):
        captured.update(kwargs)
        return {"tokens": [], "errors": []}
    monkeypatch.setattr(cmd, "process_credit_extraction", fake_orch)

    runner = CliRunner()
    result = runner.invoke(extract_credits, ["--immediate", "--act"])
    assert result.exit_code == 0, result.output
    assert captured["immediate"] is True
    assert captured["act"] is True


def test_cli_dry_run_implies_immediate(monkeypatch):
    """--dry-run must forward immediate=True so the orchestrator skips
    the random sleep without needing an extra flag."""
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
    assert captured["immediate"] is True
```

- [ ] **Step 1.10: Run CLI tests — expect failure**

Run: `.venv/bin/python -m pytest tests/test_extract_credits_cli.py -v`

Expected: 3 new ones FAIL (no such flag, no kwarg).

- [ ] **Step 1.11: Add `--immediate` to the Click command**

In `src/aleph_nodestatus/commands.py`, find the `extract_credits` Click command. Add a new option decorator and parameter.

Change the option decorator block from:

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
```

to:

```python
@click.command()
@click.option("-v", "--verbose", count=True)
@click.option("-a", "--act", help="Broadcast process() txs", is_flag=True)
@click.option("--dry-run", "dry_run", is_flag=True,
              help="No broadcast; only eth_call simulate")
@click.option("--no-transfer", "no_transfer", is_flag=True,
              help="Compute + simulate, don't broadcast")
@click.option("--immediate", "immediate", is_flag=True,
              help="Skip the anti-MEV random delay (manual retries)")
@click.option("--slippage-bps", "slippage_bps", default=None, type=int,
              help="Override per-run slippage tolerance")
def extract_credits(verbose, act, dry_run, no_transfer, immediate, slippage_bps):
```

In the function body, find the `asyncio.run(process_credit_extraction(...))` call. Change it from:

```python
    asyncio.run(process_credit_extraction(
        act=act, dry_run=dry_run, transfer=transfer,
        slippage_bps=slippage_bps,
    ))
```

to:

```python
    asyncio.run(process_credit_extraction(
        act=act, dry_run=dry_run, transfer=transfer,
        immediate=(immediate or dry_run),
        slippage_bps=slippage_bps,
    ))
```

- [ ] **Step 1.12: Run CLI tests — expect PASS**

Run: `.venv/bin/python -m pytest tests/test_extract_credits_cli.py -v`

Expected: all PASS.

- [ ] **Step 1.13: Full suite green**

Run: `.venv/bin/python -m pytest tests/ --ignore=tests/test_scores.py --ignore=tests/test_skeleton.py -v`

Expected: all PASS (107 prior + 8 new = 115 or similar).

- [ ] **Step 1.14: Commit (with spec and plan files)**

This is the first implementing commit. Per user policy, the spec and plan docs live in the working tree but were never committed; they land now with the implementation.

```bash
git add docs/specs/2026-05-18-extract-anti-mev-phase1.md \
        docs/plans/2026-05-18-extract-anti-mev-phase1.md \
        src/aleph_nodestatus/settings.py \
        src/aleph_nodestatus/credit_extraction.py \
        src/aleph_nodestatus/commands.py \
        tests/test_credit_extraction.py \
        tests/test_extract_credits_cli.py \
        tests/test_settings_new.py
git commit -m "feat(extract): anti-MEV random delay + --immediate flag (phase 1)

Hourly cron stays at the top of the hour; the CLI sleeps a uniformly
random 0..extract_random_delay_max_seconds (default 3540 = 59 min)
before invoking extract_aleph. --immediate bypasses for manual retries;
--dry-run forwards immediate=True automatically.

Also lands the spec + plan for the rest of phase 1 (Chainlink
deviation guard) since policy is no standalone doc commits."
```

---

## Task 2: Add new ABI files

Vendored minimal JSONs so the oracle module can call slot0/getPool/getSlot0/latestRoundData. Nothing consumes them yet; this task is mechanical and gives later tasks a stable base to import from.

**Files:**
- Create: `src/aleph_nodestatus/abi/IUniswapV3Pool.json`
- Create: `src/aleph_nodestatus/abi/IUniswapV3Factory.json`
- Create: `src/aleph_nodestatus/abi/IUniswapV4StateView.json`
- Create: `src/aleph_nodestatus/abi/ChainlinkAggregator.json`
- Modify: `tests/test_abi_files.py`

- [ ] **Step 2.1: Add the test that fails when an ABI is missing**

Read `tests/test_abi_files.py` to confirm the existing pattern. If it iterates a list of expected ABI names, extend the list. Otherwise, add a new test:

```python
def test_anti_mev_abis_present_and_valid():
    import json
    from pathlib import Path
    abi_dir = Path("src/aleph_nodestatus/abi")
    expected = [
        "IUniswapV3Pool.json",
        "IUniswapV3Factory.json",
        "IUniswapV4StateView.json",
        "ChainlinkAggregator.json",
    ]
    for name in expected:
        p = abi_dir / name
        assert p.exists(), f"missing ABI: {name}"
        data = json.loads(p.read_text())
        assert isinstance(data, list), f"ABI {name} must be a JSON array"
        assert len(data) > 0, f"ABI {name} is empty"
```

- [ ] **Step 2.2: Run the test — expect failure**

Run: `.venv/bin/python -m pytest tests/test_abi_files.py -v`

Expected: FAIL with `assert False, missing ABI: IUniswapV3Pool.json`.

- [ ] **Step 2.3: Create `IUniswapV3Pool.json`**

Create `src/aleph_nodestatus/abi/IUniswapV3Pool.json` with:

```json
[
  {
    "inputs": [],
    "name": "slot0",
    "outputs": [
      {"internalType": "uint160", "name": "sqrtPriceX96", "type": "uint160"},
      {"internalType": "int24",   "name": "tick",         "type": "int24"},
      {"internalType": "uint16",  "name": "observationIndex",          "type": "uint16"},
      {"internalType": "uint16",  "name": "observationCardinality",    "type": "uint16"},
      {"internalType": "uint16",  "name": "observationCardinalityNext","type": "uint16"},
      {"internalType": "uint8",   "name": "feeProtocol",  "type": "uint8"},
      {"internalType": "bool",    "name": "unlocked",     "type": "bool"}
    ],
    "stateMutability": "view",
    "type": "function"
  }
]
```

- [ ] **Step 2.4: Create `IUniswapV3Factory.json`**

Create `src/aleph_nodestatus/abi/IUniswapV3Factory.json` with:

```json
[
  {
    "inputs": [
      {"internalType": "address", "name": "tokenA", "type": "address"},
      {"internalType": "address", "name": "tokenB", "type": "address"},
      {"internalType": "uint24",  "name": "fee",    "type": "uint24"}
    ],
    "name": "getPool",
    "outputs": [
      {"internalType": "address", "name": "pool", "type": "address"}
    ],
    "stateMutability": "view",
    "type": "function"
  }
]
```

- [ ] **Step 2.5: Create `IUniswapV4StateView.json`**

Create `src/aleph_nodestatus/abi/IUniswapV4StateView.json` with:

```json
[
  {
    "inputs": [
      {"internalType": "bytes32", "name": "poolId", "type": "bytes32"}
    ],
    "name": "getSlot0",
    "outputs": [
      {"internalType": "uint160", "name": "sqrtPriceX96",   "type": "uint160"},
      {"internalType": "int24",   "name": "tick",           "type": "int24"},
      {"internalType": "uint24",  "name": "protocolFee",    "type": "uint24"},
      {"internalType": "uint24",  "name": "lpFee",          "type": "uint24"}
    ],
    "stateMutability": "view",
    "type": "function"
  }
]
```

- [ ] **Step 2.6: Create `ChainlinkAggregator.json`**

Create `src/aleph_nodestatus/abi/ChainlinkAggregator.json` with:

```json
[
  {
    "inputs": [],
    "name": "decimals",
    "outputs": [
      {"internalType": "uint8", "name": "", "type": "uint8"}
    ],
    "stateMutability": "view",
    "type": "function"
  },
  {
    "inputs": [],
    "name": "latestRoundData",
    "outputs": [
      {"internalType": "uint80", "name": "roundId",         "type": "uint80"},
      {"internalType": "int256", "name": "answer",          "type": "int256"},
      {"internalType": "uint256","name": "startedAt",       "type": "uint256"},
      {"internalType": "uint256","name": "updatedAt",       "type": "uint256"},
      {"internalType": "uint80", "name": "answeredInRound", "type": "uint80"}
    ],
    "stateMutability": "view",
    "type": "function"
  }
]
```

- [ ] **Step 2.7: Run the test — expect PASS**

Run: `.venv/bin/python -m pytest tests/test_abi_files.py -v`

Expected: PASS.

- [ ] **Step 2.8: Commit**

```bash
git add src/aleph_nodestatus/abi/IUniswapV3Pool.json \
        src/aleph_nodestatus/abi/IUniswapV3Factory.json \
        src/aleph_nodestatus/abi/IUniswapV4StateView.json \
        src/aleph_nodestatus/abi/ChainlinkAggregator.json \
        tests/test_abi_files.py
git commit -m "feat(extract): vendor minimal ABIs for V3/V4 pool reads and Chainlink"
```

---

## Task 3: `pool_oracle.py` skeleton + extract_aleph integration (permissive)

Introduce the module and wire its function into `extract_aleph`, but have the function always return `ok=True` for now. This lets us test the integration point and the `not ok` short-circuit independently of the per-version logic.

**Files:**
- Create: `src/aleph_nodestatus/pool_oracle.py`
- Create: `tests/test_pool_oracle.py`
- Modify: `src/aleph_nodestatus/payment_processor.py`
- Modify: `tests/test_payment_processor.py`

- [ ] **Step 3.1: Write the failing tests**

Create `tests/test_pool_oracle.py`:

```python
"""Unit tests for pool_oracle: spot-vs-Chainlink deviation guard."""

import sys
import types

if "plyvel" not in sys.modules:
    _plyvel_stub = types.ModuleType("plyvel")
    _plyvel_stub.DB = object
    sys.modules["plyvel"] = _plyvel_stub

from unittest.mock import MagicMock

import pytest


def test_oracle_result_is_dataclass():
    from aleph_nodestatus.pool_oracle import OracleResult

    r = OracleResult(ok=True)
    assert r.ok is True
    assert r.reason is None
    assert r.deviation_bps is None
    assert r.spot_price is None
    assert r.ref_price is None


def test_check_swap_price_deviation_returns_ok_for_now():
    """Skeleton: the function exists and returns ok=True regardless of input.
    Per-version logic lands in subsequent tasks."""
    from aleph_nodestatus.pool_oracle import check_swap_price_deviation

    w3 = MagicMock()
    cfg = {"v": 3, "t": "0x0", "v2": [], "v3": b"", "v4": []}
    result = check_swap_price_deviation(w3, cfg, token_in="0xToken")
    assert result.ok is True
```

Also add this to `tests/test_payment_processor.py` (append after existing extract_aleph tests):

```python
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
```

Note: the test asserts on `len(skipped) == 2` because `settings.process_tokens` has 3 entries (USDC, ETH, ALEPH) and ALEPH is the swap destination (no swap path → no oracle check).

- [ ] **Step 3.2: Run tests — expect failure**

Run: `.venv/bin/python -m pytest tests/test_pool_oracle.py tests/test_payment_processor.py::test_extract_aleph_skips_token_when_oracle_says_not_ok -v`

Expected: FAIL — `ModuleNotFoundError: pool_oracle`.

- [ ] **Step 3.3: Create the skeleton module**

Create `src/aleph_nodestatus/pool_oracle.py`:

```python
"""Spot-vs-Chainlink deviation guard for the extract swap path.

For each hop in the configured swap path (read at runtime from the
AlephPaymentProcessor), compares the pool's current spot price to the
Chainlink-USD-implied ratio for the same hop. Returns a verdict that
the caller (extract_aleph) uses to either proceed or skip the token.

Phase 1 reads SPOT from the pool — no TWAP. Phase 2 adds true TWAP
(V3 observe() / V4 oracle hooks) which closes the ALEPH-side hop
gap (ALEPH has no Chainlink feed today).
"""

import logging
from dataclasses import dataclass
from typing import Optional

from .settings import settings

LOGGER = logging.getLogger(__name__)


@dataclass
class OracleResult:
    ok: bool
    reason: Optional[str] = None
    deviation_bps: Optional[int] = None
    spot_price: Optional[float] = None
    ref_price: Optional[float] = None


def check_swap_price_deviation(w3, swap_config: dict, token_in: str) -> OracleResult:
    """Skeleton: always returns ok=True. Per-version logic in later tasks."""
    return OracleResult(ok=True)
```

- [ ] **Step 3.4: Wire into `extract_aleph`**

Edit `src/aleph_nodestatus/payment_processor.py`.

Add to the top-of-file imports (after the existing `.settings` import):

```python
from .pool_oracle import check_swap_price_deviation
```

Inside the per-token loop in `extract_aleph`, find the block (around line 319-321):

```python
            try:
                swap_config = processor.functions.getSwapConfig(token).call()
                cfg = _swap_config_to_dict(swap_config)
                expected_out = quote_amount_out(
                    quoters, cfg, swap_amount,
                    token_in=token,
                )
```

Replace it with:

```python
            try:
                swap_config = processor.functions.getSwapConfig(token).call()
                cfg = _swap_config_to_dict(swap_config)
            except Exception as e:
                LOGGER.exception(
                    "getSwapConfig failed for token %s: %r", symbol, e,
                )
                entry["error"] = f"swap_config_failed: {e!r}"
                out["errors"].append(entry)
                continue

            oracle = check_swap_price_deviation(w3, cfg, token_in=token)
            if not oracle.ok:
                entry["skipped_reason"] = oracle.reason
                entry["oracle"] = {
                    "deviation_bps": oracle.deviation_bps,
                    "spot_price":    oracle.spot_price,
                    "ref_price":     oracle.ref_price,
                }
                continue

            try:
                expected_out = quote_amount_out(
                    quoters, cfg, swap_amount,
                    token_in=token,
                )
```

Note that the previous single `try/except` covered both `getSwapConfig` and `quote_amount_out`. The refactor splits them — `getSwapConfig` failures now produce `error=swap_config_failed` (a real failure, recorded in `errors`), and `quote_amount_out` keeps its existing `quote_failed:` error path.

- [ ] **Step 3.5: Run the new tests — expect PASS**

Run: `.venv/bin/python -m pytest tests/test_pool_oracle.py tests/test_payment_processor.py -v`

Expected: all PASS (existing extract tests + new skeleton test + new not-ok integration test).

- [ ] **Step 3.6: Commit**

```bash
git add src/aleph_nodestatus/pool_oracle.py \
        src/aleph_nodestatus/payment_processor.py \
        tests/test_pool_oracle.py \
        tests/test_payment_processor.py
git commit -m "feat(extract): wire pool_oracle skeleton into extract_aleph

Adds the check_swap_price_deviation hook between getSwapConfig and
quote_amount_out. Returns ok=True for now; per-version logic follows."
```

---

## Task 4: Chainlink feed reader

Helper that reads a single Chainlink USD feed, returns the price as a float, or a reason string for stale/invalid/missing.

**Files:**
- Modify: `src/aleph_nodestatus/pool_oracle.py`
- Modify: `tests/test_pool_oracle.py`

- [ ] **Step 4.1: Write the failing tests**

Append to `tests/test_pool_oracle.py`:

```python
def test_read_chainlink_returns_price(monkeypatch):
    """Healthy feed: latestRoundData answer × 10^-decimals."""
    import aleph_nodestatus.pool_oracle as po

    feed = MagicMock()
    feed.functions.decimals.return_value.call.return_value = 8
    # answer = 1.234567 × 10^8
    feed.functions.latestRoundData.return_value.call.return_value = (
        1, 123_456_700, 0, 9_999_999_999, 1,  # updatedAt very recent
    )

    w3 = MagicMock()
    w3.eth.contract.return_value = feed
    w3.eth.get_block.return_value.timestamp = 10_000_000_000

    price, reason = po._read_chainlink_price(
        w3, feed_address="0xfeed",
    )
    assert reason is None
    assert abs(price - 1.234567) < 1e-9


def test_read_chainlink_stale(monkeypatch):
    """updatedAt older than chainlink_max_age_seconds → reason=chainlink_stale."""
    import aleph_nodestatus.pool_oracle as po
    from aleph_nodestatus.settings import settings as s

    monkeypatch.setattr(s, "chainlink_max_age_seconds", 3600)

    feed = MagicMock()
    feed.functions.decimals.return_value.call.return_value = 8
    feed.functions.latestRoundData.return_value.call.return_value = (
        1, 100_000_000, 0, 1000, 1,  # updatedAt = 1000
    )

    w3 = MagicMock()
    w3.eth.contract.return_value = feed
    w3.eth.get_block.return_value.timestamp = 1000 + 4000  # 4000s old, > 3600

    price, reason = po._read_chainlink_price(w3, "0xfeed")
    assert price is None
    assert reason == "chainlink_stale"


def test_read_chainlink_invalid_answer():
    """answer <= 0 → reason=chainlink_invalid."""
    import aleph_nodestatus.pool_oracle as po

    feed = MagicMock()
    feed.functions.decimals.return_value.call.return_value = 8
    feed.functions.latestRoundData.return_value.call.return_value = (
        1, 0, 0, 9_999_999_999, 1,
    )

    w3 = MagicMock()
    w3.eth.contract.return_value = feed
    w3.eth.get_block.return_value.timestamp = 10_000_000_000

    price, reason = po._read_chainlink_price(w3, "0xfeed")
    assert price is None
    assert reason == "chainlink_invalid"
```

- [ ] **Step 4.2: Run tests — expect failure**

Run: `.venv/bin/python -m pytest tests/test_pool_oracle.py -v`

Expected: 3 new tests FAIL — `_read_chainlink_price` doesn't exist.

- [ ] **Step 4.3: Add the helper**

Edit `src/aleph_nodestatus/pool_oracle.py`. Add to the imports:

```python
import json
from functools import lru_cache
from pathlib import Path
```

Add at module bottom:

```python
@lru_cache(maxsize=None)
def _load_abi(name: str):
    path = Path(__file__).parent / "abi" / f"{name}.json"
    with open(path) as f:
        return json.load(f)


def _read_chainlink_price(w3, feed_address: str):
    """Return (price_float, None) on success, (None, reason_str) on failure.

    reason ∈ {"chainlink_stale", "chainlink_invalid"}.
    """
    feed = w3.eth.contract(
        address=w3.to_checksum_address(feed_address),
        abi=_load_abi("ChainlinkAggregator"),
    )
    decimals = int(feed.functions.decimals().call())
    _, answer, _, updated_at, _ = feed.functions.latestRoundData().call()
    if answer <= 0:
        return None, "chainlink_invalid"
    now = w3.eth.get_block("latest").timestamp
    if now - updated_at > settings.chainlink_max_age_seconds:
        return None, "chainlink_stale"
    return float(answer) / (10 ** decimals), None
```

- [ ] **Step 4.4: Run tests — expect PASS**

Run: `.venv/bin/python -m pytest tests/test_pool_oracle.py -v`

Expected: all PASS.

- [ ] **Step 4.5: Commit**

```bash
git add src/aleph_nodestatus/pool_oracle.py tests/test_pool_oracle.py
git commit -m "feat(extract): chainlink USD feed reader for pool_oracle"
```

---

## Task 5: V3 spot price + path decoder + V3 dispatch

Decode the V3 path bytes into hops, derive each pool address via the factory, read slot0, convert sqrtPriceX96 to a B-per-A price, compute deviation vs the Chainlink ratio, and short-circuit if any hop exceeds the threshold.

**Files:**
- Modify: `src/aleph_nodestatus/pool_oracle.py`
- Modify: `tests/test_pool_oracle.py`

- [ ] **Step 5.1: Write the failing tests**

Append to `tests/test_pool_oracle.py`:

```python
def test_decode_v3_path_single_hop():
    from aleph_nodestatus.pool_oracle import _decode_v3_path

    # token_a (USDC) | fee 3000 | token_b (WETH)
    path = (
        bytes.fromhex("a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48")
        + (3000).to_bytes(3, "big")
        + bytes.fromhex("c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2")
    )
    hops = _decode_v3_path(path)
    assert hops == [(
        "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48", 3000,
        "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
    )]


def test_decode_v3_path_two_hops():
    from aleph_nodestatus.pool_oracle import _decode_v3_path

    # USDC | 3000 | WETH | 10000 | ALEPH
    path = (
        bytes.fromhex("a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48")
        + (3000).to_bytes(3, "big")
        + bytes.fromhex("c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2")
        + (10000).to_bytes(3, "big")
        + bytes.fromhex("27702a26126e0b3702af63ee09ac4d1a084ef628")
    )
    hops = _decode_v3_path(path)
    assert len(hops) == 2
    assert hops[0][0].lower() == "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
    assert hops[0][1] == 3000
    assert hops[0][2].lower() == "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
    assert hops[1][1] == 10000


def test_sqrt_price_to_b_per_a_decimals():
    """sqrtPriceX96 for USDC/WETH pool, USDC is token0 (lower address).
    If 1 WETH = 2500 USDC, then USDC per WETH = 2500 → WETH per USDC = 1/2500."""
    from aleph_nodestatus.pool_oracle import _sqrt_price_to_b_per_a

    # WETH is token1; USDC is token0.
    # price_raw_1_per_0 = wei_WETH per wei_USDC = (2500_USDC -> 1_WETH)
    # 1 USDC = 10^6 wei; 1 WETH = 10^18 wei
    # 2500 USDC -> 1 WETH: 2500 × 10^6 wei USDC -> 10^18 wei WETH
    # so wei_WETH / wei_USDC = 10^18 / (2500 × 10^6) = 10^12 / 2500 = 4e8
    # sqrt of 4e8 = 20000; sqrtPriceX96 = 20000 * 2^96
    sqrt_price_x96 = 20000 * (2 ** 96)

    USDC = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
    WETH = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
    # token_a = USDC, token_b = WETH → "WETH per USDC" → 1/2500
    price = _sqrt_price_to_b_per_a(
        sqrt_price_x96, token_a=USDC, token_b=WETH,
        decimals_a=6, decimals_b=18,
    )
    assert abs(price - (1 / 2500)) < 1e-9

    # token_a = WETH, token_b = USDC → "USDC per WETH" → 2500
    price2 = _sqrt_price_to_b_per_a(
        sqrt_price_x96, token_a=WETH, token_b=USDC,
        decimals_a=18, decimals_b=6,
    )
    assert abs(price2 - 2500) < 1e-9


def test_check_swap_price_deviation_v3_within_threshold(monkeypatch):
    """Spot matches Chainlink (within 200 bps): ok=True."""
    import aleph_nodestatus.pool_oracle as po
    from aleph_nodestatus.settings import settings as s

    USDC = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
    WETH = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
    USDC_USD_FEED = "0x8fFfFfd4AfB6115b954Bd326cbe7B4BA576818f6"
    ETH_USD_FEED  = "0x5f4eC3Df9cbd43714FE2740f5E3616155c5b8419"

    monkeypatch.setattr(s, "chainlink_usd_feeds", {
        USDC.lower(): USDC_USD_FEED,
        WETH.lower(): ETH_USD_FEED,
    })

    # Stub the pool spot — return 1/2500 (USDC→WETH at 2500 USDC/ETH)
    monkeypatch.setattr(po, "_v3_spot",
                        lambda w3, token_a, token_b, fee: 1 / 2500.0)
    # Stub Chainlink: USDC=1.0, ETH=2500.0 → ratio WETH/USDC = 1/2500
    chainlink_returns = {
        USDC_USD_FEED.lower(): (1.0, None),
        ETH_USD_FEED.lower():  (2500.0, None),
    }
    monkeypatch.setattr(po, "_read_chainlink_price",
        lambda w3, feed: chainlink_returns[feed.lower()])

    path = (bytes.fromhex(USDC[2:]) + (3000).to_bytes(3, "big")
            + bytes.fromhex(WETH[2:]))
    cfg = {"v": 3, "t": WETH, "v2": [], "v3": path, "v4": []}

    result = po.check_swap_price_deviation(MagicMock(), cfg, token_in=USDC)
    assert result.ok is True


def test_check_swap_price_deviation_v3_exceeds_threshold(monkeypatch):
    """Spot 3% off Chainlink (>200 bps): ok=False, reason=price_deviation."""
    import aleph_nodestatus.pool_oracle as po
    from aleph_nodestatus.settings import settings as s

    USDC = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
    WETH = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
    USDC_USD_FEED = "0x8fFfFfd4AfB6115b954Bd326cbe7B4BA576818f6"
    ETH_USD_FEED  = "0x5f4eC3Df9cbd43714FE2740f5E3616155c5b8419"

    monkeypatch.setattr(s, "chainlink_usd_feeds", {
        USDC.lower(): USDC_USD_FEED,
        WETH.lower(): ETH_USD_FEED,
    })
    monkeypatch.setattr(s, "extract_max_deviation_bps", 200)

    # Spot says 1 USDC = 1/2425 WETH (3% off "real" 1/2500)
    monkeypatch.setattr(po, "_v3_spot",
                        lambda w3, token_a, token_b, fee: 1 / 2425.0)
    chainlink_returns = {
        USDC_USD_FEED.lower(): (1.0, None),
        ETH_USD_FEED.lower():  (2500.0, None),
    }
    monkeypatch.setattr(po, "_read_chainlink_price",
        lambda w3, feed: chainlink_returns[feed.lower()])

    path = (bytes.fromhex(USDC[2:]) + (3000).to_bytes(3, "big")
            + bytes.fromhex(WETH[2:]))
    cfg = {"v": 3, "t": WETH, "v2": [], "v3": path, "v4": []}

    result = po.check_swap_price_deviation(MagicMock(), cfg, token_in=USDC)
    assert result.ok is False
    assert result.reason == "price_deviation"
    assert result.deviation_bps is not None and result.deviation_bps > 200


def test_check_swap_price_deviation_v3_pool_read_error(monkeypatch):
    """Pool read raises → ok=False, reason=pool_read_failed."""
    import aleph_nodestatus.pool_oracle as po
    from aleph_nodestatus.settings import settings as s

    USDC = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
    WETH = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
    monkeypatch.setattr(s, "chainlink_usd_feeds", {
        USDC.lower(): "0xUSDC_FEED", WETH.lower(): "0xETH_FEED",
    })

    def boom(*a, **kw):
        raise RuntimeError("rpc fell over")
    monkeypatch.setattr(po, "_v3_spot", boom)

    path = (bytes.fromhex(USDC[2:]) + (3000).to_bytes(3, "big")
            + bytes.fromhex(WETH[2:]))
    cfg = {"v": 3, "t": WETH, "v2": [], "v3": path, "v4": []}

    result = po.check_swap_price_deviation(MagicMock(), cfg, token_in=USDC)
    assert result.ok is False
    assert result.reason == "pool_read_failed"


def test_check_swap_price_deviation_v3_no_feed_best_effort(monkeypatch):
    """Hop with one side missing a feed → that hop is best-effort skipped;
    if no other hop fails, ok=True with deviation_bps=None."""
    import aleph_nodestatus.pool_oracle as po
    from aleph_nodestatus.settings import settings as s

    USDC = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
    ALEPH = "0x27702a26126e0b3702af63ee09ac4d1a084ef628"  # no feed
    monkeypatch.setattr(s, "chainlink_usd_feeds", {
        USDC.lower(): "0xUSDC_FEED",
        # No mapping for ALEPH — best-effort skip the hop.
    })
    monkeypatch.setattr(po, "_v3_spot",
                        lambda *a, **kw: 1.0)  # would never get used
    monkeypatch.setattr(po, "_read_chainlink_price",
                        lambda w3, feed: (1.0, None))

    path = (bytes.fromhex(USDC[2:]) + (10000).to_bytes(3, "big")
            + bytes.fromhex(ALEPH[2:]))
    cfg = {"v": 3, "t": ALEPH, "v2": [], "v3": path, "v4": []}

    result = po.check_swap_price_deviation(MagicMock(), cfg, token_in=USDC)
    assert result.ok is True
    assert result.deviation_bps is None
```

- [ ] **Step 5.2: Run tests — expect failure**

Run: `.venv/bin/python -m pytest tests/test_pool_oracle.py -v`

Expected: the V3-related tests FAIL — helpers not yet defined; the existing skeleton tests still PASS.

- [ ] **Step 5.3: Implement the V3 helpers + dispatch**

Edit `src/aleph_nodestatus/pool_oracle.py`. Add at module bottom (after `_read_chainlink_price`):

```python
def _decode_v3_path(path_bytes):
    """Split a Uniswap V3 encoded path into a list of (token_a, fee, token_b)
    triples. Path layout: address(20) | fee(3) | address(20) | fee(3) | …
    | address(20).
    """
    if not isinstance(path_bytes, (bytes, bytearray)):
        path_bytes = bytes(path_bytes)
    hops = []
    offset = 0
    # Each hop needs at least 20 + 3 + 20 = 43 bytes.
    while offset + 43 <= len(path_bytes):
        token_a = "0x" + path_bytes[offset:offset + 20].hex()
        fee = int.from_bytes(path_bytes[offset + 20:offset + 23], "big")
        token_b = "0x" + path_bytes[offset + 23:offset + 43].hex()
        hops.append((token_a, fee, token_b))
        offset += 23  # advance by token + fee; next iter starts at next token's address
    return hops


def _sqrt_price_to_b_per_a(
    sqrt_price_x96: int,
    token_a: str, token_b: str,
    decimals_a: int, decimals_b: int,
) -> float:
    """Convert a Uniswap V3/V4 sqrtPriceX96 into "tokenB per tokenA" in
    human (decimal-adjusted) units. The pool's token0/token1 ordering is
    derived from address comparison."""
    a_is_token0 = int(token_a, 16) < int(token_b, 16)
    decimals_0 = decimals_a if a_is_token0 else decimals_b
    decimals_1 = decimals_b if a_is_token0 else decimals_a
    price_raw_1_per_0 = (sqrt_price_x96 / (2 ** 96)) ** 2
    price_human_1_per_0 = (
        price_raw_1_per_0 * (10 ** decimals_0) / (10 ** decimals_1)
    )
    if a_is_token0:
        # A=token0, B=token1; "B per A" = "token1 per token0"
        return price_human_1_per_0
    # A=token1, B=token0; "B per A" = 1 / "token1 per token0"
    return 1.0 / price_human_1_per_0


def _erc20_decimals(w3, token_address: str) -> int:
    """Read ERC20 decimals(). Special-cases the ETH sentinel as 18.
    Cached at module scope to avoid duplicate eth_calls within a run."""
    if token_address.lower() == "0x0000000000000000000000000000000000000000":
        return 18
    return _cached_decimals(w3, w3.to_checksum_address(token_address))


_DECIMALS_CACHE = {}


def _cached_decimals(w3, addr: str) -> int:
    key = addr.lower()
    if key in _DECIMALS_CACHE:
        return _DECIMALS_CACHE[key]
    minimal_abi = [{
        "constant": True, "inputs": [],
        "name": "decimals",
        "outputs": [{"name": "", "type": "uint8"}],
        "type": "function", "stateMutability": "view",
    }]
    c = w3.eth.contract(address=addr, abi=minimal_abi)
    d = int(c.functions.decimals().call())
    _DECIMALS_CACHE[key] = d
    return d


def _v3_spot(w3, token_a: str, token_b: str, fee: int) -> float:
    """Look up the pool for (tokenA, tokenB, fee) via the V3 factory, read
    slot0, and return the spot price "tokenB per tokenA" in human units."""
    factory = w3.eth.contract(
        address=w3.to_checksum_address(settings.uniswap_v3_factory_address),
        abi=_load_abi("IUniswapV3Factory"),
    )
    pool_address = factory.functions.getPool(
        w3.to_checksum_address(token_a),
        w3.to_checksum_address(token_b),
        fee,
    ).call()
    if int(pool_address, 16) == 0:
        raise ValueError(
            f"V3 pool not found for {token_a}/{token_b} fee={fee}"
        )
    pool = w3.eth.contract(
        address=w3.to_checksum_address(pool_address),
        abi=_load_abi("IUniswapV3Pool"),
    )
    slot0 = pool.functions.slot0().call()
    sqrt_price_x96 = int(slot0[0])
    return _sqrt_price_to_b_per_a(
        sqrt_price_x96, token_a, token_b,
        _erc20_decimals(w3, token_a),
        _erc20_decimals(w3, token_b),
    )
```

Now replace the existing `check_swap_price_deviation` with the full dispatch:

```python
def check_swap_price_deviation(w3, swap_config: dict, token_in: str) -> OracleResult:
    v = swap_config["v"]
    if v == 3:
        hops = _decode_v3_path(swap_config["v3"])
        spot_fn = lambda t_a, t_b, extra: _v3_spot(w3, t_a, t_b, extra["fee"])
        hop_iter = [(a, b, {"fee": fee}) for (a, fee, b) in hops]
    else:
        # V2/V4 land in later tasks.
        return OracleResult(ok=True)

    feeds = {k.lower(): v for k, v in settings.chainlink_usd_feeds.items()}
    threshold = settings.extract_max_deviation_bps

    for token_a, token_b, extra in hop_iter:
        feed_a = feeds.get(token_a.lower())
        feed_b = feeds.get(token_b.lower())
        if not feed_a or not feed_b:
            LOGGER.info(
                "Best-effort skip on hop %s→%s (no Chainlink feed for one side)",
                token_a, token_b,
            )
            continue

        try:
            spot = spot_fn(token_a, token_b, extra)
        except Exception as e:
            LOGGER.warning(
                "Pool read failed for hop %s→%s: %r", token_a, token_b, e,
            )
            return OracleResult(ok=False, reason="pool_read_failed")

        price_a_usd, reason_a = _read_chainlink_price(w3, feed_a)
        if reason_a:
            return OracleResult(ok=False, reason=reason_a)
        price_b_usd, reason_b = _read_chainlink_price(w3, feed_b)
        if reason_b:
            return OracleResult(ok=False, reason=reason_b)

        ref = price_a_usd / price_b_usd  # B per A
        deviation_bps = int(abs(spot - ref) / ref * 10_000)
        if deviation_bps > threshold:
            return OracleResult(
                ok=False, reason="price_deviation",
                deviation_bps=deviation_bps,
                spot_price=spot, ref_price=ref,
            )

    return OracleResult(ok=True)
```

- [ ] **Step 5.4: Run tests — expect PASS**

Run: `.venv/bin/python -m pytest tests/test_pool_oracle.py -v`

Expected: all PASS.

- [ ] **Step 5.5: Commit**

```bash
git add src/aleph_nodestatus/pool_oracle.py tests/test_pool_oracle.py
git commit -m "feat(extract): V3 path decoder + spot + deviation dispatch"
```

---

## Task 6: V4 spot price + dispatch

Same shape as V3, but pool identification is via `keccak256(abi.encode(PoolKey))` and spot read via the V4 StateView contract.

**Files:**
- Modify: `src/aleph_nodestatus/pool_oracle.py`
- Modify: `tests/test_pool_oracle.py`

- [ ] **Step 6.1: Write the failing tests**

Append to `tests/test_pool_oracle.py`:

```python
def test_v4_pool_id_deterministic():
    """V4 poolId = keccak256(abi.encode(currency0, currency1, fee, tickSpacing, hooks))
    with currency0 < currency1 sorted lexicographically."""
    from aleph_nodestatus.pool_oracle import _v4_pool_id

    USDC = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
    WETH = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
    pid1 = _v4_pool_id(USDC, WETH, fee=3000, tick_spacing=60,
                       hooks="0x0000000000000000000000000000000000000000")
    pid2 = _v4_pool_id(WETH, USDC, fee=3000, tick_spacing=60,
                       hooks="0x0000000000000000000000000000000000000000")
    assert pid1 == pid2  # order-independent
    assert isinstance(pid1, (bytes, bytearray))
    assert len(pid1) == 32


def test_enumerate_v4_hops_single():
    """Single-hop V4 path: token_in → path[0].intermediateCurrency."""
    from aleph_nodestatus.pool_oracle import _enumerate_v4_hops

    USDC = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
    WETH = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
    HOOKS = "0x0000000000000000000000000000000000000000"
    path = [(WETH, 3000, 60, HOOKS, b"")]
    hops = list(_enumerate_v4_hops(path, token_in=USDC))
    assert len(hops) == 1
    assert hops[0][0].lower() == USDC.lower()
    assert hops[0][1].lower() == WETH.lower()
    assert hops[0][2]["fee"] == 3000
    assert hops[0][2]["tick_spacing"] == 60
    assert hops[0][2]["hooks"] == HOOKS


def test_check_swap_price_deviation_v4_within_threshold(monkeypatch):
    import aleph_nodestatus.pool_oracle as po
    from aleph_nodestatus.settings import settings as s

    USDC = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
    WETH = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
    USDC_USD_FEED = "0x8fFfFfd4AfB6115b954Bd326cbe7B4BA576818f6"
    ETH_USD_FEED  = "0x5f4eC3Df9cbd43714FE2740f5E3616155c5b8419"
    monkeypatch.setattr(s, "chainlink_usd_feeds", {
        USDC.lower(): USDC_USD_FEED, WETH.lower(): ETH_USD_FEED,
    })

    monkeypatch.setattr(po, "_v4_spot",
                        lambda w3, t_a, t_b, extra: 1 / 2500.0)
    monkeypatch.setattr(po, "_read_chainlink_price",
        lambda w3, feed: {
            USDC_USD_FEED.lower(): (1.0, None),
            ETH_USD_FEED.lower():  (2500.0, None),
        }[feed.lower()])

    HOOKS = "0x0000000000000000000000000000000000000000"
    cfg = {"v": 4, "t": WETH, "v2": [], "v3": b"",
           "v4": [(WETH, 3000, 60, HOOKS, b"")]}
    result = po.check_swap_price_deviation(MagicMock(), cfg, token_in=USDC)
    assert result.ok is True


def test_check_swap_price_deviation_v4_exceeds_threshold(monkeypatch):
    import aleph_nodestatus.pool_oracle as po
    from aleph_nodestatus.settings import settings as s

    USDC = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
    WETH = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
    USDC_USD_FEED = "0xfeed_usdc"
    ETH_USD_FEED  = "0xfeed_eth"
    monkeypatch.setattr(s, "chainlink_usd_feeds", {
        USDC.lower(): USDC_USD_FEED, WETH.lower(): ETH_USD_FEED,
    })
    monkeypatch.setattr(s, "extract_max_deviation_bps", 200)

    monkeypatch.setattr(po, "_v4_spot",
                        lambda w3, t_a, t_b, extra: 1 / 2425.0)
    monkeypatch.setattr(po, "_read_chainlink_price",
        lambda w3, feed: {
            USDC_USD_FEED.lower(): (1.0, None),
            ETH_USD_FEED.lower():  (2500.0, None),
        }[feed.lower()])

    HOOKS = "0x0000000000000000000000000000000000000000"
    cfg = {"v": 4, "t": WETH, "v2": [], "v3": b"",
           "v4": [(WETH, 3000, 60, HOOKS, b"")]}
    result = po.check_swap_price_deviation(MagicMock(), cfg, token_in=USDC)
    assert result.ok is False
    assert result.reason == "price_deviation"
```

- [ ] **Step 6.2: Run tests — expect failure**

Run: `.venv/bin/python -m pytest tests/test_pool_oracle.py -v`

Expected: new V4 tests FAIL, V3 tests still PASS.

- [ ] **Step 6.3: Implement V4 helpers + dispatch**

Edit `src/aleph_nodestatus/pool_oracle.py`. Add to the imports near the top of the file:

```python
from eth_abi import encode as _abi_encode
from eth_utils import keccak as _keccak
```

Append at module bottom:

```python
def _v4_pool_id(currency_a: str, currency_b: str,
                fee: int, tick_spacing: int, hooks: str) -> bytes:
    """Compute the V4 pool id = keccak256(abi.encode(PoolKey)). The
    PoolKey requires currency0 < currency1 (address-sorted)."""
    c0, c1 = sorted([currency_a.lower(), currency_b.lower()])
    encoded = _abi_encode(
        ["address", "address", "uint24", "int24", "address"],
        [c0, c1, fee, tick_spacing, hooks.lower()],
    )
    return _keccak(encoded)


def _enumerate_v4_hops(path_keys, token_in: str):
    """V4 path is a list of PathKey tuples
    (intermediateCurrency, fee, tickSpacing, hooks, hookData). Each hop's
    input is the previous hop's output (or token_in for the first)."""
    prev = token_in
    for pk in path_keys:
        intermediate, fee, tick_spacing, hooks, _hook_data = pk
        yield prev, intermediate, {
            "fee": fee, "tick_spacing": tick_spacing, "hooks": hooks,
        }
        prev = intermediate


def _v4_spot(w3, token_a: str, token_b: str, extra: dict) -> float:
    """Read sqrtPriceX96 from the V4 StateView for the pool identified by
    PoolKey(currency0, currency1, fee, tickSpacing, hooks), then convert
    to "tokenB per tokenA"."""
    pool_id = _v4_pool_id(
        token_a, token_b,
        fee=extra["fee"],
        tick_spacing=extra["tick_spacing"],
        hooks=extra["hooks"],
    )
    state_view = w3.eth.contract(
        address=w3.to_checksum_address(settings.uniswap_v4_state_view_address),
        abi=_load_abi("IUniswapV4StateView"),
    )
    slot0 = state_view.functions.getSlot0(pool_id).call()
    sqrt_price_x96 = int(slot0[0])
    return _sqrt_price_to_b_per_a(
        sqrt_price_x96, token_a, token_b,
        _erc20_decimals(w3, token_a),
        _erc20_decimals(w3, token_b),
    )
```

Update the dispatch in `check_swap_price_deviation`. Replace the body that currently reads:

```python
    v = swap_config["v"]
    if v == 3:
        hops = _decode_v3_path(swap_config["v3"])
        spot_fn = lambda t_a, t_b, extra: _v3_spot(w3, t_a, t_b, extra["fee"])
        hop_iter = [(a, b, {"fee": fee}) for (a, fee, b) in hops]
    else:
        # V2/V4 land in later tasks.
        return OracleResult(ok=True)
```

with:

```python
    v = swap_config["v"]
    if v == 3:
        hops = _decode_v3_path(swap_config["v3"])
        spot_fn = lambda t_a, t_b, extra: _v3_spot(w3, t_a, t_b, extra["fee"])
        hop_iter = [(a, b, {"fee": fee}) for (a, fee, b) in hops]
    elif v == 4:
        hop_iter = list(_enumerate_v4_hops(swap_config["v4"], token_in))
        spot_fn = lambda t_a, t_b, extra: _v4_spot(w3, t_a, t_b, extra)
    else:
        # V2 lands in the next task.
        return OracleResult(ok=True)
```

- [ ] **Step 6.4: Run tests — expect PASS**

Run: `.venv/bin/python -m pytest tests/test_pool_oracle.py -v`

Expected: all PASS.

- [ ] **Step 6.5: Commit**

```bash
git add src/aleph_nodestatus/pool_oracle.py tests/test_pool_oracle.py
git commit -m "feat(extract): V4 PoolKey hashing + StateView spot + dispatch"
```

---

## Task 7: V2 spot price + dispatch

Final path version. V2 isn't used in production today but the contract supports it; we cover it now so a path admin can switch without breaking the oracle.

**Files:**
- Modify: `src/aleph_nodestatus/pool_oracle.py`
- Modify: `tests/test_pool_oracle.py`

- [ ] **Step 7.1: Write the failing tests**

Append to `tests/test_pool_oracle.py`:

```python
def test_v2_spot_from_reserves(monkeypatch):
    """V2 pair.getReserves() with token_a being token0:
    spot_B_per_A = (reserve1 / 10^dec1) / (reserve0 / 10^dec0)."""
    import aleph_nodestatus.pool_oracle as po

    USDC = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
    WETH = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"

    pair = MagicMock()
    # USDC is token0 (lower hex). Reserve0=10_000 USDC (10_000 × 10^6),
    # Reserve1=4 WETH (4 × 10^18). Spot WETH per USDC = (4 / 10000) = 0.0004.
    pair.functions.getReserves.return_value.call.return_value = (
        10_000 * 10 ** 6, 4 * 10 ** 18, 1_700_000_000,
    )
    w3 = MagicMock()
    w3.eth.contract.return_value = pair

    monkeypatch.setattr(po, "_erc20_decimals",
                        lambda w3_, addr: 6 if addr.lower() == USDC.lower() else 18)

    spot = po._v2_spot(w3, USDC, WETH, pair_address="0xpair")
    assert abs(spot - 0.0004) < 1e-9


def test_check_swap_price_deviation_v2_dispatch(monkeypatch):
    """V2 path goes through the dispatch correctly."""
    import aleph_nodestatus.pool_oracle as po
    from aleph_nodestatus.settings import settings as s

    USDC = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
    WETH = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
    monkeypatch.setattr(s, "chainlink_usd_feeds", {
        USDC.lower(): "0xfeed_usdc", WETH.lower(): "0xfeed_eth",
    })
    monkeypatch.setattr(po, "_v2_spot",
                        lambda w3, t_a, t_b, pair_address: 1 / 2500.0)
    monkeypatch.setattr(po, "_read_chainlink_price",
        lambda w3, feed: {"0xfeed_usdc": (1.0, None),
                          "0xfeed_eth":  (2500.0, None)}[feed.lower()])
    monkeypatch.setattr(po, "_v2_pair_address",
                        lambda w3, t_a, t_b: "0xfakepair")

    cfg = {"v": 2, "t": WETH, "v2": [USDC, WETH], "v3": b"", "v4": []}
    result = po.check_swap_price_deviation(MagicMock(), cfg, token_in=USDC)
    assert result.ok is True
```

- [ ] **Step 7.2: Run tests — expect failure**

Run: `.venv/bin/python -m pytest tests/test_pool_oracle.py -v`

Expected: new V2 tests FAIL.

- [ ] **Step 7.3: Implement V2 helpers + dispatch**

Edit `src/aleph_nodestatus/pool_oracle.py`. Append at module bottom:

```python
def _v2_pair_address(w3, token_a: str, token_b: str) -> str:
    """Return the V2 pair address for (tokenA, tokenB). Today the project
    uses the Uniswap V2 router (settings.uniswap_v2_router_address) but
    the canonical way to derive a pair is via the V2 factory's
    getPair(tokenA, tokenB). The router exposes factory() — read it
    once."""
    router = w3.eth.contract(
        address=w3.to_checksum_address(settings.uniswap_v2_router_address),
        abi=_load_abi("UniswapV2Router"),
    )
    factory_addr = router.functions.factory().call()
    factory_abi = [{
        "inputs": [
            {"name": "tokenA", "type": "address"},
            {"name": "tokenB", "type": "address"},
        ],
        "name": "getPair",
        "outputs": [{"name": "pair", "type": "address"}],
        "stateMutability": "view", "type": "function",
    }]
    factory = w3.eth.contract(
        address=w3.to_checksum_address(factory_addr), abi=factory_abi,
    )
    pair = factory.functions.getPair(
        w3.to_checksum_address(token_a),
        w3.to_checksum_address(token_b),
    ).call()
    if int(pair, 16) == 0:
        raise ValueError(
            f"V2 pair not found for {token_a}/{token_b}"
        )
    return pair


def _v2_spot(w3, token_a: str, token_b: str, pair_address: str) -> float:
    """Read reserves from a V2 pair and return "tokenB per tokenA" in
    human units. token0 = lower address."""
    pair = w3.eth.contract(
        address=w3.to_checksum_address(pair_address),
        abi=_load_abi("IUniswapV2Pair"),
    )
    reserve0, reserve1, _ts = pair.functions.getReserves().call()
    a_is_token0 = int(token_a, 16) < int(token_b, 16)
    decimals_a = _erc20_decimals(w3, token_a)
    decimals_b = _erc20_decimals(w3, token_b)
    if a_is_token0:
        amt_a = reserve0 / (10 ** decimals_a)
        amt_b = reserve1 / (10 ** decimals_b)
    else:
        amt_a = reserve1 / (10 ** decimals_a)
        amt_b = reserve0 / (10 ** decimals_b)
    return amt_b / amt_a
```

Update the dispatch — change the `else: return OracleResult(ok=True)` branch and add V2 handling. Replace the block:

```python
    elif v == 4:
        hop_iter = list(_enumerate_v4_hops(swap_config["v4"], token_in))
        spot_fn = lambda t_a, t_b, extra: _v4_spot(w3, t_a, t_b, extra)
    else:
        # V2 lands in the next task.
        return OracleResult(ok=True)
```

with:

```python
    elif v == 4:
        hop_iter = list(_enumerate_v4_hops(swap_config["v4"], token_in))
        spot_fn = lambda t_a, t_b, extra: _v4_spot(w3, t_a, t_b, extra)
    elif v == 2:
        v2_path = list(swap_config["v2"])
        hop_iter = []
        for i in range(len(v2_path) - 1):
            t_a, t_b = v2_path[i], v2_path[i + 1]
            try:
                pair = _v2_pair_address(w3, t_a, t_b)
            except Exception as e:
                LOGGER.warning(
                    "V2 pair lookup failed for hop %s→%s: %r",
                    t_a, t_b, e,
                )
                return OracleResult(ok=False, reason="pool_read_failed")
            hop_iter.append((t_a, t_b, {"pair_address": pair}))
        spot_fn = lambda t_a, t_b, extra: _v2_spot(
            w3, t_a, t_b, extra["pair_address"],
        )
    else:
        LOGGER.warning("Unknown swap version: %s", v)
        return OracleResult(ok=False, reason="pool_read_failed")
```

- [ ] **Step 7.4: Run tests — expect PASS**

Run: `.venv/bin/python -m pytest tests/test_pool_oracle.py -v`

Expected: all PASS.

- [ ] **Step 7.5: Commit**

```bash
git add src/aleph_nodestatus/pool_oracle.py tests/test_pool_oracle.py
git commit -m "feat(extract): V2 pair reserves spot + dispatch (completes pool_oracle)"
```

---

## Task 8: Render `price_deviation` skips in stdout summary

The skeleton in Task 3 set `entry["skipped_reason"] = oracle.reason` on the entry. `_print_summary` in `credit_extraction.py` already prints `skipped=<reason>`. This task adds the deviation-bps detail that ops will want at a glance.

**Files:**
- Modify: `src/aleph_nodestatus/credit_extraction.py`
- Modify: `tests/test_credit_extraction.py`

- [ ] **Step 8.1: Write the failing test**

Append to `tests/test_credit_extraction.py`:

```python
@pytest.mark.asyncio
async def test_stdout_summary_renders_price_deviation(monkeypatch, capsys):
    """A token with skipped_reason='price_deviation' shows the deviation
    bps and the spot/ref prices in the summary line."""
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
             "oracle": {"deviation_bps": 350, "spot_price": 1.04,
                        "ref_price": 1.00}},
        ],
        "errors": [],
    })

    await ce.process_credit_extraction(
        act=False, dry_run=True, transfer=False,
    )

    out = capsys.readouterr().out
    assert "USDC" in out and "price_deviation" in out
    assert "350" in out  # deviation bps shown
```

- [ ] **Step 8.2: Run the test — expect failure**

Run: `.venv/bin/python -m pytest tests/test_credit_extraction.py::test_stdout_summary_renders_price_deviation -v`

Expected: FAIL — `350` not in output (only `skipped=price_deviation` shows).

- [ ] **Step 8.3: Extend `_print_summary`**

Edit `src/aleph_nodestatus/credit_extraction.py`. Find `_print_summary` and change the line that renders `skipped` from:

```python
        if entry.get("skipped_reason"):
            line += f" skipped={entry['skipped_reason']}"
```

to:

```python
        if entry.get("skipped_reason"):
            line += f" skipped={entry['skipped_reason']}"
            if entry.get("skipped_reason") == "price_deviation":
                o = entry.get("oracle") or {}
                if o.get("deviation_bps") is not None:
                    line += f" Δ={o['deviation_bps']}bps"
```

- [ ] **Step 8.4: Run the test — expect PASS**

Run: `.venv/bin/python -m pytest tests/test_credit_extraction.py -v`

Expected: all PASS (new test + existing tests).

- [ ] **Step 8.5: Commit**

```bash
git add src/aleph_nodestatus/credit_extraction.py tests/test_credit_extraction.py
git commit -m "feat(extract): render price_deviation skips with bps in stdout summary"
```

---

## Task 9: Update docker `.env.extract.example`

Document the new env knobs so operators see them when bootstrapping a new deployment.

**Files:**
- Modify: `docker/.env.extract.example`

- [ ] **Step 9.1: Append the Phase 1 overrides block**

Edit `docker/.env.extract.example`. Add a new section before the final `# All other settings inherit pydantic defaults …` line:

```
# === Anti-MEV (Phase 1) — uncomment to override defaults ===
#
# Random delay (anti-tracking): uniform 0..N seconds before the actual
# extract run. Set to 0 to disable jitter (e.g., for staging/testing).
# extract_random_delay_max_seconds=3540
#
# Pool-spot vs Chainlink deviation guard. If any hop exceeds the
# threshold, that token is skipped and retried next hour.
# extract_max_deviation_bps=200
#
# Reject Chainlink readings older than this many seconds (default 1h —
# matches the mainnet ETH/USD heartbeat).
# chainlink_max_age_seconds=3600
```

- [ ] **Step 9.2: Commit**

```bash
git add docker/.env.extract.example
git commit -m "docs(docker): document anti-MEV (phase 1) env overrides"
```

---

## Final verification

- [ ] **Step F.1: Full suite**

Run: `.venv/bin/python -m pytest tests/ --ignore=tests/test_scores.py --ignore=tests/test_skeleton.py -v`

Expected: all PASS.

- [ ] **Step F.2: Confirm CLI surface**

Run (inside an installed venv, ignoring the macOS plyvel issue if applicable):

```bash
.venv/bin/python -m pip install -e . > /dev/null && \
  .venv/bin/nodestatus-extract-credits --help 2>&1 | grep -E "immediate|dry-run|act|no-transfer|slippage-bps"
```

Expected: all five flags listed in help.

- [ ] **Step F.3: Confirm new settings discoverable**

Run:

```bash
.venv/bin/python -c "
from aleph_nodestatus.settings import settings
print('delay:',  settings.extract_random_delay_max_seconds)
print('bps:',    settings.extract_max_deviation_bps)
print('feeds:',  len(settings.chainlink_usd_feeds))
"
```

Expected: prints `delay: 3540`, `bps: 200`, `feeds: 3`.
