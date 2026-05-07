# Credit Rewards Distribution Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend `nodestatus-distribute-credits` so it (1) extracts ALEPH from the `AlephPaymentProcessor` per token (USDC → ETH → ALEPH) with off-chain slippage-protected `min_out`, (2) computes credit-revenue rewards (with optional holder-tier `rewards[]` second pass), (3) adds the 6-month linearly-decaying wage subsidy split 1/3 CCN / 1/3 CRN / 1/3 stakers, (4) merges and pays via `batchTransfer`, all gated by per-step feature flags + a dry-run mode that uses `eth_call` simulation.

**Architecture:** Single orchestrator (`process_credit_distribution`) wires four pure modules — `payment_processor.py` (extract), `credit_distribution.py` (refactored, holder-tier optional), `wage_subsidy.py` (continuous linear integral), plus a small merge/dust helper. One paginated API fetch and one snapshot walk regardless of holder-tier flag. Feature flags read from `settings` with CLI overrides; `--dry-run` blocks every outbound side-effect but still simulates `process()` via `eth_call`.

**Tech Stack:** Python 3 / asyncio / aiohttp / web3.py 6.x / aleph-sdk-python / pydantic-1 settings / pytest + pytest-asyncio. New on-chain calls: `AlephPaymentProcessor.process`, `AlephPaymentProcessor.getSwapConfig`, Uniswap V3 QuoterV2.

**Spec:** `docs/specs/2026-05-07-credit-rewards-distribution-design.md`

**Repo conventions baked into this plan:**
- Source layout: `src/aleph_nodestatus/<module>.py`; tests in `tests/test_<module>.py`; ABIs in `src/aleph_nodestatus/abi/<name>.json`.
- Tests use pytest + `@pytest.mark.asyncio` for coroutines (already configured in `setup.cfg`).
- Settings are `pydantic.BaseSettings` (pydantic-1 syntax — see existing `settings.py`).

---

## File structure

**New files (created during this plan):**

| Path | Responsibility |
|---|---|
| `src/aleph_nodestatus/wage_subsidy.py` | Continuous linear curve integral and per-period split into CCN/CRN/staker reward dicts |
| `src/aleph_nodestatus/payment_processor.py` | Read swap config from the on-chain processor, compute slippage-protected min-out via the V3 quoter, `eth_call`-simulate and (in `--act`) broadcast `process()` calls |
| `src/aleph_nodestatus/rewards_merge.py` | `merge_rewards()` + dust filter, address checksum normalization |
| `src/aleph_nodestatus/abi/AlephPaymentProcessor.json` | ABI extracted from the user-supplied JSON |
| `src/aleph_nodestatus/abi/UniswapV3QuoterV2.json` | Uniswap V3 QuoterV2 ABI |
| `tests/test_wage_subsidy.py` | Unit tests for the integral, period subsidy, split |
| `tests/test_payment_processor.py` | Unit tests for `min_out` math, dust skip, version mismatch, simulation parsing |
| `tests/test_rewards_merge.py` | Unit tests for merge + dust |
| `tests/test_credit_pipeline.py` | Integration: feature-flag matrix, dry-run E2E against recorded fixtures |
| `tests/fixtures/expense_storage.json` | Recorded sample storage message |
| `tests/fixtures/expense_execution.json` | Recorded sample execution message |
| `tests/fixtures/snapshot.json` | Recorded corechannel snapshot |

**Modified files:**

| Path | Change |
|---|---|
| `src/aleph_nodestatus/settings.py` | Add ~20 env vars (processor address, tokens, wage parameters, feature flags, cadence) |
| `src/aleph_nodestatus/credit_distribution.py` | Extract `compute_rewards()` as a pure function; add holder-tier second pass; reuse one snapshot per message |
| `src/aleph_nodestatus/commands.py` | New CLI flags; orchestrate extract → compute → wage → merge → transfer → publish; cadence guard |
| `docker/docker-compose.yml` | New cron line for `nodestatus-distribute-credits` |

---

## Task 1: Confirm test scaffolding works

**Files:**
- Read: `tests/conftest.py`, `tests/test_skeleton.py`, `setup.cfg`

- [ ] **Step 1: Install the package in editable mode + test extras**

```bash
pip install -e .[testing]
```

Expected: succeeds; `pytest` and `pytest-asyncio` installed.

- [ ] **Step 2: Run the existing test suite to confirm baseline**

```bash
pytest -q
```

Expected: existing tests pass (or at minimum, `test_skeleton.py::test_fib` passes). If anything else fails, that's a pre-existing issue — note it but don't fix it in this plan.

- [ ] **Step 3: No commit (verification only)**

---

## Task 2: Add new settings and feature flags

**Files:**
- Modify: `src/aleph_nodestatus/settings.py`

- [ ] **Step 1: Write failing test for new settings access**

Create `tests/test_settings_new.py`:

```python
from aleph_nodestatus.settings import settings


def test_payment_processor_settings_defaults():
    assert settings.payment_processor_address.lower() == \
        "0x6b55f32ea969910838defd03746ced5e2ae8cb8b"
    assert settings.distribution_recipient.lower() == \
        "0x3a5cc6abd06b601f4654035d125f9dd2fc992c25"
    assert settings.uniswap_v3_quoter_address.lower() == \
        "0x61ffe014ba17989e743c5f6cb21bf9697530b21e"


def test_token_list_default():
    syms = [s for s, _ in settings.process_tokens]
    assert syms == ["USDC", "ETH", "ALEPH"]


def test_wage_subsidy_defaults():
    assert settings.wage_initial_monthly_aleph == 900_000
    assert settings.wage_duration_months == 6
    assert abs(sum([settings.wage_ccn_share,
                    settings.wage_crn_share,
                    settings.wage_staker_share]) - 1.0) < 1e-9


def test_feature_flags_defaults():
    assert settings.credit_dist_extract_enabled is True
    assert settings.credit_dist_credit_revenue_enabled is True
    assert settings.credit_dist_wage_subsidy_enabled is True
    assert settings.credit_dist_holder_tier_enabled is False
    assert settings.credit_dist_transfer_enabled is True
    assert settings.credit_dist_publish_enabled is True


def test_cadence_defaults():
    assert settings.credit_dist_min_interval_seconds == 10 * 86400
    assert settings.credit_dist_dust_threshold_aleph == 0.01
    assert settings.process_slippage_bps == 200
    assert settings.process_ttl_seconds == 1800
```

- [ ] **Step 2: Run test to verify it fails**

```bash
pytest tests/test_settings_new.py -v
```

Expected: FAIL (attributes do not exist on `Settings`).

- [ ] **Step 3: Add the new fields to `Settings`**

In `src/aleph_nodestatus/settings.py`, immediately before `class Config:` add:

```python
    # === AlephPaymentProcessor ===
    payment_processor_address: str = "0x6b55f32ea969910838defd03746ced5e2ae8cb8b"
    payment_processor_admin_pkey: str = ""
    distribution_recipient: str = "0x3a5CC6aBd06B601f4654035d125F9DD2FC992C25"

    # Tokens to process, in order. address(0) for ETH.
    process_tokens: List[tuple] = [
        ("USDC", "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"),
        ("ETH",  "0x0000000000000000000000000000000000000000"),
        ("ALEPH","0x27702a26126e0B3702af63Ee09aC4d1A084EF628"),
    ]

    # Slippage / quoter
    uniswap_v3_quoter_address: str = "0x61fFE014bA17989E743c5F6cB21bF9697530B21e"
    process_slippage_bps: int = 200          # 2%
    process_min_token_value_usd: float = 50
    process_ttl_seconds: int = 1800          # must be <= 3600

    # === Wage subsidy ===
    wage_start_date: str = "2026-04-01T00:00:00+00:00"
    wage_duration_months: int = 6
    wage_initial_monthly_aleph: int = 900_000
    wage_ccn_share: float = 1/3
    wage_crn_share: float = 1/3
    wage_staker_share: float = 1/3

    # === Cadence & filtering ===
    credit_dist_min_interval_seconds: int = 10 * 86400
    credit_dist_dust_threshold_aleph: float = 0.01

    # === Feature flags ===
    credit_dist_extract_enabled: bool        = True
    credit_dist_credit_revenue_enabled: bool = True
    credit_dist_wage_subsidy_enabled: bool   = True
    credit_dist_holder_tier_enabled: bool    = False
    credit_dist_transfer_enabled: bool       = True
    credit_dist_publish_enabled: bool        = True
```

Note: `List[tuple]` works with pydantic-1; values can be overridden via JSON in env: `PROCESS_TOKENS='[["USDC","0x..."],["ETH","0x0"]]'`.

- [ ] **Step 4: Re-run the new tests**

```bash
pytest tests/test_settings_new.py -v
```

Expected: all 5 tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/aleph_nodestatus/settings.py tests/test_settings_new.py && \
git commit -m "feat(settings): add credit-distribution v2 settings and feature flags"
```

---

## Task 3: Vendor the AlephPaymentProcessor ABI

**Files:**
- Create: `src/aleph_nodestatus/abi/AlephPaymentProcessor.json`

- [ ] **Step 1: Write failing test that the ABI is loadable**

Create `tests/test_abi_files.py`:

```python
import json
from pathlib import Path

from aleph_nodestatus import __file__ as pkg_init


def _abi_path(name):
    return Path(pkg_init).parent / "abi" / f"{name}.json"


def test_payment_processor_abi_has_process_function():
    abi = json.loads(_abi_path("AlephPaymentProcessor").read_text())
    fns = [item for item in abi if item.get("type") == "function"
                                and item.get("name") == "process"]
    assert len(fns) == 1
    inputs = [(i["name"], i["type"]) for i in fns[0]["inputs"]]
    assert inputs == [
        ("_token", "address"),
        ("_amountIn", "uint128"),
        ("_amountOutMinimum", "uint128"),
        ("_ttl", "uint48"),
    ]
```

- [ ] **Step 2: Run to verify failure**

```bash
pytest tests/test_abi_files.py::test_payment_processor_abi_has_process_function -v
```

Expected: FAIL — file not found.

- [ ] **Step 3: Save the ABI file**

Paste the full ABI JSON the user provided (the one starting `[{"inputs":[],"stateMutability":"nonpayable","type":"constructor"}, ...]`) into `src/aleph_nodestatus/abi/AlephPaymentProcessor.json`. **Do not minify edits — keep it as a JSON array, valid `json.loads()`-able.**

- [ ] **Step 4: Re-run the test**

```bash
pytest tests/test_abi_files.py -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/aleph_nodestatus/abi/AlephPaymentProcessor.json tests/test_abi_files.py && \
git commit -m "chore(abi): vendor AlephPaymentProcessor ABI"
```

---

## Task 4: Vendor the Uniswap V3 QuoterV2 ABI

**Files:**
- Create: `src/aleph_nodestatus/abi/UniswapV3QuoterV2.json`
- Modify: `tests/test_abi_files.py`

- [ ] **Step 1: Extend the ABI test**

Append to `tests/test_abi_files.py`:

```python
def test_quoter_v2_abi_has_quote_exact_input():
    abi = json.loads(_abi_path("UniswapV3QuoterV2").read_text())
    fns = [item for item in abi if item.get("type") == "function"
                                and item.get("name") == "quoteExactInput"]
    assert len(fns) >= 1
    inputs = [(i["name"], i["type"]) for i in fns[0]["inputs"]]
    assert inputs == [("path", "bytes"), ("amountIn", "uint256")]
```

- [ ] **Step 2: Run to verify failure**

```bash
pytest tests/test_abi_files.py::test_quoter_v2_abi_has_quote_exact_input -v
```

Expected: FAIL.

- [ ] **Step 3: Save the QuoterV2 ABI**

Fetch the canonical QuoterV2 ABI from the Uniswap v3-periphery repo (file `artifacts/contracts/lens/QuoterV2.sol/QuoterV2.json`, only the `abi` array) and save the ABI array to `src/aleph_nodestatus/abi/UniswapV3QuoterV2.json`.

Minimum required functions (verify after save):

- `quoteExactInput(bytes path, uint256 amountIn) -> (uint256 amountOut, ...)`
- `quoteExactInputSingle((address tokenIn, address tokenOut, uint256 amountIn, uint24 fee, uint160 sqrtPriceLimitX96)) -> (uint256, ...)`

- [ ] **Step 4: Re-run tests**

```bash
pytest tests/test_abi_files.py -v
```

Expected: both ABI tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/aleph_nodestatus/abi/UniswapV3QuoterV2.json tests/test_abi_files.py && \
git commit -m "chore(abi): vendor Uniswap V3 QuoterV2 ABI"
```

---

## Task 5: `wage_subsidy.wage_integral()` — the cumulative integral

**Files:**
- Create: `src/aleph_nodestatus/wage_subsidy.py`
- Create: `tests/test_wage_subsidy.py`

- [ ] **Step 1: Write failing tests for the integral**

Create `tests/test_wage_subsidy.py`:

```python
import pytest

from aleph_nodestatus.settings import settings
from aleph_nodestatus.wage_subsidy import wage_integral


def test_integral_at_zero():
    assert wage_integral(0) == 0


def test_integral_at_full_duration_equals_triangle_area():
    W0 = settings.wage_initial_monthly_aleph
    T = settings.wage_duration_months
    assert wage_integral(T) == pytest.approx(W0 * T / 2)
    # = 2_700_000 with defaults
    assert wage_integral(T) == pytest.approx(2_700_000)


def test_integral_first_month_partial():
    # ∫₀¹ 900_000·(1 - x/6) dx = 900_000·(1 - 1/12) = 825_000
    assert wage_integral(1) == pytest.approx(825_000)


def test_integral_clamps_past_end():
    T = settings.wage_duration_months
    assert wage_integral(T + 0.5) == wage_integral(T)
    assert wage_integral(T + 100) == wage_integral(T)


def test_integral_negative_t_is_zero():
    assert wage_integral(-1) == 0
    assert wage_integral(-100) == 0
```

- [ ] **Step 2: Run to verify failure**

```bash
pytest tests/test_wage_subsidy.py -v
```

Expected: FAIL — module not found.

- [ ] **Step 3: Implement `wage_integral`**

Create `src/aleph_nodestatus/wage_subsidy.py`:

```python
"""Linear-decay wage subsidy for the 6-month tokenomics transition.

The curve is W0·(1 - t/T) ALEPH per month, where t is months since
settings.wage_start_date and T = settings.wage_duration_months.
"""

from datetime import datetime
from typing import Dict, Tuple

from .settings import settings

MONTH_SECONDS = 30 * 86400


def wage_integral(t: float) -> float:
    """Cumulative ALEPH paid from t=0 to t months (clamped to [0, T])."""
    T = settings.wage_duration_months
    W0 = settings.wage_initial_monthly_aleph
    if t <= 0:
        return 0.0
    if t >= T:
        return W0 * T / 2.0
    return W0 * (t - t * t / (2.0 * T))
```

- [ ] **Step 4: Run to verify pass**

```bash
pytest tests/test_wage_subsidy.py -v
```

Expected: all 5 tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/aleph_nodestatus/wage_subsidy.py tests/test_wage_subsidy.py && \
git commit -m "feat(wage): wage_integral for linear-decay subsidy curve"
```

---

## Task 6: `wage_subsidy.compute_period_subsidy()`

**Files:**
- Modify: `src/aleph_nodestatus/wage_subsidy.py`
- Modify: `tests/test_wage_subsidy.py`

- [ ] **Step 1: Write failing tests**

Append to `tests/test_wage_subsidy.py`:

```python
from aleph_nodestatus.wage_subsidy import (
    compute_period_subsidy,
    months_since_start,
    parse_wage_start,
)


def test_parse_wage_start_iso_z():
    settings.wage_start_date = "2026-04-01T00:00:00+00:00"
    assert parse_wage_start() == 1775347200.0   # 2026-04-01 UTC


def test_months_since_start_after_30_days():
    settings.wage_start_date = "2026-04-01T00:00:00+00:00"
    # +30 days exactly
    assert months_since_start(1775347200.0 + 30 * 86400) == pytest.approx(1.0)


def test_period_subsidy_within_first_month():
    settings.wage_start_date = "2026-04-01T00:00:00+00:00"
    start = parse_wage_start()
    end = start + 30 * 86400
    # First full month: integral(1) - integral(0) = 825_000
    assert compute_period_subsidy(start, end) == pytest.approx(825_000)


def test_period_subsidy_clamped_past_end():
    settings.wage_start_date = "2026-04-01T00:00:00+00:00"
    start = parse_wage_start() + 7 * 30 * 86400   # 7 months in
    end = start + 30 * 86400
    assert compute_period_subsidy(start, end) == 0.0


def test_period_subsidy_rejects_inverted_range():
    settings.wage_start_date = "2026-04-01T00:00:00+00:00"
    with pytest.raises(ValueError):
        compute_period_subsidy(100, 50)
```

- [ ] **Step 2: Run to verify failure**

```bash
pytest tests/test_wage_subsidy.py -v
```

Expected: FAIL — `compute_period_subsidy` does not exist.

- [ ] **Step 3: Implement**

Append to `src/aleph_nodestatus/wage_subsidy.py`:

```python
def parse_wage_start() -> float:
    return datetime.fromisoformat(
        settings.wage_start_date.replace("Z", "+00:00")
    ).timestamp()


def months_since_start(unix_ts: float) -> float:
    return (unix_ts - parse_wage_start()) / MONTH_SECONDS


def compute_period_subsidy(start_time: float, end_time: float) -> float:
    """Total ALEPH owed as wage subsidy over [start_time, end_time]."""
    if end_time <= start_time:
        raise ValueError(
            f"end_time ({end_time}) must be > start_time ({start_time})"
        )
    T = settings.wage_duration_months
    t1 = max(0.0, months_since_start(start_time))
    t2 = min(float(T), months_since_start(end_time))
    if t2 <= t1:
        return 0.0
    return wage_integral(t2) - wage_integral(t1)
```

- [ ] **Step 4: Run tests**

```bash
pytest tests/test_wage_subsidy.py -v
```

Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/aleph_nodestatus/wage_subsidy.py tests/test_wage_subsidy.py && \
git commit -m "feat(wage): compute_period_subsidy with start/end clamping"
```

---

## Task 7: `wage_subsidy.split_subsidy()` — distribute pools to addresses

**Files:**
- Modify: `src/aleph_nodestatus/wage_subsidy.py`
- Modify: `tests/test_wage_subsidy.py`

- [ ] **Step 1: Write failing tests**

Append to `tests/test_wage_subsidy.py`:

```python
from aleph_nodestatus.wage_subsidy import split_subsidy


def _node(hash, status, score, owner, stakers=None, reward=None,
          resource_nodes=None):
    return {
        "hash": hash, "status": status, "score": score,
        "owner": owner, "reward": reward,
        "stakers": stakers or {},
        "resource_nodes": resource_nodes or [],
        "has_bonus": False, "decentralization": 0.5,
    }


def _rnode(hash, status, score, owner, reward=None):
    return {
        "hash": hash, "status": status, "score": score,
        "owner": owner, "reward": reward,
        "decentralization": 0.5,
    }


def test_split_subsidy_three_equal_pools():
    nodes = {
        "n1": _node("n1", "active", 0.9, "0xCCN1",
                    stakers={"0xS1": 100, "0xS2": 100},
                    resource_nodes=["r1"]),
    }
    rnodes = {"r1": _rnode("r1", "linked", 0.9, "0xCRN1")}
    rewards, unallocated = split_subsidy(900.0, nodes, rnodes)
    # 1/3 to CCN owner, 1/3 to CRN owner, 1/3 split equally across stakers
    assert rewards["0xCCN1"] == pytest.approx(300.0)
    assert rewards["0xCRN1"] == pytest.approx(300.0)
    assert rewards["0xS1"]   == pytest.approx(150.0)
    assert rewards["0xS2"]   == pytest.approx(150.0)
    assert unallocated == pytest.approx(0.0)


def test_split_subsidy_zero_subsidy_returns_empty():
    rewards, unallocated = split_subsidy(0.0, {}, {})
    assert rewards == {}
    assert unallocated == 0.0


def test_split_subsidy_no_active_ccns_records_unallocated():
    rewards, unallocated = split_subsidy(900.0, {}, {})
    # All three pools unallocated
    assert rewards == {}
    assert unallocated == pytest.approx(900.0)


def test_split_subsidy_no_linked_crn_only_crn_pool_unallocated():
    nodes = {
        "n1": _node("n1", "active", 0.9, "0xCCN1",
                    stakers={"0xS1": 100}),
    }
    rnodes = {}    # no resource nodes
    rewards, unallocated = split_subsidy(900.0, nodes, rnodes)
    assert rewards["0xCCN1"] == pytest.approx(300.0)
    assert rewards["0xS1"]   == pytest.approx(300.0)
    assert "0xCRN1" not in rewards
    assert unallocated == pytest.approx(300.0)


def test_split_subsidy_score_weighted_ccn():
    nodes = {
        "n1": _node("n1", "active", 0.9, "0xCCN1", stakers={"0xS1": 1}),
        "n2": _node("n2", "active", 0.5, "0xCCN2", stakers={"0xS2": 1}),
    }
    rnodes = {}
    rewards, _ = split_subsidy(600.0, nodes, rnodes)   # CCN pool = 200
    # multiplier(0.9)=1.0, multiplier(0.5)=0.5 -> weights 1.0 and 0.5
    # 0xCCN1 gets 200 * 1.0/1.5, 0xCCN2 gets 200 * 0.5/1.5
    assert rewards["0xCCN1"] == pytest.approx(200 * 1.0 / 1.5)
    assert rewards["0xCCN2"] == pytest.approx(200 * 0.5 / 1.5)
```

- [ ] **Step 2: Run to verify failure**

```bash
pytest tests/test_wage_subsidy.py -v
```

Expected: FAIL — `split_subsidy` undefined.

- [ ] **Step 3: Implement**

Append to `src/aleph_nodestatus/wage_subsidy.py`:

```python
from .distribution import compute_score_multiplier


def _get_reward_address(entity, web3=None):
    reward = entity.get("reward")
    if reward and web3 is not None:
        validator = getattr(web3, "to_checksum_address",
                            getattr(web3, "toChecksumAddress", None))
        if validator:
            try:
                return validator(reward)
            except Exception:
                pass
    return reward or entity["owner"]


def split_subsidy(
    period_subsidy: float,
    nodes: dict,
    resource_nodes: dict,
    web3=None,
) -> Tuple[Dict[str, float], float]:
    """Split a period's wage subsidy across CCN / CRN / staker pools.

    Returns (rewards_by_address, unallocated_aleph).
    Each pool with zero eligible recipients contributes to unallocated.
    """
    if period_subsidy <= 0:
        return {}, 0.0

    ccn_pool    = period_subsidy * settings.wage_ccn_share
    crn_pool    = period_subsidy * settings.wage_crn_share
    staker_pool = period_subsidy * settings.wage_staker_share

    rewards: Dict[str, float] = {}
    unallocated = 0.0

    # CCN pool, score-weighted across active CCNs
    ccn_weights = []
    for node in nodes.values():
        if node["status"] != "active":
            continue
        score = compute_score_multiplier(node["score"])
        if score > 0:
            ccn_weights.append((_get_reward_address(node, web3), score))
    total_ccn = sum(s for _, s in ccn_weights)
    if total_ccn > 0:
        for addr, s in ccn_weights:
            rewards[addr] = rewards.get(addr, 0.0) + ccn_pool * s / total_ccn
    else:
        unallocated += ccn_pool

    # CRN pool, score-weighted across linked CRNs
    crn_weights = []
    for rnode in resource_nodes.values():
        if rnode["status"] != "linked":
            continue
        score = compute_score_multiplier(rnode["score"])
        if score > 0:
            crn_weights.append((_get_reward_address(rnode, web3), score))
    total_crn = sum(s for _, s in crn_weights)
    if total_crn > 0:
        for addr, s in crn_weights:
            rewards[addr] = rewards.get(addr, 0.0) + crn_pool * s / total_crn
    else:
        unallocated += crn_pool

    # Staker pool, proportional to stake across active CCNs
    all_stakers: Dict[str, float] = {}
    for node in nodes.values():
        if node["status"] != "active":
            continue
        for addr, amt in node["stakers"].items():
            all_stakers[addr] = all_stakers.get(addr, 0.0) + amt
    total_stake = sum(all_stakers.values())
    if total_stake > 0:
        for addr, amt in all_stakers.items():
            rewards[addr] = rewards.get(addr, 0.0) + staker_pool * amt / total_stake
    else:
        unallocated += staker_pool

    return rewards, unallocated
```

- [ ] **Step 4: Run tests**

```bash
pytest tests/test_wage_subsidy.py -v
```

Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/aleph_nodestatus/wage_subsidy.py tests/test_wage_subsidy.py && \
git commit -m "feat(wage): split_subsidy across CCN/CRN/staker pools"
```

---

## Task 8: `wage_subsidy.compute_subsidy()` — public orchestrator

**Files:**
- Modify: `src/aleph_nodestatus/wage_subsidy.py`
- Modify: `tests/test_wage_subsidy.py`

- [ ] **Step 1: Write failing test**

Append to `tests/test_wage_subsidy.py`:

```python
from aleph_nodestatus.wage_subsidy import compute_subsidy


def test_compute_subsidy_returns_rewards_totals_unallocated():
    settings.wage_start_date = "2026-04-01T00:00:00+00:00"
    start = parse_wage_start()
    end = start + 30 * 86400   # first full month
    nodes = {"n1": _node("n1", "active", 0.9, "0xCCN1",
                          stakers={"0xS1": 1}, resource_nodes=["r1"])}
    rnodes = {"r1": _rnode("r1", "linked", 0.9, "0xCRN1")}

    rewards, totals = compute_subsidy(start, end, nodes, rnodes)

    period_total = 825_000
    assert totals["period_total_aleph"] == pytest.approx(period_total)
    assert totals["unallocated_aleph"]  == pytest.approx(0.0)
    assert totals["start_t_months"]     == pytest.approx(0.0)
    assert totals["end_t_months"]       == pytest.approx(1.0)
    assert totals["split"]["ccn"]       == pytest.approx(period_total / 3)
    assert totals["split"]["crn"]       == pytest.approx(period_total / 3)
    assert totals["split"]["stakers"]   == pytest.approx(period_total / 3)
    assert sum(rewards.values()) == pytest.approx(period_total)
```

- [ ] **Step 2: Run to verify failure**

```bash
pytest tests/test_wage_subsidy.py::test_compute_subsidy_returns_rewards_totals_unallocated -v
```

Expected: FAIL — `compute_subsidy` undefined.

- [ ] **Step 3: Implement**

Append to `src/aleph_nodestatus/wage_subsidy.py`:

```python
def compute_subsidy(
    start_time: float,
    end_time: float,
    nodes: dict,
    resource_nodes: dict,
    web3=None,
) -> Tuple[Dict[str, float], dict]:
    """Compute the wage subsidy for [start_time, end_time] and split it.

    Returns (rewards_by_address, totals) where totals contains:
        period_total_aleph, unallocated_aleph, start_t_months, end_t_months,
        split={ccn, crn, stakers}.
    """
    period_total = compute_period_subsidy(start_time, end_time)
    rewards, unallocated = split_subsidy(period_total, nodes, resource_nodes, web3)

    totals = {
        "start_t_months":     months_since_start(start_time),
        "end_t_months":       months_since_start(end_time),
        "period_total_aleph": period_total,
        "unallocated_aleph":  unallocated,
        "split": {
            "ccn":     period_total * settings.wage_ccn_share,
            "crn":     period_total * settings.wage_crn_share,
            "stakers": period_total * settings.wage_staker_share,
        },
    }
    return rewards, totals
```

- [ ] **Step 4: Run test**

```bash
pytest tests/test_wage_subsidy.py -v
```

Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/aleph_nodestatus/wage_subsidy.py tests/test_wage_subsidy.py && \
git commit -m "feat(wage): compute_subsidy orchestrator with totals payload"
```

---

## Task 9: Refactor `_distribute_expense()` to accept explicit shares

**Files:**
- Modify: `src/aleph_nodestatus/credit_distribution.py:249-332`
- Create: `tests/test_credit_distribution_refactor.py`

The goal is to remove the implicit dependency on `settings.credit_*_share` so `_distribute_expense` is a pure function callable with the same shape for credits[] and rewards[].

- [ ] **Step 1: Write a failing test against the new signature**

Create `tests/test_credit_distribution_refactor.py`:

```python
import pytest

from aleph_nodestatus.credit_distribution import _distribute_expense


def _node(hash, score, stakers, resource_nodes=None):
    return {
        "hash": hash, "status": "active", "score": score,
        "owner": f"0xCCN-{hash}", "reward": None,
        "stakers": stakers, "resource_nodes": resource_nodes or [],
        "has_bonus": False, "decentralization": 0.5,
    }


def _rnode(hash, score, owner):
    return {
        "hash": hash, "status": "linked", "score": score,
        "owner": owner, "reward": None, "decentralization": 0.5,
    }


def test_distribute_expense_execution_split():
    nodes = {"n1": _node("n1", 0.9, {"0xS1": 100}, resource_nodes=["r1"])}
    rnodes = {"r1": _rnode("r1", 0.9, "0xCRN1")}
    expense = {
        "credit_price_aleph": 0.001,
        "credits": [{"amount": 1000, "node_id": "r1", "address": "0xU1"}],
    }
    rewards = {}
    storage, execution, dev = _distribute_expense(
        "execution", expense, nodes, rnodes, rewards,
        ccn_share=0.15, staker_share=0.20, crn_share=0.60, dev_share=0.05,
    )
    total_aleph = 1000 * 0.001    # 1.0
    assert execution == pytest.approx(total_aleph)
    assert storage == 0
    assert dev == pytest.approx(total_aleph * 0.05)
    # CRN gets 60%
    assert rewards["0xCRN1"] == pytest.approx(total_aleph * 0.60)
    # CCN gets 15% (single active node, full share)
    assert rewards["0xCCN-n1"] == pytest.approx(total_aleph * 0.15)
    # Stakers get 20% (single staker, full share)
    assert rewards["0xS1"] == pytest.approx(total_aleph * 0.20)
```

- [ ] **Step 2: Run to verify failure**

```bash
pytest tests/test_credit_distribution_refactor.py -v
```

Expected: FAIL — `_distribute_expense` signature does not accept share kwargs.

- [ ] **Step 3: Refactor `_distribute_expense`**

In `src/aleph_nodestatus/credit_distribution.py`, replace the existing `_distribute_expense` (lines 249-332) with:

```python
def _distribute_expense(
    expense_type, expense, nodes, resource_nodes, rewards,
    web3=None,
    *,
    ccn_share: float,
    staker_share: float,
    crn_share: float = 0.0,
    dev_share: float,
):
    """Distribute a single expense entry. Pure function: shares are explicit.

    Returns (storage_aleph, execution_aleph, dev_fund_aleph).
    """
    credit_price_aleph = expense.get("credit_price_aleph", 0)
    total_aleph = sum(
        credit["amount"] * credit_price_aleph
        for credit in expense.get("credits", [])
    )
    dev_fund = total_aleph * dev_share

    if expense_type == "execution":
        # CRN share — per-job to the specific node owner
        for credit in expense.get("credits", []):
            credit_aleph = credit["amount"] * credit_price_aleph
            node_id = credit.get("node_id")
            if node_id and node_id in resource_nodes:
                rnode = resource_nodes[node_id]
                addr = _get_reward_address(rnode, web3)
                rewards[addr] = (
                    rewards.get(addr, 0) + credit_aleph * crn_share
                )
            elif node_id:
                LOGGER.warning(
                    f"CRN {node_id} not found in resource_nodes, skipping"
                )

    # CCN pool (score-weighted)
    ccn_weights = {}
    for node_hash, node in nodes.items():
        if node["status"] != "active":
            continue
        score = compute_score_multiplier(node["score"])
        if score > 0:
            ccn_weights[node_hash] = {
                "score": score,
                "reward_address": _get_reward_address(node, web3),
            }
    total_ccn_score = sum(v["score"] for v in ccn_weights.values())
    ccn_pool = total_aleph * ccn_share
    if total_ccn_score > 0 and ccn_pool > 0:
        for info in ccn_weights.values():
            rewards[info["reward_address"]] = (
                rewards.get(info["reward_address"], 0)
                + ccn_pool * info["score"] / total_ccn_score
            )

    # Staker pool
    all_stakers = {}
    for node in nodes.values():
        if node["status"] != "active":
            continue
        for addr, amount in node["stakers"].items():
            all_stakers[addr] = all_stakers.get(addr, 0) + amount
    total_staked = sum(all_stakers.values())
    staker_pool = total_aleph * staker_share
    if total_staked > 0 and staker_pool > 0:
        for addr, staked in all_stakers.items():
            rewards[addr] = (
                rewards.get(addr, 0) + staker_pool * staked / total_staked
            )

    if expense_type == "storage":
        return total_aleph, 0, dev_fund
    else:
        return 0, total_aleph, dev_fund
```

- [ ] **Step 4: Update existing callers in this file**

Find every call to `_distribute_expense(...)` in `credit_distribution.py` (currently in `_prepare_with_snapshots` and `_prepare_with_state_machine`) and pass the shares from settings explicitly. Example:

```python
def _shares_for(expense_type):
    if expense_type == "storage":
        return dict(
            ccn_share=settings.credit_storage_ccn_share,
            staker_share=settings.credit_storage_staker_share,
            crn_share=0.0,
            dev_share=settings.credit_dev_fund_share,
        )
    return dict(
        ccn_share=settings.credit_execution_ccn_share,
        staker_share=settings.credit_execution_staker_share,
        crn_share=settings.credit_execution_crn_share,
        dev_share=settings.credit_dev_fund_share,
    )
```

Then `_distribute_expense(exp_type, expense, nodes, resource_nodes, rewards, web3=web3, **_shares_for(exp_type))`.

- [ ] **Step 5: Run all credit-distribution tests**

```bash
pytest tests/test_credit_distribution_refactor.py tests/test_settings_new.py -v
```

Expected: pass.

- [ ] **Step 6: Run the full suite to check for regressions**

```bash
pytest -q
```

Expected: no new failures.

- [ ] **Step 7: Commit**

```bash
git add src/aleph_nodestatus/credit_distribution.py tests/test_credit_distribution_refactor.py && \
git commit -m "refactor(credits): _distribute_expense takes explicit share kwargs"
```

---

## Task 10: Extract `compute_rewards()` as a pure function

**Files:**
- Modify: `src/aleph_nodestatus/credit_distribution.py`
- Modify: `tests/test_credit_distribution_refactor.py`

- [ ] **Step 1: Write failing test for the new top-level entry**

Append to `tests/test_credit_distribution_refactor.py`:

```python
import asyncio
from unittest.mock import patch, AsyncMock

from aleph_nodestatus.credit_distribution import compute_rewards


def test_compute_rewards_returns_dict_with_two_streams(monkeypatch):
    fake_messages = []  # empty: no expenses
    fake_snapshots = [(100, {}, {})]

    async def fake_fetch_msgs(*a, **kw):
        return fake_messages

    async def fake_fetch_snaps(*a, **kw):
        return fake_snapshots

    monkeypatch.setattr(
        "aleph_nodestatus.credit_distribution._fetch_expense_messages",
        fake_fetch_msgs,
    )
    monkeypatch.setattr(
        "aleph_nodestatus.credit_distribution.fetch_node_snapshots",
        fake_fetch_snaps,
    )

    result = asyncio.run(compute_rewards(
        start_time=1.0, end_time=2.0,
        full_resync=False, include_holder_tier=False,
    ))
    assert "credit_revenue" in result
    assert "holder_tier"    in result
    credit_rewards, credit_totals = result["credit_revenue"]
    holder_rewards, holder_totals = result["holder_tier"]
    assert credit_rewards == {}
    assert holder_rewards == {}
    assert credit_totals["storage_total_aleph"] == 0
```

- [ ] **Step 2: Run to verify failure**

```bash
pytest tests/test_credit_distribution_refactor.py::test_compute_rewards_returns_dict_with_two_streams -v
```

Expected: FAIL — `compute_rewards` does not exist.

- [ ] **Step 3: Implement `compute_rewards`**

In `src/aleph_nodestatus/credit_distribution.py`, add:

```python
import bisect


async def _fetch_expense_messages(api_server, start_time, end_time, sender=None):
    """Single paginated fetch of aleph_credit_expense messages.

    Returns the raw message dicts (callers parse content).
    """
    params = {
        "msgType": "POST",
        "contentTypes": "aleph_credit_expense",
        "startDate": int(start_time),
        "endDate": int(end_time),
        "pagination": 500,
        "page": 1,
        "sort_order": "1",
        "sort_by": "tx-time",
    }
    if sender:
        params["addresses"] = sender
    async with aiohttp.ClientSession() as session:
        return await _fetch_paginated_messages(session, api_server, params)


def _parse_message(msg):
    """Return (height, expense_type, expense_dict) or (None, None, None)."""
    height = _extract_eth_height(msg)
    if height is None:
        return None, None, None
    content = msg.get("content", {}).get("content", {})
    tags = content.get("tags", [])
    expense = content.get("expense", {})
    if not expense:
        return None, None, None
    if "type_storage" in tags:
        return height, "storage", expense
    if "type_execution" in tags:
        return height, "execution", expense
    return None, None, None


def _project_expense(expense, src_key):
    """Synthesize an expense object with the chosen list aliased as credits[]."""
    return {
        "credit_price_aleph": expense.get("credit_price_aleph", 0),
        "credits":            expense.get(src_key, []),
    }


def _zero_totals():
    return {"storage_total_aleph": 0,
            "execution_total_aleph": 0,
            "dev_fund_total_aleph": 0}


async def compute_rewards(
    start_time, end_time, full_resync=False,
    include_holder_tier=False, sender=None,
    dbs=None, end_height=None, web3=None,
):
    """Pure function: produce credit-revenue and optional holder-tier rewards.

    Returns:
        {"credit_revenue": (rewards_dict, totals_dict),
         "holder_tier":    (rewards_dict, totals_dict)}
    """
    api_server = PublishMode.get_publish_api_server()
    sender = sender or settings.credit_expense_sender

    msgs = await _fetch_expense_messages(api_server, start_time, end_time, sender)
    if not msgs:
        LOGGER.warning("No credit expenses found in the given time range")
        return {
            "credit_revenue": ({}, _zero_totals()),
            "holder_tier":    ({}, _zero_totals()),
        }

    if web3 is None:
        try:
            web3 = get_web3()
        except Exception:
            web3 = None

    if full_resync:
        return await _compute_rewards_full_resync(
            dbs, end_height, msgs, include_holder_tier, web3
        )
    return await _compute_rewards_snapshots(
        api_server, start_time, end_time, msgs, include_holder_tier, web3
    )


async def _compute_rewards_snapshots(
    api_server, start_time, end_time, msgs, include_holder_tier, web3
):
    snapshots = await fetch_node_snapshots(api_server, start_time, end_time)
    if not snapshots:
        raise ValueError(
            "No node status snapshots found. "
            "Use --full-resync or ensure nodestatus is running."
        )
    snapshot_heights = [s[0] for s in snapshots]

    credit_rewards, credit_totals = {}, _zero_totals()
    holder_rewards, holder_totals = {}, _zero_totals()

    for msg in msgs:
        height, exp_type, expense = _parse_message(msg)
        if expense is None:
            continue

        idx = max(0, bisect.bisect_right(snapshot_heights, height) - 1)
        _, nodes, resource_nodes = snapshots[idx]

        _apply_expense_to(
            credit_rewards, credit_totals, exp_type,
            _project_expense(expense, "credits"),
            nodes, resource_nodes, web3,
        )

        if include_holder_tier and expense.get("rewards"):
            _validate_rewards_aggregates(expense)
            _apply_expense_to(
                holder_rewards, holder_totals, exp_type,
                _project_expense(expense, "rewards"),
                nodes, resource_nodes, web3,
            )

    return {
        "credit_revenue": (credit_rewards, credit_totals),
        "holder_tier":    (holder_rewards, holder_totals),
    }


def _apply_expense_to(rewards, totals, exp_type, expense,
                      nodes, resource_nodes, web3):
    s, e, d = _distribute_expense(
        exp_type, expense, nodes, resource_nodes, rewards,
        web3=web3, **_shares_for(exp_type),
    )
    totals["storage_total_aleph"]   += s
    totals["execution_total_aleph"] += e
    totals["dev_fund_total_aleph"]  += d


def _validate_rewards_aggregates(expense):
    declared_count = expense.get("rewards_count")
    declared_amount = expense.get("rewards_amount")
    rewards = expense.get("rewards", [])
    if declared_count is not None and declared_count != len(rewards):
        LOGGER.warning(
            f"rewards_count mismatch: declared={declared_count}, "
            f"actual={len(rewards)}"
        )
    if declared_amount is not None:
        actual = sum(r.get("amount", 0) for r in rewards)
        if abs(actual - declared_amount) > 1:
            LOGGER.warning(
                f"rewards_amount mismatch: declared={declared_amount}, "
                f"actual={actual}"
            )


async def _compute_rewards_full_resync(
    dbs, end_height, msgs, include_holder_tier, web3
):
    """Identical math as snapshot mode but driven by the state machine."""
    # Parse messages once
    parsed = []
    for msg in msgs:
        height, exp_type, expense = _parse_message(msg)
        if expense is not None:
            parsed.append((height, exp_type, expense))
    parsed.sort(key=lambda x: x[0])

    state_machine = NodesStatus()
    last_seen_txs = deque([], maxlen=100)
    iterators = [
        prepare_items("balance-update",
            process_contract_history(
                settings.ethereum_token_contract,
                settings.ethereum_min_height,
                last_seen=last_seen_txs, db=dbs["erc20"], fetch_from_db=True)),
        prepare_items("balance-update",
            process_balances_history(
                settings.ethereum_min_height,
                request_count=500, db=dbs["balances"])),
        prepare_items("staking-update",
            process_message_history(
                [settings.filter_tag],
                [settings.node_post_type, "amend"],
                settings.aleph_api_server,
                yield_unconfirmed=False, request_count=5000,
                db=dbs["messages"])),
        prepare_items("score-update",
            process_message_history(
                [settings.filter_tag],
                [settings.scores_post_type],
                message_type="POST",
                addresses=settings.scores_senders,
                api_server=settings.aleph_api_server,
                request_count=1000, db=dbs["scores"])),
    ]

    credit_rewards, credit_totals = {}, _zero_totals()
    holder_rewards, holder_totals = {}, _zero_totals()
    idx = 0
    nodes = resource_nodes = None

    async for height, nodes, resource_nodes in state_machine.process(iterators):
        if end_height and height > end_height:
            break
        while idx < len(parsed) and parsed[idx][0] <= height:
            _, exp_type, expense = parsed[idx]
            _apply_expense_to(credit_rewards, credit_totals, exp_type,
                              _project_expense(expense, "credits"),
                              nodes, resource_nodes, web3)
            if include_holder_tier and expense.get("rewards"):
                _validate_rewards_aggregates(expense)
                _apply_expense_to(holder_rewards, holder_totals, exp_type,
                                  _project_expense(expense, "rewards"),
                                  nodes, resource_nodes, web3)
            idx += 1

    # Drain remaining expenses against final snapshot
    while idx < len(parsed) and nodes is not None:
        h, exp_type, expense = parsed[idx]
        if end_height and h > end_height:
            idx += 1; continue
        _apply_expense_to(credit_rewards, credit_totals, exp_type,
                          _project_expense(expense, "credits"),
                          nodes, resource_nodes, web3)
        if include_holder_tier and expense.get("rewards"):
            _validate_rewards_aggregates(expense)
            _apply_expense_to(holder_rewards, holder_totals, exp_type,
                              _project_expense(expense, "rewards"),
                              nodes, resource_nodes, web3)
        idx += 1

    if nodes is None:
        raise ValueError("No node state available")

    return {
        "credit_revenue": (credit_rewards, credit_totals),
        "holder_tier":    (holder_rewards, holder_totals),
    }
```

Also keep `prepare_credit_distribution()` as a thin wrapper that calls `compute_rewards` for backward compatibility (so existing callers don't break):

```python
async def prepare_credit_distribution(
    dbs, end_height, start_time, end_time, full_resync=False
):
    result = await compute_rewards(
        start_time, end_time, full_resync=full_resync,
        include_holder_tier=False, dbs=dbs, end_height=end_height,
    )
    rewards, totals = result["credit_revenue"]
    return (
        rewards,
        totals["storage_total_aleph"],
        totals["execution_total_aleph"],
        totals["dev_fund_total_aleph"],
    )
```

- [ ] **Step 4: Run tests**

```bash
pytest tests/test_credit_distribution_refactor.py -v
```

Expected: pass.

- [ ] **Step 5: Run full suite**

```bash
pytest -q
```

Expected: no regressions.

- [ ] **Step 6: Commit**

```bash
git add src/aleph_nodestatus/credit_distribution.py tests/test_credit_distribution_refactor.py && \
git commit -m "feat(credits): compute_rewards pure function with two-stream output"
```

---

## Task 11: Test the holder-tier branch end-to-end

**Files:**
- Modify: `tests/test_credit_distribution_refactor.py`

- [ ] **Step 1: Write failing test**

Append to `tests/test_credit_distribution_refactor.py`:

```python
def test_compute_rewards_holder_tier_processes_rewards_field(monkeypatch):
    expense = {
        "credit_price_aleph": 0.001,
        "credits": [{"amount": 1000, "node_id": "r1", "address": "0xU1"}],
        "rewards": [{"amount":  500, "node_id": "r1", "address": "0xH1"}],
        "rewards_amount": 500,
        "rewards_count": 1,
    }
    msg = {
        "item_hash": "h1",
        "confirmations": [{"chain": "ETH", "height": 100}],
        "content": {"content": {
            "tags": ["credit_expense", "type_execution"],
            "expense": expense,
        }},
    }

    async def fake_fetch_msgs(*a, **kw): return [msg]
    async def fake_fetch_snaps(*a, **kw):
        return [(50, {"n1": _node("n1", 0.9, {"0xS1": 100},
                                   resource_nodes=["r1"])},
                     {"r1": _rnode("r1", 0.9, "0xCRN1")})]

    monkeypatch.setattr(
        "aleph_nodestatus.credit_distribution._fetch_expense_messages",
        fake_fetch_msgs,
    )
    monkeypatch.setattr(
        "aleph_nodestatus.credit_distribution.fetch_node_snapshots",
        fake_fetch_snaps,
    )

    result = asyncio.run(compute_rewards(
        start_time=1.0, end_time=2.0,
        include_holder_tier=True,
    ))

    credit, credit_totals = result["credit_revenue"]
    holder, holder_totals = result["holder_tier"]

    # credits[]: 1000 * 0.001 = 1.0 ALEPH revenue
    assert credit_totals["execution_total_aleph"] == pytest.approx(1.0)
    # rewards[]: 500 * 0.001 = 0.5 ALEPH revenue
    assert holder_totals["execution_total_aleph"] == pytest.approx(0.5)
    # CRN gets 60% of each stream
    assert credit["0xCRN1"] == pytest.approx(0.60)
    assert holder["0xCRN1"] == pytest.approx(0.30)


def test_compute_rewards_holder_tier_off_ignores_rewards_field(monkeypatch):
    """Same fixture as above but include_holder_tier=False -> holder dict empty."""
    expense = {
        "credit_price_aleph": 0.001,
        "credits": [{"amount": 1000, "node_id": "r1", "address": "0xU1"}],
        "rewards": [{"amount": 500,  "node_id": "r1", "address": "0xH1"}],
    }
    msg = {
        "item_hash": "h1",
        "confirmations": [{"chain": "ETH", "height": 100}],
        "content": {"content": {
            "tags": ["credit_expense", "type_execution"],
            "expense": expense,
        }},
    }
    async def fake_fetch_msgs(*a, **kw): return [msg]
    async def fake_fetch_snaps(*a, **kw):
        return [(50, {"n1": _node("n1", 0.9, {"0xS1": 100},
                                   resource_nodes=["r1"])},
                     {"r1": _rnode("r1", 0.9, "0xCRN1")})]
    monkeypatch.setattr(
        "aleph_nodestatus.credit_distribution._fetch_expense_messages",
        fake_fetch_msgs,
    )
    monkeypatch.setattr(
        "aleph_nodestatus.credit_distribution.fetch_node_snapshots",
        fake_fetch_snaps,
    )
    result = asyncio.run(compute_rewards(
        start_time=1.0, end_time=2.0,
        include_holder_tier=False,
    ))
    assert result["holder_tier"][0] == {}
    assert result["holder_tier"][1]["execution_total_aleph"] == 0
```

- [ ] **Step 2: Run tests**

```bash
pytest tests/test_credit_distribution_refactor.py -v
```

Expected: pass (already implemented in Task 10).

- [ ] **Step 3: Commit (test-only commit, documents the behavior)**

```bash
git add tests/test_credit_distribution_refactor.py && \
git commit -m "test(credits): holder-tier path consumes rewards[] correctly"
```

---

## Task 12: `rewards_merge.py` — merge + dust filter

**Files:**
- Create: `src/aleph_nodestatus/rewards_merge.py`
- Create: `tests/test_rewards_merge.py`

- [ ] **Step 1: Write failing tests**

Create `tests/test_rewards_merge.py`:

```python
import pytest

from aleph_nodestatus.rewards_merge import merge_rewards


def test_merge_rewards_sums_addresses_across_sources():
    sources = {
        "credit_revenue": {"0xA": 1.0, "0xB": 2.0},
        "holder_tier":    {"0xA": 0.5},
        "wage_subsidy":   {"0xB": 3.0, "0xC": 4.0},
    }
    final, by_source = merge_rewards(sources, dust_threshold=0)
    assert final == {"0xA": 1.5, "0xB": 5.0, "0xC": 4.0}
    assert by_source == sources


def test_merge_rewards_filters_dust():
    sources = {"credit_revenue": {"0xA": 0.005, "0xB": 0.05}}
    final, _ = merge_rewards(sources, dust_threshold=0.01)
    assert final == {"0xB": pytest.approx(0.05)}


def test_merge_rewards_collapses_case_via_lower():
    sources = {
        "credit_revenue": {"0xAbCdEf": 1.0},
        "wage_subsidy":   {"0xabcdef": 2.0},
    }
    final, _ = merge_rewards(sources, dust_threshold=0)
    assert len(final) == 1
    assert next(iter(final.values())) == pytest.approx(3.0)
```

- [ ] **Step 2: Run to verify failure**

```bash
pytest tests/test_rewards_merge.py -v
```

Expected: FAIL — module missing.

- [ ] **Step 3: Implement**

Create `src/aleph_nodestatus/rewards_merge.py`:

```python
"""Merge per-source reward dicts and filter dust."""

from collections import defaultdict
from decimal import Decimal
from typing import Dict, Tuple


def merge_rewards(
    sources: Dict[str, Dict[str, float]],
    dust_threshold: float = 0.01,
) -> Tuple[Dict[str, float], Dict[str, Dict[str, float]]]:
    """Merge address->amount dicts across sources.

    Args:
        sources: {source_name: {address: amount}}
        dust_threshold: addresses with total < this are dropped.

    Returns:
        (final_rewards, sources)
        final_rewards: {address (lowercased): total_amount}
        sources: the input dict, addresses lowercased (caller may store for audit).
    """
    normalized_sources = {
        name: {addr.lower(): float(amt) for addr, amt in src.items()}
        for name, src in sources.items()
    }

    total: Dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
    for src in normalized_sources.values():
        for addr, amt in src.items():
            total[addr] += Decimal(str(amt))

    threshold = Decimal(str(dust_threshold))
    final = {a: float(v) for a, v in total.items() if v >= threshold}
    return final, normalized_sources
```

- [ ] **Step 4: Run tests**

```bash
pytest tests/test_rewards_merge.py -v
```

Expected: pass.

- [ ] **Step 5: Commit**

```bash
git add src/aleph_nodestatus/rewards_merge.py tests/test_rewards_merge.py && \
git commit -m "feat(rewards): merge_rewards helper with dust filter"
```

---

## Task 13: `payment_processor.py` — quoter helpers

**Files:**
- Create: `src/aleph_nodestatus/payment_processor.py`
- Create: `tests/test_payment_processor.py`

- [ ] **Step 1: Write failing tests for the quote helper**

Create `tests/test_payment_processor.py`:

```python
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
    # We don't yet support v2 routing in code; verify it raises.
    quoter = MagicMock()
    swap_config = {"v": 2, "v2": ["0xA", "0xB"]}
    with pytest.raises(NotImplementedError, match="V2"):
        quote_amount_out(quoter, swap_config, amount_in=1_000_000)


def test_quote_amount_out_v4_raises():
    quoter = MagicMock()
    swap_config = {"v": 4, "v4": []}
    with pytest.raises(NotImplementedError, match="V4"):
        quote_amount_out(quoter, swap_config, amount_in=1_000_000)
```

- [ ] **Step 2: Run to verify failure**

```bash
pytest tests/test_payment_processor.py -v
```

Expected: FAIL — module missing.

- [ ] **Step 3: Implement quoter helpers**

Create `src/aleph_nodestatus/payment_processor.py`:

```python
"""On-chain interactions with the AlephPaymentProcessor and the V3 quoter."""

import json
import logging
import os
from functools import lru_cache
from pathlib import Path
from typing import Optional

from .settings import settings

LOGGER = logging.getLogger(__name__)


def apply_slippage(amount: int, slippage_bps: int) -> int:
    """Reduce `amount` by `slippage_bps` basis points (max 9999)."""
    if slippage_bps < 0 or slippage_bps >= 10_000:
        raise ValueError(
            f"slippage_bps must be in [0, 10000), got {slippage_bps}"
        )
    return amount * (10_000 - slippage_bps) // 10_000


def quote_amount_out(quoter, swap_config: dict, amount_in: int) -> int:
    """Return expected output for a swap, in wei of the destination token."""
    v = swap_config.get("v")
    if v == 3:
        path = swap_config["v3"]
        result = quoter.functions.quoteExactInput(path, amount_in).call()
        # QuoterV2 returns (amountOut, sqrtPriceX96AfterList, ...)
        return result[0] if isinstance(result, (list, tuple)) else result
    if v == 2:
        raise NotImplementedError("Quoter for V2 path not yet supported")
    if v == 4:
        raise NotImplementedError("Quoter for V4 path not yet supported")
    raise ValueError(f"Unknown swap version: {v}")


@lru_cache(maxsize=2)
def _load_abi(name: str):
    return json.load(
        open(os.path.join(Path(__file__).resolve().parent, "abi", f"{name}.json"))
    )


def get_processor_contract(w3):
    return w3.eth.contract(
        address=w3.to_checksum_address(settings.payment_processor_address),
        abi=_load_abi("AlephPaymentProcessor"),
    )


def get_quoter_contract(w3):
    return w3.eth.contract(
        address=w3.to_checksum_address(settings.uniswap_v3_quoter_address),
        abi=_load_abi("UniswapV3QuoterV2"),
    )
```

- [ ] **Step 4: Run tests**

```bash
pytest tests/test_payment_processor.py -v
```

Expected: pass.

- [ ] **Step 5: Commit**

```bash
git add src/aleph_nodestatus/payment_processor.py tests/test_payment_processor.py && \
git commit -m "feat(processor): quoter helpers and slippage math"
```

---

## Task 14: `payment_processor.simulate_process()` — eth_call simulation

**Files:**
- Modify: `src/aleph_nodestatus/payment_processor.py`
- Modify: `tests/test_payment_processor.py`

- [ ] **Step 1: Write failing test**

Append to `tests/test_payment_processor.py`:

```python
from aleph_nodestatus.payment_processor import simulate_process


def test_simulate_process_success_returns_no_error(monkeypatch):
    w3 = MagicMock()
    w3.eth.call.return_value = b""   # succeeds, no revert
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
```

- [ ] **Step 2: Run to verify failure**

```bash
pytest tests/test_payment_processor.py -v
```

Expected: FAIL.

- [ ] **Step 3: Implement**

Append to `src/aleph_nodestatus/payment_processor.py`:

```python
def simulate_process(
    w3, processor,
    from_address: str,
    token: str, amount_in: int, min_out: int, ttl: int,
    contract_logic_error_cls=None,
) -> Optional[str]:
    """eth_call the process() tx to detect revert. Returns None on success,
    error message string on revert.
    """
    if contract_logic_error_cls is None:
        try:
            from web3.exceptions import ContractLogicError
            contract_logic_error_cls = ContractLogicError
        except ImportError:
            contract_logic_error_cls = Exception

    data = processor.encodeABI(
        fn_name="process",
        args=[token, amount_in, min_out, ttl],
    )
    try:
        w3.eth.call({
            "from": from_address,
            "to": settings.payment_processor_address,
            "data": data,
        })
        return None
    except contract_logic_error_cls as e:
        return str(e)
    except Exception as e:
        return f"unexpected error during simulate: {e!r}"
```

- [ ] **Step 4: Run tests**

```bash
pytest tests/test_payment_processor.py -v
```

Expected: pass.

- [ ] **Step 5: Commit**

```bash
git add src/aleph_nodestatus/payment_processor.py tests/test_payment_processor.py && \
git commit -m "feat(processor): simulate_process via eth_call"
```

---

## Task 15: `payment_processor.execute_process()` — real tx

**Files:**
- Modify: `src/aleph_nodestatus/payment_processor.py`
- Modify: `tests/test_payment_processor.py`

- [ ] **Step 1: Write failing test**

Append to `tests/test_payment_processor.py`:

```python
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
```

- [ ] **Step 2: Run to verify failure**

```bash
pytest tests/test_payment_processor.py::test_execute_process_signs_and_sends -v
```

Expected: FAIL.

- [ ] **Step 3: Implement**

Append to `src/aleph_nodestatus/payment_processor.py`:

```python
def execute_process(
    w3, processor, account,
    token: str, amount_in: int, min_out: int, ttl: int,
    receipt_timeout: int = 300,
) -> dict:
    """Sign and broadcast the process() tx, wait for receipt."""
    nonce = w3.eth.get_transaction_count(account.address)
    latest = w3.eth.get_block("latest")
    base_fee = latest.baseFeePerGas
    max_priority = w3.to_wei(1, "gwei")
    max_fee = 5 * base_fee + max_priority

    tx = processor.functions.process(token, amount_in, min_out, ttl)
    tx = tx.build_transaction({
        "chainId": settings.ethereum_chain_id,
        "gas": 500_000,    # generous, contract is well-known
        "nonce": nonce,
        "maxFeePerGas": max_fee,
        "maxPriorityFeePerGas": max_priority,
    })
    signed = account.sign_transaction(tx)
    tx_hash = w3.eth.send_raw_transaction(signed.rawTransaction).hex()
    LOGGER.info(f"process() tx broadcast: {tx_hash}")
    receipt = w3.eth.wait_for_transaction_receipt(
        tx_hash, timeout=receipt_timeout
    )
    return {
        "tx_hash": tx_hash,
        "status":  int(receipt["status"]),
    }
```

- [ ] **Step 4: Run tests**

```bash
pytest tests/test_payment_processor.py -v
```

Expected: pass.

- [ ] **Step 5: Commit**

```bash
git add src/aleph_nodestatus/payment_processor.py tests/test_payment_processor.py && \
git commit -m "feat(processor): execute_process broadcasts signed tx"
```

---

## Task 16: `payment_processor.extract_aleph()` — orchestrator

**Files:**
- Modify: `src/aleph_nodestatus/payment_processor.py`
- Modify: `tests/test_payment_processor.py`

- [ ] **Step 1: Write failing test**

Append to `tests/test_payment_processor.py`:

```python
from aleph_nodestatus.payment_processor import extract_aleph


def _mk_swap_config(v, t="0x000000000000000000000000000000000000abCd"):
    return {"v": v, "t": t, "v3": b"\xde\xad", "v2": [], "v4": []}


def test_extract_aleph_dry_run_does_not_broadcast(monkeypatch):
    w3 = MagicMock()
    processor = MagicMock()
    quoter = MagicMock()

    # Make every token return balance>0 and v3 swap config
    processor.functions.getSwapConfig.return_value.call.return_value = _mk_swap_config(3)
    quoter.functions.quoteExactInput.return_value.call.return_value = (
        10_000, [0], [0], 0
    )

    # Token balance call: balanceOf returns 1_000_000 for any token
    erc20_mock = MagicMock()
    erc20_mock.functions.balanceOf.return_value.call.return_value = 1_000_000
    monkeypatch.setattr(
        "aleph_nodestatus.payment_processor._erc20_contract",
        lambda w3, addr: erc20_mock,
    )
    # ALEPH balance via balanceOf same mock; OK

    # simulate_process succeeds
    monkeypatch.setattr(
        "aleph_nodestatus.payment_processor.simulate_process",
        lambda *a, **kw: None,
    )

    # execute_process should NOT be called in dry_run mode
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
    assert len(result["tokens"]) == 3   # USDC, ETH, ALEPH
    for entry in result["tokens"]:
        assert entry["simulated_only"] is True


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
```

- [ ] **Step 2: Run to verify failure**

```bash
pytest tests/test_payment_processor.py -v
```

Expected: FAIL.

- [ ] **Step 3: Implement**

Append to `src/aleph_nodestatus/payment_processor.py`:

```python
ETH_SENTINEL = "0x0000000000000000000000000000000000000000"


def _erc20_contract(w3, address):
    minimal_abi = [{
        "constant": True, "inputs": [{"name": "owner", "type": "address"}],
        "name": "balanceOf",
        "outputs": [{"name": "balance", "type": "uint256"}],
        "type": "function", "stateMutability": "view",
    }]
    return w3.eth.contract(
        address=w3.to_checksum_address(address),
        abi=minimal_abi,
    )


def _balance_of(w3, contract_address, token_address):
    if token_address == ETH_SENTINEL or token_address.lower() == \
            ETH_SENTINEL.lower():
        return w3.eth.get_balance(w3.to_checksum_address(contract_address))
    return _erc20_contract(w3, token_address).functions.balanceOf(
        w3.to_checksum_address(contract_address)
    ).call()


def extract_aleph(
    w3, processor, quoter, account,
    from_address: str,
    dry_run: bool = False,
    transfer_enabled: bool = True,
    aleph_address: Optional[str] = None,
) -> dict:
    """Run process() per token in settings.process_tokens. Returns the
    extract block for the audit post.
    """
    aleph_address = aleph_address or _aleph_token_address()
    out = {"tokens": [], "errors": []}

    for symbol, token in settings.process_tokens:
        token_lc = token.lower()
        contract_address = settings.payment_processor_address
        balance = _balance_of(w3, contract_address, token)
        entry = {
            "symbol": symbol, "token": token,
            "amount_in": str(balance), "skipped_reason": None,
            "min_out": None, "expected_out": None,
            "tx_hash": None, "simulated_only": False, "error": None,
        }
        if balance == 0:
            entry["skipped_reason"] = "zero_balance"
            out["tokens"].append(entry); continue

        if token_lc == aleph_address.lower():
            min_out = 0
            expected_out = balance
        else:
            try:
                swap_config = processor.functions.getSwapConfig(
                    w3.to_checksum_address(token)
                ).call()
                cfg = _swap_config_to_dict(swap_config)
                expected_out = quote_amount_out(quoter, cfg, balance)
                min_out = apply_slippage(
                    expected_out, settings.process_slippage_bps
                )
            except Exception as e:
                entry["error"] = f"quote_failed: {e!r}"
                out["errors"].append(entry); out["tokens"].append(entry)
                continue
            entry["expected_out"] = str(expected_out)
            entry["min_out"] = str(min_out)

        # Always simulate
        err = simulate_process(
            w3, processor,
            from_address=from_address,
            token=token, amount_in=balance, min_out=min_out,
            ttl=settings.process_ttl_seconds,
        )
        if err:
            entry["error"] = f"simulation_revert: {err}"
            out["errors"].append(entry); out["tokens"].append(entry)
            continue

        if dry_run or not transfer_enabled:
            entry["simulated_only"] = True
            out["tokens"].append(entry); continue

        try:
            tx_info = execute_process(
                w3, processor, account,
                token=token, amount_in=balance, min_out=min_out,
                ttl=settings.process_ttl_seconds,
            )
            entry["tx_hash"] = tx_info["tx_hash"]
            if tx_info["status"] == 0:
                entry["error"] = "tx_reverted_on_chain"
                out["errors"].append(entry)
        except Exception as e:
            entry["error"] = f"tx_failed: {e!r}"
            out["errors"].append(entry)

        out["tokens"].append(entry)

    return out


def _swap_config_to_dict(swap_config_tuple):
    """Map ABI-decoded tuple to a dict the quoter helper understands."""
    v, t, v2, v3, v4 = swap_config_tuple
    return {"v": v, "t": t, "v2": list(v2), "v3": bytes(v3), "v4": list(v4)}


def _aleph_token_address():
    for sym, addr in settings.process_tokens:
        if sym == "ALEPH":
            return addr
    return settings.ethereum_token_contract
```

- [ ] **Step 4: Run tests**

```bash
pytest tests/test_payment_processor.py -v
```

Expected: pass.

- [ ] **Step 5: Commit**

```bash
git add src/aleph_nodestatus/payment_processor.py tests/test_payment_processor.py && \
git commit -m "feat(processor): extract_aleph orchestrator with dry-run support"
```

---

## Task 17: Cadence guard helper

**Files:**
- Modify: `src/aleph_nodestatus/credit_distribution.py` (add `should_skip_run`)
- Create: `tests/test_cadence_guard.py`

- [ ] **Step 1: Write failing test**

Create `tests/test_cadence_guard.py`:

```python
from aleph_nodestatus.credit_distribution import should_skip_run


def test_should_skip_when_within_interval():
    assert should_skip_run(last_end=1000.0, now=1500.0,
                           min_interval=1000.0, force=False) is True


def test_should_not_skip_when_past_interval():
    assert should_skip_run(last_end=1000.0, now=3000.0,
                           min_interval=1000.0, force=False) is False


def test_force_overrides_skip():
    assert should_skip_run(last_end=1000.0, now=1500.0,
                           min_interval=1000.0, force=True) is False


def test_no_last_end_does_not_skip():
    assert should_skip_run(last_end=None, now=1500.0,
                           min_interval=1000.0, force=False) is False
```

- [ ] **Step 2: Run to verify failure**

```bash
pytest tests/test_cadence_guard.py -v
```

Expected: FAIL.

- [ ] **Step 3: Implement**

Append to `src/aleph_nodestatus/credit_distribution.py`:

```python
def should_skip_run(last_end, now, min_interval, force=False):
    """Return True if we should skip the run because the min interval
    hasn't elapsed since the last successful distribution."""
    if force:
        return False
    if not last_end:
        return False
    return (now - last_end) < min_interval
```

- [ ] **Step 4: Run tests**

```bash
pytest tests/test_cadence_guard.py -v
```

Expected: pass.

- [ ] **Step 5: Commit**

```bash
git add src/aleph_nodestatus/credit_distribution.py tests/test_cadence_guard.py && \
git commit -m "feat(credits): cadence guard helper"
```

---

## Task 18: Extend `commands.distribute_credits` CLI

**Files:**
- Modify: `src/aleph_nodestatus/commands.py:366-428`
- Create: `tests/test_distribute_credits_cli.py`

- [ ] **Step 1: Write failing test**

Create `tests/test_distribute_credits_cli.py`:

```python
from click.testing import CliRunner

from aleph_nodestatus.commands import distribute_credits


def test_cli_act_and_dry_run_mutually_exclusive():
    runner = CliRunner()
    result = runner.invoke(distribute_credits, ["--act", "--dry-run"])
    assert result.exit_code != 0
    assert "mutually exclusive" in result.output.lower() \
        or "cannot use" in result.output.lower()


def test_cli_act_and_testnet_mutually_exclusive():
    runner = CliRunner()
    result = runner.invoke(distribute_credits, ["--act", "--testnet"])
    assert result.exit_code != 0


def test_cli_help_lists_new_flags():
    runner = CliRunner()
    result = runner.invoke(distribute_credits, ["--help"])
    assert "--dry-run" in result.output
    assert "--no-extract" in result.output
    assert "--no-wage" in result.output
    assert "--enable-holder-tier" in result.output
    assert "--no-transfer" in result.output
    assert "--no-publish" in result.output
    assert "--force" in result.output
```

- [ ] **Step 2: Run to verify failure**

```bash
pytest tests/test_distribute_credits_cli.py -v
```

Expected: FAIL (flags missing).

- [ ] **Step 3: Update CLI**

In `src/aleph_nodestatus/commands.py`, replace the `@click.command()` decorated `distribute_credits` function (lines 366-428) with:

```python
@click.command()
@click.option("-v", "--verbose", count=True)
@click.option("-a", "--act", help="Execute transfers + post status=distribution",
              is_flag=True)
@click.option("-t", "--testnet",
              help="Publish to testnet API; no transfers",
              is_flag=True)
@click.option("--dry-run",
              help="No post, no transfers; simulate process() via eth_call",
              is_flag=True)
@click.option("--force", help="Bypass the cadence guard", is_flag=True)
@click.option("--start-time", "start_time", default=None, type=float,
              help="Unix seconds, default: resume from last distribution")
@click.option("--end-time", "end_time", default=None, type=float,
              help="Unix seconds, default: now")
@click.option("--full-resync", is_flag=True,
              help="Replay full state machine from genesis")
@click.option("--no-extract", "no_extract", is_flag=True,
              help="Skip process() calls")
@click.option("--no-credit-revenue", "no_credit_revenue", is_flag=True,
              help="Skip credit-expense reward computation")
@click.option("--no-wage", "no_wage", is_flag=True,
              help="Skip wage subsidy")
@click.option("--enable-holder-tier", "enable_holder_tier", is_flag=True,
              help="Process expense.rewards[] (deprecated branch)")
@click.option("--no-holder-tier", "no_holder_tier", is_flag=True,
              help="Force holder-tier OFF (overrides env=True)")
@click.option("--no-transfer", "no_transfer", is_flag=True,
              help="Compute everything but don't broadcast txs")
@click.option("--no-publish", "no_publish", is_flag=True,
              help="Don't post the Aleph distribution record")
@click.option("--slippage-bps", default=None, type=int,
              help="Override per-run slippage tolerance")
@click.option("--reward-sender", default=None,
              help="Address used to look up the previous distribution")
def distribute_credits(verbose, act, testnet, dry_run, force,
                       start_time, end_time, full_resync,
                       no_extract, no_credit_revenue, no_wage,
                       enable_holder_tier, no_holder_tier,
                       no_transfer, no_publish,
                       slippage_bps, reward_sender):
    """Credit-based distribution script (new tokenomics)."""
    setup_logging(verbose)

    if act and testnet:
        click.echo("ERROR: --act and --testnet are mutually exclusive")
        sys.exit(2)
    if act and dry_run:
        click.echo("ERROR: --act and --dry-run are mutually exclusive")
        sys.exit(2)
    if enable_holder_tier and no_holder_tier:
        click.echo("ERROR: --enable-holder-tier and --no-holder-tier are "
                   "mutually exclusive")
        sys.exit(2)

    if testnet:
        PublishMode.set_testnet(True)

    flags = _resolve_feature_flags(
        no_extract=no_extract, no_credit_revenue=no_credit_revenue,
        no_wage=no_wage, enable_holder_tier=enable_holder_tier,
        no_holder_tier=no_holder_tier,
        no_transfer=no_transfer, no_publish=no_publish,
        act=act, dry_run=dry_run,
    )

    if slippage_bps is not None:
        settings.process_slippage_bps = slippage_bps

    mode = (
        "DRY-RUN" if dry_run else
        "TESTNET (calculation)" if testnet else
        "LIVE DISTRIBUTION" if act else
        "CALCULATION ONLY"
    )
    click.echo(f"Mode: {mode}")
    click.echo(f"Flags: {flags}")

    asyncio.run(process_credit_distribution(
        start_time=start_time, end_time=end_time,
        act=act, dry_run=dry_run, force=force,
        flags=flags, reward_sender=reward_sender,
        full_resync=full_resync,
    ))


def _resolve_feature_flags(*, no_extract, no_credit_revenue, no_wage,
                           enable_holder_tier, no_holder_tier,
                           no_transfer, no_publish, act, dry_run):
    f = {
        "extract":        settings.credit_dist_extract_enabled,
        "credit_revenue": settings.credit_dist_credit_revenue_enabled,
        "wage":           settings.credit_dist_wage_subsidy_enabled,
        "holder_tier":    settings.credit_dist_holder_tier_enabled,
        "transfer":       settings.credit_dist_transfer_enabled,
        "publish":        settings.credit_dist_publish_enabled,
    }
    if no_extract:        f["extract"] = False
    if no_credit_revenue: f["credit_revenue"] = False
    if no_wage:           f["wage"] = False
    if enable_holder_tier: f["holder_tier"] = True
    if no_holder_tier:    f["holder_tier"] = False
    if no_transfer:       f["transfer"] = False
    if no_publish:        f["publish"] = False
    if dry_run:
        f["transfer"] = False
        f["publish"]  = False
    if not act and not dry_run:
        # calculation-only: never transfer, but do publish
        f["transfer"] = False
    return f
```

- [ ] **Step 4: Run tests**

```bash
pytest tests/test_distribute_credits_cli.py -v
```

Expected: pass.

- [ ] **Step 5: Commit**

```bash
git add src/aleph_nodestatus/commands.py tests/test_distribute_credits_cli.py && \
git commit -m "feat(cli): per-step feature flags and dry-run for distribute-credits"
```

---

## Task 19: Wire orchestrator `process_credit_distribution`

**Files:**
- Modify: `src/aleph_nodestatus/commands.py`

- [ ] **Step 1: Replace `process_credit_distribution`**

In `src/aleph_nodestatus/commands.py`, replace the existing `process_credit_distribution` function (around lines 269-363) with:

```python
async def process_credit_distribution(
    start_time, end_time, *,
    act=False, dry_run=False, force=False,
    flags=None, reward_sender=None, full_resync=False,
):
    from .credit_distribution import (
        compute_rewards, should_skip_run,
    )
    from .payment_processor import (
        extract_aleph, get_processor_contract, get_quoter_contract,
    )
    from .rewards_merge import merge_rewards
    from .wage_subsidy import compute_subsidy
    from .ethereum import get_eth_account, get_web3
    from hexbytes import HexBytes
    from eth_account import Account
    import time as _time

    flags = flags or {}
    dbs = get_dbs()
    if end_time is None:
        end_time = _time.time()

    # === Cadence guard ===
    if start_time is None:
        last_end, _ = await get_latest_successful_credit_distribution(reward_sender)
        if last_end and dbs is not None:
            if should_skip_run(last_end, _time.time(),
                               settings.credit_dist_min_interval_seconds,
                               force):
                click.echo(
                    f"Cadence guard: only {(_time.time()-last_end):.0f}s "
                    f"since last run; skipping (use --force to override)."
                )
                return
            start_time = last_end + 1
            click.echo(f"Resuming from last_end={last_end}; start={start_time}")
        else:
            click.echo("ERROR: --start-time required for first run.")
            return

    # === Resolve web3 + admin signer ===
    web3 = get_web3()
    admin_address = "0xC870B0Ca4B3d65f33E2a3c732ab3cD2aE555b14E"
    admin_account = None
    if flags.get("extract") and (act and not dry_run):
        pk = settings.payment_processor_admin_pkey or settings.ethereum_pkey
        if not settings.payment_processor_admin_pkey:
            LOGGER.warning("payment_processor_admin_pkey not set; "
                           "falling back to ethereum_pkey")
        admin_account = Account.from_key(HexBytes(pk))
        admin_address = admin_account.address

    # === Step 1: extract ALEPH from the processor ===
    extract_block = {"tokens": [], "errors": []}
    if flags.get("extract"):
        processor = get_processor_contract(web3)
        quoter = get_quoter_contract(web3)
        extract_block = extract_aleph(
            web3, processor, quoter,
            account=admin_account,
            from_address=admin_address,
            dry_run=dry_run or not flags.get("transfer"),
            transfer_enabled=flags.get("transfer"),
        )

    # === Step 2 + holder-tier: credit_revenue + holder_tier rewards ===
    end_block = None
    if full_resync:
        end_block = web3.eth.block_number
        block = web3.eth.get_block(end_block)
        if block.timestamp > end_time:
            lo, hi = 0, end_block
            while lo < hi:
                mid = (lo + hi + 1) // 2
                if web3.eth.get_block(mid).timestamp <= end_time:
                    lo = mid
                else:
                    hi = mid - 1
            end_block = lo

    streams = {
        "credit_revenue": ({}, {"storage_total_aleph": 0,
                                "execution_total_aleph": 0,
                                "dev_fund_total_aleph": 0}),
        "holder_tier":    ({}, {"storage_total_aleph": 0,
                                "execution_total_aleph": 0,
                                "dev_fund_total_aleph": 0}),
    }
    if flags.get("credit_revenue"):
        streams = await compute_rewards(
            start_time, end_time,
            full_resync=full_resync,
            include_holder_tier=flags.get("holder_tier", False),
            sender=settings.credit_expense_sender,
            dbs=dbs, end_height=end_block, web3=web3,
        )

    credit_rewards, credit_totals = streams["credit_revenue"]
    holder_rewards, holder_totals = streams["holder_tier"]

    # === Step 3: wage subsidy ===
    wage_rewards, wage_totals = {}, {"period_total_aleph": 0,
                                     "unallocated_aleph": 0,
                                     "start_t_months": 0, "end_t_months": 0,
                                     "split": {"ccn": 0, "crn": 0, "stakers": 0}}
    if flags.get("wage"):
        # Reuse the most recent snapshot fetched by compute_rewards via cache,
        # but for the wage we need a fresh snapshot lookup. Simplest: fetch once.
        from .credit_distribution import fetch_node_snapshots
        api_server = PublishMode.get_publish_api_server()
        snapshots = await fetch_node_snapshots(api_server, start_time, end_time)
        if snapshots:
            _, nodes, resource_nodes = snapshots[-1]
            wage_rewards, wage_totals = compute_subsidy(
                start_time, end_time, nodes, resource_nodes, web3=web3,
            )
        else:
            LOGGER.warning("No snapshots for wage subsidy; pool unallocated.")
            wage_totals["unallocated_aleph"] = wage_totals.get(
                "period_total_aleph", 0
            )

    # === Step 4: merge + dust filter ===
    final_rewards, by_source = merge_rewards(
        {"credit_revenue": credit_rewards,
         "holder_tier":    holder_rewards,
         "wage_subsidy":   wage_rewards},
        dust_threshold=settings.credit_dist_dust_threshold_aleph,
    )

    # Holder-tier safety check
    if flags.get("holder_tier") and holder_rewards:
        # The contract's distribution recipient must have at least sum(rewards) ALEPH
        from .ethereum import get_token_contract
        token = get_token_contract(web3)
        bal = token.functions.balanceOf(
            web3.to_checksum_address(settings.distribution_recipient)
        ).call() / (10 ** settings.ethereum_decimals)
        owed = sum(final_rewards.values())
        if owed > bal + 1e-6:
            click.echo(f"ABORT: owed {owed} ALEPH > balance {bal} at "
                       f"{settings.distribution_recipient}")
            return

    # === Step 5: transfer (or simulate) ===
    is_testnet = PublishMode.is_testnet()
    if flags.get("transfer") and act and not is_testnet:
        click.echo("Executing batchTransfer …")
        max_items = settings.ethereum_batch_size
        items = list(final_rewards.items())
        for i in range(math.ceil(len(items) / max_items)):
            step = items[max_items*i:max_items*(i+1)]
            click.echo(f"Batch {i+1}: transferring to {len(step)} recipients")
            await transfer_tokens(dict(step), metadata={"sources": list(by_source.keys())})
    elif not flags.get("transfer"):
        click.echo("--no-transfer / dry-run: skipping batchTransfer")
        for i in range(math.ceil(len(final_rewards) / settings.ethereum_batch_size)):
            click.echo(f"Batch {i+1}: would transfer to "
                       f"{min(settings.ethereum_batch_size, len(final_rewards) - i*settings.ethereum_batch_size)} "
                       f"recipients")

    # === Step 6: publish ===
    status = "distribution" if act else "calculation"
    distribution = {
        "incentive": "credits",
        "status": status,
        "start_time": start_time, "end_time": end_time,
        "rewards": final_rewards,
        "rewards_by_source": by_source,
        "credit_revenue_totals": credit_totals,
        "holder_tier_totals": {**holder_totals, "included": flags.get("holder_tier", False)},
        "wage_subsidy": wage_totals,
        "extract": extract_block,
        "feature_flags": flags,
        "tags": ([status] if status else []) + ["credits", settings.filter_tag],
    }
    if end_block is not None:
        distribution["end_height"] = end_block

    if flags.get("publish") and not dry_run:
        await create_distribution_tx_post(
            distribution, post_type=CREDIT_DISTRIBUTION_POST_TYPE
        )
    else:
        click.echo("--no-publish / dry-run: skipping Aleph post")
        click.echo(json.dumps({k: v for k, v in distribution.items()
                                if k != "rewards_by_source"}, indent=2, default=str)[:4000])
```

Make sure `import json` is at the top of `commands.py` if it isn't already.

- [ ] **Step 2: Run the full suite for regressions**

```bash
pytest -q
```

Expected: tests pass; CLI tests still pass.

- [ ] **Step 3: Commit**

```bash
git add src/aleph_nodestatus/commands.py && \
git commit -m "feat(orchestrator): wire extract + rewards + wage + merge + publish"
```

---

## Task 20: Update `docker/docker-compose.yml`

**Files:**
- Modify: `docker/docker-compose.yml`

- [ ] **Step 1: Add a second cron line**

Edit `docker/docker-compose.yml` and replace the `command:` line with:

```yaml
    command: /bin/bash -c "service cron start && (echo \"*/10 * * * * cd /aleph-nodestatus && /usr/local/bin/nodestatus-distribute -v >> /logs/nodestatus.log\"; echo \"*/30 * * * * cd /aleph-nodestatus && /usr/local/bin/nodestatus-distribute-credits --act -v >> /logs/credits.log\") | crontab - && /bin/sh"
```

The cadence guard inside the script enforces the actual 10-day spacing.

- [ ] **Step 2: Sanity-check docker-compose syntax**

```bash
docker compose -f docker/docker-compose.yml config > /dev/null && echo "OK"
```

Expected: `OK` (no error).

- [ ] **Step 3: Commit**

```bash
git add docker/docker-compose.yml && \
git commit -m "ops: add cron for nodestatus-distribute-credits"
```

---

## Task 21: End-to-end dry-run integration test

**Files:**
- Create: `tests/test_credit_pipeline.py`
- Create: `tests/fixtures/expense_execution.json` (recorded fixture)
- Create: `tests/fixtures/snapshot.json`

- [ ] **Step 1: Capture fixtures from production API**

Run these once to save fixtures:

```bash
curl -s "https://api2.aleph.im/api/v0/messages/c1cfc9bf519de2bbe19749a3996bd6119f940fa91e9a30f317cce8939bb6b21a" \
  > tests/fixtures/expense_execution.json
```

Snapshot: pick any recent corechannel aggregate message and save with the same approach. (`grep` for `key: corechannel` in a recent batch.) Trim the snapshot down to ~3 nodes + ~2 resource nodes for the test to keep it deterministic — anonymize addresses to `0xCCN-test-*` / `0xCRN-test-*` style.

- [ ] **Step 2: Write the integration test**

Create `tests/test_credit_pipeline.py`:

```python
import json
from pathlib import Path
from unittest.mock import patch, AsyncMock

import pytest
from click.testing import CliRunner

from aleph_nodestatus.commands import distribute_credits


FIX = Path(__file__).parent / "fixtures"


@pytest.fixture
def fixtures(monkeypatch):
    """Patch fetchers to return recorded data."""
    expense_msg = json.loads((FIX / "expense_execution.json").read_text())
    snapshot   = json.loads((FIX / "snapshot.json").read_text())

    async def fake_fetch_msgs(*a, **kw): return [expense_msg["message"]]
    async def fake_fetch_snaps(*a, **kw):
        nodes = {n["hash"]: n for n in snapshot["nodes"]}
        rnodes = {r["hash"]: r for r in snapshot["resource_nodes"]}
        return [(snapshot["height"], nodes, rnodes)]

    monkeypatch.setattr(
        "aleph_nodestatus.credit_distribution._fetch_expense_messages",
        fake_fetch_msgs,
    )
    monkeypatch.setattr(
        "aleph_nodestatus.credit_distribution.fetch_node_snapshots",
        fake_fetch_snaps,
    )


def test_dry_run_does_not_post_or_transfer(fixtures, monkeypatch):
    posts = []
    async def fake_post(*a, **kw): posts.append(1)
    monkeypatch.setattr(
        "aleph_nodestatus.distribution.create_distribution_tx_post",
        fake_post,
    )

    transfers = []
    async def fake_transfer(*a, **kw): transfers.append(1)
    monkeypatch.setattr(
        "aleph_nodestatus.ethereum.transfer_tokens", fake_transfer,
    )

    runner = CliRunner()
    result = runner.invoke(distribute_credits, [
        "--dry-run", "--no-extract",
        "--start-time", "1778050000",
        "--end-time",   "1778100000",
    ])
    assert result.exit_code == 0
    assert posts == []
    assert transfers == []
    assert "DRY-RUN" in result.output
```

- [ ] **Step 3: Run the test**

```bash
pytest tests/test_credit_pipeline.py -v
```

Expected: pass.

- [ ] **Step 4: Commit**

```bash
git add tests/fixtures tests/test_credit_pipeline.py && \
git commit -m "test(credits): end-to-end dry-run fixture test"
```

---

## Task 22: Run the whole suite + manual smoke test on testnet

**Files:** none (verification)

- [ ] **Step 1: Run the entire test suite one more time**

```bash
pytest -q
```

Expected: green.

- [ ] **Step 2: Manual smoke — dry-run on real data**

```bash
nodestatus-distribute-credits --dry-run \
    --start-time 1778050000 --end-time 1778100000 -v
```

Expected output:
- `Mode: DRY-RUN`
- Per-token extract block printed (USDC/ETH/ALEPH balances, expected_out, min_out, simulated_only=True if non-zero balance)
- Per-source totals
- "Skipping batchTransfer", "skipping Aleph post"
- Exit 0

- [ ] **Step 3: Manual smoke — testnet calculation**

```bash
nodestatus-distribute-credits --testnet \
    --start-time 1778050000 --end-time 1778100000 -v
```

Expected:
- A `credit-rewards-distribution` post appears on the testnet API server with `status=calculation`, `feature_flags`, `wage_subsidy`, `extract`, and `rewards_by_source` populated.

- [ ] **Step 4: No commit**

---

## Task 23: Update CHANGELOG

**Files:**
- Modify: `CHANGELOG.rst`

- [ ] **Step 1: Add a release entry**

```rst
Version 0.2
===========

- New ``nodestatus-distribute-credits`` flow:

  - Calls ``AlephPaymentProcessor.process()`` per token (USDC, ETH, ALEPH)
    with off-chain slippage-protected ``min_out``.
  - Adds the 6-month linearly-decaying wage subsidy
    (900,000 → 0 ALEPH/month over 6 months, split 1/3 CCN / 1/3 CRN / 1/3 stakers).
  - Holder-tier ``rewards[]`` second pass behind a feature flag (default off).
  - ``--dry-run`` mode simulates ``process()`` via ``eth_call`` without
    broadcasting or posting.
  - Per-step feature flags (``--no-extract``, ``--no-wage``, ``--no-transfer``,
    ``--no-publish``, etc.).
  - Cadence guard (``credit_dist_min_interval_seconds``, default 10 days)
    with ``--force`` override.
```

- [ ] **Step 2: Commit**

```bash
git add CHANGELOG.rst && \
git commit -m "docs(changelog): credit-distribution v2 entry"
```

---

## Self-review

Spec coverage check (against `docs/specs/2026-05-07-credit-rewards-distribution-design.md`):

| Spec section | Plan task | Notes |
|---|---|---|
| §5.1 Payment-processor extraction | Tasks 3, 4, 13–16 | ABIs + quoter helpers + simulate + execute + orchestrator |
| §5.2 Credit reward computation refactor | Tasks 9–11 | Pure `compute_rewards()`, holder-tier branch, aggregate validation |
| §5.3 Wage subsidy | Tasks 5–8 | integral → period → split → orchestrator |
| §5.4 Merge + dust filter | Task 12 | `rewards_merge.merge_rewards()` |
| §5.5 Transfer (reuse) | Task 19 | calls existing `ethereum.transfer_tokens` |
| §5.6 Publish (extended format) | Task 19 | new fields in the post payload |
| §5.7 Cadence guard | Task 17 | `should_skip_run()` + Task 19 wiring |
| §6 Settings + CLI | Tasks 2, 18 | new env vars + new CLI flags |
| §7 Deployment | Task 20 | docker-compose cron line |
| §8 Error model | Tasks 13–16, 19 | partial failures recorded in extract.errors / targets |
| §9 Tests | Tasks 5–8, 9–11, 12, 13–16, 17, 18, 21 | full coverage |
| §10 Out of scope | n/a | not implemented (correctly) |

No placeholders found; every step has explicit code or commands. Function names are consistent across tasks. The aggregated import surface of `compute_rewards`, `compute_subsidy`, `merge_rewards`, `extract_aleph`, `should_skip_run` is what Task 19 imports.

Two known carry-overs from the spec §11 open items (apply during implementation, not in plan):
1. `wage_start_date` — replace `"2026-04-01T00:00:00+00:00"` with the actual launch date when known.
2. `process_tokens` — confirm complete; add any extra ERC-20 to the env-overridable list.
