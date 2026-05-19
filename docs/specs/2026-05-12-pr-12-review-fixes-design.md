# PR #12 Review-Response — Design Spec

**Date:** 2026-05-12
**Branch:** `feat/credit-distribution-v2`
**Source review:** `docs/reviews/pr-12-review.md`
**Scope decision:** All Critical + High + Medium + Low items, plus test gaps 1–4. Architectural items deferred.

## Goals

1. Close every blocking item the reviewer flagged (C1–C4, H4) so PR #12 can merge.
2. Land the cheap-and-obvious safety, hygiene, and test items in the same review-response round (rounds 2–5 precedent on this branch).
3. Leave the invasive items (Flashbots, Decimal-everywhere, orchestrator refactor) as explicit follow-ups with the trigger conditions written down.

## Non-goals

- Architectural restructure of the orchestrator-as-CLI pattern.
- Full Decimal conversion of the reward pipeline.
- Flashbots / private-mempool integration (deferred pending production observation).
- Moving plan/spec markdown out of the repo tree.

## High-level approach

Single PR, layered commits — one commit per concern, in the order critical → high → medium → low → tests → docs. Mirrors the rounds-2-through-5 cadence already in `git log`.

## Design by behavioral surface

### 1. Resume cursor + cadence guard (C1, C2)

**`credit_distribution.py` — `get_latest_successful_credit_distribution`.**
Replace the break-on-first-success scan with two distinct booleans, `any_success` and `all_success`. Return shape becomes `(end_height, content, all_success: bool)`. When a candidate post has `any_success and not all_success`, the function emits `LOGGER.warning("partial-failure distribution at height %d; refusing to advance cursor", end_height)` and treats that post as if no successful distribution exists (does **not** update `current_post`). Operators see the warning, investigate, and must supply an explicit `--start-height` to resume.

Rationale: the conservative version. The strictly-correct alternative (track per-batch coverage in `targets[]` and resume from the lowest-failed-batch boundary) is a bigger audit-post-schema change and belongs in a follow-up.

**`commands.py` — `process_credit_distribution`.**
Restructure the cadence/start-height block so `should_skip_run` runs **unconditionally when `act=True`**, regardless of how `start_height` was derived:

```python
last_end = None
if act:
    last_end, _, _ = await get_latest_successful_credit_distribution(reward_sender)
    if last_end and should_skip_run(
        last_end, end_height, settings.credit_dist_min_interval_blocks, force
    ):
        click.echo(
            f"Cadence guard: only {end_height - last_end} blocks since last "
            f"distribution; skipping (use --force to override)."
        )
        return
if start_height in (None, -1):
    if last_end is None:
        last_end, _, _ = await get_latest_successful_credit_distribution(reward_sender)
    if last_end:
        start_height = last_end + 1
        click.echo(f"Resuming from last_end={last_end}; start={start_height}")
    else:
        click.echo("ERROR: --start-height required for first run.")
        return
```

The single `last_end` lookup is reused for both the guard and the resume cursor so we only hit the Aleph posts API once.

### 2. Async-safe extract step (C4)

**`commands.py` — `process_credit_distribution`.**
The synchronous `extract_aleph` call at Step 1 is wrapped in `asyncio.to_thread`:

```python
extract_block = await asyncio.to_thread(
    extract_aleph, web3, processor, quoters,
    account=admin_account,
    from_address=admin_address,
    dry_run=dry_run or not flags.get("transfer"),
    transfer_enabled=flags.get("transfer"),
    slippage_bps=effective_slippage,
)
```

`payment_processor.py` stays synchronous (web3.py is sync-native; wrapping individual calls would be a leaky refactor). The threadpool worker absorbs the up-to-15-minute receipt wait; the event loop stays responsive for interleaved work (today none, tomorrow likely).

### 3. Operational topology (C3)

**`docker/docker-compose.yml` — two services.**

```yaml
services:
  nodestatus-calc:
    build: { context: .., dockerfile: docker/Dockerfile }
    env_file: [ .env.calc ]
    volumes: [ ../:/aleph-nodestatus, ../logs:/logs ]
    command: /bin/bash -c "service cron start && (
        echo '*/10 * * * * cd /aleph-nodestatus && /usr/local/bin/nodestatus-distribute -v >> /logs/nodestatus.log';
        echo '0 * * * * cd /aleph-nodestatus && /usr/local/bin/nodestatus-distribute-credits -v >> /logs/credits-calc.log'
      ) | crontab - && /bin/sh"

  nodestatus-dist:
    build: { context: .., dockerfile: docker/Dockerfile }
    env_file: [ .env.dist ]
    volumes: [ ../:/aleph-nodestatus, ../logs:/logs ]
    command: /bin/bash -c "service cron start && (
        echo '0 0 * * * cd /aleph-nodestatus && /usr/local/bin/nodestatus-distribute-credits --act -v >> /logs/credits-dist.log'
      ) | crontab - && /bin/sh"
```

The old single-service `nodestatus` block is **deleted**. Dev/staging without distribution = `docker compose up nodestatus-calc` only.

**New env templates (committed):**
- `docker/.env.calc.example` — sets `ethereum_pkey=<status_sender_pkey>`; header comment: "this key publishes calculation snapshots, never signs distributions."
- `docker/.env.dist.example` — sets `ethereum_pkey=<distribution_recipient_pkey>`; header comment: "this key signs the daily batchTransfer; must hold ≥ expected daily ALEPH outflow."

Each file is self-contained (no shared baseline) to be foolproof against copy-paste. The existing `docker/.env` continues to be uncommitted per project convention.

**`CHANGELOG.rst`.** Migration note: "v2 deployments require splitting into two compose services — see `docker/.env.{calc,dist}.example`."

### 4. Payment-processor robustness (H1, H2, M5, M6, L1, L2, L3)

**`settings.py`.**
- `process_slippage_bps`: default `200 → 100`.
- `process_ttl_seconds`: add a pydantic `@field_validator` rejecting values `> 3600`. The `# must be <= 3600` comment is removed.
- New `process_gas_ceiling: int = 1_500_000` for the H2 estimation clamp.
- `process_min_token_value_usd` is **removed** (M1, dead).

**`payment_processor.py`.**
- **H2.** `execute_process` replaces `"gas": 500_000` with:
  ```python
  try:
      estimated = tx.estimate_gas(...)
      gas = min(int(estimated * 1.25), settings.process_gas_ceiling)
  except Exception as e:
      LOGGER.warning("estimate_gas failed (%r); using ceiling %d", e, settings.process_gas_ceiling)
      gas = settings.process_gas_ceiling
  ```
- **M6.** The two bare `except Exception` blocks in `extract_aleph` (quote-failed at ~line 250, tx-failed at ~line 282) are replaced with `LOGGER.exception(...)` (full stack to logs) while still stuffing the compact `repr(e)` into the audit-post entry.
- **L1.** `result[0] if isinstance(result, (list, tuple)) else result` → `result[0]` (both V3 and V4 `quoteExactInput` return tuples per ABI).
- **L2.** `os.path.join(Path(__file__).resolve().parent, ...)` → `Path(__file__).parent / "abi" / f"{name}.json"`. `import os` is dropped.
- **L3.** `@lru_cache(maxsize=2)` → `@lru_cache(maxsize=None)`.

**New `docs/operations/slippage.md`.** One page covering:
- Per-run slippage drag at 100 bps on a representative USDC→ETH→ALEPH extract.
- Rationale for 100 bps (pool depth observation; tightening protects against sandwich while staying loose enough that legitimate extracts don't revert).
- Operator override path: `--slippage-bps N` per-run, or `process_slippage_bps` env.

### 5. Distribution safety net (H5)

**`settings.py`.** New setting:

```python
credit_dist_max_total_aleph: float = 2 * wage_initial_monthly_aleph  # ~1.8M
```

**`commands.py`.** New check immediately after `merge_rewards`, **before** the balance check:

```python
owed = sum(final_rewards.values())
if act and flags.get("transfer") and owed > settings.credit_dist_max_total_aleph and not force_cap:
    click.echo(
        f"ABORT: owed {owed:.2f} ALEPH > max_total cap "
        f"{settings.credit_dist_max_total_aleph}. "
        f"Inspect upstream inputs; rerun with --force-cap to override."
    )
    sys.exit(1)
```

**New CLI flag** `--force-cap` lets operators legitimately override the cap (e.g., long catch-up after an outage). `--force` keeps its existing meaning (cadence-only). The two flags are independent — an operator cannot bypass both with a single argument.

Cap sits before balance check so we abort on inflated rewards even when the sender is flush.

### 6. Code hygiene (M2, M3, M4, M7, M8, L4, L5, L6, L7) + PR-body fix (H4)

**`credit_distribution.py`.** Delete `prepare_credit_distribution` (M3). Confirmed dead: only reference is the unused import in `commands.py:41`; no test references.

**`commands.py`.**
- Remove the `prepare_credit_distribution` from the import block (M3).
- Replace the cookiecutter skeleton docstring at the top of the file with a one-line description of the module's responsibility (L4).
- Extend the `_resolve_feature_flags` last-wins comment (M8): "Order matters only for programmatic callers — the CLI rejects mutual exclusivity. `no_holder_tier` last wins because accidentally enabling holder_tier (over-distribution) is worse than accidentally disabling it (under-distribution, recoverable)."
- Comment near `get_latest_successful_credit_distribution` clarifying the dual-sender mental model (M2): "In the documented two-instance deployment, only `distribution_recipient` publishes `status='distribution'`; `status_sender` is queried defensively to handle single-instance / dev setups."
- Comment near Step 1 (M7) acknowledging the extract-before-balance-check trade-off and the rationale (extract *is* the top-up mechanism). No code change.
- Align the legacy `metadata=distribution` mutation pattern with the credit path's separate `transfer_metadata` dict (L5). Small risk — covered by an added regression test pinning the legacy targets-list shape.

**`wage_subsidy.py`.** Hoist `from .distribution import compute_score_multiplier` from line 51 to the top-of-file import block (M4). Confirmed safe: `distribution.py` does not import from `wage_subsidy`, so no circular dep exists.

**`rewards_merge.py`.** Trim `normalized_sources` to retain only addresses present in `final` so the published `rewards_by_source` sums match `final_rewards` exactly (L6). Docstring updated: "Dust addresses are excluded from `rewards_by_source` to keep audit-post invariants tight."

**`docker-compose.yml`.** L7 concern (unrelated cleanup bundled in tokenomics PR) is acknowledged in the commit message but the cleanup naturally fits inside C3's compose restructure — keep it.

**PR body on GitHub (H4).** Description edited to reflect that `credit_dist_holder_tier_enabled` defaults to `True`, and that `--no-holder-tier` is the meaningful flag (not `--enable-holder-tier`). No code change; tracked on the done-list.

### 7. Tests (gaps 1–4 + H3 bounding)

Five files, all using existing `pytest` + `pytest-asyncio` patterns; integration tests hit real fixtures (no DB mocking).

**`tests/test_resume_partial_failure.py`** (gap 1, C1).
- Mixed-success targets fixture → `(0, None, False)` and a `LOGGER.warning` (asserted via `caplog`).
- All-success fixture → `(end_height, content, True)`.
- All-failure fixture → `(0, None, False)`, no warning.

**`tests/test_cadence_guard.py`** (gap 2, C2) — extend existing.
- Explicit `--start-height` inside cadence window + `act=True` + `force=False` → aborts with cadence message.
- Same + `force=True` → proceeds.
- `act=False` (calculation-only) with explicit start-height → proceeds regardless.

**`tests/test_async_extract.py`** (gap 3, C4).
- Mock `extract_aleph` with a `time.sleep(2)`.
- Concurrent `asyncio.sleep(0.1)` on the same loop.
- Assert the concurrent sleep completes within ~0.2s (proving threadpool wrap is in place).

**`tests/test_min_out_bounds.py`** (gap 4, H1).
- `apply_slippage(x, 100) == x * 9900 // 10000` exactly.
- `apply_slippage(x, 0) == x`.
- `apply_slippage(x, 10000)` raises `ValueError`.
- Hypothesis property: `0 ≤ apply_slippage(x, bps) ≤ x` for all valid inputs.

**`tests/test_rewards_stability.py`** (H3 bounding).
- Hypothesis: small reward dicts with values near `dust_threshold` (0.005–0.02 range).
- For each input, shuffle source iteration order N times, run `merge_rewards`, assert `final_rewards` is identical across shuffles.
- If this ever fails, the H3 follow-up (Decimal upstream) is triggered.

Plus a small regression test for the legacy `transfer_tokens` metadata path (L5).

## Commits, in order


1. `fix(distribution): require all batches successful for resume cursor (C1)`
2. `fix(orchestrator): cadence guard runs unconditionally when --act (C2)`
3. `fix(orchestrator): wrap sync extract_aleph in asyncio.to_thread (C4)`
4. `ops: split docker-compose into calc/dist services (C3)`
5. `feat(processor): gas estimation + slippage 100bps + ttl validator + stack-trace logging + path/cache hygiene (H1, H2, M5, M6, L1, L2, L3)` — payment_processor.py, settings.py, docs/operations/slippage.md
6. `feat(distribution): global per-run cap (H5)`
7. `chore: drop dead code + hoist import + comments + audit-source trim + legacy metadata alignment (M1, M2, M3, M4, M7, M8, L4, L5, L6)`
8. `test: float-accumulation stability under iteration order (H3)`
9. `docs: update PR description for holder_tier default-on (H4)` — GitHub-only, no code commit; called out in done-list.

## Acceptance criteria

- `pytest tests/` green; existing tests unchanged in pass/fail status; five new test files pass.
- `nodestatus-distribute-credits --dry-run` over a recent block range produces a calculation post identical (modulo timestamps) to pre-fix output.
- `nodestatus-distribute-credits --start-height N --act` with N inside the cadence window aborts with the new guard message.
- `docker compose up nodestatus-calc` and `docker compose up nodestatus-dist` both start cleanly with their respective `.env.{calc,dist}` files.
- GitHub PR description reflects the holder-tier-default-on reality.

## Deferred, with trigger conditions

| Item | Trigger |
|---|---|
| H1 follow-up — Flashbots Protect / MEV-blocker integration | If 100 bps slippage proves still too generous in production (measurable per-run drag exceeds 0.5%, or observed sandwich activity). |
| H3 follow-up — Decimal upstream in `_distribute_expense` / `split_subsidy` | If `test_rewards_stability.py` ever fails, or operators observe a recipient flipping above/below the 0.01 ALEPH threshold non-deterministically. |
| M7 follow-up — pre-flight balance check before extract broadcast | If an extract-partial-failure leaves the system unable to distribute and burns operator gas more than once. |
| C1 follow-up — per-batch coverage tracking in `targets[]` | If the warning-and-halt behavior of the conservative C1 fix proves operationally noisy. |
| Architectural — orchestrator extracted into a testable class | When the feature-flag matrix (6 flags × act/dry-run/testnet = 48 cells) becomes the dominant source of regressions. |
| Move plan/spec markdown out-of-tree | Team consensus only. |

## File-level summary

| File | Change |
|---|---|
| `src/aleph_nodestatus/credit_distribution.py` | C1 (resume cursor), M3 (delete prepare_credit_distribution) |
| `src/aleph_nodestatus/commands.py` | C2 (cadence guard restructure), C4 (asyncio.to_thread), H5 (global cap + --force-cap), M2/M3/M7/M8 comments, L4 docstring, L5 legacy alignment |
| `src/aleph_nodestatus/payment_processor.py` | H2 (gas estimation), M6 (LOGGER.exception), L1/L2/L3 hygiene |
| `src/aleph_nodestatus/settings.py` | H1 default, H5 new setting, M1 removal, M5 validator, H2 ceiling |
| `src/aleph_nodestatus/wage_subsidy.py` | M4 (hoist import) |
| `src/aleph_nodestatus/rewards_merge.py` | L6 (trim audit sources) |
| `docker/docker-compose.yml` | C3 (split into two services) |
| `docker/.env.calc.example` | C3 (new) |
| `docker/.env.dist.example` | C3 (new) |
| `docs/operations/slippage.md` | H1 (new) |
| `CHANGELOG.rst` | C3 migration note |
| `tests/test_resume_partial_failure.py` | gap 1 (new) |
| `tests/test_cadence_guard.py` | gap 2 (extend) |
| `tests/test_async_extract.py` | gap 3 (new) |
| `tests/test_min_out_bounds.py` | gap 4 (new) |
| `tests/test_rewards_stability.py` | H3 bounding (new) |
| (existing test file for legacy path) | L5 regression case |
| GitHub PR description | H4 (off-repo) |
