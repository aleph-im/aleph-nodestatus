# Credit Rewards Distribution — Design

**Status:** Draft
**Date:** 2026-05-07
**Owner:** amalcaraz
**Scope:** `aleph-nodestatus` (`nodestatus-distribute-credits`) — new tokenomics rollout

## 1. Purpose

Run a configurable, periodic job that:

1. Extracts ALEPH from the on-chain `AlephPaymentProcessor` by calling `process()` per accepted token (USDC, ETH, ALEPH), with slippage-protected min-output computed off-chain to prevent sandwich attacks.
2. Computes ALEPH rewards owed to CRN owners, CCN owners, stakers, and (optionally) holder-tier hosts based on credit-expense messages.
3. Adds the 6-month transition wage subsidy that decreases linearly from 900,000 ALEPH/month to 0.
4. Pays everyone in bulk transfers via the ALEPH ERC-20 `batchTransfer` (gas-saving grouping already in `ethereum.py`).
5. Publishes a single auditable distribution record to the Aleph network.

Usage-incentive subsidies and the 1,350,000 ALEPH monthly emission cap are **out of scope**: a separate service handles them by spawning VMs on the network.

## 2. Background

`aleph-nodestatus` already exposes `nodestatus-distribute-credits` (`src/aleph_nodestatus/commands.py:269-428`), which ingests `aleph_credit_expense` POST messages, walks corechannel snapshots (or replays the full state machine with `--full-resync`), and produces a `{address: aleph_amount}` rewards dict using configurable shares (`credit_storage_ccn_share`, `credit_execution_crn_share`, etc.). Transfers are done via `ethereum.transfer_tokens()`, which already calls `batchTransfer` on the ALEPH ERC-20 (`src/aleph_nodestatus/ethereum.py:112-181`). The work below extends — not replaces — that command.

References:
- Tokenomics docs: `https://tokenomics.aleph.cloud/`
- FAQ: `https://tokenomics.aleph.cloud/faq.html`
- Smart contract source: `aleph-im/aleph-contract-eth-credit`, `src/AlephPaymentProcessor.sol`
- Deployed proxy: `0x6b55f32ea969910838defd03746ced5e2ae8cb8b`
- Distribution recipient (where extracted ALEPH lands): `0x3a5CC6aBd06B601f4654035d125F9DD2FC992C25`
- Admin caller (must hold `adminRole`): `0xC870B0Ca4B3d65f33E2a3c732ab3cD2aE555b14E`

Sample expense messages used to validate the schema:
- Execution: `https://api2.aleph.im/api/v0/messages/c1cfc9bf519de2bbe19749a3996bd6119f940fa91e9a30f317cce8939bb6b21a` — tags `["credit_expense","type_execution"]`, credits with `node_id` and `execution_id`.
- Storage: `https://api2.aleph.im/api/v0/messages/e6121c0ec5eaf721c91b51443d7da9f815fbe003b490e5f22ca0f6292ca30343` — tags `["credit_expense","type_storage"]`, credits with `address` (uploader) and `size`.

Both messages carry `credit_price_aleph` and `credit_price_usdc`.

## 3. Approach (chosen)

Extend `nodestatus-distribute-credits`. Add three new modules and one ABI file. Refactor `prepare_credit_distribution` so reward computation is a pure function with no side effects, then orchestrate the new pieces inside `process_credit_distribution`. Holder-tier handling lives inside credit reward computation (gated by a feature flag), not as a separate step. Alternatives B (split commands) and C (new orchestrator service) were considered and rejected: they enlarge the change surface without improving testability and would duplicate the existing resume-from-last-distribution machinery.

## 4. Architecture

```
nodestatus-distribute-credits
   │
   ▼
process_credit_distribution(args)             ← src/aleph_nodestatus/commands.py
   │
   ├── 0. cadence guard: skip if (now - last_end) < min_interval, unless --force
   │
   ├── 1. payment_processor.extract_aleph()         [flag: extract]
   │       └── for token in [USDC, ETH, ALEPH]:
   │             quote_min_out() via Uniswap QuoterV2 (read-only)
   │             eth_call simulation (always)
   │             processor.process(token, amountIn, minOut, ttl)   (if not dry-run/no-transfer)
   │
   ├── 2. credit_distribution.compute_rewards()     [flag: credit_revenue,
   │       │                                          sub-flag: holder_tier]
   │       └── existing _distribute_expense() math
   │           optional: fold content.rewards entries (holder-tier hosting)
   │             into the same pipeline when holder_tier flag is on
   │
   ├── 3. wage_subsidy.compute_subsidy()            [flag: wage]
   │       └── ∫f(t)dt over [start_time, end_time]
   │             split 1/3 CCN / 1/3 CRN / 1/3 stakers (score-weighted / stake-weighted)
   │
   ├── 4. merge_rewards() + dust filter
   │
   ├── 5. ethereum.transfer_tokens() in batches     [flag: transfer]
   │       └── batchTransfer of up to ethereum_batch_size per tx
   │
   └── 6. create_distribution_tx_post()             [flag: publish]
           └── one Aleph post with full audit trail
```

### File layout

**New:**
- `src/aleph_nodestatus/payment_processor.py` — `extract_aleph()`, `quote_min_out()`, `simulate_process()`.
- `src/aleph_nodestatus/wage_subsidy.py` — `compute_subsidy()`, `wage_integral()`, `split_subsidy()`.
- `src/aleph_nodestatus/abi/AlephPaymentProcessor.json` — ABI (paste from the user-supplied JSON).
- `src/aleph_nodestatus/abi/UniswapV3QuoterV2.json` — QuoterV2 ABI.
- `tests/test_wage_subsidy.py`
- `tests/test_payment_processor.py`
- `tests/test_credit_pipeline.py`

**Touched (minimal):**
- `src/aleph_nodestatus/commands.py` — extends `process_credit_distribution`; adds CLI flags.
- `src/aleph_nodestatus/settings.py` — new env vars (see §6).
- `src/aleph_nodestatus/credit_distribution.py` — refactor `prepare_credit_distribution` into a pure `compute_rewards()`; add `fetch_credit_rewards()` for the holder-tier branch.
- `docker/docker-compose.yml` — add cron entry for the new job (tight schedule, behavior gated by the cadence guard inside the script).

## 5. Algorithms

### 5.1 Payment-processor extraction (`payment_processor.py`)

```python
for (symbol, token_addr) in settings.process_tokens:
    balance = contract_balance(token_addr)   # ERC20.balanceOf or address.balance
    if balance == 0:
        log.info(f"skip {symbol}: zero balance"); continue

    if symbol != "ALEPH":
        expected_out = quoter_get_amount_out(token_addr, balance)   # read-only
        value_usd    = quoter_get_value_usd(token_addr, balance)
        if value_usd < settings.process_min_token_value_usd:
            log.info(f"skip {symbol}: ${value_usd} < threshold")
            continue
        min_out = expected_out * (10_000 - slippage_bps) // 10_000
    else:
        expected_out = balance
        min_out = 0

    # ALWAYS simulate first
    try:
        eth_call(processor.encodeABI(
            fn_name="process",
            args=[token_addr, balance, min_out, settings.process_ttl_seconds]
        ), from_=admin_address)
    except ContractLogicError as e:
        record_error(symbol, str(e)); continue

    if not dry_run and transfer_enabled:
        tx_hash = send_raw(processor.functions.process(
            token_addr, balance, min_out, settings.process_ttl_seconds
        ).build_transaction(...))
        receipt = wait_for_receipt(tx_hash)
        record_event(symbol, receipt)   # parse TokenPaymentsProcessed event
    else:
        record_simulated(symbol, balance, expected_out, min_out)
```

**Quoter resolution:** read `processor.getSwapConfig(token)` to learn `v` (2/3/4) and the path. Use the corresponding quoter. Initial implementation supports V3 only (QuoterV2 at `0x61fFE014bA17989E743c5F6cB21bF9697530B21e`). If a configured token has `v != 3`, abort that token with a loud error and record it in `extract.errors` — extending to V2/V4 is a follow-up.

**Sequence:** USDC → ETH → ALEPH, each its own tx, receipt awaited. A revert on one does not poison the others; failed tokens are recorded.

**Admin signer:** `payment_processor_admin_pkey` env var. Falls back to `ethereum_pkey` with a `WARN` log if unset. The address must hold `adminRole` on the processor; mismatch causes the simulation to revert with `AccessControlUnauthorizedAccount` and we record the error.

### 5.2 Credit reward computation (`credit_distribution.py` refactor)

Each `aleph_credit_expense` message has the shape (canonical TypeScript interface for the execution variant; the storage variant is structurally identical with different per-line fields):

```typescript
interface ExecutionExpenseMessage {
  start_date: number;
  end_date:   number;
  credits:    ExecutionExpenseLine[];          // real, paid credit entries
  amount?:    number;                          // aggregate of credits[].amount
  count?:     number;                          // len(credits)
  credit_price_aleph?: number;
  credit_price_usdc?:  number;
  // @temporary: hold rewards
  rewards?:        ExecutionExpenseLine[];     // holder-tier hosting (same per-item shape as credits)
  rewards_amount?: number;                     // aggregate of rewards[].amount
  rewards_count?:  number;                     // len(rewards)
}
```

This object lives at `content.content.expense`. `rewards[]` is a sibling of `credits[]`, marked `@temporary` in the source — aligned with the holder-tier deprecation. Items inside `rewards[]` have the same fields as `credits[]` (`ref`, `amount`, `address`, plus `node_id`/`execution_id` for execution or `size` for storage). `rewards[]` is currently absent in production messages; we treat its absence as an empty list.

`rewards_amount` and `rewards_count` are informational aggregates we don't depend on for math, but we **validate** them when present: if `sum(rewards[].amount) != rewards_amount` (within rounding) or `len(rewards) != rewards_count`, log a `WARN` and continue — the per-entry sum is authoritative.

Extract a pure function. A single API fetch yields per-message data; from each message we derive **up to two entries** — one synthesized from `credits[]` (always) and one synthesized from `rewards[]` (only when `include_holder_tier` is on). The two streams feed `_distribute_expense()` separately so the audit post can attribute amounts to source. The merge for actual transfer happens later in §5.4.

```python
async def compute_rewards(start_time, end_time, full_resync=False,
                          include_holder_tier=False, web3=None):
    msgs = await _fetch_expense_messages(start_time, end_time)

    # One paginated fetch; split per-message into two synthetic expense entries.
    expense_entries = []  # (height, type, {credit_price_aleph, credits: [...]})
    holder_entries  = []  # (height, type, {credit_price_aleph, credits: rewards})
    for m in msgs:
        height, exp_type, expense = _parse(m)
        if not expense:
            continue
        if expense.get("credits"):
            expense_entries.append(
                (height, exp_type, _project(expense, "credits"))
            )
        if include_holder_tier and expense.get("rewards"):
            holder_entries.append(
                (height, exp_type, _project(expense, "rewards"))
            )

    expense_rewards, expense_totals = await _distribute(
        expense_entries, full_resync, web3
    )
    holder_rewards, holder_totals = ({}, _zero_totals())
    if include_holder_tier:
        holder_rewards, holder_totals = await _distribute(
            holder_entries, full_resync, web3
        )

    return {
        "credit_revenue": (expense_rewards, expense_totals),
        "holder_tier":    (holder_rewards, holder_totals),
    }


def _project(expense, src_key):
    """Build a synthetic expense object with the chosen list aliased as credits[].
    Lets _distribute_expense() consume rewards[] entries unchanged."""
    return {
        "credit_price_aleph": expense.get("credit_price_aleph", 0),
        "credits": expense.get(src_key, []),
    }
```

`_distribute(...)` is a thin wrapper that picks `_prepare_with_state_machine` or `_prepare_with_snapshots` based on `full_resync`. The math inside `_distribute_expense()` is unchanged — both streams go through the same CRN/CCN/staker split, same `credit_amount × credit_price_aleph` revenue calculation.

The split percentages (`credit_storage_ccn_share`, `credit_execution_crn_share`, etc.) stay as-is; per the new tokenomics this is **60 CRN / 15 CCN / 20 stakers / 5 dev / 0 burn**. Existing settings already encode this; verify on implementation.

**Caveat to record in the post (and in code as an assertion):** when `--enable-holder-tier` is on, the total ALEPH owed can exceed the ALEPH actually sitting at the distribution recipient (holders pay via balance, not credits, so no ALEPH arrives for those entries). Before transferring, the script asserts `sum(rewards.values()) <= aleph_at_distribution_recipient + epsilon`. Failure aborts the run with a non-zero exit code; the post is not created. Sourcing the gap is out of scope for this spec.

### 5.3 Wage subsidy (`wage_subsidy.py`)

Continuous linear curve, no buckets:

```python
T_MONTHS = settings.wage_duration_months          # 6
W0       = settings.wage_initial_monthly_aleph    # 900_000
MONTH_S  = 30 * 86400

def months_since_start(ts):
    return (ts - parse(settings.wage_start_date).timestamp()) / MONTH_S

def integral(t):
    # ∫₀ᵗ W0·(1 - x/T) dx   in ALEPH
    if t <= 0:
        return 0
    if t >= T_MONTHS:
        return W0 * T_MONTHS / 2      # 2,700,000 total
    return W0 * (t - t * t / (2 * T_MONTHS))

t1 = max(0, months_since_start(start_time))
t2 = min(T_MONTHS, months_since_start(end_time))
period_subsidy = integral(t2) - integral(t1)      # ALEPH

ccn_pool    = period_subsidy / 3
crn_pool    = period_subsidy / 3
staker_pool = period_subsidy / 3
```

**Split mechanics**, using the most recent snapshot at or before `end_time` (same snapshot the credit-revenue path uses):

- **CCN pool**: distributed proportionally by `compute_score_multiplier(node["score"])` across nodes with `status == "active"`, paid to `reward` address (fall back to `owner`). Idle/disabled CCNs receive zero.
- **CRN pool**: distributed proportionally by `compute_score_multiplier(rnode["score"])` across resource nodes with `status == "linked"`, paid to `rnode.reward` (fall back to `rnode.owner`).
- **Staker pool**: distributed proportionally to stake amount across all stakers of active CCNs (same set the credit-revenue staker pool uses).

If a pool has zero eligible recipients (e.g., zero active CCNs, zero stake), that pool's ALEPH is **not redistributed**. It is recorded as `wage_subsidy.unallocated_aleph` in the post. Other pools pay out normally.

**Stacking with credit revenue:** additive. A CRN that earns both organic credit revenue and a wage-pool share receives the sum. Gap-fill behavior (the "fills gap" language in the simulator) is the *other* service's job.

### 5.4 Merge + dust filter

```python
final = defaultdict(Decimal)
per_source = {
    "credit_revenue": credit_rewards,
    "holder_tier":    holder_rewards,   # empty {} when flag is off
    "wage_subsidy":   wage_rewards,
}
for src, source in per_source.items():
    for addr, amt in source.items():
        final[addr] += Decimal(amt)

final = {a: float(v) for a, v in final.items()
         if v >= Decimal(str(settings.credit_dist_dust_threshold_aleph))}
```

The per-source breakdown is preserved for the audit post (see §5.6). Addresses normalized to checksum via `web3.to_checksum_address` before the merge so that the same recipient appearing in two sources collapses to one entry.

### 5.5 Transfer

`ethereum.transfer_tokens()` already calls `batchTransfer` of up to `ethereum_batch_size` (200) per tx. Reused as-is. In `--dry-run` or `--no-transfer`, replaced by a log:

```
Batch 1/3: would batchTransfer to 200 recipients, sum=12345.6789 ALEPH
Batch 2/3: ...
```

Per-batch failures are recorded in `metadata["targets"]` and do not abort subsequent batches — same as today's `distribute` command.

### 5.6 Publish — `post_type=credit-rewards-distribution`

```json
{
  "incentive": "credits",
  "status": "calculation" | "distribution",
  "start_time": 1778060633.717,
  "end_time":   1778665433.500,
  "rewards":    { "0xabc...": 12.34, "...": "..." },
  "rewards_by_source": {
    "credit_revenue": { "0xabc...": 8.00 },
    "holder_tier":    {},
    "wage_subsidy":   { "0xabc...": 4.34 }
  },
  "credit_revenue_totals": {
    "storage_total_aleph":   "...",
    "execution_total_aleph": "...",
    "dev_fund_total_aleph":  "..."
  },
  "holder_tier_totals": {
    "storage_total_aleph":   "...",
    "execution_total_aleph": "...",
    "dev_fund_total_aleph":  "...",
    "included":              false
  },
  "wage_subsidy": {
    "start_t_months":     0.123,
    "end_t_months":       0.456,
    "period_total_aleph": 8200.0,
    "unallocated_aleph":  0.0,
    "split": {"ccn": 2733.33, "crn": 2733.33, "stakers": 2733.33}
  },
  "extract": {
    "tokens": [
      {"symbol":"USDC","amount_in":"1000.0","min_out":"...","expected_out":"...",
       "aleph_received":"...","tx":"0x...","simulated_only":false}
    ],
    "errors": []
  },
  "targets": [ /* batchTransfer results */ ],
  "feature_flags": {
    "extract": true, "credit_revenue": true, "wage": true,
    "holder_tier": false, "transfer": true, "publish": true
  },
  "tags": ["distribution", "credits", "mainnet"]
}
```

`get_latest_successful_credit_distribution()` already filters by `status == "distribution"` AND at least one successful target — unchanged.

### 5.7 Cadence guard

```python
last_end, _ = await get_latest_successful_credit_distribution(reward_sender)
if last_end and (now - last_end) < settings.credit_dist_min_interval_seconds and not args.force:
    log.info(f"Skip: only {now - last_end:.0f}s since last run, "
             f"min={settings.credit_dist_min_interval_seconds}s")
    return
```

Default `min_interval` is `10 * 86400` (10 days). Cron can run as tightly as `*/30 * * * *` — the guard governs the actual cadence and is resilient to missed runs.

## 6. Configuration

### New settings (`settings.py`)

```python
# AlephPaymentProcessor
payment_processor_address: str = "0x6b55f32ea969910838defd03746ced5e2ae8cb8b"
payment_processor_admin_pkey: str = ""                       # admin key; falls back to ethereum_pkey
distribution_recipient: str = "0x3a5CC6aBd06B601f4654035d125F9DD2FC992C25"

# Tokens to process, in order. Address(0) for ETH.
process_tokens: List[Tuple[str, str]] = [
    ("USDC",  "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"),
    ("ETH",   "0x0000000000000000000000000000000000000000"),
    ("ALEPH", "0x27702a26126e0B3702af63Ee09aC4d1A084EF628"),
]

# Slippage / quoter
uniswap_v3_quoter_address: str = "0x61fFE014bA17989E743c5F6cB21bF9697530B21e"
process_slippage_bps: int = 200
process_min_token_value_usd: float = 50
process_ttl_seconds: int = 1800                              # ≤ 3600 required by contract

# Wage subsidy
wage_start_date: str = "2026-04-01T00:00:00Z"
wage_duration_months: int = 6
wage_initial_monthly_aleph: int = 900_000
wage_ccn_share: float = 1/3
wage_crn_share: float = 1/3
wage_staker_share: float = 1/3

# Cadence + filtering
credit_dist_min_interval_seconds: int = 10 * 86400           # 10 days; --force overrides
credit_dist_dust_threshold_aleph: float = 0.01

# Feature flags
credit_dist_extract_enabled: bool         = True
credit_dist_credit_revenue_enabled: bool  = True
credit_dist_wage_subsidy_enabled: bool    = True
credit_dist_holder_tier_enabled: bool     = False
credit_dist_transfer_enabled: bool        = True
credit_dist_publish_enabled: bool         = True
```

### Feature-flag matrix

| Flag (env) | Default | CLI override | Effect when False |
|---|---|---|---|
| `credit_dist_extract_enabled` | True | `--no-extract` | Skip `process()` (real & simulated) |
| `credit_dist_credit_revenue_enabled` | True | `--no-credit-revenue` | Skip credit-expense reward computation |
| `credit_dist_wage_subsidy_enabled` | True | `--no-wage` | Skip wage subsidy |
| `credit_dist_holder_tier_enabled` | False | `--enable-holder-tier` / `--no-holder-tier` | Skip `content.rewards` (deprecated by default) |
| `credit_dist_transfer_enabled` | True | `--no-transfer` | Don't broadcast `process()` or `batchTransfer` |
| `credit_dist_publish_enabled` | True | `--no-publish` | Don't post the distribution record |

### Run-mode matrix

| Mode | Aleph post | `process()` tx | `batchTransfer` tx | On-chain `eth_call` sim |
|---|---|---|---|---|
| default (calc only) | yes (`status=calculation`) | no | no | yes |
| `--act` | yes (`status=distribution`) | yes | yes | yes |
| `--dry-run` | no | no | no | yes |
| `--testnet` | yes (testnet, `status=calculation`) | no | no | yes |

`--dry-run` and `--act` are mutually exclusive; combining them is rejected with a non-zero exit.

### CLI surface

```
nodestatus-distribute-credits [OPTIONS]
  -v, --verbose
  -a, --act                    # execute transfers + post status=distribution
  -t, --testnet                # post to testnet
      --dry-run                # no post, no transfers, simulate process() via eth_call
      --force                  # bypass cadence guard
      --start-time TS          # resume override (unix seconds)
      --end-time TS            # default: now
      --full-resync            # replay state machine from genesis
      --no-extract             # skip process()
      --no-credit-revenue      # skip credit-revenue rewards
      --no-wage                # skip wage subsidy
      --enable-holder-tier     # turn the deprecated branch on
      --no-holder-tier         # force the deprecated branch off (overrides env=True)
      --no-transfer            # don't broadcast txs
      --no-publish             # don't post Aleph record
      --slippage-bps INT       # override per run
      --reward-sender ADDR     # for resume lookup
```

## 7. Deployment

`docker/docker-compose.yml` gains a second cron line; the existing `*/10 * * * * nodestatus-distribute -v` stays:

```
*/30 * * * * cd /aleph-nodestatus && /usr/local/bin/nodestatus-distribute-credits --act -v >> /logs/credits.log
```

The cadence guard inside the script enforces the 10-day spacing; cron just gives the script regular chances to run. Operators run `--dry-run` manually before flipping `--act` on a new release.

## 8. Error and failure model

| Step | Failure | Behavior |
|---|---|---|
| Cadence guard | Min interval not elapsed | Log + exit 0 |
| Quoter call | RPC outage / missing path | Abort run, exit non-zero |
| `process(USDC)` simulation reverts | Slippage too tight, zero balance, role mismatch | Log, record in `extract.errors`, continue with ETH/ALEPH |
| `process(USDC)` real tx fails after sim passes | Reorg, gas, MEV | Record `receipt.status == 0`, continue |
| `compute_rewards()` | No expenses in range | Empty `credit_revenue` dict, wage still runs |
| Snapshot fetch | Zero snapshots | Abort unless `--full-resync` |
| Pre-transfer assertion | Owed > available ALEPH | Abort, no post |
| `batchTransfer` | One batch reverts | Mark that batch failed, continue, post with partial success |
| Publish | Aleph API down | Write JSON to `./failed_distributions/<ts>.json`, exit non-zero |

## 9. Tests

**Unit:**
- `test_wage_subsidy.py`
  - `integral(0) == 0`
  - `integral(T) == W0 * T / 2 == 2_700_000` (with defaults)
  - `integral(1) == 825_000` (first-month partial)
  - `integral(T + 1) == integral(T)` (clamp past end)
  - `compute_subsidy(t1, t2)` with `t2 < t1` raises
  - Zero active CCNs → `unallocated_aleph` reflects that pool
  - Split sums equal `period_subsidy` to within float epsilon
- `test_payment_processor.py`
  - `min_out` math at slippage_bps boundaries (0, 100, 500, 10000)
  - Skip when `value_usd < threshold`
  - Admin-key fallback emits a `WARN`
  - V2/V4 path → recorded as error, doesn't abort other tokens
  - Simulation revert → captured into `extract.errors`
- `test_credit_pipeline.py`
  - Merge + dust filter: addresses below threshold are excluded; per-source breakdown preserved
  - Feature flags: each off-permutation removes the expected key from the post
  - Holder-tier path: `content.content.expense.rewards[]` entries flow through the same `_distribute_expense()` math via the `_project()` alias; result equals credit-revenue computed from the same payload swapped between `credits[]` and `rewards[]`
  - Pre-transfer assertion fires when owed > available

**Integration:**
- `--dry-run` end-to-end against a recorded fixture (one snapshot, three expenses, one rewards entry). Verifies console output and the in-memory summary object.
- `--testnet` end-to-end posts a `status=calculation` record with all expected fields; no transfers.

**Snapshot/regression:** small fixture (5 expenses + 1 snapshot) with pinned expected per-recipient amounts to catch regressions when the existing math is touched during the refactor.

## 10. Out of scope

- Usage-incentive subsidies and the 1,350,000 ALEPH monthly emission cap (handled by a separate VM-spawning service).
- Funding the gap when holder-tier rewards exceed available ALEPH (recorded but not solved here).
- V2 and V4 Uniswap quoter support — only V3 quoting is required initially; non-V3 tokens are surfaced as errors.
- Re-enabling holder tier as a long-term feature; the branch exists for transition continuity only.

## 11. Open items

- Confirm `wage_start_date` exact value once the launch date is fixed.
- Confirm that the `process_tokens` list above is complete (only USDC, ETH, ALEPH at launch, or are there others to add?).
- Confirm whether `wage_initial_monthly_aleph` should remain `900_000` or be exposed for governance-driven changes (current design: env-overridable so a redeploy can adjust without code change).
