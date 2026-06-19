# CRN inactivity slashing — design

- **Date:** 2026-06-17
- **Status:** Draft (design review gate)
- **Repos affected:** `aleph-nodestatus` (credit distribution), `aleph-api-credit` (rewards time series)
- **Out of scope:** the legacy staking-rewards distribution (`distribution.py`) — that is rewards v1 and no longer in use.

## 1. Motivation

A Compute Resource Node (CRN) can keep accruing credit-revenue / hold / wage
rewards even after it has effectively gone dark. The status snapshots already
flag such nodes with `inactive_since` (set in `status.py` when a CRN's score
drops below 0.01). We want to keep computing every node's rewards exactly as
today, but, as a penalty, **withhold the portion attributable to CRNs that have
been inactive for at least 3 days**, recording the withheld amount in a separate
`slashed` structure instead of transferring it.

This is informational in `aleph-api-credit` (time series) and actually withheld
from the on-chain payout in `aleph-nodestatus`. The whole behaviour is behind
feature flags, default on.

## 2. Definitions

### Slashable CRN

`inactive_since` is recorded as an **ETH block height** in each resource-node
entry of the `corechannel` aggregate (published by `set_status` in
`messages.py:339`, which serialises the full `resource_nodes` dicts including
`inactive_since`). A CRN is *slashable* for a given window when, at the node
state in force at the window's close:

```
inactive_since is not None
and (close_height - inactive_since) >= slash_threshold_days * ethereum_blocks_per_day
```

`slash_threshold_days` defaults to **3** and is **independent** of the existing
`crn_inactivity_threshold_days = 90`, which *removes* the node entirely
(`status.py:287`). Between day 3 and day 90 a CRN keeps accruing rewards that get
slashed; past day 90 it is removed and accrues nothing. Duration is measured in
block height in both repos (snapshots carry `height`), consistent with the
existing 90-day check — no height→timestamp conversion is required.

### Reward streams

A CRN earns its own share in three independently-toggleable streams:

| Stream          | CRN component        | Hook point (nodestatus)                              |
|-----------------|----------------------|------------------------------------------------------|
| `credit_revenue`| `execution_crn`      | `_distribute_execution_credits` (`credit_distribution.py:572`) |
| `holder_tier`   | hold `execution_crn` | same function, hold projection via `_apply_expense_to` |
| `wage_subsidy`  | linked-CRN third     | `split_subsidy` (`wage_subsidy.py`, `crn_weights`)   |

Only the CRN's own component is slashed. The CCN-parent and staker shares that
ride alongside the same expense are paid normally.

### Window modes (`slash_retroactive`)

- **From-death (`slash_retroactive=false`):** slash only the CRN share
  accrued from the moment it went inactive. Evaluated per-expense: an expense is
  slashed when the snapshot in force for it already had `inactive_since` set for
  that CRN (and the CRN is ultimately slashable at window close). Naturally
  bounded by `inactive_since`; needs no anchor.
- **Retroactive (default, `slash_retroactive=true`):** slash the CRN's entire share over
  the window `[A, W_end]`, where `A` is the **anchor** = the close of the last
  confirmed credit distribution (defined precisely in §2.1), including what the
  CRN earned while still active early in the window. In nodestatus the run window
  already starts at `A` (via `get_latest_successful_credit_distribution`), so it
  coincides. In api-credit the slash window must be anchored to `A` as well,
  otherwise it would sweep back to the start of all expenses (some already
  distributed). `W_end` is the distribution run's `end_time` in nodestatus and
  the query `to` in api-credit.

### 2.1 The anchor `A` (single source of truth, 1:1 across repos)

Both repos resolve `A` from **the same posts** so the retroactive slash is
byte-for-byte identical:

- **Post type:** `credit-rewards-distribution` (`CREDIT_DISTRIBUTION_POST_TYPE`).
- **Owners queried:** `status_sender` (`0xa1B3bb7d2332383D96b7796B908fB7f7F3c2Be10`)
  and `distribution_recipient` (`0x3a5CC6aBd06B601f4654035d125F9DD2FC992C25`).
- **Selection:** among posts with `content.status == "distribution"` that are
  **ETH-confirmed** (`_is_eth_confirmed`), pick the one with the **largest
  `end_height`**. An amended post with the same `end_height` overrides the prior.
- **Values read:** that post carries `start_height`, `end_height`, `start_time`,
  `end_time` (published by nodestatus in `commands.py`). The anchor is
  `A_height = end_height` and `A_time = end_time`.
- **No prior distribution:** fall back to the data floor —
  `CREDIT_DIST_FLOOR_HEIGHT = 24_996_368` (height) / `REWARDS_DATA_START_ISO`
  (`2026-05-01T00:00:00Z`, time). nodestatus already applies this floor; api-credit
  uses `REWARDS_DATA_START_ISO`, which corresponds to the same launch block.

nodestatus already does exactly this in `get_latest_successful_credit_distribution`
(`credit_distribution.py:155`). api-credit must replicate the **same query**
(same type, same two owners, same `status=="distribution"` + ETH-confirmed +
max-`end_height` rule) and read `end_time` as `A_time` / `end_height` as
`A_height`. Because the expense set, snapshot routing, share formulas, dev-fund
split and `get_reward_address` are already a 1:1 port (`domain/rewards/ported/`),
the slash computed over the same `[A, W_end]` window with the same slashable
predicate yields the identical figure in both services.

### 2.2 Slash is additive per bucket (api-credit reporting)

Both modes partition into per-bucket sums whose total equals the whole-window
slash, so the time series can report `slashed` per bucket and at root without
double counting:

- The slashable CRN set `S` is evaluated **once**, at the window close `W_end`
  (the query `to`), using the close-of-window snapshot.
- An expense contributes its CRN share to `slashed` iff its CRN ∈ `S` **and** its
  routing timestamp lies in the slash window: `[A_time, W_end]` (retroactive) or
  `[from, W_end]` with the per-expense post-death filter (from-death).
- Each contributing expense is bucketed by its own routing timestamp. Buckets
  entirely before `A_time` (retroactive) contribute 0. Root `total.slashed` =
  Σ buckets = the whole-window slash. Querying `to = ` a distribution's
  `end_time` makes root `total.slashed` equal nodestatus's slashed for that
  distribution.

## 3. nodestatus design (`aleph-api-credit` mirrored in §4)

Approach: **post-pass attribution** — leave the reward math untouched, accumulate
a parallel per-CRN ledger, then run a slashing pass after all expenses/days are
applied.

### 3.1 Per-CRN accumulator

Thread an optional accumulator through the three hook points. Keyed by
`(stream, crn_node_id)`:

```python
slash_accum[(stream, node_id)] = {
    "address": crn_reward_address,   # get_reward_address(rnode, web3)
    "total": float,                  # full window share (for retroactive)
    "post_death": float,             # share while snapshot flagged inactive (for from-death)
    "inactive_since": int | None,    # from the close-of-window snapshot
}
```

- In `_distribute_execution_credits` the accumulator is updated right where
  `detailed[crn_addr]["execution_crn"] += crn_amount` happens (one call covers
  both `credit_revenue` and `holder_tier`, distinguished by the stream arg). The
  routed snapshot's `resource_nodes[node_id]["inactive_since"]` decides whether
  the amount also lands in `post_death`.
- In `split_subsidy` the per-day CRN integral is accumulated the same way using
  `crn_weights` (tracking the rnode hash, not just the address).

The accumulator is only built when slashing is enabled, so the default-off path
is a no-op.

### 3.2 Slashing pass

After `merge_rewards` produces `final_rewards` (`commands.py`,
`process_credit_distribution`), run the pass:

1. For each `(stream, node_id)` whose stream flag is on, look up the CRN's state
   in the close-of-window snapshot and test the slashable predicate (§2).
2. Pick the amount per mode: `post_death` (from-death) or `total` (retroactive).
3. Accumulate `slashed[address] += amount` and append a `slashed_meta` entry
   `{node_id, address, stream, amount, inactive_since}`.

### 3.3 Output and transfer

- The published message keeps `rewards` / `detailed` / `by_source` **intact**
  (full calculated amounts, for transparency) and **adds**:
  - `slashed: {address: amount}` — aggregate withheld amount per address.
  - `slashed_meta: {threshold_days, retroactive, nodes: [{node_id, address,
    stream, amount, inactive_since}, ...]}`.
- At transfer time (gated by `credit_dist_transfer_enabled`), the payout set is
  `final_rewards` minus the per-address `slashed` total (floored at 0). Slashed
  amounts are never sent on-chain.
- Because slashing aggregates by address, a recipient that also earns
  non-slashed rewards (other roles/nodes) still receives those; only the slashed
  portion is withheld.

### 3.4 Settings (pydantic `Settings`, `settings.py`)

```python
credit_dist_slash_enabled: bool        = True   # master
credit_dist_slash_credit_revenue: bool = False  # off by default
credit_dist_slash_holder_tier: bool    = False  # off by default
credit_dist_slash_wage_subsidy: bool   = True   # only stream slashed by default
credit_dist_slash_threshold_days: int  = 3
credit_dist_slash_retroactive: bool    = True   # default: retroactive (whole period since last distribution)
```

These compose with the existing per-stream *enable* flags
(`credit_dist_credit_revenue_enabled`, etc.): a stream that is off produces no
rewards and therefore nothing to slash.

## 4. api-credit design (`aleph-api-credit`)

Mirror the post-pass approach in the TypeScript compute path.

### 4.1 Parse `inactive_since`

`SnapshotResourceNode` (`domain/rewards/ported/types.ts`) currently exposes only
`status`. Add `inactiveSince?: number` and populate it in the snapshot parser
(`snapshotsFetcher.ts`) from the raw aggregate field. `Snapshot` already carries
`height`, which the slashable predicate uses.

### 4.2 Slashing in compute

In `distribute.ts` / `computeRewards.ts`, accumulate the same per-CRN ledger
during execution-credit and wage attribution, then run the slashing pass per
bucket/day. Time series transfers nothing, so the result is informational:

- Each time-series bucket (`fetchTimeSeries`, `bucketAggregate.ts`) gains a
  `slashed` field: aggregate withheld amount, plus per-address detail when
  `byAddress` is requested. `rewards` stay intact.

### 4.3 Anchor for retroactive mode

When `slash_retroactive` is on, the retroactive window starts at the anchor `A`,
**resolved by the identical query defined in §2.1** — same post type
(`credit-rewards-distribution`), same two owners (`status_sender` and
`distribution_recipient`), same `status=="distribution"` + ETH-confirmed +
max-`end_height` selection — reading `end_time` as `A_time` and `end_height` as
`A_height`. With no prior distribution, fall back to `REWARDS_DATA_START_ISO`.

Concretely, api-credit adds a small resolver (e.g.
`fetchLastDistributionAnchor()`) that mirrors `get_latest_successful_credit_distribution`
1:1 and caches the result for the request. The slash window is then `[A_time,
to]`, the slashable set is evaluated at `to`, and contributions are bucketed by
§2.2. This makes the root `total.slashed` equal nodestatus's slashed for a
distribution run whose `end_time == to`. From-death mode needs no anchor;
retroactive (the default) does.

> **1:1 invariant.** For any window `[A_time, end_time]` of a confirmed
> distribution, api-credit's retroactive `total.slashed` (per stream and per
> address) MUST equal nodestatus's `slashed` for that same distribution run.
> This is the acceptance criterion for the retroactive path and is enforced by a
> cross-check test fixture shared between the repos' test data.

### 4.4 Config (env vars, `utils/config.ts`)

Mirror the nodestatus flags:

| Setting              | Env var                              | Default |
|----------------------|--------------------------------------|---------|
| slash enabled        | `REWARDS_SLASH_ENABLED`              | `true`  |
| slash credit_revenue | `REWARDS_SLASH_CREDIT_REVENUE`       | `false` |
| slash holder_tier    | `REWARDS_SLASH_HOLDER_TIER`          | `false` |
| slash wage_subsidy   | `REWARDS_SLASH_WAGE_SUBSIDY`         | `true`  |
| slash threshold days | `REWARDS_SLASH_THRESHOLD_DAYS`       | `3`     |
| slash retroactive    | `REWARDS_SLASH_RETROACTIVE`          | `true`  |

## 5. Edge cases

- **Node removed at day 90:** already gone from snapshots, contributes nothing —
  no double handling needed.
- **CRN flips active→inactive→active inside the window:** `inactive_since` resets
  to `None` on recovery (`status.py:293`), so at window close it is not
  slashable; nothing withheld. From-death per-expense flagging naturally follows
  the snapshot timeline.
- **Slashed address shares with non-slashed rewards:** only the slashed portion
  is subtracted; the rest is paid.
- **Slashing disabled:** accumulator not built; output omits `slashed` /
  `slashed_meta`; transfer unchanged. Exact pre-feature behaviour.
- **Snapshot lacks `inactive_since` (old data):** treated as `None` → not
  slashable.

## 6. Testing

- nodestatus: unit tests for the slashable predicate (3-day boundary in block
  height), per-CRN attribution across the three streams, from-death vs
  retroactive amounts, address aggregation with mixed slashed/non-slashed
  rewards, and the transfer subtraction. Flag-off path leaves rewards/transfer
  byte-identical to today.
- api-credit: parser exposes `inactiveSince`; per-bucket `slashed` sums to the
  root `total.slashed`; retroactive anchor resolved by the §2.1 query; flag-off
  omits `slashed`.
- **1:1 cross-check (gating):** a shared fixture (same snapshots + expenses +
  confirmed distribution post) is run through both engines; for the window
  `[A_time, end_time]` the retroactive `slashed` totals — per stream and per
  address — must match exactly. Any divergence fails CI.

## 7. Rollout

By default only the **wage_subsidy** stream is slashed (credit_revenue and
holder_tier are off), in **retroactive** mode (the whole period since the last
confirmed distribution) with a 3-day threshold. This scopes the live penalty to
the wage subsidy while the credit/hold streams stay observable but unpenalised.
The master `credit_dist_slash_enabled` / `REWARDS_SLASH_ENABLED` allows a clean
kill switch if anomalies appear in the first live distributions.
