# Split credit extraction into its own CLI

**Date:** 2026-05-18
**Status:** Proposed
**Author:** amalcaraz

## Problem

`nodestatus-distribute-credits` currently runs two operations with very
different cadences in a single invocation:

1. **Extract** — calls `process()` on the AlephPaymentProcessor for every
   configured token (USDC, ETH, ALEPH), swapping non-ALEPH balances and
   sweeping the proceeds into the distribution wallet. Requires the
   processor admin private key and consumes Ethereum gas.
2. **Distribute** — reads snapshots, computes credit-revenue + holder-tier
   + wage-subsidy rewards, batch-transfers ALEPH to recipients, posts an
   audit record.

Operationally these now want to run at different frequencies:

- **Extract** can run more often (e.g. daily) to keep the distribution
  wallet topped up and avoid sitting on stable balances long enough to
  introduce swap-rate risk.
- **Distribute** runs on its own cadence (e.g. weekly / biweekly) driven
  by the reward window the team wants to publish.

Today the two are coupled. The `--no-extract` flag lets you skip the
extract step from inside a distribute run, but there is no inverse, and
no way to run an extract without loading the entire distribute pipeline
(reward calc, snapshots, balance checks).

## Goal

Two independent CLI entry points with overlapping primitives:

- `nodestatus-extract-credits` — sweeps the processor and logs the
  per-token outcome to stdout. **Does not publish to Aleph.** The
  on-chain tx hashes are the durable audit trail.
- `nodestatus-distribute-credits` — reduced to reward calculation +
  transfer + audit post; no longer touches the processor or admin key.

## Non-goals

- Changing the reward math, post schema for distribution (aside from
  removing the `extract` field), or the on-chain processor contract.
- Publishing extract results to Aleph. Logging only.
- Adding cadence guards / scheduling to the extract command. The
  caller (cron, ops human) decides when to run it.
- Replacing `extract_aleph` in `payment_processor.py`. It stays as the
  pure web3 primitive; only the orchestration moves.

## Design

### New module: `src/aleph_nodestatus/credit_extraction.py`

```python
async def process_credit_extraction(
    *, act: bool, dry_run: bool,
    transfer: bool,
    slippage_bps: Optional[int] = None,
) -> dict:
    """Orchestrate one extract run. Returns the extract block (for tests).

    Steps:
      1. Resolve admin account (payment_processor_admin_pkey, else
         ethereum_pkey with a WARNING — same fallback as today).
      2. ETH balance preflight on admin: warn if balance < expected gas
         headroom for len(process_tokens) txs at 50 gwei.
      3. extract_aleph(...) via asyncio.to_thread, with dry_run derived
         from `dry_run or not transfer`.
      4. Log a human-readable summary to stdout/LOGGER (per-token:
         balance, swap_amount, min_out, tx_hash | skipped_reason | error;
         plus a final tally of successes / failures).
    """
```

No Aleph post is created. The on-chain `process()` tx hashes recorded by
`extract_aleph` are the durable audit trail; any operator review of
extract activity reads from stdout / log aggregation, not from Aleph.

### CLI: `extract_credits` in `commands.py`

```
nodestatus-extract-credits [OPTIONS]

  -v, --verbose
  -a, --act               Broadcast process() txs
      --dry-run           No broadcast; only eth_call simulate
      --no-transfer       Compute + simulate, do not broadcast (calc-only)
      --slippage-bps N    Override per-run slippage tolerance
```

No `--testnet` flag: extract talks to the Ethereum payment processor,
not to Aleph, so the testnet/mainnet split (which only governs Aleph API
target) does not apply.

Mutual exclusivity rules:
- `--act` and `--dry-run` are exclusive
- `--dry-run` forces `transfer=False`

The flag resolver is a small inline helper in the command, not a shared
function — the flag sets between extract and distribute diverge enough
that sharing would be premature abstraction.

### Changes to `distribute_credits`

In `commands.py`:

1. Delete the admin-account resolution block (`commands.py:359-391`).
2. Delete Step 1 extract (`commands.py:400-424`).
3. Delete `--no-extract` option and the `extract` branch of
   `_resolve_feature_flags`.
4. Delete `"extract": extract_block` from the distribution post payload.
5. Drop the now-unused imports (`extract_aleph`,
   `get_processor_contract`, `get_quoter_contract`,
   `get_v2_router_contract`, `get_v4_quoter_contract`, `Account`,
   `HexBytes`).

In `settings.py`:

6. Remove `credit_dist_extract_enabled` (no longer wired to anything).

### Entry points (`setup.cfg`)

```
console_scripts =
    …
    nodestatus-distribute-credits = aleph_nodestatus.commands:distribute_credits
    nodestatus-extract-credits     = aleph_nodestatus.commands:extract_credits
```

## Trade-offs

**Drop `extract` from the distribution post vs. keep an empty stub.**
Drop it. The recent refactor `bb5bd0d` (drop `rewards_detailed`) set the
precedent of trimming payloads when consumers have a cleaner path. The
on-chain tx history is the canonical record of extractions; nothing on
Aleph is needed.

**No Aleph audit post for extract.** The information of record is the
on-chain `process()` tx (visible on Etherscan and indexable from logs).
Adding a parallel Aleph post would duplicate that and create a second
state to keep consistent. Stdout / log aggregation covers the human
ops-review case.

**Cadence guard on extract.** None. `extract_aleph` is naturally
idempotent: tokens with zero balance are skipped (`skipped_reason:
zero_balance`), so running every hour with nothing to extract is
harmless. Adding a guard would just be friction.

**Shared flag resolver between commands.** Skipped. Extract has 2
booleans (`transfer`, `act`); distribute has 6 plus mutually-exclusive
holder-tier overrides. The unification would be a union type with
branches, not a shared helper.

**Module placement.** New module `credit_extraction.py` parallels
`credit_distribution.py` — same package, same import depth, same naming
shape. `payment_processor.py` stays the contract-interaction primitive.

## Risks

- **Dashboards reading `post.content.extract` from
  `credit-rewards-distribution` posts** will see the field disappear.
  Mitigation: tx hashes for the swap-and-sweep are on-chain and
  recoverable from Etherscan or any block indexer keyed on the admin
  address. Called out in the commit message.
- **Running distribute before extract on a low balance** aborts on the
  balance safety check (`commands.py:520-551`), same as today. No
  regression.
- **Admin pkey fallback to `ethereum_pkey` still works**, but is now
  invoked from the extract command, not distribute. Anyone whose ops
  runbook only sets `ethereum_pkey` keeps working.

## Tests

- **Unit (`tests/test_credit_extraction.py`):**
  - `process_credit_extraction` with `extract_aleph` mocked:
    - asserts admin account is loaded from `payment_processor_admin_pkey`
      when set, falls back to `ethereum_pkey` with a warning when not.
    - asserts the returned extract block carries the per-token entries
      produced by `extract_aleph` (no transformation).
    - asserts `dry_run=True` forces `transfer=False` and the call to
      `extract_aleph` receives `dry_run=True`.
    - asserts no Aleph publish helper is invoked under any combination
      of flags.
  - Flag resolver:
    - `--act` + `--dry-run` rejected with exit code 2.

- **Manual integration:**
  - `nodestatus-extract-credits --dry-run` prints a per-token simulate
    summary.
  - `nodestatus-extract-credits --no-transfer` runs the full simulate
    path but does not broadcast.
  - `nodestatus-distribute-credits --dry-run` no longer references the
    extract step in its preview JSON.

## Migration

Single PR. No data migration. The `credit_dist_extract_enabled` env var,
if set in any deployment `.env`, becomes inert — call it out in the PR
description and remove the var from `docker/.env.calc` and any ops
runbook.

After the PR is merged, the operator's cron / scheduler config needs to
be updated to invoke `nodestatus-extract-credits` separately from
`nodestatus-distribute-credits`. The new cadence is an operations
decision, not a code change.
