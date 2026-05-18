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
extract step from inside a distribute run, but there is no inverse, no
separate audit post for extracts, and no way to run an extract without
loading the entire distribute pipeline.

## Goal

Two independent CLI entry points with overlapping primitives:

- `nodestatus-extract-credits` — sweeps the processor, publishes a
  dedicated audit post.
- `nodestatus-distribute-credits` — reduced to reward calculation +
  transfer + audit post; no longer touches the processor or admin key.

## Non-goals

- Changing the reward math, post schema for distribution (aside from
  removing the `extract` field), or the on-chain processor contract.
- Adding cadence guards / scheduling to the extract command. The
  caller (cron, ops human) decides when to run it.
- Replacing `extract_aleph` in `payment_processor.py`. It stays as the
  pure web3 primitive; only the orchestration moves.

## Design

### New module: `src/aleph_nodestatus/credit_extraction.py`

```python
CREDIT_EXTRACT_POST_TYPE = "credit-extract"

async def process_credit_extraction(
    *, act: bool, dry_run: bool,
    transfer: bool, publish: bool,
    slippage_bps: Optional[int] = None,
) -> None:
    """Orchestrate one extract run.

    Steps:
      1. Resolve admin account (payment_processor_admin_pkey, else
         ethereum_pkey with a WARNING — same fallback as today).
      2. ETH balance preflight on admin: warn if balance < expected gas
         headroom for len(process_tokens) txs at 50 gwei.
      3. extract_aleph(...) via asyncio.to_thread, with dry_run derived
         from `dry_run or not transfer`.
      4. Print human-readable summary (per-token: balance, swap_amount,
         min_out, tx_hash | skipped_reason | error).
      5. If publish and not dry_run: publish `credit-extract` post.
    """
```

### Post payload (`credit-extract`)

```jsonc
{
  "incentive": "credits",
  "operation": "extract",
  "status": "extraction" | "calculation",  // "calculation" when not --act
  "block_height": 24_996_999,
  "admin_address": "0xC870…b14E",
  "tokens": [
    {
      "symbol": "USDC", "token": "0xA0b8…eB48",
      "amount_in": "12345000000",
      "swap_amount_in": "11727750000",
      "min_out": "5432000000000000000000",
      "expected_out": "5487000000000000000000",
      "tx_hash": "0x…", "simulated_only": false,
      "skipped_reason": null, "error": null
    },
    …
  ],
  "errors": [],
  "tags": ["extraction", "credits", "extract", "<filter_tag>"]
}
```

Reuses the per-token entry schema that `extract_aleph` already produces,
so dashboards consuming the existing `distribution.extract.tokens[*]`
shape can map field-for-field to the new post type.

### CLI: `extract_credits` in `commands.py`

```
nodestatus-extract-credits [OPTIONS]

  -v, --verbose
  -t, --testnet           Publish to testnet API; no transfers
  -a, --act               Broadcast process() txs
      --dry-run           No broadcast, no publish; only eth_call simulate
      --no-transfer       Compute + simulate, do not broadcast (calc-only)
      --no-publish        Run extract but skip the audit post
      --slippage-bps N    Override per-run slippage tolerance
```

Mutual exclusivity rules mirror `distribute_credits`:
- `--act` and `--testnet` are exclusive
- `--act` and `--dry-run` are exclusive
- `--dry-run` forces `transfer=False, publish=False`

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
precedent of trimming payloads when consumers have a cleaner path. Any
external consumer that wants tx hashes from extraction can subscribe to
the new `credit-extract` post type — it is fully searchable and dated.

**Cadence guard on extract.** None. `extract_aleph` is naturally
idempotent: tokens with zero balance are skipped (`skipped_reason:
zero_balance`), so running every hour with nothing to extract is
harmless. Adding a guard would just be friction.

**Shared flag resolver between commands.** Skipped. Extract has 3
booleans (`transfer`, `publish`, `act`); distribute has 6 plus
mutually-exclusive holder-tier overrides. The unification would be a
union type with branches, not a shared helper.

**Module placement.** New module `credit_extraction.py` parallels
`credit_distribution.py` — same package, same import depth, same naming
shape. `payment_processor.py` stays the contract-interaction primitive.

## Risks

- **Dashboards reading `post.content.extract` from
  `credit-rewards-distribution` posts** will see the field disappear.
  Mitigation: the new `credit-extract` post type carries the same
  per-token schema; consumers can map directly. Called out in the commit
  message.
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
    - asserts `create_distribution_tx_post` is called with
      `post_type=CREDIT_EXTRACT_POST_TYPE` when `publish=True` and
      `dry_run=False`; not called otherwise.
    - asserts `dry_run=True` forces both `transfer=False` and
      `publish=False`.
  - Flag resolver:
    - `--act` + `--testnet` rejected with exit code 2.
    - `--act` + `--dry-run` rejected with exit code 2.

- **Manual integration:**
  - `nodestatus-extract-credits --dry-run` prints a per-token simulate
    summary and an unpublished JSON preview.
  - `nodestatus-extract-credits --no-publish --no-transfer` runs the
    full simulate path but does not broadcast or post.
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
