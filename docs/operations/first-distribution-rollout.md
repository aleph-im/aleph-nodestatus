# First Distribution — Safe Rollout Runbook

Step-by-step path for bringing both `nodestatus-extract` and
`nodestatus-dist` online for the first time, with verification
checkpoints between each stage. Every stage is reversible up to the
point where on-chain transactions get broadcast (steps 2 and 6).

> Convention: all commands assume `cd
> aleph-nodestatus/docker`. The compose services already declare
> `build`, bind mounts and `env_file`; we override the command for
> one-shot runs.

## Order of operations

| # | Phase | Side effects | Required keys |
|---|---|---|---|
| 1 | Extract dry-run | none (eth_call only) | RPC |
| 2 | Extract one-shot `--act` | broadcasts process() txs | RPC + admin pkey + ETH for gas |
| 3 | Extract cron mode | hourly `--act` | same as 2 |
| 4 | Distribute dry-run | none | RPC |
| 4.5 | Distribute fork-verified dry-run *(optional)* | only on local Anvil fork | RPC + signer pkey + Anvil |
| 5 | Distribute testnet one-shot | publishes calc post to Aleph testnet | RPC + signer pkey |
| 6 | Distribute one-shot `--act` | broadcasts batchTransfer + publishes mainnet post | RPC + signer pkey + ALEPH balance |
| 7 | Distribute cron mode | every 10 days, `--act` | same as 6 |

Do not advance to the next phase unless the current one ended cleanly.
"Cleanly" means: `no errors` in the summary, no `ERROR=` token entries,
and (for phases 2 / 6) the broadcast tx mined with `status=1`.

---

## Phase 1 — Extract dry-run

No keys required; the simulate path uses
`settings.payment_processor_admin_address` as the `from` for `eth_call`
and never derives an Account.

```bash
docker compose run --rm --no-deps \
  -e ethereum_api_server="https://<your-rpc>" \
  --entrypoint /usr/local/bin/nodestatus-extract-credits \
  nodestatus-extract --dry-run -v
```

**Pass criteria:**

- `Mode: DRY-RUN` banner.
- Each token in `=== Extract summary ===` is either `skipped=zero_balance`,
  `simulated_only min_out=…`, or `skipped=price_deviation Δ=…bps`.
- Footer reads `no errors`.

Any `ERROR=` line means the corresponding `process()` would have
reverted on chain — fix before continuing (typically wrong slippage,
stale RPC, or a paused contract).

---

## Phase 1.5 — Extract fork-verified dry-run (optional but recommended)

Same as Phase 1 except it actually signs + broadcasts each `process()`
tx against a **local Anvil fork** and reconciles the realized
`alephReceived` against the off-chain quoter prediction. Catches the
class of bug Phase 1 can't catch: a `process()` that simulates green
but, when actually executed (slippage, fee-on-transfer interactions,
hooks), produces less ALEPH than the quoter promised. Zero mainnet
side-effects.

### Setup (one terminal: Anvil fork + bootstrap, single command)

If Foundry (which ships `anvil` and `cast`) isn't installed yet:

```bash
curl -L https://foundry.paradigm.xyz | bash && foundryup
```

Then run `fork-bootstrap.sh` — it launches Anvil in the background
(reading `ethereum_api_server` from your `.env.extract` / `.env.dist`
/ `.env` the same way the nodestatus CLI does), waits for it to
come up, and applies whatever bootstrap mode you ask for. Picks
the upstream from the same files in the same order as the CLI:

```bash
scripts/fork-bootstrap.sh --mode extract
```

That single command:

- resolves the upstream RPC from `docker/.env.extract` →
  `docker/.env.dist` → `.env`,
- pings the upstream to fail fast if the URL is wrong,
- launches `anvil --fork-url <resolved> --chain-id 1 --host 0.0.0.0
  --port 8545` in the background (PID + log path printed),
- runs the bootstrap (grant `ADMIN_ROLE`, fund the processor).

Override the upstream when needed:

```bash
scripts/fork-bootstrap.sh --mode extract \
  --ethereum-api-server https://eth.llamarpc.com
```

Other useful overrides:

| Flag | Purpose |
|---|---|
| `--env-file <path>` | Read `ethereum_api_server` from a non-default file. |
| `--anvil-port <N>` | Change the Anvil port (default 8545). |
| `--anvil-block-number <N>` | Pin Anvil to a specific block (e.g. for reproducibility or to dodge archive-pruning). |
| `--no-anvil` | Skip launching Anvil; expect it to already be running at `--rpc`. |

Anvil keeps running after the script exits. The script prints its
PID and log path so you can `tail -f $LOG` while iterating and
`kill $PID` when you're done.

If you'd rather manage Anvil yourself (e.g. you have a long-lived
fork session for an afternoon of verification):

```bash
anvil --fork-url "https://<your-rpc>" \
      --chain-id 1 --host 0.0.0.0 --port 8545
# in another terminal:
scripts/fork-bootstrap.sh --mode extract --no-anvil
```

`--host 0.0.0.0` is required so the docker container can reach it.

> **Linux host gotcha:** `host.docker.internal` only resolves out of
> the box on Docker Desktop (macOS/Windows). On Linux (including
> production servers) the alias must be mapped explicitly to the
> bridge gateway via `extra_hosts`. The compose services in
> `docker/docker-compose.yml` already do this, so the command below
> works as-is. If you're using a stand-alone `docker run`, add
> `--add-host=host.docker.internal:host-gateway`. Requires Docker
> ≥ 20.10.

### Pre-flight (verify the fork is reachable from inside the container)

5-second sanity check before burning a full run on a hostname that
can't be resolved or a port that's closed:

```bash
docker compose run --rm --no-deps --entrypoint /bin/bash \
  nodestatus-extract -c \
  'getent hosts host.docker.internal && \
   (exec 3<>/dev/tcp/host.docker.internal/8545 && echo "8545 open") \
   || echo "FAIL"'
```

Expect: a non-loopback IP followed by `8545 open`. If you see
`FAIL` it's one of (in decreasing likelihood): Anvil bound to
`127.0.0.1` instead of `0.0.0.0`, a host firewall blocking the
bridge subnet, or the host running a Docker too old for
`host-gateway`.

> **RPC archive gotcha:** Anvil resolves contract state by querying
> the upstream RPC. Free providers without archive support reject
> those queries with `historical state … is not available`, which
> kills the audit before it can read a single `balanceOf`. Verify
> the upstream first: `cast call <ALEPH_TOKEN>
> "balanceOf(address)(uint256)" <ADDR> --rpc-url $YOUR_RPC` from the
> Anvil host. If it errors, switch to an archive-capable upstream
> (e.g. `https://eth.llamarpc.com`) or pin Anvil to an older block:
> `--fork-block-number $(( $(cast block-number --rpc-url $YOUR_RPC)
> - 32 ))`.

### Fork bootstrap (grant roles + seed balances)

After Anvil is up, the fork has the mainnet state at the chosen
block — which means the AlephPaymentProcessor still gates
`process()` by `ADMIN_ROLE`, and whatever USDC / ETH / ALEPH
balances were in the processor at the fork moment are what extract
will see. Two things you almost always need to do:

1. Give the signing address `ADMIN_ROLE` so `process()` doesn't
   revert with `AccessControlUnauthorizedAccount`.
2. Top up the processor with USDC, ETH, and ALEPH so all three
   token paths actually have something to sweep (otherwise extract
   just prints `skipped=zero_balance` and you've verified nothing).

The helper script `scripts/fork-bootstrap.sh` automates both via
`cast rpc anvil_impersonateAccount` + `anvil_setBalance`. Pick the
scenario that matches how you'll sign:

**Scenario A — no production pkey (sign as Anvil dev account #0):**

```bash
scripts/fork-bootstrap.sh --mode extract
# then later, in the docker compose run:
#   -e payment_processor_admin_pkey="0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
```

The script grants `ADMIN_ROLE` to `0xf39F…2266` (Anvil dev #0) by
impersonating the configured admin (no real pkey involved), then
funds the processor.

**Scenario B — you have the real admin pkey, just need the
balances seeded:**

```bash
scripts/fork-bootstrap.sh --mode extract \
  --signer 0xC870B0Ca4B3d65f33E2a3c732ab3cD2aE555b14E
# the role grant becomes a no-op because the admin already holds it;
# only the USDC / ETH / ALEPH funding happens.
```

**Scenario C — throwaway address you generated for testing:**

```bash
scripts/fork-bootstrap.sh --mode extract --signer 0xMY_TEST_ADDR
```

**Scenario D — mainnet-state verification (no bootstrap):**

When the goal is "show me what the next `--act` run would do
*right now*, against the real on-chain state" — i.e. you don't
want synthetic balances or a granted role muddying the picture.

```bash
scripts/fork-bootstrap.sh --mode none \
  --signer 0xC870B0Ca4B3d65f33E2a3c732ab3cD2aE555b14E
```

The script confirms Anvil is up, prints a "no mutation" banner, and
exits. The fork stays exactly as mainnet was at the fork block:
the configured admin holds `ADMIN_ROLE`, the processor's balances
are whatever they actually were on chain. **You must use the real
production pkey** for the nodestatus run (`-e
payment_processor_admin_pkey=<real_admin_pkey>`); any other signer
would revert exactly as it would in mainnet, which is the *whole
point* of this mode but easy to misread as a bug. The script
prints a warning if it sees `--mode none` combined with the Anvil
dev account because that combination is almost always an oversight.

All four are idempotent — re-running against the same fork only
re-funds and re-grants (or, for `none`, does nothing). Use
`--mode all` instead of `--mode extract` to bootstrap distribute at
the same time (covered in Phase 4.5).

### Run (another terminal: nodestatus inside docker)

```bash
docker compose run --rm --no-deps \
  -e ethereum_api_server="http://host.docker.internal:8545" \
  -e payment_processor_admin_pkey="0x<admin_pkey>" \
  --entrypoint /usr/local/bin/nodestatus-extract-credits \
  nodestatus-extract --dry-run \
    --fork-rpc=http://host.docker.internal:8545 \
    --reconcile-bps=200 -v
```

The CLI hard-refuses if any of these is true (so the foot-guns are
real refusals, not warnings):

- `--fork-rpc` is combined with `--act`
- `--fork-rpc` is set without `--dry-run`
- the fork URL is not `localhost` / `127.0.0.1` / `host.docker.internal`

`--reconcile-bps` is the maximum acceptable delta between realized
`alephReceived` and the quoter's `expected_out`, in basis points.
Default 200 (2%). Per-token: if the realized output is more than
±2% off from what the quoter predicted, the run exits **non-zero**
and the summary marks the token `✗RECONCILE_FAIL`. `--reconcile-bps`
can also be set via the env var `extract_max_reconcile_delta_bps`.

`extract_fork_rpc` can be put in `.env.extract` for convenience —
the CLI flag wins when both are set. The hard guard against `--act`
applies to **either** source.

### What gets printed (per executed token)

```
=== Extract tx audit: USDC === PASS
  built_tx:
    from    : 0xC870…cB14E
    to      : 0x6b55…cB8B
    fn      : process(
                token    = 0xA0b8…eB48,
                amountIn = 582330000,
                minOut   = 27549289165794598496994,
                ttl      = 1800s)
    gas     : 250000
    nonce   : 42
    chainId : 1
  receipt:
    tx_hash : 0xabc…def (FORK)
    status  : 1
    block   : 19,482,103
    gas_used: 234,812
  TokenPaymentsProcessed:
    amount              : 582330000        (582.330000 input)
    swapAmount          : 553213500        (553.213500 input)
    alephReceived       : 27827564813933937875752  (27827.564813 ALEPH)
    amountToDistribution: 27827564813933937875752  (27827.564813 ALEPH)
    amountToDevelopers  : 29116500         (29.116500 input)
    amountBurned        : 0                (0.000000 ALEPH)
    swapVersion         : 4
    isStable            : True
  reconciliation:
    quoter expected_out : 27827564813933937875752  (27827.564813 ALEPH)
    quoter min_out      : 27549289165794598496994  (27549.289166 ALEPH)
    realized            : 27827564813933937875752  (27827.564813 ALEPH)
    realized vs expected: +0 bps (+0.0000%) ✓ (limit ±200 bps)
    realized vs min_out : +101 bps (+1.0101%) ✓
```

ALEPH passthrough (no swap) is audited too: `swapAmount=0`,
`alephReceived == amount`, `delta_expected_bps == 0`.

**Pass criteria:**

- Mode banner: `Mode: DRY-RUN (FORK)`.
- Per-token block ends with `=== … === PASS`.
- Summary line shows `realized=… Δexp=±Nbps` per executed token.
- Footer reads `no errors` and there is no
  `!!! RECONCILIATION FAILED` banner.
- Exit code is `0`.

If a token fails (e.g. `Δexp=-2218bps`), that's the same price-impact
problem as the deviation-skip path: thin liquidity makes the realized
output land outside the tolerance. Decide before Phase 2 whether to
(a) raise `--reconcile-bps`, (b) raise `process_slippage_bps`, (c)
update the on-chain `swap_config`, or (d) hold this token off the
process list until liquidity improves.

---

## Phase 2 — Extract one-shot mainnet `--act`

Broadcasts one `process()` per token with a non-zero balance. Pkey
preflight (`credit_extraction.py:46-71`) verifies the admin holds
≥ `process_gas_ceiling × 50 gwei × len(process_tokens)` ETH; if not,
a WARNING is logged and the run still proceeds.

```bash
docker compose run --rm --no-deps \
  -e ethereum_api_server="https://<your-rpc>" \
  -e payment_processor_admin_pkey="0x<admin_pkey>" \
  --entrypoint /usr/local/bin/nodestatus-extract-credits \
  nodestatus-extract --act --immediate -v
```

`--immediate` skips the 0–3540s anti-MEV jitter so the run is
predictable. Drop it once you trust the flow.

**Pass criteria:**

- Mode banner: `Mode: LIVE EXTRACT`.
- Each non-zero-balance token shows `tx_hash=0x…`.
- Footer reads `no errors`.
- Look the tx hashes up on Etherscan — every receipt should have
  `status: 1` (success). If you see `status: 0` an on-chain revert
  slipped past the simulation; do **not** proceed to cron until you
  understand why (typically: pool moved between simulate and
  broadcast, slippage too tight, TTL too short).

---

## Phase 3 — Extract cron mode

Same flags as Phase 2 but driven by the in-container crontab.

```bash
# .env.extract should contain payment_processor_admin_pkey and
# ethereum_api_server, optionally process_slippage_bps overrides.
cp .env.extract.example .env.extract        # only if you haven't yet
$EDITOR .env.extract
docker compose up -d --build nodestatus-extract
docker compose logs -f nodestatus-extract
```

The image installs:

```
0 * * * * cd /aleph-nodestatus \
  && /usr/local/bin/nodestatus-extract-credits --act -v \
  >> /logs/credits-extract.log
```

Tail `logs/credits-extract.log` on the host (bind-mounted) for hourly
output. Each run goes through 0–59 minutes of jitter, so don't expect
the actual broadcast at HH:00.

---

## Phase 4 — Distribute dry-run

Computes the full reward set but does not publish or transfer.

```bash
docker compose run --rm --no-deps \
  -e ethereum_api_server="https://<your-rpc>" \
  --entrypoint /usr/local/bin/nodestatus-distribute-credits \
  nodestatus-dist --dry-run -v
```

**Pass criteria:**

- `Mode: DRY-RUN`.
- The dumped JSON contains a non-empty `rewards: {…}` map.
- `--no-publish / dry-run: skipping Aleph post` appears (expected).
- `--no-transfer / dry-run: skipping batchTransfer` appears (expected).
- The summary at the end lists batches with sane recipient counts.
- The `=== rewards_detailed ===` and `=== source item_hashes ===`
  blocks are non-empty for the windows you expect.

If recipient counts or total ALEPH look wrong, this is the moment to
sanity-check against `aleph-api-credit` (see also the NS vs api-credit
note — API > NS on recent windows is usually expected, not a bug).

If the cadence guard blocks the first ever run, append `--force`. The
guard exists to prevent double-distribution within
`credit_dist_min_interval_blocks` (~10 days); on a cold start there is
no prior distribution to clash with.

---

## Phase 4.5 — Distribute fork-verified dry-run (optional but recommended)

Same as Phase 4 except every `batchTransfer` is signed, broadcast,
and confirmed against a **local Anvil fork**, with every Transfer
event reconciled against the calculated per-recipient amount. This
is the only mode that exercises the full transfer code path
(`transfer_tokens_audited`, gas estimation, nonce sequencing, mining
order across batches) without writing anything to mainnet. Catches:

- Per-recipient drift (a recipient gets the wrong amount).
- Unexpected transfers (a Transfer event lands on an address not in
  the batch — which would only happen if the token contract grew a
  hook between deploy and now).
- Reverted batches (the fork mines status=0, fails the audit cleanly
  instead of leaving the run in an ambiguous state).

### Setup (one terminal: Anvil + distribute bootstrap, single command)

```bash
scripts/fork-bootstrap.sh --mode distribute
```

Same one-command flow as Phase 1.5 — the script reads
`ethereum_api_server` from `.env.dist` / `.env.extract` / `.env`,
launches Anvil if it isn't already up, and tops up the signer
with ALEPH. The Linux host-gateway gotcha (see Phase 1.5), the
pre-flight `getent hosts host.docker.internal` check, and the
upstream-archive caveat apply equally here — `extra_hosts` in
`docker/docker-compose.yml` already handles the alias for the
`nodestatus-dist` service.

If you're already running Anvil for the extract verification, pass
`--no-anvil` to reuse it:

```bash
scripts/fork-bootstrap.sh --mode distribute --no-anvil
```

### Fork bootstrap (top up signer with ALEPH)

`batchTransfer` requires the signer to actually hold the ALEPH it's
transferring. Anvil's dev account #0 has zero ALEPH on mainnet;
even your throwaway test address won't. Run the same helper script
in distribute mode:

**Scenario A — no production pkey (sign as Anvil dev account #0):**

```bash
scripts/fork-bootstrap.sh --mode distribute
# then later:
#   -e ethereum_pkey="0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
```

Impersonates the configured `distribution_recipient` on the fork
and transfers 1,000,000 ALEPH to Anvil dev #0 (enough for any
realistic distribution batch).

**Scenario B — you have the real distribution-recipient pkey:**

```bash
scripts/fork-bootstrap.sh --mode distribute \
  --signer 0x3a5CC6aBd06B601f4654035d125F9DD2FC992C25
# ALEPH top-up becomes a no-op — the recipient's mainnet balance
# is inherited by the fork. Only the gas top-up runs.
```

**Scenario C — throwaway address:**

```bash
scripts/fork-bootstrap.sh --mode distribute --signer 0xMY_TEST_ADDR
```

**Scenario D — mainnet-state verification (no bootstrap):**

Same as the Phase 1.5 Scenario D: skip funding so the
distribution recipient's actual mainnet ALEPH balance is what
gets exercised. Use the real `ethereum_pkey` of
`distribution_recipient` for the nodestatus run.

```bash
scripts/fork-bootstrap.sh --mode none \
  --signer 0x3a5CC6aBd06B601f4654035d125F9DD2FC992C25
```

If you're verifying both phases in the same Anvil session, do one
combined pass:

```bash
scripts/fork-bootstrap.sh --mode all [--signer 0x…]
```

### Run (another terminal: nodestatus inside docker)

```bash
docker compose run --rm --no-deps \
  -e ethereum_api_server="http://host.docker.internal:8545" \
  -e ethereum_pkey="0x<distribution_signer_pkey>" \
  --entrypoint /usr/local/bin/nodestatus-distribute-credits \
  nodestatus-dist --dry-run \
    --fork-rpc=http://host.docker.internal:8545 \
    --reconcile-bps=0 -v
```

The CLI refuses (same guards as the extract variant):

- `--fork-rpc` combined with `--act`
- `--fork-rpc` combined with `--testnet`
- `--fork-rpc` without `--dry-run`
- URL outside `{localhost, 127.0.0.1, host.docker.internal}`

`--reconcile-bps=0` is the default and means **exact-wei matching** —
the off-chain reward map produces integer wei amounts, the Transfer
events must echo them exactly. Any non-zero delta fails the batch.
Raise this only if you knowingly accept rounding drift (very rare).

`distribute_fork_rpc` can live in `.env.dist` for convenience — the
CLI flag wins when both are set. The hard guard against `--act` and
`--testnet` applies to either source.

### What gets printed (per batch)

```
Batch 1: transferring to 50 recipients (nonce=7)

=== Distribute tx audit: Batch 1 (50 recipients) === PASS
  built_tx:
    from    : 0x3a5C…2C25
    to      : 0x2770…F628 (ALEPH token)
    fn      : batchTransfer(
                recipients = [50 addresses],
                amounts    = [50 uint256, sum=12_345_678_…_000 wei])
    gas     : 1_030_000
    nonce   : 7
    chainId : 1
  sample (first 3 + last):
    0xabc…001  →  100_000_000_000_000_000_000 wei  (100.000000 ALEPH)
    0xabc…002  →   75_000_000_000_000_000_000 wei  (75.000000 ALEPH)
    0xabc…003  →   12_500_000_000_000_000_000 wei  (12.500000 ALEPH)
    … 46 more …
    0xabc…050  →    1_337_000_000_000_000_000 wei  (1.337000 ALEPH)
  receipt:
    tx_hash : 0xfeed…beef (FORK)
    status  : 1
    block   : 19_482_103
    gas_used: 987_654
  Transfer events:
    matching ALEPH transfers from sender: 50
    total expected (wei): 12_345_678_901_234_567_890_000
    total realized (wei): 12_345_678_901_234_567_890_000
    delta         (wei): 0
  reconciliation:
    per-recipient matched: 50/50 (limit ±0 bps)
```

If a batch fails, mismatched recipients are listed (first 5) along
with any "unexpected" Transfers (recipients not in the batch).
At end of run a fork audit summary aggregates pass/fail across
batches and the process exits non-zero.

**Pass criteria:**

- Mode banner: `Mode: DRY-RUN (FORK)`.
- Every batch ends with `=== … === PASS`.
- `=== Distribute fork audit summary ===` reports `failed: 0`.
- Exit code is `0`.

This is the highest-confidence way to verify the calculator's
recipient list and amounts match what the chain will actually
transfer, without touching mainnet.

---

## Phase 5 — Distribute testnet one-shot

Publishes the calculation post to the Aleph **testnet** API
(`https://api.twentysix.testnet.network` by default). The `not
is_testnet` gate at `commands.py:571` keeps the on-chain
`batchTransfer` off. The Aleph post is still signed by
`ethereum_pkey`, so the key is required — use a throwaway one if you
don't want the address tied to the mainnet signer.

```bash
docker compose run --rm --no-deps \
  -e ethereum_api_server="https://<your-rpc>" \
  -e ethereum_pkey="0x<testnet_signer_pkey>" \
  --entrypoint /usr/local/bin/nodestatus-distribute-credits \
  nodestatus-dist --testnet -v
```

Optional override if you target a different testnet node:

```bash
  -e aleph_testnet_api_server="https://<other-testnet>"
```

Optional tag isolation so the testnet post doesn't share the
`mainnet` tag bucket:

```bash
  -e filter_tag="testnet-rc"
```

**Pass criteria:**

- `Mode: TESTNET (calculation)`.
- No `--no-transfer` line (the gate is `not is_testnet`, not the
  flag; transfer is silently skipped because testnet wins).
- Post lands on the testnet API — verify with
  `aleph-client message find --channel <…>` against the testnet
  endpoint, or open the testnet explorer and look for a `POST` of
  type `credit-distribution` signed by your testnet signer.

---

## Phase 6 — Distribute one-shot mainnet `--act`

This is the first real on-chain distribution. The signer address must
hold the full per-period ALEPH outflow before this command runs;
preflight (`commands.py` step 4) aborts with
`ABORT: owed N ALEPH > balance M at <sender>` if it doesn't.

```bash
docker compose run --rm --no-deps \
  -e ethereum_api_server="https://<your-rpc>" \
  -e ethereum_pkey="0x<distribution_signer_pkey>" \
  --entrypoint /usr/local/bin/nodestatus-distribute-credits \
  nodestatus-dist --act -v
```

If this is the very first run and the cadence guard refuses, add
`--force` (see Phase 4 note).

**Pass criteria:**

- `Mode: LIVE DISTRIBUTION`.
- `Executing batchTransfer …` per batch, each with a tx hash.
- No `=== N batch(es) failed === ` block at the end. If batches fail,
  the published distribution post has the recipient lists; manually
  retransfer the failing recipients and AMEND the post (the runbook
  for that lives in `docs/operations/` once it's written).
- The post lands on mainnet Aleph API with `status="distribution"`.

After this completes, the cadence guard now has a real prior
distribution to anchor against — the cron in Phase 7 can run safely.

---

## Phase 7 — Distribute cron mode

Existing `nodestatus-dist` compose service is hard-wired to
`--act` and runs at midnight every 10 days. Once Phase 6 has
succeeded, simply bring it up:

```bash
cp .env.dist.example .env.dist              # only if you haven't yet
$EDITOR .env.dist
docker compose up -d --build nodestatus-dist
docker compose logs -f nodestatus-dist
```

Installed crontab:

```
0 0 */10 * * cd /aleph-nodestatus \
  && /usr/local/bin/nodestatus-distribute-credits --act -v \
  >> /logs/credits-dist.log
```

The app's cadence guard (`credit_dist_min_interval_blocks`, default
~10 days) is a second backstop; even if cron mis-fires, a run too
close to the previous one logs and skips.

---

## Env var reference

`Settings` uses pydantic-settings with `env_file=".env"`, no prefix,
no nested delimiter, case-insensitive. Env var names == field names
verbatim.

### Extract (`nodestatus-extract`)

| Var | Required for | Notes |
|---|---|---|
| `payment_processor_admin_pkey` | `--act` | Preferred. Must derive `payment_processor_admin_address` (default `0xC870…cB8B`). |
| `ethereum_pkey` | `--act` fallback | Used only if `payment_processor_admin_pkey` is empty — orchestrator logs a WARNING. |
| `ethereum_api_server` | dry-run + act | RPC URL. |
| `process_slippage_bps` | optional | Default 100 (1%). See `docs/operations/slippage.md`. |
| `extract_random_delay_max_seconds` | optional | Default 3540s anti-MEV jitter; bypass with `--immediate` or `--dry-run`. |
| `extract_fork_rpc` | optional (Phase 1.5) | URL of a local Anvil fork. Enables fork-verified dry-run. CLI flag `--fork-rpc` overrides. Refused with `--act`. |
| `extract_max_reconcile_delta_bps` | optional (Phase 1.5) | Default 200 (2%). Max allowed delta between realized and quoter `expected_out` in fork mode. CLI flag `--reconcile-bps` overrides. |

### Distribute (`nodestatus-dist`)

| Var | Required for | Notes |
|---|---|---|
| `ethereum_pkey` | `--testnet`, `--act` | Signs the Aleph post in both modes; also signs `batchTransfer` in `--act`. |
| `ethereum_api_server` | all modes | RPC URL; reads on-chain state even on testnet. |
| `aleph_testnet_api_server` | optional override (testnet) | Default `https://api.twentysix.testnet.network`. |
| `filter_tag` | optional | Default `"mainnet"`. Override to e.g. `"testnet-rc"` to isolate testnet posts. |
| `distribute_fork_rpc` | optional (Phase 4.5) | URL of a local Anvil fork. Enables fork-verified dry-run. CLI flag `--fork-rpc` overrides. Refused with `--act` or `--testnet`. |
| `distribute_max_reconcile_delta_bps` | optional (Phase 4.5) | Default 0 (exact wei match). Per-recipient max delta vs calculated reward in fork mode. CLI flag `--reconcile-bps` overrides. |

---

## Abort / rollback notes

- **Phases 1, 1.5, 4, 4.5, 5:** abort by Ctrl-C. Nothing on mainnet
  happened. Fork phases (1.5, 4.5) leave state on the Anvil fork
  which evaporates when you kill Anvil; testnet post (Phase 5)
  stays on the testnet API but is harmless (clearly tagged
  `status="calculation"`).
- **Phase 2:** once a `process()` tx is broadcast it cannot be
  unbroadcast. Subsequent retries with `--act` are idempotent for
  zero-balance tokens, so re-running after partial success only
  re-sweeps the tokens that weren't drained yet.
- **Phase 6:** same — once `batchTransfer` is broadcast it's
  on-chain. The published distribution post is the canonical record;
  if a batch fails, the `=== N batch(es) failed ===` block lists them
  and the runbook for manual retransfer + AMEND applies.
- **Phases 3, 7:** stop the cron at any time with
  `docker compose stop nodestatus-{extract,dist}`. Pending crontab
  entries die with the container.
