# Anti-MEV protections for the extract CLI — Phase 1

**Date:** 2026-05-18
**Status:** Proposed
**Author:** amalcaraz
**Phase:** 1 of 2 (Phase 2 covers TWAP)

## Problem

`nodestatus-extract-credits` runs hourly via cron at a fixed minute. A
bot watching the schedule can pre-position liquidity in the
AlephPaymentProcessor's swap pools immediately before the hourly tx and
sandwich the swap (`process()` call) for profit. The existing
`process_slippage_bps` (default 1%) limits the worst-case in-flight
sandwich loss but does not detect manipulation that's *already happened*
when the extract starts.

Two gaps:

1. **Predictable timing.** Top-of-hour cron tells the bot exactly when
   to position.
2. **No pre-execution price sanity check.** The orchestrator quotes via
   the configured Uniswap path and trusts the quote. If a bot has
   already pumped/dumped that path's pools, the quote is bad and the
   swap executes at a bad rate (still within slippage tolerance).

## Goal — Phase 1

Two independent protections, both shipped together:

1. **Random execution delay (anti-tracking).** The hourly cron fires at
   the top of every hour, but the CLI sleeps a uniformly random
   `[0, extract_random_delay_max_seconds]` interval before doing
   anything. The bot can no longer schedule sandwich liquidity for a
   specific block.
2. **Spot-vs-Chainlink deviation guard (anti-pre-manipulation).** Before
   the swap quote is requested, for each hop in the configured swap
   path that has Chainlink USD feeds for both sides, compare the pool's
   spot price to the Chainlink-implied price. If the deviation exceeds
   `extract_max_deviation_bps` (default 200 bps = 2%), skip that token
   with `skipped_reason="price_deviation"`. Other tokens proceed
   normally.

Phase 2 (separate spec, later) replaces the spot-vs-Chainlink check
with a proper TWAP check — for V3 via the pool's native `observe()`,
for V4 via the pool's oracle hook (when standardized). Phase 2 also
covers hops with no Chainlink feed (notably `X → ALEPH`).

## Non-goals

- TWAP from any source. Phase 1 reads spot only.
- Replacing or modifying `process_slippage_bps`. Existing slippage
  protection stays as-is (default 100 bps).
- Anti-MEV protection for hops where ALEPH is one side (no Chainlink
  feed). Best-effort skip with INFO log; Phase 2 closes this.
- Aleph-side publishing — extract still does not publish, as established
  in the prior spec.
- Adding a wrapper script. The delay lives in the Python CLI; the cron
  line stays single-line and bypass is via `--immediate`.

## Design

### 1. Random execution delay

**Settings (`settings.py`):**
```python
extract_random_delay_max_seconds: int = 3540   # 0..59 min uniform
```

**CLI (`commands.py`):** new `extract_credits` flag:
```
--immediate    Skip the random anti-MEV delay (for manual retries
               and tests). --dry-run implies --immediate.
```

**Orchestrator (`credit_extraction.py`):** in
`process_credit_extraction`, after admin/preflight resolution and
before invoking `extract_aleph`:

```python
if not dry_run and not immediate and settings.extract_random_delay_max_seconds > 0:
    delay = random.randint(0, settings.extract_random_delay_max_seconds)
    click.echo(f"Sleeping {delay}s before extract (anti-MEV jitter)…")
    await asyncio.sleep(delay)
```

`process_credit_extraction` grows an `immediate: bool = False` keyword.
The Click command forwards `immediate=immediate or dry_run`.

Setting `extract_random_delay_max_seconds=0` disables jitter (useful in
local/test environments).

### 2. Spot-vs-Chainlink deviation guard

**New module `src/aleph_nodestatus/pool_oracle.py`:**

```python
@dataclass
class OracleResult:
    ok: bool
    reason: Optional[str]     # "price_deviation" | "chainlink_stale"
                              # | "chainlink_invalid" | "pool_read_failed"
                              # | None
    deviation_bps: Optional[int]
    spot_price: Optional[float]
    ref_price: Optional[float]

def check_swap_price_deviation(
    w3, swap_config: dict, token_in: str,
) -> OracleResult: ...
```

The function reads `swap_config["v"]` (2/3/4) and dispatches:

- **V2** (`swap_config["v2"]` is a list of token addresses): for each
  consecutive pair `(tokenA, tokenB)` in the path,
  compute the pool address via `IUniswapV2Factory.getPair` (or read
  from the existing router), then `pair.getReserves()` → spot.
- **V3** (`swap_config["v3"]` is the encoded bytes path
  `address(20) | fee(3) | address(20) | fee(3) | ...`): decode into a
  list of hops `(tokenA, fee, tokenB)`. For each hop,
  `IUniswapV3Factory.getPool(tokenA, tokenB, fee)` → `pool.slot0()` →
  `sqrtPriceX96` → spot price (with token decimal adjustment).
- **V4** (`swap_config["v4"]` is a list of `PathKey` tuples
  `(intermediateCurrency, fee, tickSpacing, hooks, hookData)`): for each
  hop, derive `PoolKey` (sorting tokens lexicographically), compute
  `poolId = keccak256(abi.encode(PoolKey))`, then
  `IUniswapV4StateView.getSlot0(poolId)` → `sqrtPriceX96` → spot.

**Spot price from `sqrtPriceX96`:** `price = (sqrtPriceX96 / 2**96)**2`,
then adjusted for token0/token1 decimals. Helper `_sqrt_price_to_price`
factored out for testability.

**Chainlink reference price per hop:**
- Lookup `chainlink_usd_feeds[tokenA]` and `chainlink_usd_feeds[tokenB]`
  in settings.
- If either is absent → log INFO `"skipping hop A→B (no chainlink
  feed)"`, continue to next hop. Last hop of a path that's `X → ALEPH`
  is the canonical case.
- For both feeds present, call `feed.latestRoundData()` →
  `(roundId, answer, startedAt, updatedAt, answeredInRound)`. Reject if
  `answer <= 0` (`chainlink_invalid`) or `block.timestamp - updatedAt
  > chainlink_max_age_seconds` (`chainlink_stale`).
- `ref_price_a_per_b = (feed_a.answer / 10**feed_a.decimals) / (feed_b.answer / 10**feed_b.decimals)`.

**Deviation check:**
- `deviation_bps = abs(spot - ref) / ref * 10_000`
- If `deviation_bps > extract_max_deviation_bps` (200) →
  `OracleResult(ok=False, reason="price_deviation", deviation_bps=…,
  spot=…, ref=…)` and the outer loop short-circuits — no need to check
  remaining hops.

If every hop passes (or is best-effort skipped), return
`OracleResult(ok=True, …)`.

**Pool/oracle read errors (RPC failure, pool not deployed at the
derived address, slot0/getSlot0 revert):** the function logs WARNING
with the underlying exception and returns
`OracleResult(ok=False, reason="pool_read_failed", …)`. Conservative
skip — if we can't verify, we don't swap. Operationally recoverable on
the next hourly run.

**No hops have feeds at all** (e.g., a hypothetical path with only
unmapped tokens): every hop best-effort skips; the function returns
`ok=True` with `deviation_bps=None`. The slippage tolerance is the only
in-flight protection — this is the Phase 2 gap that TWAP will close.

### 3. Integration in `payment_processor.extract_aleph`

After `cfg = _swap_config_to_dict(swap_config)` (around line 321),
before `expected_out = quote_amount_out(...)`:

```python
oracle = check_swap_price_deviation(w3, cfg, token_in=token)
if not oracle.ok:
    entry["skipped_reason"] = oracle.reason
    entry["oracle"] = {
        "deviation_bps": oracle.deviation_bps,
        "spot_price":    oracle.spot_price,
        "ref_price":     oracle.ref_price,
    }
    continue
```

`_print_summary` in `credit_extraction.py` learns one new branch:
```python
if entry.get("skipped_reason") == "price_deviation":
    o = entry.get("oracle", {})
    line += f" skipped=price_deviation Δ={o.get('deviation_bps')}bps"
```
(Other `skipped_reason` values keep the existing rendering.)

### 4. New ABIs (minimal, only the calls we make)

- `abi/IUniswapV3Pool.json` — `slot0()` only
- `abi/IUniswapV3Factory.json` — `getPool(address,address,uint24)`
  only
- `abi/IUniswapV4StateView.json` — `getSlot0(bytes32)` only
- `abi/ChainlinkAggregator.json` — `latestRoundData()`, `decimals()`
- `abi/IUniswapV2Pair.json` — already in `abi/`, reused

### 5. New settings (`settings.py`)

```python
# Random delay
extract_random_delay_max_seconds: int = 3540

# Spot-vs-Chainlink deviation guard
extract_max_deviation_bps: int = 200
chainlink_max_age_seconds:  int = 3600

# Contracts
uniswap_v3_factory_address:    str = "0x1F98431c8aD98523631AE4a59f267346ea31F984"
uniswap_v4_state_view_address: str = "0x7fFE42C4a5DEeA5b0feC41C94C136Cf115597227"

# Chainlink USD feed addresses keyed by lowercase token address.
# ETH sentinel (0x000…000) maps to the ETH/USD feed. ALEPH has no feed
# (Phase 1 best-effort skips that hop).
chainlink_usd_feeds: Dict[str, str] = {
    "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48": "0x8fFfFfd4AfB6115b954Bd326cbe7B4BA576818f6",  # USDC/USD
    "0x0000000000000000000000000000000000000000": "0x5f4eC3Df9cbd43714FE2740f5E3616155c5b8419",  # ETH/USD (sentinel — settings.process_tokens uses this)
    "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2": "0x5f4eC3Df9cbd43714FE2740f5E3616155c5b8419",  # WETH/USD (same feed; swap paths use WETH on the pool side)
}
```

Both ETH-sentinel and WETH map to the same ETH/USD feed because the
processor's `process_tokens` list refers to ETH via the sentinel
(`0x000…000`) while the on-chain swap path encodes WETH
(`0xC02a…6cc2`). Either appearing in a hop resolves to the same
Chainlink feed.

### 6. Operator UX

`docker/.env.extract.example` gains an optional override block:
```bash
# Anti-MEV (Phase 1) — uncomment to override defaults
# extract_random_delay_max_seconds=3540
# extract_max_deviation_bps=200
# chainlink_max_age_seconds=3600
```

The cron line in `docker-compose.yml` does NOT change. The delay lives
in the CLI. A test invocation in dry-run is unchanged:

```
docker compose -f docker/docker-compose.yml run --rm --no-deps \
  nodestatus-extract \
  nodestatus-extract-credits --dry-run -v
```

For an **immediate manual** retry of `--act` (bypassing the random
delay):
```
docker compose -f docker/docker-compose.yml run --rm --no-deps \
  nodestatus-extract \
  nodestatus-extract-credits --act --immediate -v
```

## Trade-offs

**Why spot-vs-Chainlink instead of TWAP for Phase 1.** TWAP requires
either V3 pool `observe()` (works only for V3 paths, and current paths
are V4) or a V4 oracle hook (not standardized, not present in current
mainnet pools). Spot-vs-Chainlink is path-agnostic and ships now;
Phase 2 adds true TWAP where available.

**Why not raise `process_slippage_bps`.** The slippage tolerance bounds
in-flight movement between simulate and execute. Random delay is
external to that window — once we wake up and start simulating, the
existing 100 bps still applies. Raising slippage would *loosen* a
protection, not tighten it.

**Why "skip token" not "abort run".** Confirmed in design discussion:
ETH being volatile or one Chainlink feed lagging should not block
USDC/ALEPH extraction. A skipped token retries next hour. Per-token
isolation = continuity.

**Why ALEPH-side hop is best-effort.** ALEPH has no Chainlink USD feed.
A cross-pool reference (e.g., a different DEX) would introduce its own
manipulation surface. Phase 2's TWAP-based check is the right tool
here. In the meantime `process_slippage_bps` covers in-flight sandwich
on this hop.

**Why store the chainlink_usd_feeds in settings as a dict.** The
operator may need to override per-deployment (e.g., testnet feeds,
different stable used). Dict in settings is overridable via env (pydantic
parses JSON). Hardcoding would force a code change.

## Risks

- **Chainlink feed downtime / staleness.** If a feed stalls beyond
  `chainlink_max_age_seconds`, the corresponding token is skipped. On a
  prolonged Chainlink outage (multi-hour), no extractions happen until
  the feed recovers. Acceptable — better safe than sorry. Mitigation: an
  ops alert if `skipped_reason == "chainlink_stale"` appears in the log.
- **False positives during legitimate market volatility.** If ETH/USD
  moves > 2% within Chainlink's heartbeat window (~1h on the mainnet
  ETH/USD aggregator), spot and Chainlink may diverge legitimately. The
  next hour's extract retries; in volatile markets a few hours may pass
  without extraction. Acceptable for hourly cadence with no hard
  deadline.
- **V4 PoolKey layout drift.** If Uniswap V4 changes the PoolKey struct
  (or the StateView API) before mainnet stabilization, `getSlot0` calls
  start failing. ABI is vendored in `abi/IUniswapV4StateView.json` so a
  contract upgrade requires re-vendoring. Same risk profile as the
  existing V4 quoter ABI.

## Tests

**`tests/test_pool_oracle.py` (new):**
- `_sqrt_price_to_price` returns correct price with various decimal
  pairs (USDC 6 / WETH 18, etc.).
- V3 path bytes decoder splits `address|fee|address|fee|address` into
  the expected hop list.
- V4 PoolKey → poolId derivation matches a fixture vector.
- `check_swap_price_deviation` end-to-end with mocked pool reads and
  Chainlink reads:
  - All hops have feeds, all within threshold → `ok=True`.
  - One hop exceeds threshold → `ok=False, reason="price_deviation"`.
  - One side of a hop has no feed → that hop best-effort skipped;
    if remaining hops pass → `ok=True`.
  - Chainlink stale → `ok=False, reason="chainlink_stale"`.
  - Chainlink `answer <= 0` → `ok=False, reason="chainlink_invalid"`.
  - `pool.slot0()` raises (RPC error / pool address is `0x0…0`) →
    `ok=False, reason="pool_read_failed"`.
  - All hops best-effort skipped (no feeds anywhere) → `ok=True,
    deviation_bps=None`.

**`tests/test_credit_extraction.py` (extended):**
- New test: random sleep is called by default for `act=True,
  dry_run=False, transfer=True, immediate=False`. (Mock `random.randint`
  + `asyncio.sleep`.)
- New test: `immediate=True` bypasses the sleep.
- New test: `dry_run=True` bypasses the sleep (regardless of
  `immediate`).
- New test: `extract_random_delay_max_seconds=0` bypasses the sleep.

**`tests/test_extract_credits_cli.py` (extended):**
- `--immediate` flag is parsed and forwarded to the orchestrator.
- `--help` lists `--immediate`.

**`tests/test_payment_processor.py` (extended):**
- `extract_aleph` invokes `check_swap_price_deviation` after
  `getSwapConfig` and before `quote_amount_out`.
- When the oracle returns `ok=False`, the entry gets
  `skipped_reason=oracle.reason` and the swap is not attempted (no
  `quote_amount_out` call, no `simulate_process` call).
- When the oracle returns `ok=True`, normal flow proceeds.

## Migration / rollout

Single PR. No data migration. No flag-gated rollout — the random delay
and price check both ship enabled-by-default.

Operators can disable jitter via env: `extract_random_delay_max_seconds=0`.
They can loosen the deviation guard via
`extract_max_deviation_bps=<higher>` if false positives spike.

If the ops team wants stricter checks later (Phase 2 TWAP), that lands
as a separate spec/PR; the settings introduced here (notably
`extract_max_deviation_bps`) are reused.
