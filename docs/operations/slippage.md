# Slippage Settings

## Default: 100 bps (1%)

`settings.process_slippage_bps = 100` applies a 1% slippage tolerance to
the `min_out` parameter of every `process()` call the extract step makes.
The Uniswap router will revert if the realized output is less than
`expected_out * 0.99`.

## Why not tighter

ALEPH liquidity is thin enough that legitimate market drift over the
`ttl` window (default 1800s) can exceed 50 bps, especially for the
USDC -> ETH -> ALEPH path. Tighter than 100 bps risks reverting
otherwise-good extracts and stranding tokens on the processor.

## Why not looser

Every basis point of slippage tolerance is potential MEV-sandwich extraction.
`min_out` is visible in the broadcast tx; a searcher can sandwich up to
exactly that bound. 100 bps caps per-swap loss at 1%.

## Operator overrides

- Per-run: `nodestatus-distribute-credits --slippage-bps N`.
- Environment: `process_slippage_bps=N` in `docker/.env.dist`.

## Follow-up: private mempool

If observed sandwich activity or per-run drag pushes consistently above
0.5%, route `process()` through Flashbots Protect or MEV-blocker. Not
implemented today; see `docs/specs/2026-05-12-pr-12-review-fixes-design.md`
follow-ups section.
