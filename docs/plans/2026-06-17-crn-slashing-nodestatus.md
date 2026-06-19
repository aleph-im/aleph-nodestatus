# CRN inactivity slashing — nodestatus implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** In `aleph-nodestatus` credit distribution, withhold (slash) the reward share of CRNs inactive ≥ 3 days, recording the withheld amounts in a `slashed` structure and skipping their on-chain transfer, behind feature flags (default on).

**Architecture:** A new isolated `slashing.py` module holds the pure slashable predicate, a per-`(stream, crn_node_id)` accumulator, and the slashing pass. The three reward paths (`credit_revenue`, `holder_tier` via `_distribute_execution_credits`; `wage_subsidy` via `split_subsidy`) optionally feed the accumulator. After `merge_rewards`, the orchestrator runs the pass, publishes `rewards` intact plus `slashed`/`slashed_meta`, and transfers `final_rewards − slashed`.

**Tech Stack:** Python 3, pydantic-settings, pytest / pytest-asyncio. Tests live in `tests/`, run with `pytest` (`testpaths = tests`).

**Spec:** `docs/specs/2026-06-17-crn-slashing-design.md` (§2 predicate & modes, §3 nodestatus design).

---

## File structure

- **Create** `src/aleph_nodestatus/slashing.py` — pure predicate, `SlashAccumulator`, `compute_slashing`, `enabled_slash_streams`. One responsibility: deciding what gets slashed. No I/O.
- **Create** `tests/test_slashing.py` — unit tests for the module.
- **Modify** `src/aleph_nodestatus/settings.py` — new flags.
- **Modify** `src/aleph_nodestatus/credit_distribution.py` — thread accumulator + stream through `_distribute_execution_credits` / `_distribute_expense` / `_apply_expense_to` / `_apply_expenses_to_snapshots`; accept an optional accumulator in `compute_rewards` and the two `_compute_rewards_*` paths.
- **Modify** `src/aleph_nodestatus/wage_subsidy.py` — thread accumulator through `split_subsidy` / `compute_subsidy_daily`.
- **Modify** `src/aleph_nodestatus/commands.py` — build accumulator, run the pass after `merge_rewards`, compute `payout_rewards`, use it for balance + transfer, add `slashed`/`slashed_meta` to the published post.
- **Modify** `tests/test_distribute_credits_cli.py` (or a new `tests/test_slashing_integration.py`) — end-to-end + flag-off invariance.

---

## Task 1: Settings flags

**Files:**
- Modify: `src/aleph_nodestatus/settings.py:194-199`

- [ ] **Step 1: Add the slashing flags after the existing credit-dist feature flags**

In `settings.py`, immediately after line 199 (`credit_dist_publish_enabled`), add:

```python
    # === Slashing feature flags (CRN inactivity penalty) ===
    credit_dist_slash_enabled: bool        = True   # master kill switch
    credit_dist_slash_credit_revenue: bool = True
    credit_dist_slash_holder_tier: bool    = True
    credit_dist_slash_wage_subsidy: bool   = True
    credit_dist_slash_threshold_days: int  = 3
    credit_dist_slash_retroactive: bool    = False  # default: from-death window
```

- [ ] **Step 2: Verify settings import cleanly**

Run: `python -c "from aleph_nodestatus.settings import settings; print(settings.credit_dist_slash_threshold_days, settings.credit_dist_slash_retroactive)"`
Expected: `3 False`

- [ ] **Step 3: Commit**

```bash
git add src/aleph_nodestatus/settings.py
git commit -m "feat(slashing): add CRN inactivity slashing feature flags"
```

---

## Task 2: Slashing module — predicate

**Files:**
- Create: `src/aleph_nodestatus/slashing.py`
- Test: `tests/test_slashing.py`

- [ ] **Step 1: Write the failing test for `is_slashable`**

Create `tests/test_slashing.py`:

```python
from aleph_nodestatus.slashing import is_slashable

BLOCKS_PER_DAY = 7130


def test_not_slashable_when_active():
    assert is_slashable(
        None, close_height=1_000_000,
        threshold_days=3, blocks_per_day=BLOCKS_PER_DAY,
    ) is False


def test_not_slashable_below_threshold():
    inactive_since = 1_000_000
    close = inactive_since + 2 * BLOCKS_PER_DAY  # 2 days < 3
    assert is_slashable(
        inactive_since, close_height=close,
        threshold_days=3, blocks_per_day=BLOCKS_PER_DAY,
    ) is False


def test_slashable_at_exact_threshold():
    inactive_since = 1_000_000
    close = inactive_since + 3 * BLOCKS_PER_DAY  # exactly 3 days
    assert is_slashable(
        inactive_since, close_height=close,
        threshold_days=3, blocks_per_day=BLOCKS_PER_DAY,
    ) is True
```

- [ ] **Step 2: Run to verify it fails**

Run: `pytest tests/test_slashing.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'aleph_nodestatus.slashing'`

- [ ] **Step 3: Create the module with `is_slashable`**

Create `src/aleph_nodestatus/slashing.py`:

```python
"""CRN inactivity slashing — pure decision logic, no I/O.

A CRN is "slashable" when it has been inactive (score < 0.01, recorded as
`inactive_since` block height in the status snapshots) for at least
`threshold_days`, measured in ETH block height to mirror the existing 90-day
removal check in status.py.
"""
from .settings import settings

STREAMS = ("credit_revenue", "holder_tier", "wage_subsidy")


def is_slashable(inactive_since, close_height, *,
                 threshold_days=None, blocks_per_day=None):
    """True when a CRN with the given `inactive_since` (block height or None)
    has been inactive for >= threshold_days at `close_height`."""
    if inactive_since is None:
        return False
    if threshold_days is None:
        threshold_days = settings.credit_dist_slash_threshold_days
    if blocks_per_day is None:
        blocks_per_day = settings.ethereum_blocks_per_day
    return (close_height - inactive_since) >= threshold_days * blocks_per_day
```

- [ ] **Step 4: Run to verify pass**

Run: `pytest tests/test_slashing.py -v`
Expected: PASS (3 tests)

- [ ] **Step 5: Commit**

```bash
git add src/aleph_nodestatus/slashing.py tests/test_slashing.py
git commit -m "feat(slashing): add is_slashable predicate"
```

---

## Task 3: Slashing module — accumulator

**Files:**
- Modify: `src/aleph_nodestatus/slashing.py`
- Test: `tests/test_slashing.py`

- [ ] **Step 1: Write the failing test for `SlashAccumulator`**

Append to `tests/test_slashing.py`:

```python
from aleph_nodestatus.slashing import SlashAccumulator


def test_accumulator_tracks_total_and_post_death():
    acc = SlashAccumulator()
    # Two expenses for the same CRN: first while active, second while inactive.
    acc.add("credit_revenue", "crn1", "0xAddr1", 10.0,
            inactive_at_expense=False, inactive_since=None)
    acc.add("credit_revenue", "crn1", "0xAddr1", 5.0,
            inactive_at_expense=True, inactive_since=999)
    entries = acc.entries()
    e = entries[("credit_revenue", "crn1")]
    assert e["address"] == "0xAddr1"
    assert e["total"] == 15.0
    assert e["post_death"] == 5.0
    assert e["inactive_since"] == 999


def test_accumulator_separates_streams_and_nodes():
    acc = SlashAccumulator()
    acc.add("credit_revenue", "crn1", "0xA", 1.0,
            inactive_at_expense=True, inactive_since=1)
    acc.add("holder_tier", "crn1", "0xA", 2.0,
            inactive_at_expense=True, inactive_since=1)
    acc.add("credit_revenue", "crn2", "0xB", 4.0,
            inactive_at_expense=False, inactive_since=None)
    assert set(acc.entries().keys()) == {
        ("credit_revenue", "crn1"),
        ("holder_tier", "crn1"),
        ("credit_revenue", "crn2"),
    }
```

- [ ] **Step 2: Run to verify it fails**

Run: `pytest tests/test_slashing.py -v`
Expected: FAIL with `ImportError: cannot import name 'SlashAccumulator'`

- [ ] **Step 3: Add `SlashAccumulator` to the module**

Append to `src/aleph_nodestatus/slashing.py`:

```python
class SlashAccumulator:
    """Per-(stream, crn_node_id) ledger of CRN reward shares.

    `total` is the full window share (retroactive mode); `post_death` is the
    subset accrued while the routed snapshot already flagged the CRN inactive
    (from-death mode). `inactive_since` keeps the most recent non-None height
    seen, used only as a fallback when the close-of-window snapshot no longer
    contains the node (e.g. removed at day 90 mid-window).
    """

    def __init__(self):
        self._entries = {}

    def add(self, stream, node_id, address, amount, *,
            inactive_at_expense, inactive_since):
        if not node_id:
            return
        key = (stream, node_id)
        entry = self._entries.get(key)
        if entry is None:
            entry = {"address": address, "total": 0.0,
                     "post_death": 0.0, "inactive_since": inactive_since}
            self._entries[key] = entry
        entry["address"] = address
        entry["total"] += amount
        if inactive_at_expense:
            entry["post_death"] += amount
        if inactive_since is not None:
            entry["inactive_since"] = inactive_since

    def entries(self):
        return self._entries
```

- [ ] **Step 4: Run to verify pass**

Run: `pytest tests/test_slashing.py -v`
Expected: PASS (5 tests)

- [ ] **Step 5: Commit**

```bash
git add src/aleph_nodestatus/slashing.py tests/test_slashing.py
git commit -m "feat(slashing): add SlashAccumulator ledger"
```

---

## Task 4: Slashing module — `compute_slashing` pass and `enabled_slash_streams`

**Files:**
- Modify: `src/aleph_nodestatus/slashing.py`
- Test: `tests/test_slashing.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_slashing.py`:

```python
from aleph_nodestatus.slashing import compute_slashing

BPD = 7130


def _close_rnodes(node_id, inactive_since):
    return {node_id: {"inactive_since": inactive_since}}


def test_compute_slashing_from_death_uses_post_death():
    acc = SlashAccumulator()
    acc.add("credit_revenue", "crn1", "0xA", 10.0,
            inactive_at_expense=False, inactive_since=None)
    acc.add("credit_revenue", "crn1", "0xA", 4.0,
            inactive_at_expense=True, inactive_since=1_000)
    close = _close_rnodes("crn1", 1_000)
    slashed, meta = compute_slashing(
        acc, close, close_height=1_000 + 3 * BPD,
        enabled_streams=("credit_revenue",), retroactive=False,
        threshold_days=3, blocks_per_day=BPD,
    )
    assert slashed == {"0xA": 4.0}
    assert meta == [{"node_id": "crn1", "address": "0xA",
                     "stream": "credit_revenue", "amount": 4.0,
                     "inactive_since": 1_000}]


def test_compute_slashing_retroactive_uses_total():
    acc = SlashAccumulator()
    acc.add("credit_revenue", "crn1", "0xA", 10.0,
            inactive_at_expense=False, inactive_since=None)
    acc.add("credit_revenue", "crn1", "0xA", 4.0,
            inactive_at_expense=True, inactive_since=1_000)
    close = _close_rnodes("crn1", 1_000)
    slashed, _ = compute_slashing(
        acc, close, close_height=1_000 + 3 * BPD,
        enabled_streams=("credit_revenue",), retroactive=True,
        threshold_days=3, blocks_per_day=BPD,
    )
    assert slashed == {"0xA": 14.0}


def test_compute_slashing_skips_disabled_stream():
    acc = SlashAccumulator()
    acc.add("wage_subsidy", "crn1", "0xA", 4.0,
            inactive_at_expense=True, inactive_since=1_000)
    close = _close_rnodes("crn1", 1_000)
    slashed, meta = compute_slashing(
        acc, close, close_height=1_000 + 3 * BPD,
        enabled_streams=("credit_revenue",), retroactive=False,
        threshold_days=3, blocks_per_day=BPD,
    )
    assert slashed == {}
    assert meta == []


def test_compute_slashing_skips_not_slashable():
    acc = SlashAccumulator()
    acc.add("credit_revenue", "crn1", "0xA", 4.0,
            inactive_at_expense=True, inactive_since=1_000)
    close = _close_rnodes("crn1", 1_000)
    slashed, _ = compute_slashing(
        acc, close, close_height=1_000 + 2 * BPD,  # only 2 days
        enabled_streams=("credit_revenue",), retroactive=False,
        threshold_days=3, blocks_per_day=BPD,
    )
    assert slashed == {}


def test_compute_slashing_aggregates_addresses():
    acc = SlashAccumulator()
    acc.add("credit_revenue", "crn1", "0xA", 4.0,
            inactive_at_expense=True, inactive_since=1_000)
    acc.add("holder_tier", "crn2", "0xA", 1.0,
            inactive_at_expense=True, inactive_since=1_000)
    close = {"crn1": {"inactive_since": 1_000},
             "crn2": {"inactive_since": 1_000}}
    slashed, meta = compute_slashing(
        acc, close, close_height=1_000 + 3 * BPD,
        enabled_streams=("credit_revenue", "holder_tier"),
        retroactive=False, threshold_days=3, blocks_per_day=BPD,
    )
    assert slashed == {"0xA": 5.0}
    assert len(meta) == 2
```

- [ ] **Step 2: Run to verify it fails**

Run: `pytest tests/test_slashing.py -v`
Expected: FAIL with `ImportError: cannot import name 'compute_slashing'`

- [ ] **Step 3: Add `compute_slashing` and `enabled_slash_streams`**

Append to `src/aleph_nodestatus/slashing.py`:

```python
def enabled_slash_streams():
    """Streams whose slashing is enabled, per settings flags."""
    streams = []
    if settings.credit_dist_slash_credit_revenue:
        streams.append("credit_revenue")
    if settings.credit_dist_slash_holder_tier:
        streams.append("holder_tier")
    if settings.credit_dist_slash_wage_subsidy:
        streams.append("wage_subsidy")
    return tuple(streams)


def compute_slashing(accumulator, close_resource_nodes, close_height, *,
                     enabled_streams, retroactive,
                     threshold_days=None, blocks_per_day=None):
    """Run the slashing pass.

    Returns `(slashed, meta_nodes)`:
      - `slashed`: {reward_address: withheld_amount} aggregated across all
        slashed CRNs/streams.
      - `meta_nodes`: list of {node_id, address, stream, amount,
        inactive_since} for the audit `slashed_meta`.

    A CRN's `inactive_since` is read from the close-of-window snapshot; if the
    node is no longer present there (removed mid-window), the accumulator's
    last-seen value is used as a fallback.
    """
    slashed = {}
    meta_nodes = []
    for (stream, node_id), entry in accumulator.entries().items():
        if stream not in enabled_streams:
            continue
        rnode = close_resource_nodes.get(node_id)
        inactive_since = (
            rnode.get("inactive_since") if rnode is not None
            else entry["inactive_since"]
        )
        if not is_slashable(inactive_since, close_height,
                            threshold_days=threshold_days,
                            blocks_per_day=blocks_per_day):
            continue
        amount = entry["total"] if retroactive else entry["post_death"]
        if amount <= 0:
            continue
        addr = entry["address"]
        slashed[addr] = slashed.get(addr, 0.0) + amount
        meta_nodes.append({
            "node_id": node_id, "address": addr, "stream": stream,
            "amount": amount, "inactive_since": inactive_since,
        })
    return slashed, meta_nodes
```

- [ ] **Step 4: Run to verify pass**

Run: `pytest tests/test_slashing.py -v`
Expected: PASS (10 tests)

- [ ] **Step 5: Commit**

```bash
git add src/aleph_nodestatus/slashing.py tests/test_slashing.py
git commit -m "feat(slashing): add compute_slashing pass and stream flags"
```

---

## Task 5: Feed the accumulator from the execution-credit path (credit_revenue + holder_tier)

**Files:**
- Modify: `src/aleph_nodestatus/credit_distribution.py:572-604` (`_distribute_execution_credits`), `:506-516` (`_distribute_expense`), `:878-886` (`_apply_expense_to`), `:1084-1150` (`_apply_expenses_to_snapshots`), `:964-1031` (`compute_rewards` and the two `_compute_rewards_*`).
- Test: `tests/test_slashing.py`

- [ ] **Step 1: Write a failing integration test for the execution-path accumulator**

Append to `tests/test_slashing.py`:

```python
from aleph_nodestatus.credit_distribution import _distribute_execution_credits
from aleph_nodestatus.slashing import SlashAccumulator as _Acc


def test_execution_path_feeds_accumulator():
    # One execution credit, CRN linked, CRN snapshot flagged inactive.
    expense = {"credits": [{"amount": 100.0, "node_id": "crn1"}]}
    resource_nodes = {
        "crn1": {"status": "linked", "owner": "0xCRN",
                 "reward": "0xCRN", "inactive_since": 555},
    }
    nodes = {}  # no linked CCN -> CCN/staker shares go unallocated, fine
    rewards, detailed = {}, {}
    from collections import defaultdict
    detailed = defaultdict(lambda: defaultdict(float))
    unalloc = {"crn": defaultdict(float), "ccn": defaultdict(float),
               "staker": defaultdict(float)}
    acc = _Acc()
    _distribute_execution_credits(
        expense, credit_price_aleph=1.0, nodes=nodes,
        resource_nodes=resource_nodes, rewards=rewards, detailed=detailed,
        unallocated=unalloc, web3=None,
        ccn_share=0.15, staker_share=0.20, crn_share=0.60,
        accumulator=acc, stream="credit_revenue",
    )
    e = acc.entries()[("credit_revenue", "crn1")]
    assert e["address"] == "0xCRN"
    assert e["total"] == 60.0          # 100 * 0.60
    assert e["post_death"] == 60.0     # inactive_since set -> counted
    assert e["inactive_since"] == 555
```

> Note: confirm the exact `_distribute_execution_credits` call signature against the source. The keyword names above (`credit_price_aleph`, `ccn_share`, `staker_share`, `crn_share`) match `credit_distribution.py`. If `web3=None` causes `get_reward_address` to fail, pass a stub with `to_checksum_address = lambda a: a` instead.

- [ ] **Step 2: Run to verify it fails**

Run: `pytest tests/test_slashing.py::test_execution_path_feeds_accumulator -v`
Expected: FAIL with `TypeError: _distribute_execution_credits() got an unexpected keyword argument 'accumulator'`

- [ ] **Step 3: Add `accumulator`/`stream` params and the `acc.add` call in `_distribute_execution_credits`**

In `credit_distribution.py`, change the signature (around line 572):

```python
def _distribute_execution_credits(
    expense, credit_price_aleph, nodes, resource_nodes,
    rewards, detailed, unallocated, web3,
    *, ccn_share, staker_share, crn_share,
    accumulator=None, stream=None,
):
```

Then inside the CRN-share branch, right after the existing
`detailed[crn_addr]["execution_crn"] += crn_amount` line (around line 597), add:

```python
            if accumulator is not None and stream is not None:
                inactive_since = rnode.get("inactive_since")
                accumulator.add(
                    stream, node_id, crn_addr, crn_amount,
                    inactive_at_expense=inactive_since is not None,
                    inactive_since=inactive_since,
                )
```

- [ ] **Step 4: Run to verify pass**

Run: `pytest tests/test_slashing.py::test_execution_path_feeds_accumulator -v`
Expected: PASS

- [ ] **Step 5: Thread `accumulator`/`stream` through the callers**

In `_distribute_expense` (around line 506), add `accumulator=None, stream=None` to its keyword-only params and forward them to `_distribute_execution_credits`:

```python
    if expense_type == "execution":
        _distribute_execution_credits(
            expense, credit_price_aleph, nodes, resource_nodes,
            rewards, detailed, unallocated, web3,
            ccn_share=ccn_share, staker_share=staker_share,
            crn_share=crn_share,
            accumulator=accumulator, stream=stream,
        )
        return 0, total_aleph, dev_fund
```

In `_apply_expense_to` (around line 878), add `accumulator=None, stream=None` and forward:

```python
def _apply_expense_to(rewards, totals, detailed, unallocated, exp_type, expense,
                      nodes, resource_nodes, web3,
                      accumulator=None, stream=None):
    s, e, d = _distribute_expense(
        exp_type, expense, nodes, resource_nodes, rewards,
        detailed=detailed, unallocated=unallocated,
        web3=web3, **_shares_for(exp_type),
        accumulator=accumulator, stream=stream,
    )
```

In `_apply_expenses_to_snapshots` (around line 1084), accept an `accumulator=None`
param and pass the right stream label on each `_apply_expense_to` call:

```python
def _apply_expenses_to_snapshots(
    parsed, snapshots, include_holder_tier, web3, accumulator=None,
):
    ...
        _apply_expense_to(
            credit_rewards, credit_totals, credit_detailed, credit_unallocated,
            exp_type,
            _project_expense(expense, "credits"),
            nodes, resource_nodes, web3,
            accumulator=accumulator, stream="credit_revenue",
        )

        if include_holder_tier and expense.get("hold"):
            _validate_hold_aggregates(expense)
            _apply_expense_to(
                holder_rewards, holder_totals, holder_detailed,
                holder_unallocated, exp_type,
                _project_expense(expense, "hold"),
                nodes, resource_nodes, web3,
                accumulator=accumulator, stream="holder_tier",
            )
```

- [ ] **Step 6: Thread `accumulator` through `compute_rewards` and `_compute_rewards_snapshots`/`_compute_rewards_full_resync`**

Add `slash_accumulator=None` to `compute_rewards` (line 964) and forward it to
both compute paths; each forwards to `_apply_expenses_to_snapshots`:

```python
async def compute_rewards(
    start_time, end_time, full_resync=False,
    include_holder_tier=False, sender=None,
    dbs=None, end_height=None, web3=None,
    snapshots=None, last_end_height=None,
    out_expense_hashes=None, slash_accumulator=None,
):
    ...
    if full_resync:
        result = await _compute_rewards_full_resync(
            dbs, end_height, msgs, include_holder_tier, web3,
            out_expense_hashes=out_expense_hashes,
            start_time=start_time, end_time=end_time,
            slash_accumulator=slash_accumulator,
        )
    else:
        result = await _compute_rewards_snapshots(
            api_server, start_time, end_time, msgs, include_holder_tier, web3,
            snapshots=snapshots,
            out_expense_hashes=out_expense_hashes,
            slash_accumulator=slash_accumulator,
        )
```

In `_compute_rewards_snapshots` (line 1153) add `slash_accumulator=None` and pass
it to `_apply_expenses_to_snapshots(parsed, snapshots, include_holder_tier, web3, accumulator=slash_accumulator)`.
In `_compute_rewards_full_resync` (line 1175) add `slash_accumulator=None` and, at
its `_apply_expenses_to_snapshots(...)` call, pass `accumulator=slash_accumulator`.

- [ ] **Step 7: Run the full credit-distribution suite to verify no regressions**

Run: `pytest tests/test_credit_distribution.py tests/test_credit_distribution_refactor.py tests/test_slashing.py -v`
Expected: PASS (pre-existing `credit_pipeline` failures noted in project memory are unrelated and out of scope)

- [ ] **Step 8: Commit**

```bash
git add src/aleph_nodestatus/credit_distribution.py tests/test_slashing.py
git commit -m "feat(slashing): feed per-CRN accumulator from execution-credit path"
```

---

## Task 6: Feed the accumulator from the wage-subsidy path

**Files:**
- Modify: `src/aleph_nodestatus/wage_subsidy.py:55-128` (`split_subsidy`), `:167+` (`compute_subsidy_daily`).
- Test: `tests/test_slashing.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_slashing.py`:

```python
from aleph_nodestatus.wage_subsidy import split_subsidy


def test_wage_path_feeds_accumulator():
    nodes = {}
    resource_nodes = {
        "crn1": {"status": "linked", "owner": "0xW", "reward": "0xW",
                 "score": 1.0, "inactive_since": 42},
    }
    acc = _Acc()
    rewards, _unalloc, _detailed = split_subsidy(
        period_subsidy=300.0, nodes=nodes, resource_nodes=resource_nodes,
        web3=None, accumulator=acc,
    )
    e = acc.entries()[("wage_subsidy", "crn1")]
    assert e["address"] == "0xW"
    assert e["post_death"] > 0          # inactive_since set
    assert e["total"] == e["post_death"]
    assert e["inactive_since"] == 42
```

> Note: `wage_crn_share` is a settings value; the asserted amount is the only
> linked CRN so it receives the whole `crn_pool`. Assert with `> 0` / equality
> between `total` and `post_death` rather than a hard-coded number to stay
> independent of the configured share.

- [ ] **Step 2: Run to verify it fails**

Run: `pytest tests/test_slashing.py::test_wage_path_feeds_accumulator -v`
Expected: FAIL with `TypeError: split_subsidy() got an unexpected keyword argument 'accumulator'`

- [ ] **Step 3: Add `accumulator` param + capture rnode hash in `crn_weights`**

In `wage_subsidy.py`, change `split_subsidy` (line 55) signature to add
`accumulator=None`, and change the CRN loop (lines 97-111) to keep the rnode
hash and its `inactive_since`, feeding the accumulator on payout:

```python
def split_subsidy(
    period_subsidy: float,
    nodes: dict,
    resource_nodes: dict,
    web3=None,
    accumulator=None,
) -> Tuple[Dict[str, float], float, Dict[str, Dict[str, float]]]:
    ...
    crn_weights = []
    for rnode_hash, rnode in resource_nodes.items():
        if rnode["status"] != "linked":
            continue
        score = compute_score_multiplier(rnode["score"])
        if score > 0:
            crn_weights.append((
                rnode_hash, get_reward_address(rnode, web3), score,
                rnode.get("inactive_since"),
            ))
    total_crn = sum(w[2] for w in crn_weights)
    if total_crn > 0:
        for rnode_hash, addr, s, inactive_since in crn_weights:
            share = crn_pool * s / total_crn
            rewards[addr] = rewards.get(addr, 0.0) + share
            detailed[addr]["crn"] += share
            if accumulator is not None:
                accumulator.add(
                    "wage_subsidy", rnode_hash, addr, share,
                    inactive_at_expense=inactive_since is not None,
                    inactive_since=inactive_since,
                )
    else:
        unallocated += crn_pool
```

- [ ] **Step 4: Run to verify pass**

Run: `pytest tests/test_slashing.py::test_wage_path_feeds_accumulator -v`
Expected: PASS

- [ ] **Step 5: Thread `accumulator` through `compute_subsidy_daily`**

In `compute_subsidy_daily` (line 167), add `accumulator=None` to the signature
and forward it to every internal `split_subsidy(...)` call (the per-day split).
Locate each `split_subsidy(` call inside the function body and add
`accumulator=accumulator`.

- [ ] **Step 6: Run the wage suite to verify no regressions**

Run: `pytest tests/test_wage_subsidy.py tests/test_slashing.py -v`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add src/aleph_nodestatus/wage_subsidy.py tests/test_slashing.py
git commit -m "feat(slashing): feed per-CRN accumulator from wage-subsidy path"
```

---

## Task 7: Orchestrate in `process_credit_distribution` — run pass, withhold transfer, publish

**Files:**
- Modify: `src/aleph_nodestatus/commands.py` (`process_credit_distribution`: compute call ~438-446, after `merge_rewards` ~511, balance/transfer ~557-671, publish ~691-706).
- Test: `tests/test_slashing_integration.py` (new).

- [ ] **Step 1: Build the accumulator and pass it to `compute_rewards`**

In `process_credit_distribution`, before the `compute_rewards` call (~line 438),
add:

```python
    from .slashing import (
        SlashAccumulator, compute_slashing, enabled_slash_streams,
    )
    slash_on = settings.credit_dist_slash_enabled
    slash_accumulator = SlashAccumulator() if slash_on else None
```

Then pass `slash_accumulator=slash_accumulator` to the `compute_rewards(...)`
call (~line 438-446). The wage call (`compute_subsidy_daily`, ~line 477) also
gets `accumulator=slash_accumulator`.

- [ ] **Step 2: Run the slashing pass right after `merge_rewards`**

Immediately after the `by_address_detailed` enrichment loop (~line 534), add:

```python
    # === Step 4b: slashing pass ===
    # `rewards`/`final_rewards` stay intact (full calculation, published as-is).
    # `slashed` is withheld from the on-chain payout below.
    slashed, slashed_meta_nodes = {}, []
    if slash_on:
        close_resource_nodes = snapshots[-1][2] if snapshots else {}
        slashed, slashed_meta_nodes = compute_slashing(
            slash_accumulator, close_resource_nodes, end_height,
            enabled_streams=enabled_slash_streams(),
            retroactive=settings.credit_dist_slash_retroactive,
            threshold_days=settings.credit_dist_slash_threshold_days,
            blocks_per_day=settings.ethereum_blocks_per_day,
        )

    # Payout view: full rewards minus the withheld slash per address, floored
    # at 0. Used for the balance check and the actual transfer only.
    payout_rewards = {
        addr: max(0.0, amount - slashed.get(addr, 0.0))
        for addr, amount in final_rewards.items()
    }
```

- [ ] **Step 3: Use `payout_rewards` for the balance check and transfer**

In the balance-safety block (~line 557-583) replace `final_rewards` with
`payout_rewards` in the `if act and flags.get("transfer") and payout_rewards:`
guard and in the `owed = sum(... payout_rewards.values() ...)` computation.

In the transfer block (~line 664-678) replace every `final_rewards.items()` /
`final_rewards` used to build the transfer batches with `payout_rewards`. Leave
the global-cap block (~line 540-550) on `final_rewards` — it is an anti-inflation
guard on the calculation, not the payout.

> Drop recipients whose payout floored to 0 so empty transfers aren't queued:
> `items = [(a, v) for a, v in payout_rewards.items() if v > 0]` in each batch
> builder.

- [ ] **Step 4: Add `slashed` / `slashed_meta` to the published post**

In the `distribution = {...}` dict (~line 691), after `"rewards": final_rewards,`
add:

```python
        "slashed": slashed,
        "slashed_meta": {
            "enabled": slash_on,
            "threshold_days": settings.credit_dist_slash_threshold_days,
            "retroactive": settings.credit_dist_slash_retroactive,
            "streams": list(enabled_slash_streams()) if slash_on else [],
            "nodes": slashed_meta_nodes,
        },
```

`rewards` remains `final_rewards` (intact). The transfer used `payout_rewards`.

- [ ] **Step 5: Write the integration test (flag-on withholds, flag-off identical)**

Create `tests/test_slashing_integration.py`. Use the existing
`test_distribute_credits_cli.py` / `test_credit_distribution.py` fixtures as a
template for constructing snapshots + expenses. The test must assert:

```python
# Pseudostructure — adapt to the repo's existing credit-dist test harness.
# 1. One CRN inactive >= 3 days at end_height with execution credits in-window.
# 2. With slashing enabled (from-death default):
#      - published distribution["slashed"][crn_addr] == expected_post_death
#      - published distribution["rewards"][crn_addr] == full (intact)
#      - the transfer batch (payout) for crn_addr == full - slashed
# 3. With settings.credit_dist_slash_enabled = False:
#      - distribution has no non-empty "slashed"
#      - transfer batches identical to pre-feature behaviour (byte-equal rewards)
```

Implement it concretely against the harness those tests already use (they build
`nodes`/`resource_nodes` snapshot dicts and `aleph_credit_expense` post dicts and
drive `compute_rewards` / `process_credit_distribution`). Mirror their fixture
construction; add `inactive_since` to the target CRN's resource-node dict.

- [ ] **Step 6: Run the integration test**

Run: `pytest tests/test_slashing_integration.py -v`
Expected: PASS

- [ ] **Step 7: Run the full credit suite for regressions**

Run: `pytest tests/test_credit_distribution.py tests/test_credit_distribution_refactor.py tests/test_distribute_credits_cli.py tests/test_wage_subsidy.py tests/test_slashing.py tests/test_slashing_integration.py -v`
Expected: PASS (excluding the known-unrelated `credit_pipeline` failures)

- [ ] **Step 8: Commit**

```bash
git add src/aleph_nodestatus/commands.py tests/test_slashing_integration.py
git commit -m "feat(slashing): withhold slashed CRN rewards from transfer and publish slashed_meta"
```

---

## Task 8: Final verification

- [ ] **Step 1: Flag-off byte-equality check**

Confirm that with `credit_dist_slash_enabled=False` the published `rewards`,
`targets`, and transfer batches are byte-identical to a run on the same fixtures
before this feature (the integration test in Task 7 Step 5 covers this; re-run it
explicitly).

Run: `pytest tests/test_slashing_integration.py -v -k flag_off`
Expected: PASS

- [ ] **Step 2: Lint/import sanity**

Run: `python -c "import aleph_nodestatus.commands, aleph_nodestatus.credit_distribution, aleph_nodestatus.wage_subsidy, aleph_nodestatus.slashing"`
Expected: no output, exit 0

- [ ] **Step 3: Full test run**

Run: `pytest tests/ -q`
Expected: only the pre-existing unrelated failures (`credit_pipeline`, `test_scores`/`test_skeleton` collection errors) remain; everything else green.

---

## Spec coverage check

- §2 predicate (3-day, block-height) → Task 2.
- §2 modes (from-death default / retroactive) → Tasks 3-4 (`post_death`/`total`), Task 7 (flag wiring).
- §2.1 anchor → nodestatus run window already starts at the anchor; no extra work (verified: `process_credit_distribution` resolves `start_height` from `get_latest_successful_credit_distribution`).
- §3.1 accumulator → Tasks 3, 5, 6.
- §3.2 slashing pass → Task 4.
- §3.3 output + transfer withholding (rewards intact) → Task 7.
- §3.4 settings → Task 1.
- §5 edge cases (recovery resets `inactive_since`, removed-mid-window fallback, mixed address) → covered by `compute_slashing` fallback + Task 4 tests.
- §6 testing (flag-off invariance) → Tasks 7-8.
