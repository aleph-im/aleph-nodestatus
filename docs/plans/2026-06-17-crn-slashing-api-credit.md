# CRN inactivity slashing — api-credit implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.
>
> **This plan lives in the `aleph-nodestatus` repo but is executed in `aleph-api-credit`** (`/Users/angelmanzano/Projects/aleph/back/aleph-api-credit`). All paths below are relative to that repo.

**Goal:** In `aleph-api-credit` rewards time series, compute the withheld (slashed) reward share of CRNs inactive ≥ 3 days and surface it as a `slashed` field, 1:1 with the nodestatus calculation, behind env-var feature flags (default on).

**Architecture:** Mirror the nodestatus `slashing.py` as an isolated `domain/rewards/slashing.ts` (pure predicate + accumulator + pass). Add `inactiveSince` to the snapshot type/parser. Thread an optional accumulator through `distributeExpense` and the wage split. A `computeSlashingForRange` re-runs the exact same per-expense attribution over the slash window `[A, to]` (retroactive) or `[from, to]` (from-death), where `A` is the last confirmed credit distribution resolved by the identical query nodestatus uses. The time series response gains `slashed` at root and per bucket.

**Tech Stack:** TypeScript, Jest (`npm test`). Tests in `src/__tests__/`.

**Spec:** `aleph-nodestatus/docs/specs/2026-06-17-crn-slashing-design.md` (§2 predicate & modes, §2.1 anchor, §2.2 additivity, §4 api-credit design).

---

## File structure

- **Create** `src/domain/rewards/slashing.ts` — `isSlashable`, `SlashAccumulator`, `computeSlashing`, `enabledSlashStreams`. Pure, no I/O.
- **Create** `src/domain/rewards/slashingCompute.ts` — `computeSlashingForRange` (loads anchor + snapshots + expenses, runs the accumulator, returns slashed). Orchestration.
- **Create** `src/dal/rewards/lastDistributionFetcher.ts` — `fetchLastDistributionAnchor()` (the §2.1 query).
- **Create** test files under `src/__tests__/`.
- **Modify** `src/domain/rewards/ported/types.ts` — add `inactiveSince?` to `SnapshotResourceNode`.
- **Modify** `src/dal/rewards/snapshotsFetcher.ts` — populate `inactiveSince` in `normalizeRn`.
- **Modify** `src/domain/rewards/ported/distribute.ts` — optional accumulator in `distributeExpense`.
- **Modify** `src/domain/rewards/ported/wageSubsidy.ts` — optional accumulator in the wage split.
- **Modify** `src/utils/config.ts` — `rewards` slashing config.
- **Modify** `src/domain/rewards/timeSeries.ts` — `slashed` on `BucketResponse`/`RootBlock`; populate from `computeSlashingForRange`.

---

## Task 1: Config flags

**Files:**
- Modify: `src/utils/config.ts` (`rewards` block, ~125-177)

- [ ] **Step 1: Add slashing config keys at the end of the `rewards` block**

In `config.ts`, before the closing `},` of the `rewards: { ... }` object, add:

```typescript
    // === Slashing (CRN inactivity penalty) — mirrors aleph-nodestatus ===
    slashEnabled: process.env.REWARDS_SLASH_ENABLED !== "false",
    slashCreditRevenue: process.env.REWARDS_SLASH_CREDIT_REVENUE !== "false",
    slashHolderTier: process.env.REWARDS_SLASH_HOLDER_TIER !== "false",
    slashWageSubsidy: process.env.REWARDS_SLASH_WAGE_SUBSIDY !== "false",
    slashThresholdDays: Number(process.env.REWARDS_SLASH_THRESHOLD_DAYS) || 3,
    slashRetroactive: process.env.REWARDS_SLASH_RETROACTIVE === "true",
    // Block-height-per-day constant, must equal nodestatus ethereum_blocks_per_day.
    slashBlocksPerDay: Number(process.env.REWARDS_SLASH_BLOCKS_PER_DAY) || 7130,
    // Owner of the credit-rewards-distribution posts that anchor retroactive
    // slashing. Mirrors aleph-nodestatus settings.distribution_recipient.
    distributionRecipient:
      process.env.REWARDS_DISTRIBUTION_RECIPIENT ||
      "0x3a5CC6aBd06B601f4654035d125F9DD2FC992C25",
```

- [ ] **Step 2: Type-check**

Run: `npx tsc --noEmit`
Expected: no errors.

- [ ] **Step 3: Commit**

```bash
git add src/utils/config.ts
git commit -m "feat(slashing): add rewards slashing config flags"
```

---

## Task 2: Add `inactiveSince` to the snapshot type and parser

**Files:**
- Modify: `src/domain/rewards/ported/types.ts:36-43`
- Modify: `src/dal/rewards/snapshotsFetcher.ts:101-114` (`normalizeRn`)
- Test: `src/__tests__/rewards-snapshot-parse.test.ts` (new)

- [ ] **Step 1: Write the failing parser test**

Create `src/__tests__/rewards-snapshot-parse.test.ts`:

```typescript
import { __test__normalizeRn } from "../dal/rewards/snapshotsFetcher";

describe("normalizeRn inactiveSince", () => {
  it("parses inactive_since block height", () => {
    const rn = __test__normalizeRn({
      hash: "crn1",
      status: "linked",
      score: 1,
      owner: "0xOwner",
      inactive_since: 12345,
    });
    expect(rn.inactiveSince).toBe(12345);
  });

  it("maps null/absent inactive_since to undefined", () => {
    expect(
      __test__normalizeRn({ hash: "c", status: "linked", score: 1, owner: "0x" })
        .inactiveSince,
    ).toBeUndefined();
    expect(
      __test__normalizeRn({
        hash: "c", status: "linked", score: 1, owner: "0x",
        inactive_since: null,
      }).inactiveSince,
    ).toBeUndefined();
  });
});
```

- [ ] **Step 2: Run to verify it fails**

Run: `npx jest rewards-snapshot-parse -t inactiveSince`
Expected: FAIL — `__test__normalizeRn` not exported / `inactiveSince` missing.

- [ ] **Step 3: Add the field to the type**

In `types.ts`, change `SnapshotResourceNode` (lines 36-43) to add `inactiveSince`:

```typescript
export interface SnapshotResourceNode {
  hash: string;
  status: "linked" | "unlinked";
  score: number;
  owner: Address;
  reward?: Address;
  decentralization?: number;
  // ETH block height at which this CRN's score first dropped below 0.01
  // (set by aleph-nodestatus status.py), or undefined when active. Drives
  // inactivity slashing. Raw aggregate field: `inactive_since` (may be null).
  inactiveSince?: number;
}
```

- [ ] **Step 4: Populate it in `normalizeRn` and export a test shim**

In `snapshotsFetcher.ts`, in `normalizeRn` (lines 101-114) add the field inside
the returned object:

```typescript
    inactiveSince:
      typeof raw.inactive_since === "number"
        ? (raw.inactive_since as number)
        : undefined,
```

At the bottom of `snapshotsFetcher.ts`, export a test shim:

```typescript
export const __test__normalizeRn = normalizeRn;
```

- [ ] **Step 5: Run to verify pass**

Run: `npx jest rewards-snapshot-parse -t inactiveSince`
Expected: PASS (2 tests).

- [ ] **Step 6: Commit**

```bash
git add src/domain/rewards/ported/types.ts src/dal/rewards/snapshotsFetcher.ts src/__tests__/rewards-snapshot-parse.test.ts
git commit -m "feat(slashing): parse CRN inactive_since into snapshots"
```

---

## Task 3: Slashing module — predicate, accumulator, pass

**Files:**
- Create: `src/domain/rewards/slashing.ts`
- Test: `src/__tests__/rewards-slashing.test.ts` (new)

- [ ] **Step 1: Write the failing tests**

Create `src/__tests__/rewards-slashing.test.ts`:

```typescript
import {
  isSlashable,
  SlashAccumulator,
  computeSlashing,
} from "../domain/rewards/slashing";

const BPD = 7130;

describe("isSlashable", () => {
  it("false when active (undefined inactiveSince)", () => {
    expect(isSlashable(undefined, 1_000_000, 3, BPD)).toBe(false);
  });
  it("false below threshold", () => {
    expect(isSlashable(1_000_000, 1_000_000 + 2 * BPD, 3, BPD)).toBe(false);
  });
  it("true at exact threshold", () => {
    expect(isSlashable(1_000_000, 1_000_000 + 3 * BPD, 3, BPD)).toBe(true);
  });
});

describe("SlashAccumulator + computeSlashing", () => {
  const close = { crn1: { inactiveSince: 1_000 } } as any;
  const closeHeight = 1_000 + 3 * BPD;

  it("from-death uses postDeath; retroactive uses total", () => {
    const acc = new SlashAccumulator();
    acc.add("credit_revenue", "crn1", "0xa", 10, false, undefined);
    acc.add("credit_revenue", "crn1", "0xa", 4, true, 1_000);

    const fromDeath = computeSlashing(acc, close, closeHeight, {
      enabledStreams: new Set(["credit_revenue"]),
      retroactive: false, thresholdDays: 3, blocksPerDay: BPD,
    });
    expect(Object.fromEntries(fromDeath.slashed)).toEqual({ "0xa": 4 });

    const retro = computeSlashing(acc, close, closeHeight, {
      enabledStreams: new Set(["credit_revenue"]),
      retroactive: true, thresholdDays: 3, blocksPerDay: BPD,
    });
    expect(Object.fromEntries(retro.slashed)).toEqual({ "0xa": 14 });
  });

  it("skips disabled streams and non-slashable nodes; aggregates addresses", () => {
    const acc = new SlashAccumulator();
    acc.add("wage_subsidy", "crn1", "0xa", 4, true, 1_000); // disabled stream
    acc.add("credit_revenue", "crn1", "0xa", 4, true, 1_000);
    acc.add("holder_tier", "crn2", "0xa", 1, true, 1_000);
    const closeBoth = {
      crn1: { inactiveSince: 1_000 },
      crn2: { inactiveSince: 1_000 },
    } as any;
    const res = computeSlashing(acc, closeBoth, closeHeight, {
      enabledStreams: new Set(["credit_revenue", "holder_tier"]),
      retroactive: false, thresholdDays: 3, blocksPerDay: BPD,
    });
    expect(Object.fromEntries(res.slashed)).toEqual({ "0xa": 5 });
    expect(res.meta).toHaveLength(2);
  });
});
```

- [ ] **Step 2: Run to verify it fails**

Run: `npx jest rewards-slashing`
Expected: FAIL — module not found.

- [ ] **Step 3: Create the module**

Create `src/domain/rewards/slashing.ts`:

```typescript
import type { Address } from "./ported/types";

export type SlashStream = "credit_revenue" | "holder_tier" | "wage_subsidy";

export interface SlashMetaNode {
  nodeId: string;
  address: Address;
  stream: SlashStream;
  amount: number;
  inactiveSince: number;
  // ms timestamp of the contributing expense, for per-bucket attribution.
  tsMs?: number;
}

interface SlashEntry {
  address: Address;
  total: number;
  postDeath: number;
  inactiveSince?: number;
  // ms timestamps + amounts of each contribution, for bucketing in §2.2.
  contributions: { tsMs: number; amount: number; postDeath: boolean }[];
}

/** A CRN is slashable when inactive (height set) for >= thresholdDays. */
export function isSlashable(
  inactiveSince: number | undefined,
  closeHeight: number,
  thresholdDays: number,
  blocksPerDay: number,
): boolean {
  if (inactiveSince === undefined || inactiveSince === null) return false;
  return closeHeight - inactiveSince >= thresholdDays * blocksPerDay;
}

/** Per-(stream, crnNodeId) ledger of CRN reward shares. */
export class SlashAccumulator {
  private entries = new Map<string, SlashEntry>();

  add(
    stream: SlashStream,
    nodeId: string,
    address: Address,
    amount: number,
    inactiveAtExpense: boolean,
    inactiveSince: number | undefined,
    tsMs = 0,
  ): void {
    if (!nodeId) return;
    const key = `${stream} ${nodeId}`;
    let e = this.entries.get(key);
    if (!e) {
      e = { address, total: 0, postDeath: 0, inactiveSince, contributions: [] };
      this.entries.set(key, e);
    }
    e.address = address;
    e.total += amount;
    if (inactiveAtExpense) e.postDeath += amount;
    if (inactiveSince !== undefined) e.inactiveSince = inactiveSince;
    e.contributions.push({ tsMs, amount, postDeath: inactiveAtExpense });
  }

  list(): { stream: SlashStream; nodeId: string; entry: SlashEntry }[] {
    const out: { stream: SlashStream; nodeId: string; entry: SlashEntry }[] = [];
    for (const [key, entry] of this.entries) {
      const [stream, nodeId] = key.split(" ");
      out.push({ stream: stream as SlashStream, nodeId, entry });
    }
    return out;
  }
}

export interface ComputeSlashingOpts {
  enabledStreams: Set<SlashStream>;
  retroactive: boolean;
  thresholdDays: number;
  blocksPerDay: number;
}

export interface SlashingResult {
  slashed: Map<Address, number>; // aggregate withheld per address
  meta: SlashMetaNode[];
}

export function computeSlashing(
  acc: SlashAccumulator,
  closeResourceNodes: Record<string, { inactiveSince?: number }>,
  closeHeight: number,
  opts: ComputeSlashingOpts,
): SlashingResult {
  const slashed = new Map<Address, number>();
  const meta: SlashMetaNode[] = [];
  for (const { stream, nodeId, entry } of acc.list()) {
    if (!opts.enabledStreams.has(stream)) continue;
    const rn = closeResourceNodes[nodeId];
    const inactiveSince =
      rn !== undefined ? rn.inactiveSince : entry.inactiveSince;
    if (!isSlashable(inactiveSince, closeHeight, opts.thresholdDays, opts.blocksPerDay))
      continue;
    const amount = opts.retroactive ? entry.total : entry.postDeath;
    if (amount <= 0) continue;
    slashed.set(entry.address, (slashed.get(entry.address) ?? 0) + amount);
    // One meta row per contributing expense (so callers can bucket by tsMs);
    // collapse to a single row when contributions are empty (shouldn't happen).
    const rows = opts.retroactive
      ? entry.contributions
      : entry.contributions.filter((c) => c.postDeath);
    for (const c of rows) {
      meta.push({
        nodeId, address: entry.address, stream,
        amount: c.amount, inactiveSince: inactiveSince!, tsMs: c.tsMs,
      });
    }
  }
  return { slashed, meta };
}
```

- [ ] **Step 4: Run to verify pass**

Run: `npx jest rewards-slashing`
Expected: PASS.

> Note: the `meta` rows above are per-contribution; the address aggregate in
> `slashed` already sums them. The two assertions in Step 1 check `slashed`
> (aggregate) and `meta.length` (2 nodes → here 2 contributions). If a node has
> multiple in-window expenses, `meta` will have one row per expense — adjust the
> length assertion to the fixture's expense count when you extend it.

- [ ] **Step 5: Commit**

```bash
git add src/domain/rewards/slashing.ts src/__tests__/rewards-slashing.test.ts
git commit -m "feat(slashing): add pure slashing module (predicate, accumulator, pass)"
```

---

## Task 4: Thread the accumulator through `distributeExpense`

**Files:**
- Modify: `src/domain/rewards/ported/distribute.ts:167-371`
- Test: `src/__tests__/rewards-slashing.test.ts`

- [ ] **Step 1: Write the failing test**

Append to `src/__tests__/rewards-slashing.test.ts`:

```typescript
import { distributeExpense } from "../domain/rewards/ported/distribute";
import type { ExpenseMessage, Snapshot } from "../domain/rewards/ported/types";

function execSnapshot(inactiveSince?: number): Snapshot {
  return {
    item_hash: "s", height: 100, ts: 0, confirmed: true,
    nodes: {},
    resource_nodes: {
      crn1: {
        hash: "crn1", status: "linked", score: 1,
        owner: "0xcrn", reward: "0xcrn", inactiveSince,
      },
    },
  };
}

function execExpense(): ExpenseMessage {
  return {
    item_hash: "e", original_item_hash: "e", publication_time_ms: 0,
    confirmed: true, ts_ms: 1234, start_date_ms: 0, end_date_ms: 0,
    kind: "execution", credit_price_aleph: 1,
    credits: [{ amount: 100, node_id: "crn1" } as any],
    hold: [],
  };
}

describe("distributeExpense accumulator", () => {
  it("feeds the per-CRN accumulator on execution_crn", () => {
    const acc = new SlashAccumulator();
    distributeExpense(execExpense(), "credits", execSnapshot(555), undefined, {
      accumulator: acc, stream: "credit_revenue",
    });
    const row = acc.list().find((r) => r.nodeId === "crn1")!;
    expect(row.entry.address).toBe("0xcrn");
    expect(row.entry.total).toBe(60); // 100 * CREDIT_EXECUTION_CRN_SHARE (0.60)
    expect(row.entry.postDeath).toBe(60); // inactiveSince set -> post-death
    expect(row.entry.contributions[0].tsMs).toBe(1234);
  });
});
```

- [ ] **Step 2: Run to verify it fails**

Run: `npx jest rewards-slashing -t accumulator`
Expected: FAIL — `distributeExpense` takes 4 args, the 5th options arg is rejected by the type checker / ignored.

- [ ] **Step 3: Add an optional options arg to `distributeExpense`**

In `distribute.ts`, extend the signature (line 167) with a 5th optional param:

```typescript
export function distributeExpense(
  expense: ExpenseMessage,
  source: "credits" | "hold",
  snapshot: Snapshot,
  targetAddresses?: Set<Address>,
  slash?: { accumulator: import("../slashing").SlashAccumulator; stream: import("../slashing").SlashStream },
): RewardStream {
```

Inside the execution branch, where the CRN share is credited (the
`if (crn && crn.status === "linked") { const a = rewardAddrOf(crn); ... }`
block), add the accumulator call right after `addTo(perAddress, a, "execution_crn", crnShare);`:

```typescript
      if (crn && crn.status === "linked") {
        const a = rewardAddrOf(crn);
        if (want(a)) addTo(perAddress, a, "execution_crn", crnShare);
        if (slash) {
          slash.accumulator.add(
            slash.stream, item.node_id!, a, crnShare,
            crn.inactiveSince !== undefined,
            crn.inactiveSince,
            expense.ts_ms,
          );
        }
      } else if (wantAll) {
```

> The accumulator must capture the CRN share regardless of `want(a)` (the
> single-address fast path) so the slash total is complete — note it is fed
> unconditionally inside the `if (slash)` block, not gated by `want`.

- [ ] **Step 4: Run to verify pass**

Run: `npx jest rewards-slashing -t accumulator`
Expected: PASS.

- [ ] **Step 5: Run the existing distribute tests for regressions**

Run: `npx jest rewards-distribute`
Expected: PASS (the new optional arg is backward compatible).

- [ ] **Step 6: Commit**

```bash
git add src/domain/rewards/ported/distribute.ts src/__tests__/rewards-slashing.test.ts
git commit -m "feat(slashing): feed accumulator from distributeExpense execution path"
```

---

## Task 5: Thread the accumulator through the wage split

**Files:**
- Modify: `src/domain/rewards/ported/wageSubsidy.ts` (the CRN-weights split, ~112-124, and `distributeWageSubsidy` ~253-263)
- Test: `src/__tests__/rewards-slashing.test.ts`

- [ ] **Step 1: Write the failing test**

Append to `src/__tests__/rewards-slashing.test.ts`:

```typescript
import { distributeWageSubsidy } from "../domain/rewards/ported/wageSubsidy";

describe("distributeWageSubsidy accumulator", () => {
  it("feeds the accumulator for linked CRNs", () => {
    const snap: Snapshot = {
      item_hash: "s", height: 100, ts: 0, confirmed: true,
      nodes: {},
      resource_nodes: {
        crnW: {
          hash: "crnW", status: "linked", score: 1,
          owner: "0xw", reward: "0xw", inactiveSince: 42,
        },
      },
    };
    const acc = new SlashAccumulator();
    // A window far enough into the subsidy period that the total is > 0.
    distributeWageSubsidy(
      Date.UTC(2026, 5, 1), Date.UTC(2026, 5, 2), snap,
      { accumulator: acc, stream: "wage_subsidy" },
    );
    const row = acc.list().find((r) => r.nodeId === "crnW");
    expect(row).toBeDefined();
    expect(row!.entry.address).toBe("0xw");
    expect(row!.entry.postDeath).toBeGreaterThan(0);
    expect(row!.entry.total).toBe(row!.entry.postDeath);
  });
});
```

- [ ] **Step 2: Run to verify it fails**

Run: `npx jest rewards-slashing -t "distributeWageSubsidy accumulator"`
Expected: FAIL — extra arg rejected.

- [ ] **Step 3: Thread the optional arg through `distributeWageSubsidy` → `applyWageSplit`**

In `wageSubsidy.ts`, add the optional `slash` param to `distributeWageSubsidy`
(line 253) and forward it to `applyWageSplit`:

```typescript
export function distributeWageSubsidy(
  t0Ms: number,
  t1Ms: number,
  snapshot: Snapshot,
  slash?: { accumulator: import("../slashing").SlashAccumulator; stream: import("../slashing").SlashStream },
): RewardStream {
  const total = wagePeriodTotal(t0Ms, t1Ms);
  return applyWageSplit(computeWageSplitCached(snapshot), total, slash, t1Ms);
}
```

Then in `applyWageSplit` (find its definition in the same file), add the
matching optional params `slash?` and `tsMs?: number`, and in the CRN payout
loop (where each linked CRN's `share` is added to the stream) add:

```typescript
    if (slash) {
      slash.accumulator.add(
        slash.stream, node.hash, rewardAddrOf(node), share,
        node.inactiveSince !== undefined,
        node.inactiveSince,
        tsMs ?? 0,
      );
    }
```

> `crnWeights` (lines 112-124) already holds `node` (the full
> `SnapshotResourceNode`), so `node.hash`, `node.inactiveSince`, and
> `rewardAddrOf(node)` are all in scope at the payout site. If `applyWageSplit`
> receives precomputed weights rather than the node, extend the weight tuple to
> carry `{ hash, inactiveSince, address }` so the payout loop can feed the
> accumulator.

- [ ] **Step 4: Run to verify pass**

Run: `npx jest rewards-slashing -t "distributeWageSubsidy accumulator"`
Expected: PASS.

- [ ] **Step 5: Run wage tests for regressions**

Run: `npx jest wage`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/domain/rewards/ported/wageSubsidy.ts src/__tests__/rewards-slashing.test.ts
git commit -m "feat(slashing): feed accumulator from wage subsidy split"
```

---

## Task 6: Anchor resolver — `fetchLastDistributionAnchor`

**Files:**
- Create: `src/dal/rewards/lastDistributionFetcher.ts`
- Test: `src/__tests__/rewards-last-distribution.test.ts` (new)

This mirrors nodestatus `get_latest_successful_credit_distribution`
(`credit_distribution.py:155`) 1:1 — same post type `credit-rewards-distribution`,
owners `[statusAccount, distributionRecipient]`, `status === "distribution"`,
ETH-confirmed, max `end_height`. Returns `{ endTimeMs, endHeight }` with
`endTimeMs = end_time * 1000` (the post stores `end_time` in **seconds**).

- [ ] **Step 1: Write the failing test for the pure selector**

Create `src/__tests__/rewards-last-distribution.test.ts`:

```typescript
import { selectLatestDistribution } from "../dal/rewards/lastDistributionFetcher";

describe("selectLatestDistribution", () => {
  const confirmed = (h: number, end_height: number, end_time: number) => ({
    content: { status: "distribution", end_height, end_time },
    confirmations: [{ height: h }],
  });

  it("picks the max end_height among confirmed distribution posts", () => {
    const posts = [
      confirmed(10, 100, 1000),
      confirmed(11, 200, 2000),
      { content: { status: "calculation", end_height: 999, end_time: 9 }, confirmations: [{ height: 12 }] },
      { content: { status: "distribution", end_height: 300, end_time: 3000 }, confirmations: [] }, // unconfirmed
    ];
    expect(selectLatestDistribution(posts as any)).toEqual({
      endHeight: 200, endTimeMs: 2000 * 1000,
    });
  });

  it("returns undefined when none qualify", () => {
    expect(selectLatestDistribution([] as any)).toBeUndefined();
  });
});
```

- [ ] **Step 2: Run to verify it fails**

Run: `npx jest rewards-last-distribution`
Expected: FAIL — module not found.

- [ ] **Step 3: Create the fetcher with a pure selector + a network call**

Create `src/dal/rewards/lastDistributionFetcher.ts`:

```typescript
import { config } from "../../utils/config";

const POST_TYPE = "credit-rewards-distribution";

interface RawPost {
  content: { status?: string; end_height?: number; end_time?: number };
  confirmations?: { height: number }[];
  confirmed?: boolean;
}

export interface DistributionAnchor {
  endHeight: number;
  endTimeMs: number;
}

function isEthConfirmed(p: RawPost): boolean {
  return (
    p.confirmed === true ||
    (Array.isArray(p.confirmations) && p.confirmations.length > 0)
  );
}

/** Pure: pick the confirmed `status==="distribution"` post with max end_height. */
export function selectLatestDistribution(
  posts: RawPost[],
): DistributionAnchor | undefined {
  let best: DistributionAnchor | undefined;
  let bestHeight = -1;
  for (const p of posts) {
    if (p.content?.status !== "distribution") continue;
    if (!isEthConfirmed(p)) continue;
    const eh = p.content.end_height;
    const et = p.content.end_time;
    if (typeof eh !== "number" || typeof et !== "number") continue;
    if (eh >= bestHeight) {
      bestHeight = eh;
      best = { endHeight: eh, endTimeMs: et * 1000 };
    }
  }
  return best;
}

/**
 * Resolve the anchor `A` from Aleph. Queries posts of type
 * `credit-rewards-distribution` owned by statusAccount or distributionRecipient.
 * Falls back to the data start when no confirmed distribution exists.
 */
export async function fetchLastDistributionAnchor(): Promise<DistributionAnchor> {
  const owners = [
    config.rewards.statusAccount.toLowerCase(),
    config.rewards.distributionRecipient.toLowerCase(),
  ].join(",");
  const url =
    `${config.aleph.apiServer}/api/v0/posts.json` +
    `?types=${POST_TYPE}&addresses=${owners}&pagination=200`;
  const res = await fetch(url);
  const json = (await res.json()) as { posts?: RawPost[] };
  const anchor = selectLatestDistribution(json.posts ?? []);
  if (anchor) return anchor;
  // Fallback: data start (no prior distribution). endHeight 0 means "no height
  // anchor"; retroactive slashing then includes the full data range, which is
  // correct pre-first-distribution.
  return {
    endHeight: 0,
    endTimeMs: new Date(config.rewards.dataStartIso).getTime(),
  };
}
```

> Confirm `config.aleph.apiServer` is the correct config path for the Aleph API
> base URL (the expenses fetcher uses the same base — match its exact usage). If
> the repo wraps Aleph fetches in a shared client, use that client instead of a
> raw `fetch`, keeping `selectLatestDistribution` as the pure tested core.

- [ ] **Step 4: Run to verify pass**

Run: `npx jest rewards-last-distribution`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/dal/rewards/lastDistributionFetcher.ts src/__tests__/rewards-last-distribution.test.ts
git commit -m "feat(slashing): add last-distribution anchor resolver"
```

---

## Task 7: `computeSlashingForRange` — orchestrate the slash for a request

**Files:**
- Create: `src/domain/rewards/slashingCompute.ts`
- Test: `src/__tests__/rewards-slashing-compute.test.ts` (new)

- [ ] **Step 1: Write the failing test (pure core, snapshots/expenses injected)**

Create `src/__tests__/rewards-slashing-compute.test.ts`:

```typescript
import { runSlashing } from "../domain/rewards/slashingCompute";
import type { ExpenseMessage, Snapshot } from "../domain/rewards/ported/types";

const BPD = 7130;

function snap(tsMs: number, height: number, inactiveSince?: number): Snapshot {
  return {
    item_hash: `s${tsMs}`, height, ts: tsMs, confirmed: true,
    nodes: {},
    resource_nodes: {
      crn1: { hash: "crn1", status: "linked", score: 1,
              owner: "0xc", reward: "0xc", inactiveSince },
    },
  };
}
function exp(tsMs: number): ExpenseMessage {
  return {
    item_hash: `e${tsMs}`, original_item_hash: `e${tsMs}`,
    publication_time_ms: 0, confirmed: true, ts_ms: tsMs,
    start_date_ms: 0, end_date_ms: 0, kind: "execution",
    credit_price_aleph: 1, credits: [{ amount: 100, node_id: "crn1" } as any],
    hold: [],
  };
}

describe("runSlashing (from-death)", () => {
  it("slashes only post-death execution_crn share", () => {
    // CRN active at first expense, inactive (height set) at the second.
    const closeHeight = 1_000 + 3 * BPD;
    const snapshots = [snap(0, 100, undefined), snap(50, closeHeight, 1_000)];
    const expenses = [exp(10), exp(60)]; // first active, second post-death
    const res = runSlashing({
      windowStartMs: 0, windowEndMs: 1_000_000,
      snapshots, expenses, includeHolderTier: false,
      enabledStreams: new Set(["credit_revenue"]),
      retroactive: false, thresholdDays: 3, blocksPerDay: BPD,
    });
    // Only the second expense's CRN share (100 * 0.60) is slashed.
    expect(Object.fromEntries(res.slashed)).toEqual({ "0xc": 60 });
  });

  it("retroactive slashes both expenses", () => {
    const closeHeight = 1_000 + 3 * BPD;
    const snapshots = [snap(0, 100, undefined), snap(50, closeHeight, 1_000)];
    const expenses = [exp(10), exp(60)];
    const res = runSlashing({
      windowStartMs: 0, windowEndMs: 1_000_000,
      snapshots, expenses, includeHolderTier: false,
      enabledStreams: new Set(["credit_revenue"]),
      retroactive: true, thresholdDays: 3, blocksPerDay: BPD,
    });
    expect(Object.fromEntries(res.slashed)).toEqual({ "0xc": 120 });
  });
});
```

- [ ] **Step 2: Run to verify it fails**

Run: `npx jest rewards-slashing-compute`
Expected: FAIL — module not found.

- [ ] **Step 3: Create the module**

Create `src/domain/rewards/slashingCompute.ts`:

```typescript
import {
  SlashAccumulator,
  computeSlashing,
  type SlashStream,
  type SlashingResult,
} from "./slashing";
import { distributeExpense } from "./ported/distribute";
import { distributeWageSubsidy } from "./ported/wageSubsidy";
import { findSnapshotAtOrBeforeByTs } from "./ported/computeRewards";
import type { ExpenseMessage, Snapshot } from "./ported/types";

export interface RunSlashingArgs {
  windowStartMs: number; // slash window start (A_time for retroactive, from for from-death)
  windowEndMs: number; // W_end (query `to`)
  snapshots: Snapshot[]; // sorted ascending by ts
  expenses: ExpenseMessage[];
  includeHolderTier: boolean;
  enabledStreams: Set<SlashStream>;
  retroactive: boolean;
  thresholdDays: number;
  blocksPerDay: number;
}

/**
 * Re-run the exact per-expense attribution (distributeExpense / wage split)
 * over the slash window with an accumulator, then run the slashing pass. The
 * slashable set is evaluated once, at the close-of-window snapshot (the latest
 * snapshot with ts <= windowEndMs). Pure: all inputs injected.
 */
export function runSlashing(args: RunSlashingArgs): SlashingResult {
  const acc = new SlashAccumulator();
  for (const exp of args.expenses) {
    if (exp.ts_ms < args.windowStartMs || exp.ts_ms >= args.windowEndMs) continue;
    const snap = findSnapshotAtOrBeforeByTs(args.snapshots, exp.ts_ms);
    if (!snap) continue;
    distributeExpense(exp, "credits", snap, undefined, {
      accumulator: acc, stream: "credit_revenue",
    });
    if (args.includeHolderTier && exp.hold.length > 0) {
      distributeExpense(exp, "hold", snap, undefined, {
        accumulator: acc, stream: "holder_tier",
      });
    }
  }
  // Wage: integrate over the window using the close snapshot, same as the
  // reward path. Fed once; the per-expense bucketing for wage uses windowEndMs.
  const closeSnap =
    findSnapshotAtOrBeforeByTs(args.snapshots, args.windowEndMs) ??
    args.snapshots[args.snapshots.length - 1];
  if (closeSnap) {
    distributeWageSubsidy(args.windowStartMs, args.windowEndMs, closeSnap, {
      accumulator: acc, stream: "wage_subsidy",
    });
  }
  const closeHeight = closeSnap?.height ?? 0;
  return computeSlashing(acc, closeSnap?.resource_nodes ?? {}, closeHeight, {
    enabledStreams: args.enabledStreams,
    retroactive: args.retroactive,
    thresholdDays: args.thresholdDays,
    blocksPerDay: args.blocksPerDay,
  });
}
```

> `findSnapshotAtOrBeforeByTs` is imported from `computeRewards.ts` — verify it
> is exported there; if not, export it (it is used internally already) or move it
> to a shared `snapshotLookup.ts` and import from both.

- [ ] **Step 4: Run to verify pass**

Run: `npx jest rewards-slashing-compute`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/domain/rewards/slashingCompute.ts src/__tests__/rewards-slashing-compute.test.ts
git commit -m "feat(slashing): add runSlashing range computation"
```

---

## Task 8: Surface `slashed` in the time series response

**Files:**
- Modify: `src/domain/rewards/timeSeries.ts` (types `BucketResponse`/`RootBlock` ~58-92; the assembly in `fetchTimeSeries` that builds `total` and `buckets`).
- Test: `src/__tests__/rewards-timeseries-slashed.test.ts` (new) or extend an existing timeSeries test.

- [ ] **Step 1: Add `slashed` to the response types**

In `timeSeries.ts`, add an optional `slashed` field to both `BucketResponse`
and `RootBlock`:

```typescript
export interface SlashedBlock {
  total: number; // aleph withheld
  byAddress?: Record<Address, number>;
  byStream?: Record<RewardSource, number>;
}
```

Add `slashed?: SlashedBlock;` to `BucketResponse` and `RootBlock`.

- [ ] **Step 2: Resolve the slash window and run `runSlashing` once per request**

In `fetchTimeSeries`, after the buckets are computed and before assembling the
response, add (gated by the master flag):

```typescript
  let rootSlashed: SlashedBlock | undefined;
  let perBucketSlashed: Map<string, SlashedBlock> | undefined; // keyed by bucket start ISO
  if (config.rewards.slashEnabled) {
    const enabledStreams = enabledSlashStreams(config);
    if (enabledStreams.size > 0) {
      const fromMs = /* actualRange.from in ms */;
      const toMs = /* actualRange.to in ms */;
      let windowStartMs = fromMs;
      if (config.rewards.slashRetroactive) {
        const anchor = await fetchLastDistributionAnchor();
        windowStartMs = anchor.endTimeMs; // §2.1: anchor to last confirmed distribution
      }
      const snapshots = await loadSnapshotsForRange(windowStartMs, toMs);
      const expenses = await loadExpensesForRange(windowStartMs, toMs); // confirmed-overlay loader
      const result = runSlashing({
        windowStartMs, windowEndMs: toMs, snapshots, expenses,
        includeHolderTier: enabledStreams.has("holder_tier"),
        enabledStreams,
        retroactive: config.rewards.slashRetroactive,
        thresholdDays: config.rewards.slashThresholdDays,
        blocksPerDay: config.rewards.slashBlocksPerDay,
      });
      rootSlashed = shapeSlashed(result, byAddress);
      perBucketSlashed = bucketizeSlashed(result, buckets); // by meta[].tsMs into bucket ranges
    }
  }
```

Add two small helpers in `timeSeries.ts`:

```typescript
function shapeSlashed(result: SlashingResult, byAddress: ByAddressLevel): SlashedBlock {
  let total = 0;
  const byStream: Record<string, number> = {};
  const byAddr: Record<string, number> = {};
  for (const m of result.meta) {
    total += m.amount;
    byStream[m.stream] = (byStream[m.stream] ?? 0) + m.amount;
  }
  for (const [addr, amt] of result.slashed) byAddr[addr] = amt;
  const block: SlashedBlock = { total, byStream: byStream as any };
  if (byAddress > 0) block.byAddress = byAddr;
  return block;
}

function bucketizeSlashed(
  result: SlashingResult,
  buckets: { startMs: number; endMs: number; startIso: string }[],
): Map<string, SlashedBlock> {
  const out = new Map<string, SlashedBlock>();
  for (const m of result.meta) {
    const b = buckets.find((x) => m.tsMs! >= x.startMs && m.tsMs! < x.endMs);
    if (!b) continue;
    const blk = out.get(b.startIso) ?? { total: 0, byStream: {} as any };
    blk.total += m.amount;
    (blk.byStream as any)[m.stream] = ((blk.byStream as any)[m.stream] ?? 0) + m.amount;
    out.set(b.startIso, blk);
  }
  return out;
}
```

> Fill the `/* ... in ms */` placeholders with the existing parsed range values
> in `fetchTimeSeries` (the function already computes `actualRange`/bucket bounds
> in epoch ms — reuse them; do not re-parse). Use the repo's existing snapshot/
> expense range loaders (the raw-path pieces in `bucketAggregate.ts` already call
> them — reuse the same functions, e.g. `loadSnapshotsForRange` / the expense day
> loader, rather than introducing new ones).

- [ ] **Step 3: Attach `slashed` to root and buckets**

Where `total: shapeRoot({...})` is built, add `slashed: rootSlashed` to the root
block after shaping. Where each `BucketResponse` is pushed, set
`slashed: perBucketSlashed?.get(bucket.startIso)`.

- [ ] **Step 4: Import the helpers**

At the top of `timeSeries.ts` add:

```typescript
import { runSlashing } from "./slashingCompute";
import { enabledSlashStreams } from "./slashing";
import { fetchLastDistributionAnchor } from "../../dal/rewards/lastDistributionFetcher";
import type { SlashingResult } from "./slashing";
```

And add `enabledSlashStreams` to `slashing.ts`:

```typescript
import { config as _config } from "../../utils/config";
export function enabledSlashStreams(
  config: typeof _config = _config,
): Set<SlashStream> {
  const s = new Set<SlashStream>();
  if (config.rewards.slashCreditRevenue) s.add("credit_revenue");
  if (config.rewards.slashHolderTier) s.add("holder_tier");
  if (config.rewards.slashWageSubsidy) s.add("wage_subsidy");
  return s;
}
```

- [ ] **Step 5: Write a time-series-level test**

Create `src/__tests__/rewards-timeseries-slashed.test.ts` that drives
`fetchTimeSeries` over a small in-memory range (mock the snapshot/expense
loaders and `fetchLastDistributionAnchor`), with one CRN inactive ≥ 3 days, and
asserts `response.total.slashed.total` equals the expected withheld amount and
that `response.total.slashed` is absent when `config.rewards.slashEnabled` is
false. Follow the mocking style of the existing time-series tests in
`src/__tests__/` (mirror how they stub the loaders).

- [ ] **Step 6: Run and type-check**

Run: `npx jest rewards-timeseries-slashed && npx tsc --noEmit`
Expected: PASS, no type errors.

- [ ] **Step 7: Commit**

```bash
git add src/domain/rewards/timeSeries.ts src/domain/rewards/slashing.ts src/__tests__/rewards-timeseries-slashed.test.ts
git commit -m "feat(slashing): surface slashed in time series response"
```

---

## Task 9: 1:1 cross-check fixture (gating)

**Files:**
- Create: `src/__tests__/rewards-slashing-parity.test.ts`
- Create: `src/__tests__/fixtures/slashing-parity.json` (shared fixture: snapshots + expenses + a confirmed distribution post with `end_time`/`end_height`)

This is the §6 acceptance gate: for the window `[A_time, end_time]` the
retroactive `slashed` totals — per stream and per address — must match the
nodestatus calculation exactly.

- [ ] **Step 1: Build the shared fixture**

Create `src/__tests__/fixtures/slashing-parity.json` containing: a small set of
snapshots (with `height`, `ts`, `resource_nodes` carrying `inactive_since`), a set
of `aleph_credit_expense`-shaped expenses, and one confirmed
`credit-rewards-distribution` post with known `end_height`/`end_time`. The SAME
JSON must be importable by a nodestatus test (Task in the nodestatus plan can
read this file path or a copy) so both engines run identical inputs.

> Coordinate the exact numbers with the nodestatus integration test (nodestatus
> plan Task 7). The fixture is the contract: identical snapshots + expenses +
> anchor → identical `slashed`.

- [ ] **Step 2: Write the parity test (api-credit side)**

Create `src/__tests__/rewards-slashing-parity.test.ts`:

```typescript
import { runSlashing } from "../domain/rewards/slashingCompute";
import fixture from "./fixtures/slashing-parity.json";
// EXPECTED is the nodestatus-produced slashed map for the same window, pasted
// here as the contract (and asserted equal in the nodestatus test too).
const EXPECTED_BY_ADDRESS: Record<string, number> = {
  /* "0x...": amount, ... */
};

describe("slashing parity with nodestatus (retroactive)", () => {
  it("matches nodestatus slashed for [A_time, end_time]", () => {
    const A_time_ms = fixture.distribution.end_time * 1000;
    const end_time_ms = fixture.windowEndMs;
    const res = runSlashing({
      windowStartMs: A_time_ms, windowEndMs: end_time_ms,
      snapshots: fixture.snapshots as any,
      expenses: fixture.expenses as any,
      includeHolderTier: true,
      enabledStreams: new Set(["credit_revenue", "holder_tier", "wage_subsidy"]),
      retroactive: true, thresholdDays: 3, blocksPerDay: 7130,
    });
    expect(Object.fromEntries(res.slashed)).toEqual(EXPECTED_BY_ADDRESS);
  });
});
```

> Populate `EXPECTED_BY_ADDRESS` from the nodestatus integration test's printed
> `slashed` for the identical fixture window. Any divergence is a real bug — do
> not "adjust to match"; investigate which engine diverges from the spec.

- [ ] **Step 3: Run the parity test**

Run: `npx jest rewards-slashing-parity`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add src/__tests__/rewards-slashing-parity.test.ts src/__tests__/fixtures/slashing-parity.json
git commit -m "test(slashing): 1:1 parity cross-check with nodestatus"
```

---

## Task 10: Final verification

- [ ] **Step 1: Full type-check**

Run: `npx tsc --noEmit`
Expected: no errors.

- [ ] **Step 2: Full test run**

Run: `npm test`
Expected: all rewards tests green, including the new slashing + parity tests.

- [ ] **Step 3: Flag-off sanity**

Set `REWARDS_SLASH_ENABLED=false` and confirm `fetchTimeSeries` output omits
`slashed` (the timeseries-slashed test covers this; re-run it).

Run: `REWARDS_SLASH_ENABLED=false npx jest rewards-timeseries-slashed`
Expected: PASS (the flag-off assertion).

---

## Spec coverage check

- §2 predicate (3-day, block-height) → Task 3 (`isSlashable`).
- §2 modes (from-death default / retroactive) → Tasks 3, 7 (`postDeath`/`total`).
- §2.1 anchor (exact query, `end_time`×1000) → Task 6 (`fetchLastDistributionAnchor` / `selectLatestDistribution`).
- §2.2 additivity (bucketing by expense ts) → Task 8 (`bucketizeSlashed`).
- §4.1 parse `inactiveSince` → Task 2.
- §4.2 slashing in compute, `slashed` per bucket/root → Tasks 4, 5, 7, 8.
- §4.3 retroactive anchor resolver + 1:1 invariant → Tasks 6, 9.
- §4.4 config → Task 1.
- §6 testing (flag-off, parity gate) → Tasks 8, 9, 10.
