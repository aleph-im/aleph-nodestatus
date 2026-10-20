#!/usr/bin/env python3
"""Compare per-node ALEPH from CCN revenue periods vs aleph_credit_expense posts.

Cutover tool: always exits 0.

Usage:
    python scripts/compare_revenue_vs_expenses.py --from TS --to TS \
        [--api-server URL] [--expense-sender ADDR] [--json] [--old-only]

The default --expense-sender is the signer (message `sender`) of the
production expense posts. The old backoffice address
0x2E4454fAD1906c0Ce6e45cBFA05cE898Ac3AC1dC is the content owner, not the
signer; messages.json?addresses= filters on the sender.

The new-source half needs the CCN endpoint: run without --old-only once the
CCN exposes /api/v0/credits/revenue (live validation is a cutover step).
"""
import argparse
import asyncio
import json
import logging
import sys
from datetime import datetime, timezone
from collections import defaultdict

import aiohttp

LOGGER = logging.getLogger("compare_revenue")
# Expense posts before this date use the old 100-credits-per-USD unit
# (x10,000 relative to today's); we warn but do not convert.
UNIT_CUTOFF = int(datetime(2026, 2, 2, tzinfo=timezone.utc).timestamp())

DEFAULT_SENDER = "0x6aeaEEb08720DEc9d6dae1A8fc49344Dd99391Ac"


async def fetch_expense_messages(api_server, start_time, end_time, sender):
    params = {
        "msgType": "POST",
        "contentTypes": "aleph_credit_expense",
        "startDate": int(start_time),
        "endDate": int(end_time),
        "pagination": 500,
        "page": 1,
        "sort_order": "1",
        "sort_by": "tx-time",
        "addresses": sender,
    }
    seen, out = set(), []
    async with aiohttp.ClientSession() as session:
        while True:
            async with session.get(
                f"{api_server}/api/v0/messages.json", params=params,
                timeout=aiohttp.ClientTimeout(total=60),
            ) as resp:
                resp.raise_for_status()
                data = await resp.json()
            page = data.get("messages", [])
            if not page:
                break
            for m in page:
                if m["item_hash"] not in seen:
                    seen.add(m["item_hash"])
                    out.append(m)
            per_page = data.get("pagination_per_page", 500)
            if params["page"] * per_page >= data.get("pagination_total", 0):
                break
            params["page"] += 1
    return out


def parse_message(msg):
    """Return (expense_type, expense) or (None, None)."""
    content = msg.get("content", {}).get("content", {})
    tags = content.get("tags", [])
    expense = content.get("expense", {})
    if not expense:
        return None, None
    if "type_storage" in tags:
        return "storage", expense
    if "type_execution" in tags:
        return "execution", expense
    return None, None


def aggregate_old(messages):
    """-> (exec_by_node, hold_by_node, storage_total) in ALEPH."""
    execs, holds, storage = defaultdict(float), defaultdict(float), 0.0
    dropped = 0
    for msg in messages:
        etype, exp = parse_message(msg)
        if etype is None:
            continue
        price = float(exp.get("credit_price_aleph", 0))
        if etype == "storage":
            storage += sum(c.get("amount", 0) for c in exp.get("credits", [])) * price
            continue
        for c in exp.get("credits", []):
            if c.get("node_id"):
                execs[c["node_id"]] += c.get("amount", 0) * price
            else:
                dropped += 1
        for c in exp.get("rewards") or exp.get("hold") or []:
            if c.get("node_id"):
                holds[c["node_id"]] += c.get("amount", 0) * price
    if dropped:
        LOGGER.warning("dropped %d execution credit lines with no node_id", dropped)
    return dict(execs), dict(holds), storage


def aggregate_new(periods):
    """-> (exec_by_node, hold_by_node, storage_total) in ALEPH."""
    execs, holds, storage = defaultdict(float), defaultdict(float), 0.0
    for p in periods:
        price = float(p["credit_price_aleph"])
        storage += (p.get("storage") or 0) * price
        for nid, n in (p.get("nodes") or {}).items():
            execs[nid] += n.get("execution", 0) * price
            holds[nid] += n.get("hold_equivalent", 0) * price
    return dict(execs), dict(holds), storage


def diff_rows(old, new):
    rows = []
    for nid in set(old) | set(new):
        o, n = old.get(nid, 0.0), new.get(nid, 0.0)
        d = n - o
        rows.append({
            "node_id": nid, "old_aleph": o, "new_aleph": n, "diff": d,
            "diff_pct": (d / o * 100) if o else None,
        })
    rows.sort(key=lambda r: abs(r["diff"]), reverse=True)
    return rows


def build_report(old, new):
    (oe, oh, os_), (ne, nh, ns) = old, new
    return {
        "execution": diff_rows(oe, ne),
        "hold": diff_rows(oh, nh),
        "totals": {
            "execution": {"old": sum(oe.values()), "new": sum(ne.values())},
            "hold": {"old": sum(oh.values()), "new": sum(nh.values())},
            "storage": {"old": os_, "new": ns},
        },
    }


def _pct(r):
    return "n/a" if r["diff_pct"] is None else f"{r['diff_pct']:.2f}%"


def print_report(rep):
    for key in ("execution", "hold"):
        print(f"\n== {key} ==")
        print(f"{'node_id':<66} {'old_aleph':>14} {'new_aleph':>14} {'diff':>14} {'diff_pct':>10}")
        for r in rep[key]:
            print(f"{r['node_id']:<66} {r['old_aleph']:>14.4f} {r['new_aleph']:>14.4f} "
                  f"{r['diff']:>14.4f} {_pct(r):>10}")
    print("\n== totals ==")
    for k, t in rep["totals"].items():
        d = t["new"] - t["old"]
        pct = f"{d / t['old'] * 100:.2f}%" if t["old"] else "n/a"
        print(f"{k:<10} old={t['old']:.4f} new={t['new']:.4f} diff={d:.4f} ({pct})")


async def main_async(args):
    if args.from_time < UNIT_CUTOFF:
        LOGGER.warning(
            "window starts before 2026-02-02 00:00 UTC: pre-cutoff expense posts "
            "use the old 100-credits-per-USD unit (x10,000 vs today's); not converted")
    msgs = await fetch_expense_messages(
        args.api_server, args.from_time, args.to_time, args.expense_sender)
    old = aggregate_old(msgs)
    if args.old_only:
        new = ({}, {}, 0.0)
    else:
        from aleph_nodestatus.revenue import fetch_revenue
        periods = await fetch_revenue(args.api_server, args.from_time, args.to_time)
        new = aggregate_new(periods)
    rep = build_report(old, new)
    if args.json:
        print(json.dumps(rep, indent=2))
    else:
        print(f"expense posts: {len(msgs)}")
        print_report(rep)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--from", dest="from_time", type=int, required=True)
    ap.add_argument("--to", dest="to_time", type=int, required=True)
    ap.add_argument("--api-server", default="https://api2.aleph.im")
    ap.add_argument(
        "--expense-sender", default=DEFAULT_SENDER,
        help="signer (message sender) of the production expense posts; "
             "0x2E4454fAD1906c0Ce6e45cBFA05cE898Ac3AC1dC is the content owner, "
             "not the signer, and messages.json?addresses= filters on the sender")
    ap.add_argument("--json", action="store_true")
    ap.add_argument("--old-only", action="store_true",
                    help="skip the CCN revenue source (not deployed yet)")
    args = ap.parse_args()
    logging.basicConfig(level=logging.WARNING, format="%(levelname)s: %(message)s")
    try:
        asyncio.run(main_async(args))
    except Exception as e:  # cutover tool: never fail the shell
        print(f"error: {e}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
