"""Boundary-snapshot guarantee for `fetch_node_snapshots`.

Mirrors aleph-api-credit `snapshotsStore.loadSnapshotsForRange`: the
returned set must always include the latest snapshot with
`ts <= start_time` (the "boundary"), walking back day-by-day (up to 30
days) past the default 1-day lookback when the near window has none —
e.g. after a corechannel publisher outage. Without the boundary,
`_apply_expenses_to_snapshots` silently drops every expense billed
before the first snapshot in range.
"""

import asyncio

import pytest

from aleph_nodestatus import credit_distribution as cd


START = 1_782_734_171.0   # 2026-06-29T11:56:11Z — real calc window start
END = 1_784_890_163.0     # 2026-07-24T10:49:23Z
DAY = 86_400.0


def _agg_msg(ts, item_hash, confirmed=True):
    """Minimal corechannel AGGREGATE message dict as parsed by
    `fetch_node_snapshots` (post `_iter_messages_dedup`)."""
    return {
        "item_hash": item_hash,
        "time": ts,
        "confirmations": (
            [{"chain": "ETH", "height": 1}] if confirmed else []
        ),
        "content": {
            "key": "corechannel",
            "content": {
                "nodes": [{"hash": f"ccn-{item_hash}"}],
                "resource_nodes": [{"hash": f"crn-{item_hash}"}],
            },
        },
    }


class _Client:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False


def _install_fake_feed(monkeypatch, messages, windows):
    """Serve `messages` filtered by each MessageFilter window; record
    every (start_date, end_date) requested in `windows`."""

    async def fake_iter(client, message_filter):
        windows.append((message_filter.start_date, message_filter.end_date))
        for m in messages:
            if message_filter.start_date <= m["time"] <= message_filter.end_date:
                yield m

    monkeypatch.setattr(cd, "_aleph_client", lambda api: _Client())
    monkeypatch.setattr(cd, "_iter_messages_dedup", fake_iter)


def test_walks_back_for_boundary_snapshot_after_gap(monkeypatch):
    """Publisher outage: nothing in [start-1d, start]; an older aggregate
    exists 3.5 days before start. It must be returned as the boundary
    (first element, ts <= start) instead of being invisible."""
    windows = []
    msgs = [
        _agg_msg(START - 3.5 * DAY, "old-boundary"),
        _agg_msg(START + 2 * DAY, "in-window-1"),
        _agg_msg(START + 3 * DAY, "in-window-2"),
    ]
    _install_fake_feed(monkeypatch, msgs, windows)

    snaps = asyncio.run(cd.fetch_node_snapshots("http://x", START, END))

    assert len(snaps) == 3
    assert snaps[0][0] == START - 3.5 * DAY
    assert snaps[0][0] <= START
    assert "ccn-old-boundary" in snaps[0][1]


def test_no_walk_back_when_boundary_in_default_window(monkeypatch):
    """Healthy case: a snapshot within the default 1-day lookback already
    covers the boundary — exactly one fetch, no widening."""
    windows = []
    msgs = [
        _agg_msg(START - 3_600, "near-boundary"),
        _agg_msg(START + DAY, "in-window"),
    ]
    _install_fake_feed(monkeypatch, msgs, windows)

    snaps = asyncio.run(cd.fetch_node_snapshots("http://x", START, END))

    assert len(windows) == 1
    assert [s[0] for s in snaps] == [START - 3_600, START + DAY]


def test_walk_back_keeps_only_latest_confirmed_boundary(monkeypatch):
    """The walk-back day may hold several aggregates; only the LATEST
    ETH-confirmed one is kept (minimal covering set, mirroring
    api-credit's single boundary snapshot)."""
    windows = []
    day1_start = START - 2 * DAY   # first walk-back window
    msgs = [
        _agg_msg(day1_start + 1_000, "older-confirmed"),
        _agg_msg(day1_start + 5_000, "latest-confirmed"),
        _agg_msg(day1_start + 9_000, "later-unconfirmed", confirmed=False),
        _agg_msg(START + DAY, "in-window"),
    ]
    _install_fake_feed(monkeypatch, msgs, windows)

    snaps = asyncio.run(cd.fetch_node_snapshots("http://x", START, END))

    assert [s[0] for s in snaps] == [day1_start + 5_000, START + DAY]
    assert "ccn-latest-confirmed" in snaps[0][1]


def test_gives_up_after_30_walk_back_days(monkeypatch):
    """No boundary within 30 extra days: return the in-window snapshots
    unchanged (expenses before them keep the existing skip+warning
    behavior, same as api-credit with an undefined boundary)."""
    windows = []
    msgs = [_agg_msg(START + 2 * DAY, "in-window")]
    _install_fake_feed(monkeypatch, msgs, windows)

    snaps = asyncio.run(cd.fetch_node_snapshots("http://x", START, END))

    assert [s[0] for s in snaps] == [START + 2 * DAY]
    # 1 main window + 30 walk-back days, then stop.
    assert len(windows) == 31


def test_boundary_hash_reported_in_out_hashes(monkeypatch):
    """Dry-run debug: the walk-back boundary snapshot is part of the
    consumed input set, so its hash must land in `out_hashes` — and the
    non-boundary aggregates of the walk-back day must not."""
    windows = []
    day1_start = START - 2 * DAY
    msgs = [
        _agg_msg(day1_start + 1_000, "older-confirmed"),
        _agg_msg(day1_start + 5_000, "latest-confirmed"),
        _agg_msg(START + DAY, "in-window"),
    ]
    _install_fake_feed(monkeypatch, msgs, windows)

    out_hashes = []
    asyncio.run(cd.fetch_node_snapshots(
        "http://x", START, END, out_hashes=out_hashes,
    ))

    assert "latest-confirmed" in out_hashes
    assert "in-window" in out_hashes
    assert "older-confirmed" not in out_hashes
