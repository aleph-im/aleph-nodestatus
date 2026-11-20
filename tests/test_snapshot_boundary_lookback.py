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


def _agg_msg(ts, item_hash, confirmed=True, sender=None):
    """Minimal corechannel AGGREGATE message dict as parsed by
    `fetch_node_snapshots` (post `_iter_messages_dedup`)."""
    message = {
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
    if sender is not None:
        message["sender"] = sender
    return message


class _Client:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False


def _install_fake_feed(monkeypatch, messages, windows, senders=None):
    """Serve `messages` filtered by each MessageFilter window; record
    every (addresses, start_date, end_date) requested in `windows`."""
    if senders is None:
        senders = [cd.settings.status_sender]
    monkeypatch.setattr(cd.settings, "snapshot_senders", senders)

    async def fake_iter(client, message_filter):
        windows.append((
            tuple(message_filter.addresses or []),
            message_filter.start_date,
            message_filter.end_date,
        ))
        addresses = set(message_filter.addresses or [])
        for m in messages:
            if (m.get("sender") is not None
                    and addresses
                    and m["sender"] not in addresses):
                continue
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
    unchanged; the calculation layer then refuses any expense that falls
    before the first snapshot instead of silently dropping it."""
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


OLD_SENDER = "0xold-snapshot-publisher"
NEW_SENDER = "0xnew-snapshot-publisher"


def test_merges_historical_and_current_publishers_during_transition(monkeypatch):
    """The old publisher supplies the boundary and pre-rotation snapshots;
    the new publisher takes over at its first in-range aggregate. Older
    sender messages after that point must be ignored."""
    windows = []
    msgs = [
        _agg_msg(START - 3 * DAY, "old-boundary", sender=OLD_SENDER),
        _agg_msg(START + DAY, "old-in-window", sender=OLD_SENDER),
        _agg_msg(START + 3 * DAY, "old-after-transition", sender=OLD_SENDER),
        _agg_msg(START + 2 * DAY, "new-first", sender=NEW_SENDER),
        _agg_msg(START + 4 * DAY, "new-second", sender=NEW_SENDER),
    ]
    _install_fake_feed(
        monkeypatch, msgs, windows, senders=[NEW_SENDER, OLD_SENDER],
    )

    out_hashes = []
    snaps = asyncio.run(cd.fetch_node_snapshots(
        "http://x", START, END,
        sender=[NEW_SENDER, OLD_SENDER],
        out_hashes=out_hashes,
    ))

    assert [s[0] for s in snaps] == [
        START - 3 * DAY,
        START + DAY,
        START + 2 * DAY,
        START + 4 * DAY,
    ]
    node_hashes = {next(iter(s[1])) for s in snaps}
    assert "ccn-old-boundary" in node_hashes
    assert "ccn-old-in-window" in node_hashes
    assert "ccn-new-first" in node_hashes
    assert "ccn-new-second" in node_hashes
    assert "ccn-old-after-transition" not in node_hashes
    assert "old-after-transition" not in out_hashes


def test_stale_historical_boundary_is_ignored_without_in_window_data(monkeypatch):
    """A historical publisher with only pre-window data must not provide a
    stale boundary for a later calculation."""
    windows = []
    msgs = [
        _agg_msg(START - 3 * DAY, "stale-old-boundary", sender=OLD_SENDER),
        _agg_msg(START + 2 * DAY, "new-first", sender=NEW_SENDER),
        _agg_msg(START + 4 * DAY, "new-second", sender=NEW_SENDER),
    ]
    _install_fake_feed(
        monkeypatch, msgs, windows, senders=[NEW_SENDER, OLD_SENDER],
    )

    snaps = asyncio.run(cd.fetch_node_snapshots(
        "http://x", START, END,
        sender=[NEW_SENDER, OLD_SENDER],
    ))

    assert [s[0] for s in snaps] == [START + 2 * DAY, START + 4 * DAY]
    node_hashes = {next(iter(s[1])) for s in snaps}
    assert "ccn-stale-old-boundary" not in node_hashes


def test_uncovered_expense_fails_closed():
    """A missing boundary must abort, not silently produce a partial total."""
    with pytest.raises(ValueError, match="refusing to calculate partial rewards"):
        cd._apply_expenses_to_snapshots(
            [(START - 1, "execution", {})],
            [(START, {}, {})],
            False,
            None,
        )
