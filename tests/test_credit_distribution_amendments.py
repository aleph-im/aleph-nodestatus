"""Tests for the /posts.json-based expense fetcher and the amendment-
aware `_resolve_original_height` path."""

import asyncio
from unittest.mock import MagicMock

import pytest

from aleph.sdk.exceptions import MessageNotFoundError

from aleph_nodestatus import credit_distribution as cd


def _post(item_hash, *, original_item_hash=None, confirmations=None,
          time=1.5, end_date_ms=1500, credits=None):
    """Build a post dict in the same shape `Post.model_dump(mode='json')`
    returns. Defaults are tuned so that, with snapshot ts=1.0, lookup
    routes the expense onto the snapshot.
    """
    if confirmations is None:
        confirmations = [{"chain": "ETH", "height": 100}]
    if credits is None:
        credits = [{"amount": 1000, "node_id": "r1", "address": "0xU1"}]
    return {
        "item_hash": item_hash,
        "original_item_hash": original_item_hash or item_hash,
        "time": time,
        "confirmations": confirmations,
        "content": {
            "tags": ["credit_expense", "type_execution"],
            "expense": {
                "credit_price_aleph": 0.001,
                "end_date": end_date_ms,
                "credits": credits,
            },
        },
    }


def _fake_post_obj(post_dict):
    """Wrap a dict so it quacks like a pydantic model (model_dump returns it)."""
    m = MagicMock()
    m.item_hash = post_dict["item_hash"]
    m.model_dump.return_value = post_dict
    return m


def _fake_message_obj(msg_dict):
    m = MagicMock()
    m.model_dump.return_value = msg_dict
    return m


class _FakeCursorResponse:
    def __init__(self, posts, next_cursor=""):
        self.posts = posts
        self.next_cursor = next_cursor


class _FakeClient:
    """Records calls so tests can assert on cache hits, retry counts, etc."""

    def __init__(self, post_pages, originals=None,
                 original_fetch_errors=None):
        # post_pages: list of (posts_list_for_page, next_cursor)
        self.post_pages = list(post_pages)
        self.cursor_calls = 0
        # originals: dict[item_hash, message_dict_or_None]
        self.originals = originals or {}
        self.original_calls = []
        # original_fetch_errors: dict[item_hash, [exc_or_None_per_attempt]]
        # If an entry is set, errors fire in order; once exhausted, fall
        # through to `originals`.
        self.original_fetch_errors = original_fetch_errors or {}

    async def get_posts_cursor(self, page_size=None, cursor="",
                               post_filter=None, **kwargs):
        self.cursor_calls += 1
        posts, next_cursor = self.post_pages.pop(0)
        return _FakeCursorResponse(
            [_fake_post_obj(p) for p in posts],
            next_cursor=next_cursor,
        )

    async def get_message(self, item_hash):
        self.original_calls.append(item_hash)
        errs = self.original_fetch_errors.get(item_hash)
        if errs:
            err = errs.pop(0)
            if err is not None:
                raise err
        if item_hash not in self.originals:
            raise MessageNotFoundError(f"No such hash {item_hash}")
        return _fake_message_obj(self.originals[item_hash])


def _drain(client, post_filter=None, last_end_height=None):
    async def go():
        out = []
        async for p in cd._iter_posts_dedup(
            client, post_filter, last_end_height=last_end_height,
        ):
            out.append(p)
        return out
    return asyncio.run(go())


def test_iter_posts_dedup_skips_unconfirmed_posts():
    """Posts with no ETH confirmation are dropped silently."""
    client = _FakeClient(
        post_pages=[
            ([_post("h_unc", confirmations=[]),
              _post("h_ok")],
             ""),
        ],
    )
    out = _drain(client)
    assert [p["item_hash"] for p in out] == ["h_ok"]


def test_iter_posts_dedup_stamps_original_height_for_non_amends():
    """For non-amended posts, `_original_height` = the post's own ETH height."""
    client = _FakeClient(post_pages=[(
        [_post("h1", confirmations=[{"chain": "ETH", "height": 999}])],
        "",
    )])
    out = _drain(client)
    assert out[0]["_original_height"] == 999
    # No extra `get_message` calls for non-amends.
    assert client.original_calls == []


def test_iter_posts_dedup_resolves_original_height_for_amends():
    """Amend posts trigger a `get_message` to the original; the resolved
    height is stamped as `_original_height` (not the amend's height)."""
    original = {
        "item_hash": "h_orig",
        "confirmations": [{"chain": "ETH", "height": 100}],
        "time": 1.0,
    }
    client = _FakeClient(
        post_pages=[(
            [_post("h_amend",
                   original_item_hash="h_orig",
                   confirmations=[{"chain": "ETH", "height": 200}])],
            "",
        )],
        originals={"h_orig": original},
    )
    out = _drain(client)
    assert out[0]["_original_height"] == 100
    assert client.original_calls == ["h_orig"]


def test_iter_posts_dedup_caches_originals_across_amends():
    """Two amends of the same original cause exactly ONE `get_message` call."""
    original = {
        "item_hash": "h_orig",
        "confirmations": [{"chain": "ETH", "height": 100}],
        "time": 1.0,
    }
    client = _FakeClient(
        post_pages=[(
            [_post("amend_a", original_item_hash="h_orig"),
             _post("amend_b", original_item_hash="h_orig")],
            "",
        )],
        originals={"h_orig": original},
    )
    out = _drain(client)
    assert len(out) == 2
    assert all(p["_original_height"] == 100 for p in out)
    assert client.original_calls == ["h_orig"]


def test_iter_posts_dedup_drops_amend_when_original_unconfirmed():
    """An amend whose original lacks ETH confirmation gets dropped (warn)."""
    unconfirmed_original = {
        "item_hash": "h_orig",
        "confirmations": [],
        "time": 1.0,
    }
    client = _FakeClient(
        post_pages=[(
            [_post("h_amend", original_item_hash="h_orig")],
            "",
        )],
        originals={"h_orig": unconfirmed_original},
    )
    out = _drain(client)
    assert out == []


def test_iter_posts_dedup_drops_amend_when_original_missing():
    """An amend whose original 404s gets dropped (warn) — never crashes."""
    client = _FakeClient(
        post_pages=[(
            [_post("h_amend", original_item_hash="h_orig_404")],
            "",
        )],
        originals={},  # nothing — get_message will raise MessageNotFoundError
    )
    out = _drain(client)
    assert out == []


def test_resolve_original_height_retries_then_raises(monkeypatch):
    """Persistent transient failures propagate after exhausting retries."""
    import aiohttp

    async def _no_sleep(_s):
        return
    monkeypatch.setattr(cd.asyncio, "sleep", _no_sleep)  # cap wallclock
    client = _FakeClient(
        post_pages=[],
        originals={},
        original_fetch_errors={
            "h_orig": [aiohttp.ClientError("boom")] * cd._ORIGINAL_HEIGHT_RETRIES,
        },
    )

    async def go():
        return await cd._resolve_original_height(client, "h_orig", {})

    with pytest.raises(RuntimeError, match="Could not fetch original"):
        asyncio.run(go())
    # All 5 attempts were made.
    assert len(client.original_calls) == cd._ORIGINAL_HEIGHT_RETRIES


def test_resolve_original_height_retries_then_succeeds(monkeypatch):
    """If a transient error clears before the retry budget is gone, the
    fetched height is returned."""
    import aiohttp

    async def _no_sleep(_s):
        return
    monkeypatch.setattr(cd.asyncio, "sleep", _no_sleep)
    # 2 failures, then success.
    client = _FakeClient(
        post_pages=[],
        originals={"h_orig": {
            "confirmations": [{"chain": "ETH", "height": 42}],
        }},
        original_fetch_errors={
            "h_orig": [aiohttp.ClientError("boom"),
                       aiohttp.ClientError("boom"),
                       None],  # third attempt: no error → fall through
        },
    )

    async def go():
        return await cd._resolve_original_height(client, "h_orig", {})

    assert asyncio.run(go()) == 42
    assert len(client.original_calls) == 3


def test_parse_message_prefers_expense_end_date_over_msg_time():
    """`_parse_message` uses `expense.end_date` (ms) for ts, not `msg.time`.
    This is the routing decision that protects against amendments whose
    envelope `time` is days after the real consumption."""
    msg = _post(
        "h1",
        time=1778000000.0,            # seconds
        end_date_ms=1778059834782,    # ms → 1778059834.782 s (later)
    )
    ts_s, exp_type, expense = cd._parse_message(msg)
    assert ts_s == pytest.approx(1778059834.782)
    assert exp_type == "execution"


def test_parse_message_falls_back_to_msg_time_when_no_billing_window():
    """No `end_date` / `start_date` → fall back to `msg.time` (s) so legacy
    payloads aren't silently dropped."""
    msg = _post("h1", time=42.0)
    msg["content"]["expense"].pop("end_date", None)
    msg["content"]["expense"].pop("start_date", None)
    ts_s, _, _ = cd._parse_message(msg)
    assert ts_s == 42.0


def test_parse_message_skips_unconfirmed():
    """Confirmed-only guard is explicit (not just a side effect of needing
    `height` for the old bisect)."""
    msg = _post("h1", confirmations=[])
    assert cd._parse_message(msg) == (None, None, None)


def test_normalize_ts_handles_ms_seconds_iso_string():
    """Magnitude-based detection: > 1e12 → ms, else seconds. ISO strings
    (how `AlephMessage.time` serializes in `model_dump(mode='json')`) are
    parsed via `datetime.fromisoformat`."""
    assert cd._normalize_ts_to_seconds(None) is None
    assert cd._normalize_ts_to_seconds(1500.0) == 1500.0       # already s
    assert cd._normalize_ts_to_seconds(1778059834782) == pytest.approx(
        1778059834.782)                                         # ms
    # ISO 8601 with trailing Z → UTC seconds.
    s = cd._normalize_ts_to_seconds("2023-11-14T22:05:47.105772Z")
    assert s == pytest.approx(1699999547.105772, abs=1)


# ────────── cross-run leak mitigation (lookback + conf_height dedup) ──────────

def test_iter_posts_dedup_skips_post_already_seen_by_previous_run():
    """A post whose ETH confirmation height is ≤ last_end_height was
    already visible to the previous successful run and must not be
    re-processed (no double-pay). This is the dedup half of the
    cross-run leak mitigation."""
    client = _FakeClient(post_pages=[(
        [_post("h_old", confirmations=[{"chain": "ETH", "height": 100}])],
        "",
    )])
    # Previous run closed at height 110 → 100 ≤ 110 → skip.
    out = _drain(client, last_end_height=110)
    assert out == []


def test_iter_posts_dedup_recovers_post_confirmed_after_previous_run():
    """A post whose ETH confirmation landed AFTER the previous run's
    cursor (conf_height > last_end_height) is the leak case the
    mitigation exists for. It must be yielded so the current run pays it."""
    client = _FakeClient(post_pages=[(
        [_post("h_new", confirmations=[{"chain": "ETH", "height": 200}])],
        "",
    )])
    # Previous run closed at 110; this post confirmed at 200 (afterwards).
    out = _drain(client, last_end_height=110)
    assert [p["item_hash"] for p in out] == ["h_new"]


def test_iter_posts_dedup_last_end_height_none_disables_dedup():
    """First run (no previous distribution post on Aleph) → no cursor to
    compare against. Process everything as before."""
    client = _FakeClient(post_pages=[(
        [_post("h_any", confirmations=[{"chain": "ETH", "height": 100}])],
        "",
    )])
    out = _drain(client, last_end_height=None)
    assert [p["item_hash"] for p in out] == ["h_any"]


def test_iter_posts_dedup_amend_dedup_uses_original_height():
    """For an amended post the dedup boundary is the ORIGINAL'S conf
    height — that's the one the previous run could have seen and paid.
    The amend itself might be at a much higher block; using the amend's
    height would mistakenly re-process expenses already paid via their
    original."""
    original = {
        "item_hash": "h_orig",
        "confirmations": [{"chain": "ETH", "height": 90}],
        "time": 1.0,
    }
    client = _FakeClient(
        post_pages=[(
            [_post("h_amend",
                   original_item_hash="h_orig",
                   confirmations=[{"chain": "ETH", "height": 300}])],
            "",
        )],
        originals={"h_orig": original},
    )
    # last_end_height=110 → original (conf=90) was visible → skip
    # despite the amend (conf=300) sitting past the cursor.
    out = _drain(client, last_end_height=110)
    assert out == []


def test_fetch_expense_messages_widens_window_when_last_end_height_set(
        monkeypatch):
    """`_fetch_expense_messages(last_end_height=…)` extends `start_date`
    backwards by `_EXPENSE_FETCH_LOOKBACK_SECONDS` so the dedup rule
    has older expenses to filter through."""
    captured = {}

    class _C:
        async def __aenter__(self): return self
        async def __aexit__(self, *a): return False
        async def get_posts_cursor(self, post_filter=None, cursor="", **kw):
            captured["start_date"] = post_filter.start_date
            captured["end_date"] = post_filter.end_date
            return _FakeCursorResponse([], next_cursor="")

    monkeypatch.setattr(cd, "_aleph_client", lambda api: _C())

    asyncio.run(cd._fetch_expense_messages(
        "http://x", start_time=1000.0, end_time=2000.0,
        sender="0xS", last_end_height=42,
    ))
    assert captured["start_date"] == 1000.0 - cd._EXPENSE_FETCH_LOOKBACK_SECONDS
    assert captured["end_date"] == 2000.0


def test_fetch_expense_messages_no_widening_when_last_end_height_none(
        monkeypatch):
    """First-run behavior: when last_end_height is None, fetch uses the
    plain run window with no backwards extension."""
    captured = {}

    class _C:
        async def __aenter__(self): return self
        async def __aexit__(self, *a): return False
        async def get_posts_cursor(self, post_filter=None, cursor="", **kw):
            captured["start_date"] = post_filter.start_date
            return _FakeCursorResponse([], next_cursor="")

    monkeypatch.setattr(cd, "_aleph_client", lambda api: _C())

    asyncio.run(cd._fetch_expense_messages(
        "http://x", start_time=1000.0, end_time=2000.0,
        sender="0xS", last_end_height=None,
    ))
    assert captured["start_date"] == 1000.0
