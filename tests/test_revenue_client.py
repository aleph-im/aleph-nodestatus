import logging
import re

import aiohttp
import pytest
from aioresponses import CallbackResult, aioresponses

from aleph_nodestatus import revenue
from aleph_nodestatus.revenue import RevenueUnavailable, fetch_revenue

API = "http://ccn.test"
URL_RE = re.compile(r"^http://ccn\.test/api/v0/credits/revenue.*$")
H = 3600


def _periods(start, end):
    return [
        {"period": p, "credit_price_aleph": "0.000001", "nodes": {},
         "storage": 0, "execution_total": 0}
        for p in range(start, end, H)
    ]


def _qs(request_key):
    return dict(request_key[1].query)


@pytest.fixture(autouse=True)
def no_retry_delay(monkeypatch):
    """Record retry sleeps instead of waiting."""
    sleeps = []

    async def fake_sleep(s):
        sleeps.append(s)

    monkeypatch.setattr(revenue, "_sleep", fake_sleep)
    return sleeps


@pytest.mark.asyncio
async def test_fetch_revenue_pages_in_744_windows():
    start = 1_000_000 * H
    end = start + 2000 * H
    with aioresponses() as m:
        m.get(URL_RE, payload={"periods": _periods(start, start + 744 * H)})
        m.get(URL_RE, payload={"periods": _periods(start + 744 * H, start + 1488 * H)})
        m.get(URL_RE, payload={"periods": _periods(start + 1488 * H, end)})
        result = await fetch_revenue(API, start + 5, end + 5)
        calls = [_qs(k) for k in m.requests.keys()]
    assert len(calls) == 3
    assert calls[0] == {"from": str(start), "to": str(start + 744 * H)}
    assert calls[1] == {"from": str(start + 744 * H), "to": str(start + 1488 * H)}
    assert calls[2] == {"from": str(start + 1488 * H), "to": str(end)}
    assert len(result) == 2000
    assert [p["period"] for p in result] == sorted(p["period"] for p in result)


@pytest.mark.asyncio
async def test_fetch_revenue_raises_on_short_tail():
    start = 1_000_000 * H
    end = start + 10 * H
    with aioresponses() as m:
        m.get(URL_RE, payload={"periods": _periods(start, start + 5 * H)})
        with pytest.raises(RevenueUnavailable) as exc:
            await fetch_revenue(API, start, end)
    assert "closed periods only up to" in str(exc.value)


@pytest.mark.asyncio
async def test_fetch_revenue_raises_when_no_period_closed():
    start = 1_000_000 * H
    with aioresponses() as m:
        m.get(URL_RE, payload={"periods": []})
        with pytest.raises(RevenueUnavailable):
            await fetch_revenue(API, start, start + 3 * H)


@pytest.mark.asyncio
async def test_window_shorter_than_an_hour_makes_no_call():
    start = 1_000_000 * H + 60
    with aioresponses() as m:
        result = await fetch_revenue(API, start, start + 600)
        assert not m.requests
    assert result == []


@pytest.mark.asyncio
async def test_end_is_floored_so_the_open_hour_is_not_requested():
    # cron at 12:05: the 12:00 period is still open, 11:00 is the last one
    # whose end (12:00) is inside the window.
    start = 1_000_000 * H + 5 * 60
    end = start + 24 * H
    calls = []
    with aioresponses() as m:
        m.get(URL_RE, callback=_range_cb(calls), repeat=True)
        result = await fetch_revenue(API, start, end)
    assert calls == [{"from": str(start - 300), "to": str(end - 300)}]
    assert result[-1]["period"] == end - 300 - H


@pytest.mark.asyncio
async def test_consecutive_runs_tile_hours_exactly():
    day = 24 * H
    t0 = 1_000_000 * H + 30 * 60  # 12:30-style, not hour-aligned
    calls = []
    with aioresponses() as m:
        m.get(URL_RE, callback=_range_cb(calls), repeat=True)
        run1 = await fetch_revenue(API, t0, t0 + day)
        run2 = await fetch_revenue(API, t0 + day, t0 + 2 * day)
    r1 = (int(calls[0]["from"]), int(calls[0]["to"]))
    r2 = (int(calls[1]["from"]), int(calls[1]["to"]))
    for f, t in (r1, r2):
        assert f % H == 0 and t % H == 0
    assert r1[1] == r2[0]  # adjacent: no hour missing
    p1 = [p["period"] for p in run1]
    p2 = [p["period"] for p in run2]
    assert not set(p1) & set(p2)  # disjoint: no hour repeated
    assert p1 + p2 == list(range(r1[0], r2[1], H))
    # each period ends inside its run's window (start, end]
    assert all(t0 < p + H <= t0 + day for p in p1)
    assert all(t0 + day < p + H <= t0 + 2 * day for p in p2)


@pytest.mark.asyncio
async def test_fetch_revenue_raises_on_500():
    with aioresponses() as m:
        m.get(URL_RE, status=500, repeat=True)
        with pytest.raises(RevenueUnavailable) as exc:
            await fetch_revenue(API, 1_000_000 * H, 1_000_000 * H + H)
        assert sum(len(v) for v in m.requests.values()) == 3
    assert "500" in str(exc.value)


@pytest.mark.asyncio
async def test_retries_502_then_succeeds(no_retry_delay):
    start = 1_000_000 * H
    with aioresponses() as m:
        m.get(URL_RE, status=502)
        m.get(URL_RE, payload={"periods": _periods(start, start + H)})
        result = await fetch_revenue(API, start, start + H)
    assert [p["period"] for p in result] == [start]
    assert no_retry_delay == [5]


@pytest.mark.asyncio
async def test_retries_timeout_then_succeeds(no_retry_delay):
    import asyncio
    start = 1_000_000 * H
    with aioresponses() as m:
        m.get(URL_RE, exception=asyncio.TimeoutError())
        m.get(URL_RE, payload={"periods": _periods(start, start + H)})
        result = await fetch_revenue(API, start, start + H)
    assert len(result) == 1
    assert no_retry_delay == [5]


@pytest.mark.asyncio
async def test_4xx_is_not_retried(no_retry_delay):
    with aioresponses() as m:
        m.get(URL_RE, status=400, repeat=True)
        with pytest.raises(RevenueUnavailable) as exc:
            await fetch_revenue(API, 1_000_000 * H, 1_000_000 * H + H)
        assert sum(len(v) for v in m.requests.values()) == 1
    assert "400" in str(exc.value)
    assert no_retry_delay == []


def _range_cb(calls):
    def cb(url, **kwargs):
        q = dict(url.query)
        calls.append(q)
        return CallbackResult(
            payload={"periods": _periods(int(q["from"]), int(q["to"]))}
        )
    return cb


@pytest.mark.asyncio
async def test_windows_share_no_period_and_miss_none():
    start = 1_000_000 * H
    end = start + 2000 * H
    calls = []
    with aioresponses() as m:
        m.get(URL_RE, callback=_range_cb(calls), repeat=True)
        result = await fetch_revenue(API, start, end)
    got = [p["period"] for p in result]
    assert got == list(range(start, end, H))
    assert len(got) == (end - start) // H
    assert len(calls) == 3


@pytest.mark.asyncio
async def test_boundary_period_returned_twice_is_deduplicated():
    start = 1_000_000 * H
    end = start + 800 * H
    boundary = start + 744 * H
    with aioresponses() as m:
        m.get(URL_RE, payload={"periods": _periods(start, boundary + H)})
        m.get(URL_RE, payload={"periods": _periods(boundary, end)})
        result = await fetch_revenue(API, start, end)
    got = [p["period"] for p in result]
    assert got.count(boundary) == 1
    assert got == list(range(start, end, H))


@pytest.mark.asyncio
async def test_client_error_is_wrapped():
    with aioresponses() as m:
        m.get(URL_RE, exception=aiohttp.ClientConnectionError("boom"),
              repeat=True)
        with pytest.raises(RevenueUnavailable):
            await fetch_revenue(API, 1_000_000 * H, 1_000_000 * H + H)


@pytest.mark.asyncio
@pytest.mark.parametrize("body", ["not json", '{"nope": 1}', "[1]"])
async def test_malformed_body_is_wrapped(body):
    with aioresponses() as m:
        m.get(URL_RE, body=body, content_type="application/json")
        with pytest.raises(RevenueUnavailable):
            await fetch_revenue(API, 1_000_000 * H, 1_000_000 * H + H)


@pytest.mark.asyncio
async def test_no_warning_when_tail_complete(caplog):
    start = 1_000_000 * H
    end = start + 5 * H
    with aioresponses() as m:
        m.get(URL_RE, payload={"periods": _periods(start, end)})
        with caplog.at_level(logging.WARNING):
            await fetch_revenue(API, start, end)
    assert not [r for r in caplog.records if r.levelno >= logging.WARNING]


def test_resolve_credit_revenue_api_server(monkeypatch):
    from aleph_nodestatus import settings as st

    monkeypatch.setattr(st.settings, "credit_revenue_api_server", "http://rev.test")
    assert st.resolve_credit_revenue_api_server() == "http://rev.test"
    monkeypatch.setattr(st.settings, "credit_revenue_api_server", None)
    assert st.resolve_credit_revenue_api_server() == st.PublishMode.get_publish_api_server()
