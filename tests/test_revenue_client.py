import logging
import re

import pytest
from aioresponses import aioresponses

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


@pytest.mark.asyncio
async def test_fetch_revenue_pages_in_744_windows():
    start = 1_000_000 * H
    end = start + 2000 * H
    with aioresponses() as m:
        m.get(URL_RE, payload={"periods": _periods(start, start + 744 * H)})
        m.get(URL_RE, payload={"periods": _periods(start + 744 * H, start + 1488 * H)})
        m.get(URL_RE, payload={"periods": _periods(start + 1488 * H, end)})
        result = await fetch_revenue(API, start + 5, end - 5)
        calls = [_qs(k) for k in m.requests.keys()]
    assert len(calls) == 3
    assert calls[0] == {"from": str(start), "to": str(start + 744 * H)}
    assert calls[1] == {"from": str(start + 744 * H), "to": str(start + 1488 * H)}
    assert calls[2] == {"from": str(start + 1488 * H), "to": str(end)}
    assert len(result) == 2000
    assert [p["period"] for p in result] == sorted(p["period"] for p in result)


@pytest.mark.asyncio
async def test_fetch_revenue_warns_on_short_tail(caplog):
    start = 1_000_000 * H
    end = start + 10 * H
    with aioresponses() as m:
        m.get(URL_RE, payload={"periods": _periods(start, start + 5 * H)})
        with caplog.at_level(logging.WARNING):
            result = await fetch_revenue(API, start, end)
    assert len(result) == 5
    assert any("has not closed periods after" in r.getMessage() for r in caplog.records)


@pytest.mark.asyncio
async def test_fetch_revenue_raises_on_500():
    with aioresponses() as m:
        m.get(URL_RE, status=500)
        with pytest.raises(RevenueUnavailable) as exc:
            await fetch_revenue(API, 1_000_000 * H, 1_000_000 * H + H)
    assert "500" in str(exc.value)
