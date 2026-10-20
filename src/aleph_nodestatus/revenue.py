"""Client for the CCN credits revenue endpoint."""
import asyncio
import logging
from typing import List, Optional

import aiohttp

LOGGER = logging.getLogger(__name__)

HOUR = 3600
MAX_PERIODS_PER_CALL = 744
MAX_ATTEMPTS = 3
RETRY_DELAY = 5

# Indirection so tests can skip the retry delay without patching asyncio.
_sleep = asyncio.sleep


class RevenueUnavailable(Exception):
    """The CCN revenue endpoint could not be queried successfully."""


class _Retryable(Exception):
    """Transient failure (5xx, connection error, timeout): worth retrying."""


async def _fetch_window_once(session, url, start, end):
    try:
        async with session.get(
            url, params={"from": start, "to": end},
            timeout=aiohttp.ClientTimeout(total=60),
        ) as resp:
            if not 200 <= resp.status < 300:
                msg = (
                    f"Revenue endpoint returned HTTP {resp.status} for {url} "
                    f"(from={start}, to={end})"
                )
                if resp.status >= 500:
                    raise _Retryable(msg)
                raise RevenueUnavailable(msg)
            data = await resp.json()
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        raise _Retryable(f"Revenue endpoint error for {url}: {e}") from e
    except ValueError as e:
        raise RevenueUnavailable(f"Malformed revenue body from {url}: {e}") from e
    if not isinstance(data, dict) or not isinstance(data.get("periods"), list):
        raise RevenueUnavailable(f"Malformed revenue body from {url}: no periods list")
    return data["periods"]


async def _fetch_window(session, api_server, start, end):
    """Fetch one [start, end) window, retrying transient failures (5xx,
    connection errors, timeouts) up to MAX_ATTEMPTS times RETRY_DELAY
    seconds apart. 4xx and malformed bodies are not retried."""
    url = f"{api_server}/api/v0/credits/revenue"
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            return await _fetch_window_once(session, url, start, end)
        except _Retryable as e:
            if attempt == MAX_ATTEMPTS:
                raise RevenueUnavailable(
                    f"{e} (after {MAX_ATTEMPTS} attempts)"
                ) from e
            LOGGER.warning(
                "%s (attempt %d/%d), retrying in %ss",
                e, attempt, MAX_ATTEMPTS, RETRY_DELAY,
            )
            await _sleep(RETRY_DELAY)


def revenue_window(start_time: int, end_time: int):
    """Hour-aligned [from, to) for a run window [start_time, end_time).

    Both ends are floored: a period belongs to the run whose window contains
    the period's end (period + 1h in (start_time, end_time]). Consecutive runs
    where run N+1 starts at run N's end_time therefore request adjacent,
    disjoint ranges; `to <= end_time`, so the hour in progress (possibly
    partially folded by the CCN) is never requested.
    """
    return int(start_time) // HOUR * HOUR, int(end_time) // HOUR * HOUR


async def fetch_revenue(
    api_server: str,
    start_time: int,
    end_time: int,
    session: Optional[aiohttp.ClientSession] = None,
) -> List[dict]:
    """Fetch the hourly revenue periods ending in (start_time, end_time].

    The request range is `revenue_window(start_time, end_time)`, split into
    windows of at most 744 periods.

    Endpoint contract: GET /api/v0/credits/revenue?from=&to= (unix seconds,
    hour-aligned) returns closed periods with from <= period < to (from
    inclusive, to exclusive), every closed period even with zero revenue,
    and rejects ranges where (to - from) / 3600 > 744. Windows are [from, to)
    with next from = previous to, so none overlap or leave gaps.

    Raises RevenueUnavailable if the endpoint fails (after retries) or if the
    CCN has not closed every period up to `to` yet: the run must abort rather
    than skip hours that a later run would never ask for again.

    Returns periods sorted by `period`, de-duplicated.
    """
    start, end = revenue_window(start_time, end_time)

    own_session = session is None
    if own_session:
        session = aiohttp.ClientSession()
    try:
        by_period = {}
        cursor = start
        while cursor < end:
            window_end = min(cursor + MAX_PERIODS_PER_CALL * HOUR, end)
            for p in await _fetch_window(session, api_server, cursor, window_end):
                by_period[p["period"]] = p
            cursor = window_end
    finally:
        if own_session:
            await session.close()

    periods = [by_period[k] for k in sorted(by_period)]
    if end > start and (not periods or periods[-1]["period"] < end - HOUR):
        last = periods[-1]["period"] if periods else None
        raise RevenueUnavailable(
            f"CCN has closed periods only up to {last}, "
            f"requested up to {end - HOUR} (to={end})"
        )
    return periods
