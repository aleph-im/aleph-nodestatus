"""Client for the CCN credits revenue endpoint."""
import logging
from typing import List, Optional

import aiohttp

LOGGER = logging.getLogger(__name__)

HOUR = 3600
MAX_PERIODS_PER_CALL = 744


class RevenueUnavailable(Exception):
    """The CCN revenue endpoint could not be queried successfully."""


async def _fetch_window(session, api_server, start, end):
    url = f"{api_server}/api/v0/credits/revenue"
    try:
        async with session.get(
            url, params={"from": start, "to": end},
            timeout=aiohttp.ClientTimeout(total=60),
        ) as resp:
            if not 200 <= resp.status < 300:
                raise RevenueUnavailable(
                    f"Revenue endpoint returned HTTP {resp.status} for {url} "
                    f"(from={start}, to={end})"
                )
            data = await resp.json()
    except (aiohttp.ClientError, TimeoutError) as e:
        raise RevenueUnavailable(f"Revenue endpoint error for {url}: {e}") from e
    return data.get("periods", [])


async def fetch_revenue(
    api_server: str,
    start_time: int,
    end_time: int,
    session: Optional[aiohttp.ClientSession] = None,
) -> List[dict]:
    """Fetch hourly revenue periods in [start_time, end_time) from the CCN.

    start_time is floored and end_time ceiled to the hour; the range is split
    into windows of at most 744 periods. Returns periods sorted by `period`,
    de-duplicated.
    """
    start = int(start_time) // HOUR * HOUR
    end = -(-int(end_time) // HOUR) * HOUR

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
    if not periods or periods[-1]["period"] < end_time - HOUR:
        last = periods[-1]["period"] if periods else None
        LOGGER.warning(
            "CCN has not closed periods after %s (requested up to %s)", last, end_time
        )
    return periods
