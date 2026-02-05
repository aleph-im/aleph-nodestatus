import asyncio
import logging
import re
import time
from functools import lru_cache

import aiohttp
from aleph.sdk.chains.ethereum import ETHAccount
from aleph.sdk.client import AuthenticatedAlephHttpClient
from hexbytes import HexBytes

from .erc20 import DECIMALS
from .ethereum import get_web3, lookup_timestamp
from .settings import settings, PublishMode

logger = logging.getLogger(__name__)

# Default window size for timestamp-based pagination (1 day in seconds)
DEFAULT_WINDOW_SIZE = 86400

# Default start timestamp (Jan 1, 2020) - before Aleph mainnet launch
DEFAULT_START_TIMESTAMP = 1577836800


def parse_window_size(window_str: str) -> int:
    """
    Parse a human-readable window size string into seconds.

    Supported formats:
        - "1d", "2d", "7d" - days
        - "1w", "2w" - weeks
        - "1h", "12h" - hours
        - "30m" - minutes
        - "86400" - raw seconds (integer)

    Examples:
        parse_window_size("1d") -> 86400
        parse_window_size("1w") -> 604800
        parse_window_size("12h") -> 43200
    """
    if window_str is None:
        return DEFAULT_WINDOW_SIZE

    window_str = str(window_str).strip().lower()

    # Try to parse as raw integer (seconds)
    try:
        return int(window_str)
    except ValueError:
        pass

    # Parse human-readable format
    match = re.match(r'^(\d+)\s*([dhwm])$', window_str)
    if not match:
        raise ValueError(
            f"Invalid window size format: '{window_str}'. "
            "Use formats like: 1d, 2d, 1w, 12h, 30m, or integer seconds"
        )

    value = int(match.group(1))
    unit = match.group(2)

    multipliers = {
        'm': 60,           # minutes
        'h': 3600,         # hours
        'd': 86400,        # days
        'w': 604800,       # weeks
    }

    return value * multipliers[unit]


@lru_cache(maxsize=2)
def get_aleph_account():
    if settings.ethereum_pkey:
        account = ETHAccount(settings.ethereum_pkey)
        return account
    else:
        return None


@lru_cache(maxsize=32)
def get_aleph_address():
    return (get_aleph_account()).get_address()


# Use set for O(1) lookups
UNCONFIRMED_MESSAGES = set()


async def get_message_result(
    message, yield_unconfirmed=True, last_block=0, min_height=0,
    last_seen=None, addresses=None, db=None, db_prefix=None
):
    earliest = None
    for conf in message.get("confirmations", []):
        if conf["chain"] == "ETH":
            if earliest is None or conf["height"] < earliest:
                earliest = conf["height"]

    key = f"{earliest}_{int(message['time'])}_{message['item_hash']}"

    item_hash = message["item_hash"]

    if item_hash in UNCONFIRMED_MESSAGES:
        if earliest is not None:
            # we store it in the db
            if db is not None:
                await db.store_entry(key, {"height": earliest, "message": message}, prefix=db_prefix)
            UNCONFIRMED_MESSAGES.discard(item_hash)  # O(1) removal
        return None

    if item_hash in last_seen:
        return None

    if addresses is not None:
        if message["sender"] not in addresses:
            return None

    if earliest is None and not yield_unconfirmed:
        return None

    if earliest is None and yield_unconfirmed:
        # let's assign the current block height... (ugly)
        earliest = last_block
        UNCONFIRMED_MESSAGES.add(item_hash)  # O(1) add
        last_seen.add(item_hash)
        return earliest, message

    elif earliest >= min_height:
        last_seen.add(item_hash)

        if db is not None:
            await db.store_entry(key, {"height": earliest, "message": message}, prefix=db_prefix)
        return earliest, message


async def process_message_history(
    tags,
    content_types,
    api_server,
    min_height=0,
    request_count=None,  # Deprecated: kept for backward compatibility, ignored
    message_type="POST",
    request_sort="1",
    yield_unconfirmed=True,
    addresses=None,
    crawl_history=True,
    db=None,
    window_size=DEFAULT_WINDOW_SIZE,
    start_timestamp=None,
    end_timestamp=None
):
    """
    Process message history using timestamp-based pagination with fixed date windows.

    This approach avoids the page drift problem where new messages arriving during
    iteration can cause duplicates or skipped messages.

    Args:
        tags: Tags to filter messages
        content_types: Content types to filter
        api_server: Aleph API server URL
        min_height: Minimum block height to process
        request_count: DEPRECATED - kept for backward compatibility, ignored
        message_type: Type of message (POST, AGGREGATE, etc.)
        request_sort: Sort order ("1" ascending, "-1" descending)
        yield_unconfirmed: Whether to yield unconfirmed messages
        addresses: Filter by sender addresses
        crawl_history: Whether to crawl historical messages
        db: Database for caching
        window_size: Size of each time window in seconds (default: 1 day)
        start_timestamp: Start timestamp for iteration (auto-detected if None)
        end_timestamp: End timestamp for iteration (current time if None)
    """
    web3 = get_web3()
    last_block = web3.eth.block_number

    prefix = f"{message_type}_{','.join(tags)}_{','.join(content_types)}"

    # Use sets for O(1) lookups (handles window boundary overlaps)
    seen_hashes = set()
    last_seen = set()  # For get_message_result compatibility

    fetch_from_db = db is not None
    if (not crawl_history) or request_sort == "-1":
        fetch_from_db = False

    # Determine start timestamp from DB cache if available
    db_start_timestamp = None

    if fetch_from_db:
        last_key = await db.get_last_available_key(prefix=prefix)
        if last_key:
            last_height = int(last_key.split("_")[0])

            # Yield cached messages from DB
            async for key, values in db.retrieve_entries(prefix=prefix):
                if values["height"] >= min_height:
                    seen_hashes.add(values["message"]["item_hash"])
                    result = await get_message_result(
                        values["message"],
                        yield_unconfirmed=yield_unconfirmed,
                        last_block=last_block,
                        min_height=min_height,
                        last_seen=last_seen,
                        addresses=addresses
                    )
                    if result is not None:
                        yield result

            if last_height > min_height:
                # Start from 1 week before last cached height to catch any gaps
                db_start_timestamp = await lookup_timestamp(web3, last_height)
                db_start_timestamp = db_start_timestamp - (60 * 60 * 24 * 7)

    # Determine time range for iteration
    if end_timestamp is None:
        end_timestamp = int(time.time())

    if start_timestamp is None:
        if db_start_timestamp is not None:
            start_timestamp = db_start_timestamp
        else:
            # Default: start from Jan 1, 2020 (before Aleph mainnet)
            start_timestamp = DEFAULT_START_TIMESTAMP

    if addresses is not None:
        address_param = ",".join(addresses)
    else:
        address_param = None

    # Base params for all requests
    base_params = {
        "msgType": message_type,
        "tags": ",".join(tags),
        "contentTypes": ",".join(content_types),
        "pagination": 10000000000,  # High value to get all messages in window
        "sort_order": "1",  # Always ascending within window for consistency
        "sort_by": "tx-time"
    }

    if address_param:
        base_params["addresses"] = address_param

    logger.info(f"Starting window-based iteration from {start_timestamp} to {end_timestamp} (window_size={window_size}s)")

    max_retries = 3
    retry_delay = 5  # seconds

    async with aiohttp.ClientSession() as session:
        current_start = start_timestamp
        window_count = 0
        message_count = 0

        while current_start < end_timestamp:
            window_end = min(current_start + window_size, end_timestamp)
            window_count += 1

            params = {
                **base_params,
                "startDate": current_start,
                "endDate": window_end
            }

            # Retry logic for failed API calls
            success = False
            for attempt in range(max_retries):
                try:
                    async with session.get(
                        f"{api_server}/api/v0/messages.json", params=params, timeout=30
                    ) as resp:
                        if resp.status != 200:
                            logger.warning(f"API error {resp.status} for window {current_start}-{window_end} (attempt {attempt + 1}/{max_retries})")
                            if attempt < max_retries - 1:
                                await asyncio.sleep(retry_delay)
                                continue
                            else:
                                logger.error(f"Failed to fetch window after {max_retries} attempts, skipping")
                                break

                        items = await resp.json()
                        messages = items.get("messages", [])

                        logger.debug(f"Window {window_count}: {current_start}-{window_end}, got {len(messages)} messages")

                        for message in messages:
                            item_hash = message["item_hash"]

                            # Skip if already seen (handles boundary duplicates)
                            if item_hash in seen_hashes:
                                continue

                            seen_hashes.add(item_hash)

                            result = await get_message_result(
                                message,
                                yield_unconfirmed=yield_unconfirmed,
                                last_block=last_block,
                                min_height=min_height,
                                last_seen=last_seen,
                                addresses=addresses,
                                db=db,
                                db_prefix=prefix
                            )
                            if result is not None:
                                message_count += 1
                                yield result

                        success = True
                        break  # Success, exit retry loop

                except asyncio.TimeoutError:
                    logger.warning(f"Timeout fetching window {current_start}-{window_end} (attempt {attempt + 1}/{max_retries})")
                    if attempt < max_retries - 1:
                        await asyncio.sleep(retry_delay)
                except aiohttp.ClientError as e:
                    logger.warning(f"Network error fetching window {current_start}-{window_end}: {e} (attempt {attempt + 1}/{max_retries})")
                    if attempt < max_retries - 1:
                        await asyncio.sleep(retry_delay)

            if not success:
                logger.error(f"SKIPPING window {current_start}-{window_end} after {max_retries} failed attempts")

            current_start = window_end

            # Log progress every 100 windows
            if window_count % 100 == 0:
                logger.info(f"Progress: {window_count} windows processed, {message_count} messages yielded")

    logger.info(f"Network iteration finished: {window_count} windows, {message_count} messages yielded")


async def set_status(account, nodes, resource_nodes):
    nodes = [
        {
            **node.copy(),
            **{
                "total_staked": node["total_staked"] / DECIMALS,
                "stakers": {
                    addr: amt / DECIMALS for addr, amt in node["stakers"].items()
                },
                # Convert set to list for JSON serialization
                "resource_nodes": list(node["resource_nodes"]),
            },
        }
        for node in nodes.values()
    ]
    # Use PublishMode to determine API server (mainnet or testnet)
    api_server = PublishMode.get_publish_api_server()
    logger.info(f"Publishing status to {api_server}")
    async with AuthenticatedAlephHttpClient(account, api_server=api_server) as client:
        await client.create_aggregate(
            "corechannel",
            {"nodes": nodes, "resource_nodes": list(resource_nodes.values())},
            channel=settings.aleph_channel,
        )
