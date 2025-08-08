""" Code taken from
https://github.com/joshp123/heapq_async/blob/master/heapq_async.py
"""
import logging
from datetime import datetime
from heapq import heapify, heappop, heapreplace
from heapq import merge as stdlib_merge

from web3 import Web3

from .settings import settings
from .ethereum import get_web3

log = logging.getLogger(__name__)


async def merge(*iterables, key=None, reverse=False):
    """This is a reimplementation of the stdlib heapq.merge function, with a
    few minor tweaks to allow it to work with async generators.
    I basically copy-pasted the implementation directly from cpython:
    https://github.com/python/cpython/blob/master/Lib/heapq.py
    The only important changes are:
        - __next__ calls replaced with __anext__
        - catching StopIteration should also catch StopAsyncIteration
        - for loops on the fast case when only a single iterator remains
          reaplced with async for loops.
    """
    # Try naive implementation first I guess
    try:
        for line in stdlib_merge(*iterables, key, reverse):
            yield line
    except TypeError:
        log.debug("Couldn't fall back to naive implementation. Using async version.")

    h = []
    h_append = h.append
    if reverse:
        raise NotImplementedError
    else:
        _heapify = heapify
        _heappop = heappop
        _heapreplace = heapreplace
        direction = 1

    if key is None:
        for order, it in enumerate(iterables):
            try:
                next = it.__anext__
                h_append([await next(), order * direction, next])
            except (StopIteration, StopAsyncIteration):
                pass
        _heapify(h)
        while len(h) > 1:
            try:
                while True:
                    value, order, next = s = h[0]
                    yield value
                    s[0] = await next()  # raises StopIteration when exhausted
                    _heapreplace(h, s)  # restore heap condition
            except (StopIteration, StopAsyncIteration):
                _heappop(h)  # remove empty iterator
        if h:
            # fast case when only a single iterator remains
            value, order, next = h[0]
            yield value
            async for item in next.__self__:
                yield item
        return

    for order, it in enumerate(iterables):
        try:
            next = it.__anext__
            value = await next()
            h_append([key(value), order * direction, value, next])
        except (StopIteration, StopAsyncIteration):
            pass
    _heapify(h)
    while len(h) > 1:
        try:
            while True:
                key_value, order, value, next = s = h[0]
                yield value
                value = await next()
                s[0] = key(value)
                s[2] = value
                _heapreplace(h, s)
        except (StopIteration, StopAsyncIteration):
            _heappop(h)
    if h:
        key_value, order, value, next = h[0]
        yield value
        async for item in next.__self__:
            yield item


async def fetch_last_ethereum_block_before(target_datetime: datetime):
    # Connect to an Ethereum node using Web3
    #w3 = Web3(Web3.HTTPProvider(settings.ethereum_api_server))
    w3 = get_web3()

    # Get the latest block number
    latest_block_number = w3.eth.block_number

    # Set the initial block number and timestamp for the search range
    start_block_number = 0
    end_block_number = latest_block_number
    end_block_timestamp = w3.eth.get_block(latest_block_number).timestamp

    # Perform bisection search to find the last block before the target date
    while start_block_number <= end_block_number:
        mid_block_number = (start_block_number + end_block_number) // 2
        mid_block_timestamp = w3.eth.get_block(mid_block_number).timestamp

        if mid_block_timestamp > target_datetime.timestamp():
            end_block_number = mid_block_number - 1
            end_block_timestamp = mid_block_timestamp
        else:
            start_block_number = mid_block_number + 1
            end_block_timestamp = w3.eth.get_block(end_block_number).timestamp

    last_block_before_target_date = w3.eth.get_block(end_block_number)

    print(f"Last block before {target_datetime}: {last_block_before_target_date}")
    print(last_block_before_target_date.number)
