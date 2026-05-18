"""Spot-vs-Chainlink deviation guard for the extract swap path.

For each hop in the configured swap path (read at runtime from the
AlephPaymentProcessor), compares the pool's current spot price to the
Chainlink-USD-implied ratio for the same hop. Returns a verdict that
the caller (extract_aleph) uses to either proceed or skip the token.

Phase 1 reads SPOT from the pool — no TWAP. Phase 2 adds true TWAP
(V3 observe() / V4 oracle hooks) which closes the ALEPH-side hop
gap (ALEPH has no Chainlink feed today).
"""

import json
import logging
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Optional

from .settings import settings

LOGGER = logging.getLogger(__name__)


@dataclass
class OracleResult:
    ok: bool
    reason: Optional[str] = None
    deviation_bps: Optional[int] = None
    spot_price: Optional[float] = None
    ref_price: Optional[float] = None


def check_swap_price_deviation(w3, swap_config: dict, token_in: str) -> OracleResult:
    v = swap_config["v"]
    if v == 3:
        hops = _decode_v3_path(swap_config["v3"])
        spot_fn = lambda t_a, t_b, extra: _v3_spot(w3, t_a, t_b, extra["fee"])
        hop_iter = [(a, b, {"fee": fee}) for (a, fee, b) in hops]
    else:
        # V2/V4 land in later tasks.
        return OracleResult(ok=True)

    feeds = {k.lower(): feed_addr for k, feed_addr in settings.chainlink_usd_feeds.items()}
    threshold = settings.extract_max_deviation_bps

    for token_a, token_b, extra in hop_iter:
        feed_a = feeds.get(token_a.lower())
        feed_b = feeds.get(token_b.lower())
        if not feed_a or not feed_b:
            LOGGER.info(
                "Best-effort skip on hop %s→%s (no Chainlink feed for one side)",
                token_a, token_b,
            )
            continue

        try:
            spot = spot_fn(token_a, token_b, extra)
        except Exception as e:
            LOGGER.warning(
                "Pool read failed for hop %s→%s: %r", token_a, token_b, e,
            )
            return OracleResult(ok=False, reason="pool_read_failed")

        price_a_usd, reason_a = _read_chainlink_price(w3, feed_a)
        if reason_a:
            return OracleResult(ok=False, reason=reason_a)
        price_b_usd, reason_b = _read_chainlink_price(w3, feed_b)
        if reason_b:
            return OracleResult(ok=False, reason=reason_b)

        ref = price_a_usd / price_b_usd  # B per A
        deviation_bps = int(abs(spot - ref) / ref * 10_000)
        if deviation_bps > threshold:
            return OracleResult(
                ok=False, reason="price_deviation",
                deviation_bps=deviation_bps,
                spot_price=spot, ref_price=ref,
            )

    return OracleResult(ok=True)


@lru_cache(maxsize=None)
def _load_abi(name: str):
    path = Path(__file__).parent / "abi" / f"{name}.json"
    with open(path) as f:
        return json.load(f)


def _read_chainlink_price(w3, feed_address: str):
    """Return (price_float, None) on success, (None, reason_str) on failure.

    reason ∈ {"chainlink_stale", "chainlink_invalid"}.
    """
    feed = w3.eth.contract(
        address=w3.to_checksum_address(feed_address),
        abi=_load_abi("ChainlinkAggregator"),
    )
    decimals = int(feed.functions.decimals().call())
    _, answer, _, updated_at, _ = feed.functions.latestRoundData().call()
    if answer <= 0:
        return None, "chainlink_invalid"
    now = w3.eth.get_block("latest").timestamp
    if now - updated_at > settings.chainlink_max_age_seconds:
        return None, "chainlink_stale"
    return float(answer) / (10 ** decimals), None


def _decode_v3_path(path_bytes):
    """Split a Uniswap V3 encoded path into a list of (token_a, fee, token_b)
    triples. Path layout: address(20) | fee(3) | address(20) | fee(3) | …
    | address(20).
    """
    if not isinstance(path_bytes, (bytes, bytearray)):
        path_bytes = bytes(path_bytes)
    hops = []
    offset = 0
    # Each hop needs at least 20 + 3 + 20 = 43 bytes.
    while offset + 43 <= len(path_bytes):
        token_a = "0x" + path_bytes[offset:offset + 20].hex()
        fee = int.from_bytes(path_bytes[offset + 20:offset + 23], "big")
        token_b = "0x" + path_bytes[offset + 23:offset + 43].hex()
        hops.append((token_a, fee, token_b))
        offset += 23  # advance by token + fee; next iter starts at next token's address
    return hops


def _sqrt_price_to_b_per_a(
    sqrt_price_x96: int,
    token_a: str, token_b: str,
    decimals_a: int, decimals_b: int,
) -> float:
    """Convert a Uniswap V3/V4 sqrtPriceX96 into "tokenB per tokenA" in
    human (decimal-adjusted) units. The pool's token0/token1 ordering is
    derived from address comparison."""
    a_is_token0 = int(token_a, 16) < int(token_b, 16)
    decimals_0 = decimals_a if a_is_token0 else decimals_b
    decimals_1 = decimals_b if a_is_token0 else decimals_a
    price_raw_1_per_0 = (sqrt_price_x96 / (2 ** 96)) ** 2
    price_human_1_per_0 = (
        price_raw_1_per_0 * (10 ** decimals_0) / (10 ** decimals_1)
    )
    if a_is_token0:
        # A=token0, B=token1; "B per A" = "token1 per token0"
        return price_human_1_per_0
    # A=token1, B=token0; "B per A" = 1 / "token1 per token0"
    return 1.0 / price_human_1_per_0


def _erc20_decimals(w3, token_address: str) -> int:
    """Read ERC20 decimals(). Special-cases the ETH sentinel as 18.
    Cached at module scope to avoid duplicate eth_calls within a run."""
    if token_address.lower() == "0x0000000000000000000000000000000000000000":
        return 18
    return _cached_decimals(w3, w3.to_checksum_address(token_address))


_DECIMALS_CACHE = {}


def _cached_decimals(w3, addr: str) -> int:
    key = addr.lower()
    if key in _DECIMALS_CACHE:
        return _DECIMALS_CACHE[key]
    minimal_abi = [{
        "constant": True, "inputs": [],
        "name": "decimals",
        "outputs": [{"name": "", "type": "uint8"}],
        "type": "function", "stateMutability": "view",
    }]
    c = w3.eth.contract(address=addr, abi=minimal_abi)
    d = int(c.functions.decimals().call())
    _DECIMALS_CACHE[key] = d
    return d


def _v3_spot(w3, token_a: str, token_b: str, fee: int) -> float:
    """Look up the pool for (tokenA, tokenB, fee) via the V3 factory, read
    slot0, and return the spot price "tokenB per tokenA" in human units."""
    factory = w3.eth.contract(
        address=w3.to_checksum_address(settings.uniswap_v3_factory_address),
        abi=_load_abi("IUniswapV3Factory"),
    )
    pool_address = factory.functions.getPool(
        w3.to_checksum_address(token_a),
        w3.to_checksum_address(token_b),
        fee,
    ).call()
    if int(pool_address, 16) == 0:
        raise ValueError(
            f"V3 pool not found for {token_a}/{token_b} fee={fee}"
        )
    pool = w3.eth.contract(
        address=w3.to_checksum_address(pool_address),
        abi=_load_abi("IUniswapV3Pool"),
    )
    slot0 = pool.functions.slot0().call()
    sqrt_price_x96 = int(slot0[0])
    return _sqrt_price_to_b_per_a(
        sqrt_price_x96, token_a, token_b,
        _erc20_decimals(w3, token_a),
        _erc20_decimals(w3, token_b),
    )
