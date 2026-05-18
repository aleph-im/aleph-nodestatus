"""Unit tests for pool_oracle: spot-vs-Chainlink deviation guard."""

import sys
import types

if "plyvel" not in sys.modules:
    _plyvel_stub = types.ModuleType("plyvel")
    _plyvel_stub.DB = object
    sys.modules["plyvel"] = _plyvel_stub

from unittest.mock import MagicMock

import pytest


@pytest.fixture(autouse=True)
def _clear_decimals_cache():
    """Module-level _DECIMALS_CACHE could leak across tests if any test
    exercises _erc20_decimals through a non-mocked path. Reset before
    each test for safety."""
    import aleph_nodestatus.pool_oracle as po
    po._DECIMALS_CACHE.clear()
    yield
    po._DECIMALS_CACHE.clear()


def test_oracle_result_is_dataclass():
    from aleph_nodestatus.pool_oracle import OracleResult

    r = OracleResult(ok=True)
    assert r.ok is True
    assert r.reason is None
    assert r.deviation_bps is None
    assert r.spot_price is None
    assert r.ref_price is None


def test_check_swap_price_deviation_returns_ok_for_now():
    """Skeleton: the function exists and returns ok=True regardless of input.
    Per-version logic lands in subsequent tasks."""
    from aleph_nodestatus.pool_oracle import check_swap_price_deviation

    w3 = MagicMock()
    cfg = {"v": 3, "t": "0x0", "v2": [], "v3": b"", "v4": []}
    result = check_swap_price_deviation(w3, cfg, token_in="0xToken")
    assert result.ok is True


def test_read_chainlink_returns_price(monkeypatch):
    """Healthy feed: latestRoundData answer × 10^-decimals."""
    import aleph_nodestatus.pool_oracle as po

    feed = MagicMock()
    feed.functions.decimals.return_value.call.return_value = 8
    # answer = 1.234567 × 10^8
    feed.functions.latestRoundData.return_value.call.return_value = (
        1, 123_456_700, 0, 9_999_999_999, 1,  # updatedAt very recent
    )

    w3 = MagicMock()
    w3.eth.contract.return_value = feed
    w3.eth.get_block.return_value.timestamp = 10_000_000_000

    price, reason = po._read_chainlink_price(
        w3, feed_address="0xfeed",
    )
    assert reason is None
    assert abs(price - 1.234567) < 1e-9


def test_read_chainlink_stale(monkeypatch):
    """updatedAt older than chainlink_max_age_seconds → reason=chainlink_stale."""
    import aleph_nodestatus.pool_oracle as po
    from aleph_nodestatus.settings import settings as s

    monkeypatch.setattr(s, "chainlink_max_age_seconds", 3600)

    feed = MagicMock()
    feed.functions.decimals.return_value.call.return_value = 8
    feed.functions.latestRoundData.return_value.call.return_value = (
        1, 100_000_000, 0, 1000, 1,  # updatedAt = 1000
    )

    w3 = MagicMock()
    w3.eth.contract.return_value = feed
    w3.eth.get_block.return_value.timestamp = 1000 + 4000  # 4000s old, > 3600

    price, reason = po._read_chainlink_price(w3, "0xfeed")
    assert price is None
    assert reason == "chainlink_stale"


def test_read_chainlink_invalid_answer():
    """answer <= 0 → reason=chainlink_invalid."""
    import aleph_nodestatus.pool_oracle as po

    feed = MagicMock()
    feed.functions.decimals.return_value.call.return_value = 8
    feed.functions.latestRoundData.return_value.call.return_value = (
        1, 0, 0, 9_999_999_999, 1,
    )

    w3 = MagicMock()
    w3.eth.contract.return_value = feed
    w3.eth.get_block.return_value.timestamp = 10_000_000_000

    price, reason = po._read_chainlink_price(w3, "0xfeed")
    assert price is None
    assert reason == "chainlink_invalid"


def test_decode_v3_path_single_hop():
    from aleph_nodestatus.pool_oracle import _decode_v3_path

    # token_a (USDC) | fee 3000 | token_b (WETH)
    path = (
        bytes.fromhex("a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48")
        + (3000).to_bytes(3, "big")
        + bytes.fromhex("c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2")
    )
    hops = _decode_v3_path(path)
    assert hops == [(
        "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48", 3000,
        "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
    )]


def test_decode_v3_path_two_hops():
    from aleph_nodestatus.pool_oracle import _decode_v3_path

    # USDC | 3000 | WETH | 10000 | ALEPH
    path = (
        bytes.fromhex("a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48")
        + (3000).to_bytes(3, "big")
        + bytes.fromhex("c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2")
        + (10000).to_bytes(3, "big")
        + bytes.fromhex("27702a26126e0b3702af63ee09ac4d1a084ef628")
    )
    hops = _decode_v3_path(path)
    assert len(hops) == 2
    assert hops[0][0].lower() == "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
    assert hops[0][1] == 3000
    assert hops[0][2].lower() == "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
    assert hops[1][1] == 10000


def test_sqrt_price_to_b_per_a_decimals():
    """sqrtPriceX96 for USDC/WETH pool, USDC is token0 (lower address).
    If 1 WETH = 2500 USDC, then USDC per WETH = 2500 → WETH per USDC = 1/2500."""
    from aleph_nodestatus.pool_oracle import _sqrt_price_to_b_per_a

    # WETH is token1; USDC is token0.
    # price_raw_1_per_0 = wei_WETH per wei_USDC = (2500_USDC -> 1_WETH)
    # 1 USDC = 10^6 wei; 1 WETH = 10^18 wei
    # 2500 USDC -> 1 WETH: 2500 × 10^6 wei USDC -> 10^18 wei WETH
    # so wei_WETH / wei_USDC = 10^18 / (2500 × 10^6) = 10^12 / 2500 = 4e8
    # sqrt of 4e8 = 20000; sqrtPriceX96 = 20000 * 2^96
    sqrt_price_x96 = 20000 * (2 ** 96)

    USDC = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
    WETH = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
    # token_a = USDC, token_b = WETH → "WETH per USDC" → 1/2500
    price = _sqrt_price_to_b_per_a(
        sqrt_price_x96, token_a=USDC, token_b=WETH,
        decimals_a=6, decimals_b=18,
    )
    assert abs(price - (1 / 2500)) < 1e-9

    # token_a = WETH, token_b = USDC → "USDC per WETH" → 2500
    price2 = _sqrt_price_to_b_per_a(
        sqrt_price_x96, token_a=WETH, token_b=USDC,
        decimals_a=18, decimals_b=6,
    )
    assert abs(price2 - 2500) < 1e-9


def test_check_swap_price_deviation_v3_within_threshold(monkeypatch):
    """Spot matches Chainlink (within 200 bps): ok=True."""
    import aleph_nodestatus.pool_oracle as po
    from aleph_nodestatus.settings import settings as s

    USDC = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
    WETH = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
    USDC_USD_FEED = "0x8fFfFfd4AfB6115b954Bd326cbe7B4BA576818f6"
    ETH_USD_FEED  = "0x5f4eC3Df9cbd43714FE2740f5E3616155c5b8419"

    monkeypatch.setattr(s, "chainlink_usd_feeds", {
        USDC.lower(): USDC_USD_FEED,
        WETH.lower(): ETH_USD_FEED,
    })

    # Stub the pool spot — return 1/2500 (USDC→WETH at 2500 USDC/ETH)
    monkeypatch.setattr(po, "_v3_spot",
                        lambda w3, token_a, token_b, fee: 1 / 2500.0)
    # Stub Chainlink: USDC=1.0, ETH=2500.0 → ratio WETH/USDC = 1/2500
    chainlink_returns = {
        USDC_USD_FEED.lower(): (1.0, None),
        ETH_USD_FEED.lower():  (2500.0, None),
    }
    monkeypatch.setattr(po, "_read_chainlink_price",
        lambda w3, feed: chainlink_returns[feed.lower()])

    path = (bytes.fromhex(USDC[2:]) + (3000).to_bytes(3, "big")
            + bytes.fromhex(WETH[2:]))
    cfg = {"v": 3, "t": WETH, "v2": [], "v3": path, "v4": []}

    result = po.check_swap_price_deviation(MagicMock(), cfg, token_in=USDC)
    assert result.ok is True


def test_check_swap_price_deviation_v3_exceeds_threshold(monkeypatch):
    """Spot 3% off Chainlink (>200 bps): ok=False, reason=price_deviation."""
    import aleph_nodestatus.pool_oracle as po
    from aleph_nodestatus.settings import settings as s

    USDC = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
    WETH = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
    USDC_USD_FEED = "0x8fFfFfd4AfB6115b954Bd326cbe7B4BA576818f6"
    ETH_USD_FEED  = "0x5f4eC3Df9cbd43714FE2740f5E3616155c5b8419"

    monkeypatch.setattr(s, "chainlink_usd_feeds", {
        USDC.lower(): USDC_USD_FEED,
        WETH.lower(): ETH_USD_FEED,
    })
    monkeypatch.setattr(s, "extract_max_deviation_bps", 200)

    # Spot says 1 USDC = 1/2425 WETH (3% off "real" 1/2500)
    monkeypatch.setattr(po, "_v3_spot",
                        lambda w3, token_a, token_b, fee: 1 / 2425.0)
    chainlink_returns = {
        USDC_USD_FEED.lower(): (1.0, None),
        ETH_USD_FEED.lower():  (2500.0, None),
    }
    monkeypatch.setattr(po, "_read_chainlink_price",
        lambda w3, feed: chainlink_returns[feed.lower()])

    path = (bytes.fromhex(USDC[2:]) + (3000).to_bytes(3, "big")
            + bytes.fromhex(WETH[2:]))
    cfg = {"v": 3, "t": WETH, "v2": [], "v3": path, "v4": []}

    result = po.check_swap_price_deviation(MagicMock(), cfg, token_in=USDC)
    assert result.ok is False
    assert result.reason == "price_deviation"
    assert result.deviation_bps is not None and result.deviation_bps > 200


def test_check_swap_price_deviation_v3_pool_read_error(monkeypatch):
    """Pool read raises → ok=False, reason=pool_read_failed."""
    import aleph_nodestatus.pool_oracle as po
    from aleph_nodestatus.settings import settings as s

    USDC = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
    WETH = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
    monkeypatch.setattr(s, "chainlink_usd_feeds", {
        USDC.lower(): "0xUSDC_FEED", WETH.lower(): "0xETH_FEED",
    })

    def boom(*a, **kw):
        raise RuntimeError("rpc fell over")
    monkeypatch.setattr(po, "_v3_spot", boom)

    path = (bytes.fromhex(USDC[2:]) + (3000).to_bytes(3, "big")
            + bytes.fromhex(WETH[2:]))
    cfg = {"v": 3, "t": WETH, "v2": [], "v3": path, "v4": []}

    result = po.check_swap_price_deviation(MagicMock(), cfg, token_in=USDC)
    assert result.ok is False
    assert result.reason == "pool_read_failed"


def test_check_swap_price_deviation_v3_no_feed_best_effort(monkeypatch):
    """Hop with one side missing a feed → that hop is best-effort skipped;
    if no other hop fails, ok=True with deviation_bps=None."""
    import aleph_nodestatus.pool_oracle as po
    from aleph_nodestatus.settings import settings as s

    USDC = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
    ALEPH = "0x27702a26126e0b3702af63ee09ac4d1a084ef628"  # no feed
    monkeypatch.setattr(s, "chainlink_usd_feeds", {
        USDC.lower(): "0xUSDC_FEED",
        # No mapping for ALEPH — best-effort skip the hop.
    })
    monkeypatch.setattr(po, "_v3_spot",
                        lambda *a, **kw: 1.0)  # would never get used
    monkeypatch.setattr(po, "_read_chainlink_price",
                        lambda w3, feed: (1.0, None))

    path = (bytes.fromhex(USDC[2:]) + (10000).to_bytes(3, "big")
            + bytes.fromhex(ALEPH[2:]))
    cfg = {"v": 3, "t": ALEPH, "v2": [], "v3": path, "v4": []}

    result = po.check_swap_price_deviation(MagicMock(), cfg, token_in=USDC)
    assert result.ok is True
    assert result.deviation_bps is None


def test_v4_pool_id_deterministic():
    """V4 poolId = keccak256(abi.encode(currency0, currency1, fee, tickSpacing, hooks))
    with currency0 < currency1 sorted lexicographically."""
    from aleph_nodestatus.pool_oracle import _v4_pool_id

    USDC = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
    WETH = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
    pid1 = _v4_pool_id(USDC, WETH, fee=3000, tick_spacing=60,
                       hooks="0x0000000000000000000000000000000000000000")
    pid2 = _v4_pool_id(WETH, USDC, fee=3000, tick_spacing=60,
                       hooks="0x0000000000000000000000000000000000000000")
    assert pid1 == pid2  # order-independent
    assert isinstance(pid1, (bytes, bytearray))
    assert len(pid1) == 32


def test_enumerate_v4_hops_single():
    """Single-hop V4 path: token_in → path[0].intermediateCurrency."""
    from aleph_nodestatus.pool_oracle import _enumerate_v4_hops

    USDC = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
    WETH = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
    HOOKS = "0x0000000000000000000000000000000000000000"
    path = [(WETH, 3000, 60, HOOKS, b"")]
    hops = list(_enumerate_v4_hops(path, token_in=USDC))
    assert len(hops) == 1
    assert hops[0][0].lower() == USDC.lower()
    assert hops[0][1].lower() == WETH.lower()
    assert hops[0][2]["fee"] == 3000
    assert hops[0][2]["tick_spacing"] == 60
    assert hops[0][2]["hooks"] == HOOKS


def test_check_swap_price_deviation_v4_within_threshold(monkeypatch):
    import aleph_nodestatus.pool_oracle as po
    from aleph_nodestatus.settings import settings as s

    USDC = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
    WETH = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
    USDC_USD_FEED = "0x8fFfFfd4AfB6115b954Bd326cbe7B4BA576818f6"
    ETH_USD_FEED  = "0x5f4eC3Df9cbd43714FE2740f5E3616155c5b8419"
    monkeypatch.setattr(s, "chainlink_usd_feeds", {
        USDC.lower(): USDC_USD_FEED, WETH.lower(): ETH_USD_FEED,
    })

    monkeypatch.setattr(po, "_v4_spot",
                        lambda w3, t_a, t_b, extra: 1 / 2500.0)
    monkeypatch.setattr(po, "_read_chainlink_price",
        lambda w3, feed: {
            USDC_USD_FEED.lower(): (1.0, None),
            ETH_USD_FEED.lower():  (2500.0, None),
        }[feed.lower()])

    HOOKS = "0x0000000000000000000000000000000000000000"
    cfg = {"v": 4, "t": WETH, "v2": [], "v3": b"",
           "v4": [(WETH, 3000, 60, HOOKS, b"")]}
    result = po.check_swap_price_deviation(MagicMock(), cfg, token_in=USDC)
    assert result.ok is True


def test_check_swap_price_deviation_v4_exceeds_threshold(monkeypatch):
    import aleph_nodestatus.pool_oracle as po
    from aleph_nodestatus.settings import settings as s

    USDC = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
    WETH = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
    USDC_USD_FEED = "0xfeed_usdc"
    ETH_USD_FEED  = "0xfeed_eth"
    monkeypatch.setattr(s, "chainlink_usd_feeds", {
        USDC.lower(): USDC_USD_FEED, WETH.lower(): ETH_USD_FEED,
    })
    monkeypatch.setattr(s, "extract_max_deviation_bps", 200)

    monkeypatch.setattr(po, "_v4_spot",
                        lambda w3, t_a, t_b, extra: 1 / 2425.0)
    monkeypatch.setattr(po, "_read_chainlink_price",
        lambda w3, feed: {
            USDC_USD_FEED.lower(): (1.0, None),
            ETH_USD_FEED.lower():  (2500.0, None),
        }[feed.lower()])

    HOOKS = "0x0000000000000000000000000000000000000000"
    cfg = {"v": 4, "t": WETH, "v2": [], "v3": b"",
           "v4": [(WETH, 3000, 60, HOOKS, b"")]}
    result = po.check_swap_price_deviation(MagicMock(), cfg, token_in=USDC)
    assert result.ok is False
    assert result.reason == "price_deviation"


def test_v2_spot_from_reserves(monkeypatch):
    """V2 pair.getReserves() with token_a being token0:
    spot_B_per_A = (reserve1 / 10^dec1) / (reserve0 / 10^dec0)."""
    import aleph_nodestatus.pool_oracle as po

    USDC = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
    WETH = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"

    pair = MagicMock()
    # USDC is token0 (lower hex). Reserve0=10_000 USDC (10_000 × 10^6),
    # Reserve1=4 WETH (4 × 10^18). Spot WETH per USDC = (4 / 10000) = 0.0004.
    pair.functions.getReserves.return_value.call.return_value = (
        10_000 * 10 ** 6, 4 * 10 ** 18, 1_700_000_000,
    )
    w3 = MagicMock()
    w3.eth.contract.return_value = pair

    monkeypatch.setattr(po, "_erc20_decimals",
                        lambda w3_, addr: 6 if addr.lower() == USDC.lower() else 18)

    spot = po._v2_spot(w3, USDC, WETH, pair_address="0xpair")
    assert abs(spot - 0.0004) < 1e-9


def test_check_swap_price_deviation_v2_dispatch(monkeypatch):
    """V2 path goes through the dispatch correctly."""
    import aleph_nodestatus.pool_oracle as po
    from aleph_nodestatus.settings import settings as s

    USDC = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
    WETH = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
    monkeypatch.setattr(s, "chainlink_usd_feeds", {
        USDC.lower(): "0xfeed_usdc", WETH.lower(): "0xfeed_eth",
    })
    monkeypatch.setattr(po, "_v2_spot",
                        lambda w3, t_a, t_b, pair_address: 1 / 2500.0)
    monkeypatch.setattr(po, "_read_chainlink_price",
        lambda w3, feed: {"0xfeed_usdc": (1.0, None),
                          "0xfeed_eth":  (2500.0, None)}[feed.lower()])
    monkeypatch.setattr(po, "_v2_pair_address",
                        lambda w3, t_a, t_b: "0xfakepair")

    cfg = {"v": 2, "t": WETH, "v2": [USDC, WETH], "v3": b"", "v4": []}
    result = po.check_swap_price_deviation(MagicMock(), cfg, token_in=USDC)
    assert result.ok is True
