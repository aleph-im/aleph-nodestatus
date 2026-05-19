import json
from pathlib import Path

from aleph_nodestatus import __file__ as pkg_init


def _abi_path(name):
    return Path(pkg_init).parent / "abi" / f"{name}.json"


def test_payment_processor_abi_has_process_function():
    abi = json.loads(_abi_path("AlephPaymentProcessor").read_text())
    fns = [item for item in abi if item.get("type") == "function"
                                and item.get("name") == "process"]
    assert len(fns) == 1
    inputs = [(i["name"], i["type"]) for i in fns[0]["inputs"]]
    assert inputs == [
        ("_token", "address"),
        ("_amountIn", "uint128"),
        ("_amountOutMinimum", "uint128"),
        ("_ttl", "uint48"),
    ]


def test_quoter_v2_abi_has_quote_exact_input():
    abi = json.loads(_abi_path("UniswapV3QuoterV2").read_text())
    fns = [item for item in abi if item.get("type") == "function"
                                and item.get("name") == "quoteExactInput"]
    assert len(fns) >= 1
    inputs = [(i["name"], i["type"]) for i in fns[0]["inputs"]]
    assert inputs == [("path", "bytes"), ("amountIn", "uint256")]


def test_anti_mev_abis_present_and_valid():
    expected = [
        "IUniswapV3Pool.json",
        "IUniswapV3Factory.json",
        "IUniswapV4StateView.json",
        "ChainlinkAggregator.json",
    ]
    for name in expected:
        p = _abi_path(name.replace(".json", ""))
        assert p.exists(), f"missing ABI: {name}"
        data = json.loads(p.read_text())
        assert isinstance(data, list), f"ABI {name} must be a JSON array"
        assert len(data) > 0, f"ABI {name} is empty"
