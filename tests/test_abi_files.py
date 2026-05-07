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
