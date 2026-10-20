import importlib.util
import pathlib

_p = pathlib.Path(__file__).parent.parent / "scripts" / "compare_revenue_vs_expenses.py"
_s = importlib.util.spec_from_file_location("cmp_script", _p)
cmp = importlib.util.module_from_spec(_s)
_s.loader.exec_module(cmp)


def test_aggregate_and_diff():
    periods = [
        {"period": 0, "credit_price_aleph": "0.5", "storage": 10,
         "nodes": {"a": {"execution": 100, "hold_equivalent": 20}}},
        {"period": 3600, "credit_price_aleph": "1", "storage": 0,
         "nodes": {"a": {"execution": 10, "hold_equivalent": 0},
                   "b": {"execution": 4, "hold_equivalent": 0}}},
    ]
    ex, hold, st = cmp.aggregate_new(periods)
    assert ex == {"a": 60.0, "b": 4.0}
    assert hold["a"] == 10.0 and st == 5.0
    msg = {"content": {"content": {"tags": ["type_execution"], "expense": {
        "credit_price_aleph": 0.5,
        "credits": [{"node_id": "a", "amount": 100}],
        "rewards": [{"node_id": "a", "amount": 20}]}}}}
    old = cmp.aggregate_old([msg])
    assert old[0] == {"a": 50.0} and old[1] == {"a": 10.0}
    rep = cmp.build_report(old, (ex, hold, st))
    assert rep["execution"][0]["node_id"] == "a"
    assert rep["execution"][0]["diff"] == 10.0
    assert rep["execution"][1]["diff_pct"] is None


def test_old_storage_and_hold_key(caplog):
    storage = {"content": {"content": {"tags": ["type_storage"], "expense": {
        "credit_price_aleph": 0.25,
        "credits": [{"address": "0x1", "amount": 40}, {"address": "0x2", "amount": 20}]}}}}
    execm = {"content": {"content": {"tags": ["type_execution"], "expense": {
        "credit_price_aleph": 2,
        "credits": [{"node_id": "a", "amount": 3}, {"amount": 9}],
        "hold": [{"node_id": "a", "amount": 5}, {"node_id": "b"}]}}}}
    with caplog.at_level("WARNING"):
        ex, hold, st = cmp.aggregate_old([storage, execm])
    assert st == (40 + 20) * 0.25
    assert ex == {"a": 6.0}
    assert hold == {"a": 10.0, "b": 0.0}
    assert "dropped 1" in caplog.text
