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
