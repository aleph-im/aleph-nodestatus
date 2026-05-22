import pytest

from aleph_nodestatus.credit_distribution import _distribute_expense
from aleph_nodestatus.settings import settings


def _make_node(owner, score=0.9, stakers=None, status="active"):
    return {
        "owner": owner,
        "score": score,
        "stakers": stakers or {},
        "status": status,
        "resource_nodes": set(),
    }


def _make_rnode(owner, score=0.9, parent=None):
    return {
        "owner": owner,
        "score": score,
        "parent": parent,
        "status": "linked",
        "decentralization": 0.5,
    }


def _make_expense(credits_list, credit_price_aleph=1.0):
    return {
        "credit_price_aleph": credit_price_aleph,
        "credits": credits_list,
    }


class TestDistributeStorageExpense:
    def test_basic_storage_distribution(self):
        """Storage expense: 75% to CCNs, 20% to stakers, 5% dev fund."""
        nodes = {
            "node1": _make_node("0xOwnerA", score=0.9, stakers={"0xStaker1": 100000}),
        }
        expense = _make_expense([{"amount": 100}], credit_price_aleph=1.0)
        rewards = {}

        storage, execution, dev_fund = _distribute_expense(
            "storage", expense, nodes, {}, rewards
        )

        assert storage == 100.0
        assert execution == 0
        assert dev_fund == pytest.approx(5.0)

        # CCN gets 75%, staker gets 20%
        assert rewards["0xOwnerA"] == pytest.approx(75.0)
        assert rewards["0xStaker1"] == pytest.approx(20.0)
        assert sum(rewards.values()) == pytest.approx(95.0)

    def test_storage_two_ccns_weighted_by_score(self):
        """Two CCNs with different scores get proportional shares."""
        nodes = {
            "node1": _make_node("0xOwnerA", score=0.9),  # multiplier=1.0
            "node2": _make_node("0xOwnerB", score=0.5),  # multiplier=0.5
        }
        expense = _make_expense([{"amount": 100}])
        rewards = {}

        _distribute_expense("storage", expense, nodes, {}, rewards)

        # Owner A has score multiplier 1.0, Owner B has 0.5
        # Total score = 1.5, A gets 2/3, B gets 1/3
        assert rewards["0xOwnerA"] > rewards["0xOwnerB"]
        assert rewards["0xOwnerA"] + rewards["0xOwnerB"] == pytest.approx(75.0)

    def test_storage_ccn_below_score_threshold_gets_nothing(self):
        """CCN with score < 0.2 is excluded from rewards."""
        nodes = {
            "node1": _make_node("0xOwnerA", score=0.9),
            "node2": _make_node("0xOwnerB", score=0.1),  # below threshold
        }
        expense = _make_expense([{"amount": 100}])
        rewards = {}

        _distribute_expense("storage", expense, nodes, {}, rewards)

        assert rewards.get("0xOwnerA", 0) == pytest.approx(75.0)
        assert rewards.get("0xOwnerB", 0) == 0

    def test_storage_inactive_node_excluded(self):
        """Inactive nodes don't receive rewards."""
        nodes = {
            "node1": _make_node("0xOwnerA", score=0.9, status="active"),
            "node2": _make_node("0xOwnerB", score=0.9, status="waiting"),
        }
        expense = _make_expense([{"amount": 100}])
        rewards = {}

        _distribute_expense("storage", expense, nodes, {}, rewards)

        assert "0xOwnerA" in rewards
        assert "0xOwnerB" not in rewards

    def test_storage_staker_split_proportional(self):
        """Multiple stakers split 20% proportionally."""
        nodes = {
            "node1": _make_node(
                "0xOwnerA", score=0.9,
                stakers={"0xStaker1": 75000, "0xStaker2": 25000}
            ),
        }
        expense = _make_expense([{"amount": 100}])
        rewards = {}

        _distribute_expense("storage", expense, nodes, {}, rewards)

        assert rewards["0xStaker1"] == pytest.approx(15.0)
        assert rewards["0xStaker2"] == pytest.approx(5.0)

    def test_zero_credit_price_yields_zero_rewards(self):
        """If credit_price_aleph is 0, nothing is distributed."""
        nodes = {
            "node1": _make_node("0xOwnerA", score=0.9, stakers={"0xStaker1": 100000}),
        }
        expense = _make_expense([{"amount": 100}], credit_price_aleph=0)
        rewards = {}

        _distribute_expense("storage", expense, nodes, {}, rewards)

        assert sum(rewards.values()) == 0

    def test_all_ccns_zero_score(self):
        """If all CCN scores are below threshold, CCN pool is not distributed."""
        nodes = {
            "node1": _make_node("0xOwnerA", score=0.1, stakers={"0xStaker1": 100000}),
        }
        expense = _make_expense([{"amount": 100}])
        rewards = {}

        _distribute_expense("storage", expense, nodes, {}, rewards)

        # CCN gets nothing (score too low), staker still gets 20%
        assert rewards.get("0xOwnerA", 0) == 0
        assert rewards["0xStaker1"] == pytest.approx(20.0)

    def test_no_stakers(self):
        """If no stakers, staker pool is not distributed."""
        nodes = {
            "node1": _make_node("0xOwnerA", score=0.9, stakers={}),
        }
        expense = _make_expense([{"amount": 100}])
        rewards = {}

        _distribute_expense("storage", expense, nodes, {}, rewards)

        assert rewards["0xOwnerA"] == pytest.approx(75.0)
        assert len(rewards) == 1  # Only the CCN owner


class TestDistributeExecutionExpense:
    def test_basic_execution_distribution(self):
        """Execution: 60% to CRN, 15% to CCNs, 20% to stakers, 5% dev."""
        nodes = {
            "node1": _make_node("0xOwnerA", score=0.9, stakers={"0xStaker1": 100000}),
        }
        rnodes = {
            "rnode1": _make_rnode("0xCRN_Owner", parent="node1"),
        }
        expense = _make_expense(
            [{"amount": 100, "node_id": "rnode1"}], credit_price_aleph=1.0
        )
        rewards = {}

        storage, execution, dev_fund = _distribute_expense(
            "execution", expense, nodes, rnodes, rewards
        )

        assert storage == 0
        assert execution == 100.0
        assert dev_fund == pytest.approx(5.0)

        # CRN gets 60%, CCN gets 15%, staker gets 20%
        assert rewards["0xCRN_Owner"] == pytest.approx(60.0)
        assert rewards["0xOwnerA"] == pytest.approx(15.0)
        assert rewards["0xStaker1"] == pytest.approx(20.0)
        assert sum(rewards.values()) == pytest.approx(95.0)

    def test_execution_missing_crn(self):
        """If CRN not found, its 60% share is dropped."""
        nodes = {
            "node1": _make_node("0xOwnerA", score=0.9, stakers={"0xStaker1": 100000}),
        }
        expense = _make_expense(
            [{"amount": 100, "node_id": "nonexistent"}]
        )
        rewards = {}

        _distribute_expense("execution", expense, nodes, {}, rewards)

        # CRN share (60%) is lost, CCN and staker still get theirs
        assert rewards["0xOwnerA"] == pytest.approx(15.0)
        assert rewards["0xStaker1"] == pytest.approx(20.0)
        assert sum(rewards.values()) == pytest.approx(35.0)

    def test_execution_multiple_credits(self):
        """Multiple credits in one expense go to different CRNs."""
        nodes = {
            "node1": _make_node("0xOwnerA", score=0.9),
        }
        rnodes = {
            "rnode1": _make_rnode("0xCRN1", parent="node1"),
            "rnode2": _make_rnode("0xCRN2", parent="node1"),
        }
        expense = _make_expense([
            {"amount": 60, "node_id": "rnode1"},
            {"amount": 40, "node_id": "rnode2"},
        ])
        rewards = {}

        _distribute_expense("execution", expense, nodes, rnodes, rewards)

        assert rewards["0xCRN1"] == pytest.approx(36.0)  # 60 * 0.6
        assert rewards["0xCRN2"] == pytest.approx(24.0)  # 40 * 0.6


class TestDistributeExpenseAccumulation:
    def test_rewards_accumulate_across_calls(self):
        """Multiple calls to _distribute_expense accumulate in same dict."""
        nodes = {
            "node1": _make_node("0xOwnerA", score=0.9),
        }
        expense = _make_expense([{"amount": 50}])
        rewards = {}

        _distribute_expense("storage", expense, nodes, {}, rewards)
        _distribute_expense("storage", expense, nodes, {}, rewards)

        assert rewards["0xOwnerA"] == pytest.approx(75.0)  # 37.5 * 2
