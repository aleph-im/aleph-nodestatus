"""H3 bounding: merge_rewards output must be order-independent for inputs
near the dust threshold. If float accumulation ever introduces non-determinism,
this test breaks loudly and we revisit the Decimal-everywhere refactor."""

import random

from hypothesis import given, settings as hyp_settings, strategies as st

from aleph_nodestatus.rewards_merge import merge_rewards


def _eth_address(i):
    return "0x" + f"{i:040x}"


# Generate small dicts with values clustered around the 0.01 dust threshold.
amount_strategy = st.floats(
    min_value=0.005, max_value=0.02,
    allow_nan=False, allow_infinity=False,
)


@st.composite
def reward_set(draw):
    """A {source_name: {address: amount}} dict with overlapping addresses."""
    n_sources = draw(st.integers(min_value=2, max_value=4))
    n_addresses = draw(st.integers(min_value=2, max_value=10))
    addresses = [_eth_address(i) for i in range(n_addresses)]
    sources = {}
    for s in range(n_sources):
        per_source_n = draw(st.integers(min_value=1, max_value=n_addresses))
        per_source_addrs = draw(
            st.lists(
                st.sampled_from(addresses),
                min_size=per_source_n, max_size=per_source_n, unique=True,
            )
        )
        sources[f"src_{s}"] = {
            a: draw(amount_strategy) for a in per_source_addrs
        }
    return sources


@given(reward_set())
@hyp_settings(max_examples=200, deadline=None)
def test_merge_rewards_is_order_independent(reward_dict):
    """Shuffling the per-source iteration order must not change final output."""
    final_a, _ = merge_rewards(reward_dict, dust_threshold=0.01)

    shuffled = dict(list(reward_dict.items()))
    items = list(shuffled.items())
    random.shuffle(items)
    shuffled = dict(items)

    # Also shuffle each inner dict.
    for k in shuffled:
        inner = list(shuffled[k].items())
        random.shuffle(inner)
        shuffled[k] = dict(inner)

    final_b, _ = merge_rewards(shuffled, dust_threshold=0.01)

    assert set(final_a.keys()) == set(final_b.keys())
    for addr in final_a:
        assert final_a[addr] == final_b[addr], (
            f"Float accumulation produced order-dependent output for {addr}: "
            f"{final_a[addr]!r} vs {final_b[addr]!r}. "
            f"Trigger the H3 Decimal-upstream follow-up."
        )
