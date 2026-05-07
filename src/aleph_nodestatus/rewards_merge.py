"""Merge per-source reward dicts and filter dust."""

from collections import defaultdict
from decimal import Decimal
from typing import Dict, Tuple


def merge_rewards(
    sources: Dict[str, Dict[str, float]],
    dust_threshold: float = 0.01,
) -> Tuple[Dict[str, float], Dict[str, Dict[str, float]]]:
    """Merge address->amount dicts across sources.

    Args:
        sources: {source_name: {address: amount}}
        dust_threshold: addresses with total < this are dropped.

    Returns:
        (final_rewards, sources)
        final_rewards: {address (lowercased): total_amount}
        sources: the input dict, addresses lowercased (caller may store for audit).
    """
    normalized_sources = {
        name: {addr.lower(): float(amt) for addr, amt in src.items()}
        for name, src in sources.items()
    }

    total: Dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
    for src in normalized_sources.values():
        for addr, amt in src.items():
            total[addr] += Decimal(str(amt))

    threshold = Decimal(str(dust_threshold))
    final = {a: float(v) for a, v in total.items() if v >= threshold}
    return final, normalized_sources
