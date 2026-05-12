"""Merge per-source reward dicts and filter dust."""

from collections import defaultdict
from decimal import Decimal
from typing import Dict, Tuple

from eth_utils import to_checksum_address


def merge_rewards(
    sources: Dict[str, Dict[str, float]],
    dust_threshold: float = 0.01,
) -> Tuple[Dict[str, float], Dict[str, Dict[str, float]]]:
    """Merge address->amount dicts across sources, dropping dust.

    Args:
        sources: {source_name: {address: amount}}
        dust_threshold: addresses with total < this are dropped from
                        BOTH the final dict and the audit-source dict.

    Returns:
        (final_rewards, audit_sources)
        final_rewards: {address (EIP-55 checksum): total_amount}
        audit_sources: per-source view restricted to addresses that survived
                       the dust filter. The published rewards_by_source thus
                       sums exactly to final_rewards.
    """
    normalized_sources = {
        name: {to_checksum_address(addr): float(amt) for addr, amt in src.items()}
        for name, src in sources.items()
    }

    total: Dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
    for src in normalized_sources.values():
        for addr, amt in src.items():
            total[addr] += Decimal(str(amt))

    threshold = Decimal(str(dust_threshold))
    final = {a: float(v) for a, v in total.items() if v >= threshold}
    survivors = set(final.keys())

    audit_sources = {
        name: {addr: amt for addr, amt in src.items() if addr in survivors}
        for name, src in normalized_sources.items()
    }
    return final, audit_sources
