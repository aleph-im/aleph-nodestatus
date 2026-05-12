"""Merge per-source reward dicts and filter dust."""

from collections import defaultdict
from decimal import Decimal
from typing import Dict, Optional, Tuple

from eth_utils import to_checksum_address


def merge_rewards(
    sources: Dict[str, Dict[str, float]],
    details: Optional[Dict[str, Dict[str, Dict[str, float]]]] = None,
    dust_threshold: float = 0.01,
) -> Tuple[
    Dict[str, float],
    Dict[str, Dict[str, float]],
    Dict[str, Dict[str, Dict[str, float]]],
]:
    """Merge address->amount dicts across sources, dropping dust.

    Args:
        sources: {source_name: {address: amount}}
        details: optional {source_name: {address: {component: amount}}} —
                 per-account, per-component breakdown to be inverted into the
                 returned per-address audit view.
        dust_threshold: addresses with total < this are dropped from the
                        final dict, the audit-source dict, and the audit-
                        details dict so all three views agree on survivors.

    Returns:
        (final_rewards, audit_sources, audit_details)
        final_rewards:  {address (EIP-55 checksum): total_amount}
        audit_sources:  {source_name: {address: amount}} — restricted to
                        dust survivors.
        audit_details:  {address: {source_name: {component: amount}}} —
                        restricted to dust survivors. Each address's nested
                        dict sums to final_rewards[address].
                        Empty `{}` when no `details` argument is supplied.
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

    audit_details_acc: Dict[str, Dict[str, Dict[str, float]]] = defaultdict(
        lambda: defaultdict(lambda: defaultdict(float))
    )
    if details:
        for src_name, src_details in details.items():
            for addr, components in src_details.items():
                normalized_addr = to_checksum_address(addr)
                if normalized_addr not in survivors:
                    continue
                for component, amount in components.items():
                    audit_details_acc[normalized_addr][src_name][component] += (
                        float(amount)
                    )
    audit_details = {
        addr: {src: dict(comps) for src, comps in src_dict.items()}
        for addr, src_dict in audit_details_acc.items()
    }

    return final, audit_sources, audit_details
