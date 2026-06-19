"""CRN inactivity slashing — pure decision logic, no I/O.

A CRN is "slashable" when it has been inactive (score < 0.01, recorded as
`inactive_since` block height in the status snapshots) for at least
`threshold_days`, measured in ETH block height to mirror the existing 90-day
removal check in status.py.
"""
from .settings import settings

STREAMS = ("credit_revenue", "holder_tier", "wage_subsidy")


def is_slashable(inactive_since, close_height, *,
                 threshold_days=None, blocks_per_day=None):
    """True when a CRN with the given `inactive_since` (block height or None)
    has been inactive for >= threshold_days at `close_height`."""
    if inactive_since is None:
        return False
    if threshold_days is None:
        threshold_days = settings.credit_dist_slash_threshold_days
    if blocks_per_day is None:
        blocks_per_day = settings.ethereum_blocks_per_day
    return (close_height - inactive_since) >= threshold_days * blocks_per_day


class SlashAccumulator:
    """Per-(stream, crn_node_id) ledger of CRN reward shares.

    `total` is the full window share (retroactive mode); `post_death` is the
    subset accrued while the routed snapshot already flagged the CRN inactive
    (from-death mode). `inactive_since` keeps the most recent non-None height
    seen, used only as a fallback when the close-of-window snapshot no longer
    contains the node (e.g. removed at day 90 mid-window).
    """

    def __init__(self):
        self._entries = {}

    def add(self, stream, node_id, address, amount, *,
            inactive_at_expense, inactive_since):
        if not node_id:
            return
        key = (stream, node_id)
        entry = self._entries.get(key)
        if entry is None:
            entry = {"address": address, "total": 0.0,
                     "post_death": 0.0, "inactive_since": inactive_since}
            self._entries[key] = entry
        entry["address"] = address
        entry["total"] += amount
        if inactive_at_expense:
            entry["post_death"] += amount
        if inactive_since is not None:
            entry["inactive_since"] = inactive_since

    def entries(self):
        return self._entries


def enabled_slash_streams():
    """Streams whose slashing is enabled, per settings flags."""
    streams = []
    if settings.credit_dist_slash_credit_revenue:
        streams.append("credit_revenue")
    if settings.credit_dist_slash_holder_tier:
        streams.append("holder_tier")
    if settings.credit_dist_slash_wage_subsidy:
        streams.append("wage_subsidy")
    return tuple(streams)


def compute_slashing(accumulator, close_resource_nodes, close_height, *,
                     enabled_streams, retroactive,
                     threshold_days=None, blocks_per_day=None):
    """Run the slashing pass.

    Returns `(slashed, meta_nodes)`:
      - `slashed`: {reward_address: withheld_amount} aggregated across all
        slashed CRNs/streams.
      - `meta_nodes`: list of {node_id, address, stream, amount,
        inactive_since} for the audit `slashed_meta`.

    A CRN's `inactive_since` is read from the close-of-window snapshot; if the
    node is no longer present there (removed mid-window), the accumulator's
    last-seen value is used as a fallback.
    """
    slashed = {}
    meta_nodes = []
    for (stream, node_id), entry in accumulator.entries().items():
        if stream not in enabled_streams:
            continue
        rnode = close_resource_nodes.get(node_id)
        inactive_since = (
            rnode.get("inactive_since") if rnode is not None
            else entry["inactive_since"]
        )
        if not is_slashable(inactive_since, close_height,
                            threshold_days=threshold_days,
                            blocks_per_day=blocks_per_day):
            continue
        amount = entry["total"] if retroactive else entry["post_death"]
        if amount <= 0:
            continue
        addr = entry["address"]
        slashed[addr] = slashed.get(addr, 0.0) + amount
        meta_nodes.append({
            "node_id": node_id, "address": addr, "stream": stream,
            "amount": amount, "inactive_since": inactive_since,
        })
    return slashed, meta_nodes
