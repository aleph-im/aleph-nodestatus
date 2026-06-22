"""Pre-flight nonce-continuity guard for credit distribution.

Refuses to transfer when the signer's on-chain nonce does not cleanly continue
from the last distribution's transfer batches — i.e. transactions happened from
that account since the last distribution (a prior batchTransfer that was never
published, or a manual operator tx). Re-running would double-pay, so we block
and require an explicit operator override after manual review.
"""

import sys
import types

if "plyvel" not in sys.modules:
    _plyvel_stub = types.ModuleType("plyvel")
    _plyvel_stub.DB = object
    sys.modules["plyvel"] = _plyvel_stub

from aleph_nodestatus.commands import max_successful_nonce, nonce_gap_reason


def _dist(*nonces_success):
    """Build a distribution-post content with one target per (nonce, success)."""
    return {
        "targets": [
            {"success": ok, "nonce": n, "tx": f"0x{n}"}
            for (n, ok) in nonces_success
        ]
    }


class TestMaxSuccessfulNonce:
    def test_picks_max_among_successful(self):
        c = _dist((10, True), (11, True), (12, False))
        assert max_successful_nonce(c) == 11

    def test_none_when_no_content(self):
        assert max_successful_nonce(None) is None

    def test_none_when_no_recorded_nonce_and_no_resolver(self):
        # pre-nonce-tracking post: targets without a nonce field, no resolver
        c = {"targets": [{"success": True, "tx": "0xabc"}]}
        assert max_successful_nonce(c) is None

    def test_resolves_nonce_from_tx_hash_when_not_recorded(self):
        # pre-nonce-tracking post (like the real last distribution): only `tx`.
        c = {
            "targets": [
                {"success": True, "tx": "0xaaa"},
                {"success": True, "tx": "0xbbb"},
                {"success": False, "tx": "0xccc"},  # failed -> ignored
            ]
        }
        on_chain = {"0xaaa": 41, "0xbbb": 42, "0xccc": 99}
        assert max_successful_nonce(c, on_chain.get) == 42

    def test_prefers_recorded_nonce_over_resolver(self):
        c = {"targets": [{"success": True, "nonce": 7, "tx": "0xaaa"}]}
        # resolver would say 99, but the recorded nonce wins (no RPC needed)
        assert max_successful_nonce(c, lambda _tx: 99) == 7

    def test_none_when_tx_unresolvable(self):
        c = {"targets": [{"success": True, "tx": "0xdropped"}]}
        assert max_successful_nonce(c, lambda _tx: None) is None


class TestNonceGapReason:
    def test_first_distribution_ever_is_ok(self):
        # no prior distribution -> nothing to continue from
        assert nonce_gap_reason(current_nonce=5, last_content=None) is None

    def test_clean_continuation_is_ok(self):
        # last successful nonce 11 -> account nonce should be 12
        c = _dist((10, True), (11, True))
        assert nonce_gap_reason(current_nonce=12, last_content=c) is None

    def test_gap_is_blocked(self):
        # account nonce 15 but last distribution ended at nonce 11 -> 3 extra txs
        c = _dist((10, True), (11, True))
        reason = nonce_gap_reason(current_nonce=15, last_content=c)
        assert reason is not None
        assert "intermediate" in reason.lower() or "beyond" in reason.lower()

    def test_unverifiable_prior_is_blocked(self):
        # prior distribution exists but nonce can't be determined (no recorded
        # nonce and no resolver) -> cannot verify
        c = {"targets": [{"success": True, "tx": "0xabc"}]}
        reason = nonce_gap_reason(current_nonce=12, last_content=c)
        assert reason is not None
        assert "verify" in reason.lower()

    def test_nonce_resolved_from_tx_allows_clean_continuation(self):
        # the real second-distribution case: prior post has only tx hashes;
        # resolving them on-chain gives max nonce 42, current 43 -> clean.
        c = {"targets": [{"success": True, "tx": "0xaaa"}]}
        assert (
            nonce_gap_reason(43, c, get_tx_nonce=lambda _tx: 42) is None
        )
        # but current 50 with resolved max 42 -> gap blocked
        assert nonce_gap_reason(50, c, get_tx_nonce=lambda _tx: 42) is not None
