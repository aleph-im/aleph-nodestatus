"""Fork-mode reconciliation tests for distribute (ethereum.audit_distribution_tx).

Builds synthetic transaction receipts whose `logs` contain ERC20 Transfer
events and runs `audit_distribution_tx` against them. Each test pins one
filter or comparison rule so that a future drift (e.g., the filter
forgetting to check `from == sender`) fails loudly here instead of at
runtime when somebody's distribution silently audits the wrong txs.
"""

from aleph_nodestatus.ethereum import (
    DECIMALS, audit_distribution_tx,
)


def _mk_real_w3():
    """Web3 with no provider — only used for codec/keccak helpers."""
    from web3 import Web3
    return Web3()


def _mk_transfer_log(w3, *, token: str, from_addr: str, to_addr: str,
                    value_wei: int):
    """Build an ERC20 Transfer log in the exact shape
    `eth_getTransactionReceipt` returns. Indexed addresses are
    left-padded to 32 bytes; the value goes in `data`."""
    topic0 = w3.keccak(text="Transfer(address,address,uint256)")
    pad_addr = lambda a: bytes(12) + bytes.fromhex(a[2:].rjust(40, "0"))
    topics = [topic0, pad_addr(from_addr), pad_addr(to_addr)]
    data = value_wei.to_bytes(32, "big")
    return {
        "address":          token,
        "topics":           topics,
        "data":             data,
        "blockNumber":      19_482_103,
        "transactionIndex": 0,
        "logIndex":         0,
        "transactionHash":  b"\xab" * 32,
        "blockHash":        b"\xcd" * 32,
        "removed":          False,
    }


def _mk_audit_tx_info(logs, *, recipients=None, amounts_wei=None,
                     tx_hash="0xbatch", gas_used=987_654, nonce=7):
    """Mimic transfer_tokens_audited's return shape."""
    return {
        "tx_hash":  tx_hash,
        "status":   1,
        "receipt":  {
            "status":      1,
            "blockNumber": 19_482_103,
            "gasUsed":     gas_used,
            "logs":        logs,
        },
        "built_tx": {
            "from":    "0x3a5CC6aBd06B601f4654035d125F9DD2FC992C25",
            "to":      "0x27702a26126e0B3702af63Ee09aC4d1A084EF628",
            "gas":     1_030_000,
            "nonce":   nonce, "chainId": 1,
            "fn":      "batchTransfer",
            "args":    {
                "recipients":  recipients or [],
                "amounts_wei": amounts_wei or [],
            },
        },
    }


# Canonical fixtures
ALEPH_TOKEN = "0x27702a26126e0B3702af63Ee09aC4d1A084EF628"
SENDER      = "0x3a5CC6aBd06B601f4654035d125F9DD2FC992C25"
OTHER_TOKEN = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"  # USDC
OTHER_SENDER = "0x000000000000000000000000000000000000DEAD"


def test_audit_distribution_tx_clean_match_returns_pass(capsys):
    """All recipients receive exactly their expected amounts → pass,
    zero mismatches, zero unexpected, reconcile_failed=False."""
    w3 = _mk_real_w3()
    targets = {
        "0x0000000000000000000000000000000000000001": 1.5,
        "0x0000000000000000000000000000000000000002": 2.25,
        "0x0000000000000000000000000000000000000003": 100.0,
    }
    logs = [
        _mk_transfer_log(
            w3, token=ALEPH_TOKEN, from_addr=SENDER,
            to_addr=r, value_wei=int(v * DECIMALS),
        )
        for r, v in targets.items()
    ]
    result = audit_distribution_tx(
        w3,
        tx_info=_mk_audit_tx_info(
            logs,
            recipients=list(targets.keys()),
            amounts_wei=[int(v * DECIMALS) for v in targets.values()],
        ),
        batch_targets=targets, batch_index=1,
        token_address=ALEPH_TOKEN,
        sender_address=SENDER,
        reconcile_bps=0,
    )
    assert result["reconcile_failed"] is False
    assert result["mismatches"] == []
    assert result["unexpected"] == []
    assert result["total_expected"] == result["total_realized"]
    out = capsys.readouterr().out
    assert "=== Distribute tx audit: Batch 1 (3 recipients) === PASS" in out
    assert "per-recipient matched: 3/3" in out


def test_audit_distribution_tx_one_wei_short_with_zero_bps_fails(capsys):
    """`reconcile_bps=0` means exact-wei matching: even a 1-wei
    shortfall on a large transfer must trip reconcile_failed. Without
    this, sub-bps drift on large amounts (1 wei out of 2e18 floors to
    0 bps) would silently slip through."""
    w3 = _mk_real_w3()
    targets = {
        "0x0000000000000000000000000000000000000001": 1.0,
        "0x0000000000000000000000000000000000000002": 2.0,
    }
    logs = [
        _mk_transfer_log(
            w3, token=ALEPH_TOKEN, from_addr=SENDER,
            to_addr="0x0000000000000000000000000000000000000001",
            value_wei=int(1.0 * DECIMALS),
        ),
        _mk_transfer_log(
            w3, token=ALEPH_TOKEN, from_addr=SENDER,
            to_addr="0x0000000000000000000000000000000000000002",
            value_wei=int(2.0 * DECIMALS) - 1,  # exactly 1 wei short
        ),
    ]
    result = audit_distribution_tx(
        w3,
        tx_info=_mk_audit_tx_info(logs),
        batch_targets=targets, batch_index=1,
        token_address=ALEPH_TOKEN,
        sender_address=SENDER,
        reconcile_bps=0,
    )
    assert result["reconcile_failed"] is True
    assert len(result["mismatches"]) == 1
    m = result["mismatches"][0]
    assert m["recipient"] == "0x0000000000000000000000000000000000000002"
    assert m["delta_wei"] == -1
    assert m["delta_bps"] == 0  # the rounding gap the strict path closes


def test_audit_distribution_tx_within_bps_tolerance_passes(capsys):
    """With `reconcile_bps > 0` the bps tolerance kicks back in:
    1 wei short on a 2-ALEPH transfer is comfortably within any
    non-zero bps limit."""
    w3 = _mk_real_w3()
    targets = {
        "0x0000000000000000000000000000000000000002": 2.0,
    }
    logs = [
        _mk_transfer_log(
            w3, token=ALEPH_TOKEN, from_addr=SENDER,
            to_addr="0x0000000000000000000000000000000000000002",
            value_wei=int(2.0 * DECIMALS) - 1,
        ),
    ]
    result = audit_distribution_tx(
        w3,
        tx_info=_mk_audit_tx_info(logs),
        batch_targets=targets, batch_index=1,
        token_address=ALEPH_TOKEN,
        sender_address=SENDER,
        reconcile_bps=10,  # any tolerance > 0 lets a sub-bps gap through
    )
    assert result["reconcile_failed"] is False
    assert result["mismatches"] == []


def test_audit_distribution_tx_significant_short_marks_fail(capsys):
    """A meaningful shortfall (e.g. 1% short on a 2-ALEPH transfer)
    must fail under either reconcile_bps=0 or any sensible bps limit."""
    w3 = _mk_real_w3()
    targets = {
        "0x0000000000000000000000000000000000000001": 1.0,
        "0x0000000000000000000000000000000000000002": 2.0,
    }
    big_short = int(2.0 * DECIMALS) - int(0.01 * DECIMALS)  # 1% short
    logs = [
        _mk_transfer_log(
            w3, token=ALEPH_TOKEN, from_addr=SENDER,
            to_addr="0x0000000000000000000000000000000000000001",
            value_wei=int(1.0 * DECIMALS),
        ),
        _mk_transfer_log(
            w3, token=ALEPH_TOKEN, from_addr=SENDER,
            to_addr="0x0000000000000000000000000000000000000002",
            value_wei=big_short,
        ),
    ]
    result = audit_distribution_tx(
        w3,
        tx_info=_mk_audit_tx_info(logs),
        batch_targets=targets, batch_index=2,
        token_address=ALEPH_TOKEN,
        sender_address=SENDER,
        reconcile_bps=10,  # 10 bps = 0.1%; a 1% short blows past this
    )
    assert result["reconcile_failed"] is True
    assert len(result["mismatches"]) == 1
    assert result["mismatches"][0]["delta_bps"] > 10


def test_audit_distribution_tx_unexpected_recipient_marks_fail(capsys):
    """Receipt has a Transfer to an address NOT in our batch → flagged
    under unexpected and reconcile_failed=True. This catches a class
    of bugs where the token contract has a hook that re-routes some
    portion of a transfer to another address."""
    w3 = _mk_real_w3()
    targets = {"0x0000000000000000000000000000000000000001": 1.0}
    logs = [
        _mk_transfer_log(
            w3, token=ALEPH_TOKEN, from_addr=SENDER,
            to_addr="0x0000000000000000000000000000000000000001",
            value_wei=int(1.0 * DECIMALS),
        ),
        _mk_transfer_log(  # not in our targets!
            w3, token=ALEPH_TOKEN, from_addr=SENDER,
            to_addr="0x0000000000000000000000000000000000000999",
            value_wei=int(0.1 * DECIMALS),
        ),
    ]
    result = audit_distribution_tx(
        w3,
        tx_info=_mk_audit_tx_info(logs),
        batch_targets=targets, batch_index=1,
        token_address=ALEPH_TOKEN,
        sender_address=SENDER,
        reconcile_bps=0,
    )
    assert result["reconcile_failed"] is True
    assert len(result["unexpected"]) == 1
    assert (result["unexpected"][0]["recipient"]
            == "0x0000000000000000000000000000000000000999")
    assert result["unexpected"][0]["realized_wei"] == int(0.1 * DECIMALS)


def test_audit_distribution_tx_ignores_other_sender_transfers(capsys):
    """A Transfer of the right token from a DIFFERENT sender must not
    count toward our reconciliation — that's somebody else's tx
    that happens to be in the same block, not our batchTransfer."""
    w3 = _mk_real_w3()
    targets = {"0x0000000000000000000000000000000000000001": 1.0}
    logs = [
        _mk_transfer_log(
            w3, token=ALEPH_TOKEN, from_addr=OTHER_SENDER,
            to_addr="0x0000000000000000000000000000000000000001",
            value_wei=int(1.0 * DECIMALS),
        ),
    ]
    result = audit_distribution_tx(
        w3,
        tx_info=_mk_audit_tx_info(logs),
        batch_targets=targets, batch_index=1,
        token_address=ALEPH_TOKEN,
        sender_address=SENDER,  # different from log
        reconcile_bps=0,
    )
    # Our recipient gets 0 realized → looks like a missed transfer →
    # reconcile_failed=True
    assert result["reconcile_failed"] is True
    # Recipient is in mismatches (0 realized vs expected 1 ALEPH)
    assert len(result["mismatches"]) == 1
    assert result["mismatches"][0]["realized_wei"] == 0
    # No unexpected — the other sender's Transfer was correctly filtered
    # out, not just "unexpected".
    assert result["unexpected"] == []


def test_audit_distribution_tx_ignores_other_token_transfers(capsys):
    """A Transfer FROM our sender of a DIFFERENT token must not count.
    This guards against accidental cross-token attribution if a
    multicall ever mixes ALEPH with a stable."""
    w3 = _mk_real_w3()
    targets = {"0x0000000000000000000000000000000000000001": 1.0}
    logs = [
        _mk_transfer_log(
            w3, token=OTHER_TOKEN, from_addr=SENDER,
            to_addr="0x0000000000000000000000000000000000000001",
            value_wei=int(1.0 * DECIMALS),  # in USDC wei, not ALEPH wei
        ),
    ]
    result = audit_distribution_tx(
        w3,
        tx_info=_mk_audit_tx_info(logs),
        batch_targets=targets, batch_index=1,
        token_address=ALEPH_TOKEN,
        sender_address=SENDER,
        reconcile_bps=0,
    )
    assert result["reconcile_failed"] is True
    assert result["mismatches"][0]["realized_wei"] == 0
    assert result["unexpected"] == []
