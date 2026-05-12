# C1 Rollback — Design Spec

**Date:** 2026-05-12
**Branch:** `feat/credit-distribution-v2` (follow-up after T1-T8)
**Source request:** Roll back T1's aggressive partial-failure handling; use manual transactions + Aleph AMEND for cleanup.

## Goal

Revert the orchestrator-side aspect of T1 so partial-failure distribution posts behave like every other distribution post: the script attempts every batch, records per-batch outcomes in `targets[]`, publishes the audit post, and advances the cursor to `post.end_height + 1` on the next run. Failed batches are surfaced clearly in the run output so operators can retransfer manually and AMEND the post.

## Non-goals

- Auto-retry of failed batches.
- Halting the orchestrator on partial-failure.
- Changes to the batch-execution loop (it already tries every batch).
- Changes to the legacy staking distribution (it already uses break-on-first-success cursor semantics).
- Adding tooling for manual amendment (out-of-band operator work).
- Fixing the pre-existing `NONCE` non-increment-on-failure quirk in `transfer_tokens` (separate ticket).

## Rationale

T1's design assumed partial-failure was rare enough that operators would always intervene with explicit `--start-height`. In practice the orchestrator doesn't HALT on partial-failure detection — it falls back to the older all-success cursor (which would re-cover the partial-failure window and double-pay) or halts only when no older cursor exists. Operators have indicated they prefer the legacy behavior: cursor advances on any-success, failed batches are tracked manually via the published audit post + Aleph AMEND.

## Design

### 1. Revert `get_latest_successful_credit_distribution`

Restore the pre-T1 logic: a post is "successful" if any batch in `targets[]` has `success: true`. Returns 2-tuple `(end_height, content)`. No `all_success` flag. No partial-failure WARNING. Match the legacy `get_latest_successful_distribution`'s contract.

```python
async def get_latest_successful_credit_distribution(sender=None):
    """Find the last credit distribution that had successful transfers.

    Treats a post as 'successful' if ANY batch in `targets[]` succeeded.
    Failed batches in such posts must be manually retransferred and the
    post AMENDed out-of-band; the cursor advances normally regardless.

    In the documented two-instance deployment, only `distribution_recipient`
    publishes status='distribution'; `status_sender` is queried defensively
    to handle single-instance / dev setups.
    """
    if sender is None:
        senders = [settings.status_sender, settings.distribution_recipient]
    elif isinstance(sender, (list, tuple)):
        senders = list(sender)
    else:
        senders = [sender]

    async with AlephHttpClient(
        api_server=PublishMode.get_publish_api_server()
    ) as client:
        posts = await client.get_posts(
            post_filter=PostFilter(
                types=[CREDIT_DISTRIBUTION_POST_TYPE],
                addresses=senders,
            )
        )

    current_post = None
    current_end_height = 0
    for post in posts.posts:
        if post.content.get("status") != "distribution":
            continue
        end_height = post.content.get("end_height")
        if end_height is None:
            continue
        if any(t.get("success") for t in post.content.get("targets", [])):
            if end_height >= current_end_height:
                current_post = post
                current_end_height = end_height

    if current_post is not None:
        return current_end_height, current_post.content
    return 0, None
```

### 2. Revert call sites in `commands.py`

Both call sites of `get_latest_successful_credit_distribution` (cadence-guard fetch and resume-cursor fetch) revert to 2-tuple unpack: `last_end, _ = await ...`. The cadence-guard restructure introduced by T2 is preserved — only the unpack arity changes.

### 3. Add a failed-batch summary at run end

After the audit post is published, surface failed batches in stdout for manual operator review. This is new — not part of the pre-T1 behavior — because the failure information is currently buried in per-batch echo lines.

```python
# === Step 7: failure summary (after publish) ===
if flags.get("transfer") and act and not is_testnet:
    failed = [
        (i, t) for i, t in enumerate(transfer_metadata["targets"])
        if not t.get("success")
    ]
    if failed:
        click.echo(
            f"\n=== {len(failed)} batch(es) failed — "
            f"manual action required ==="
        )
        for i, t in failed:
            click.echo(
                f"  Batch {i+1}: {len(t['targets'])} recipients, "
                f"{t['total']} ALEPH, tx={t.get('tx') or 'none'}"
            )
        click.echo(
            "Manually retransfer the failed recipients (see the published "
            "distribution post for recipient lists), then AMEND the post "
            "to mark them success=true."
        )
```

The audit post itself is unchanged — every batch still records `success`, `status`, `tx`, `chain`, `sender`, `targets`, `total`, `contract_total`. Operators use the post for the recipient list when issuing manual transactions.

### 4. Delete `tests/test_resume_partial_failure.py`

The five tests pinned T1's now-reverted behavior (3-tuple return, partial-failure WARNING, refusal to advance). They're no longer applicable. The legacy lookup behavior is already covered by `tests/test_cadence_guard.py` and indirectly by `tests/test_global_cap.py`, so we don't need a replacement test for `get_latest_successful_credit_distribution`'s simple any-success scan.

### 5. Keep T7's M2 dual-sender comment

The docstring comment introduced in T1 about the two-instance deployment ("In the documented two-instance deployment, only distribution_recipient publishes status='distribution'; status_sender is queried defensively to handle single-instance / dev setups.") is retained in the reverted docstring. It's still accurate.

## File-level changes

| File | Change |
|---|---|
| `src/aleph_nodestatus/credit_distribution.py` | Revert `get_latest_successful_credit_distribution` body to pre-T1 logic with retained dual-sender docstring |
| `src/aleph_nodestatus/commands.py` | Revert two call-site unpacks from 3-tuple to 2-tuple; add Step 7 failed-batch summary block |
| `tests/test_resume_partial_failure.py` | Delete |

## Commit shape

One commit, no `Co-Authored-By` trailer:

```
revert(distribution): roll back T1 partial-failure halt; surface failed batches at run end

T1's any/all-success classification + WARNING was not paired with an
orchestrator halt, so on the next --act run after partial failure the
cursor would advance via an older all-success post and re-cover the
partial-failure window (double-pay risk). Roll back the classification:
treat any-success as cursor-advancing (legacy behavior), and add a
run-end summary listing failed batches so operators can retransfer
manually and AMEND the audit post.
```

## Acceptance criteria

- `pytest tests/` green; existing pass/fail status unchanged minus the deleted file (was 5 tests, now 0).
- A partial-failure run prints the new "manual action required" summary block at the end with batch indices, recipient counts, ALEPH totals, and tx hashes (or `none`).
- The next `--act` run after a partial-failure run uses `start_height = partial_failure_post.end_height + 1` and does NOT halt.
- The published audit post for a partial-failure run is unchanged in shape from pre-revert.

## Out of scope / follow-ups

- Pre-existing `NONCE` non-increment-on-broadcast-failure in `transfer_tokens` — separate ticket if operationally meaningful.
- Tooling to assist with the manual retransfer + AMEND workflow — could be a small `nodestatus-amend-distribution` CLI but not required.
- Porting any of this to the legacy staking distribution — explicitly out of scope (legacy already uses the rolled-back semantics).
