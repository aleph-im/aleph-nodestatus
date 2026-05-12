import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from aleph_nodestatus.credit_distribution import get_latest_successful_credit_distribution


def _make_post(end_height, target_successes):
    """Build a fake Aleph post whose content.targets has the given success flags."""
    return SimpleNamespace(
        content={
            "status": "distribution",
            "end_height": end_height,
            "targets": [{"success": s} for s in target_successes],
        }
    )


@pytest.fixture
def patch_posts_client(monkeypatch):
    """Replace AlephHttpClient with a context manager whose get_posts is configurable."""

    def _install(posts):
        client = MagicMock()
        client.__aenter__ = AsyncMock(return_value=client)
        client.__aexit__ = AsyncMock(return_value=False)
        client.get_posts = AsyncMock(return_value=SimpleNamespace(posts=posts))
        monkeypatch.setattr(
            "aleph_nodestatus.credit_distribution.AlephHttpClient",
            lambda **_: client,
        )
        return client

    return _install


@pytest.mark.asyncio
async def test_all_targets_successful_returns_end_height(patch_posts_client):
    patch_posts_client([_make_post(end_height=1000, target_successes=[True, True, True])])
    end_height, content, all_success = await get_latest_successful_credit_distribution(
        sender="0xabc"
    )
    assert end_height == 1000
    assert all_success is True
    assert content is not None


@pytest.mark.asyncio
async def test_partial_failure_does_not_advance_cursor(patch_posts_client, caplog):
    patch_posts_client([_make_post(end_height=1000, target_successes=[True, False, False])])
    with caplog.at_level(logging.WARNING):
        end_height, content, all_success = await get_latest_successful_credit_distribution(
            sender="0xabc"
        )
    assert end_height == 0
    assert content is None
    assert all_success is False
    assert any("partial" in r.message.lower() for r in caplog.records)


@pytest.mark.asyncio
async def test_all_targets_failed_silently_skipped(patch_posts_client, caplog):
    patch_posts_client([_make_post(end_height=1000, target_successes=[False, False])])
    with caplog.at_level(logging.WARNING):
        end_height, content, all_success = await get_latest_successful_credit_distribution(
            sender="0xabc"
        )
    assert end_height == 0
    assert content is None
    assert all_success is False
    # All-failure is NOT a partial — no warning expected for this case.
    assert not any("partial" in r.message.lower() for r in caplog.records)


@pytest.mark.asyncio
async def test_empty_targets_silently_skipped(patch_posts_client, caplog):
    """A distribution post with no targets is malformed — skip silently, no warning."""
    patch_posts_client([_make_post(end_height=1000, target_successes=[])])
    with caplog.at_level(logging.WARNING):
        end_height, content, all_success = await get_latest_successful_credit_distribution(
            sender="0xabc"
        )
    assert end_height == 0
    assert content is None
    assert all_success is False
    assert not any("partial" in r.message.lower() for r in caplog.records)


@pytest.mark.asyncio
async def test_picks_most_recent_all_success_post(patch_posts_client):
    patch_posts_client([
        _make_post(end_height=500, target_successes=[True]),
        _make_post(end_height=1500, target_successes=[True, True]),
        _make_post(end_height=1000, target_successes=[True]),
    ])
    end_height, _, all_success = await get_latest_successful_credit_distribution(
        sender="0xabc"
    )
    assert end_height == 1500
    assert all_success is True


@pytest.mark.asyncio
async def test_partial_failure_skipped_even_if_most_recent(patch_posts_client, caplog):
    """A more recent partial-failure post must NOT shadow an older all-success post."""
    patch_posts_client([
        _make_post(end_height=500, target_successes=[True]),
        _make_post(end_height=1500, target_successes=[True, False]),
    ])
    with caplog.at_level(logging.WARNING):
        end_height, _, all_success = await get_latest_successful_credit_distribution(
            sender="0xabc"
        )
    # Partial-failure at 1500 warned about, falls back to all-success at 500.
    assert end_height == 500
    assert all_success is True
    assert any("partial" in r.message.lower() for r in caplog.records)
