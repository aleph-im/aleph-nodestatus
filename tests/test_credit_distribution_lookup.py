from types import SimpleNamespace

import pytest

from aleph_nodestatus import credit_distribution as cd
from aleph_nodestatus.settings import settings


class _FakeClient:
    def __init__(self, captured_addresses):
        self._captured_addresses = captured_addresses

    async def get_posts_cursor(self, post_filter=None, cursor=""):
        self._captured_addresses.extend(post_filter.addresses)
        return SimpleNamespace(posts=[], next_cursor="")


class _FakeClientContext:
    def __init__(self, captured_addresses):
        self._client = _FakeClient(captured_addresses)

    async def __aenter__(self):
        return self._client

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _FakePostFilter:
    def __init__(self, **kwargs):
        self.types = kwargs["types"]
        self.addresses = kwargs["addresses"]


@pytest.mark.asyncio
async def test_default_lookup_includes_old_and_new_distribution_senders(monkeypatch):
    captured_addresses = []

    monkeypatch.setattr(cd, "PostFilter", _FakePostFilter)
    monkeypatch.setattr(
        cd, "_aleph_client",
        lambda _api_server: _FakeClientContext(captured_addresses),
    )

    result = await cd.get_latest_successful_credit_distribution()

    assert result == (0, None)
    addresses = {a.lower() for a in captured_addresses}
    assert settings.status_sender.lower() in addresses
    assert settings.distribution_recipient.lower() in addresses
    for sender in settings.credit_distribution_lookup_senders:
        assert sender.lower() in addresses
