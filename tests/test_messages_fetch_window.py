import asyncio
import sys
import types

import pytest

try:
    from aleph.sdk.chains.ethereum import ETHAccount  # noqa: F401
    from aleph.sdk.client import AuthenticatedAlephHttpClient  # noqa: F401
except ImportError:
    for module_name in (
        "aleph",
        "aleph.sdk",
        "aleph.sdk.chains",
        "aleph.sdk.chains.ethereum",
        "aleph.sdk.client",
    ):
        sys.modules[module_name] = types.ModuleType(module_name)
    sys.modules["aleph.sdk.chains.ethereum"].ETHAccount = object
    sys.modules["aleph.sdk.client"].AuthenticatedAlephHttpClient = object

from aleph_nodestatus.messages import _fetch_message_window  # noqa: I001


BASE_PARAMS = {
    "msgType": "POST",
    "tags": "mainnet",
    "contentTypes": "corechan-operation",
    "sort_order": "1",
    "sort_by": "tx-time",
}


class FakeResponse:
    def __init__(self, status, payload=None):
        self.status = status
        self.payload = payload

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def json(self):
        if isinstance(self.payload, Exception):
            raise self.payload
        return self.payload


class FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def get(self, url, params=None, timeout=None):
        self.calls.append(dict(params or {}))
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


def _run(session, **kwargs):
    return asyncio.run(
        _fetch_message_window(
            session,
            "https://api.example",
            BASE_PARAMS,
            1,
            2,
            **kwargs,
        )
    )


def test_fetch_window_paginates_until_total():
    session = FakeSession(
        [
            FakeResponse(
                200,
                {
                    "messages": [{"item_hash": "a"}, {"item_hash": "b"}],
                    "pagination_total": 5,
                },
            ),
            FakeResponse(
                200,
                {
                    "messages": [{"item_hash": "c"}, {"item_hash": "d"}],
                    "pagination_total": 5,
                },
            ),
            FakeResponse(
                200,
                {
                    "messages": [{"item_hash": "e"}],
                    "pagination_total": 5,
                },
            ),
        ]
    )

    messages = _run(session, page_size=2, max_retries=1)

    assert [message["item_hash"] for message in messages] == [
        "a",
        "b",
        "c",
        "d",
        "e",
    ]
    assert [call["page"] for call in session.calls] == [1, 2, 3]
    assert all(call["pagination"] == 2 for call in session.calls)


def test_fetch_window_retries_server_errors():
    session = FakeSession(
        [
            FakeResponse(500),
            FakeResponse(502),
            FakeResponse(
                200,
                {
                    "messages": [{"item_hash": "a"}],
                    "pagination_total": 1,
                },
            ),
        ]
    )

    messages = _run(session, max_retries=3, retry_delay=0)

    assert [message["item_hash"] for message in messages] == ["a"]
    assert len(session.calls) == 3


def test_fetch_window_raises_after_retry_exhaustion():
    session = FakeSession([FakeResponse(503), FakeResponse(503)])

    with pytest.raises(RuntimeError, match="after 2 attempts"):
        _run(session, max_retries=2, retry_delay=0)

    assert len(session.calls) == 2


def test_fetch_window_does_not_accept_incomplete_response():
    session = FakeSession(
        [
            FakeResponse(
                200,
                {
                    "messages": [{"item_hash": "a"}],
                    "pagination_total": 2,
                },
            ),
            FakeResponse(200, {"messages": [], "pagination_total": 2}),
        ]
    )

    with pytest.raises(RuntimeError, match="Incomplete response"):
        _run(session, max_retries=1)


def test_fetch_window_fails_fast_on_non_retryable_status():
    session = FakeSession([FakeResponse(400)])

    with pytest.raises(RuntimeError, match="API error 400"):
        _run(session, max_retries=3, retry_delay=0)

    assert len(session.calls) == 1


def test_fetch_window_deduplicates_overlapping_pages():
    session = FakeSession(
        [
            FakeResponse(
                200,
                {
                    "messages": [{"item_hash": "a"}, {"item_hash": "b"}],
                    "pagination_total": 3,
                },
            ),
            FakeResponse(
                200,
                {
                    "messages": [{"item_hash": "b"}, {"item_hash": "c"}],
                    "pagination_total": 3,
                },
            ),
        ]
    )

    messages = _run(session, max_retries=1)

    assert [message["item_hash"] for message in messages] == ["a", "b", "c"]
    assert len(session.calls) == 2
