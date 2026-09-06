"""Upload behaviour of the object-storage adapter.

``put_if_absent`` is the landing zone's write path, so it has to be right on
every backend: it must never overwrite, must report honestly whether it wrote,
and must not fail against an older S3 implementation that has never heard of
conditional writes. The client is stubbed so these run without Docker.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from botocore.exceptions import ClientError

from wrc_pipeline.config import get_settings
from wrc_pipeline.storage.objectstore import ObjectStore, parse_uri


def _client_error(status: int, code: str) -> ClientError:
    return ClientError(
        {"Error": {"Code": code}, "ResponseMetadata": {"HTTPStatusCode": status}},
        "PutObject",
    )


def _store() -> ObjectStore:
    store = ObjectStore(get_settings())
    store._client = MagicMock()
    return store


def test_new_object_is_written_in_a_single_request():
    """The whole point of the conditional write: one round trip, no HEAD."""
    store = _store()
    assert store.put_if_absent("landing", "k", b"body", "text/html") is True
    assert store._client.put_object.call_count == 1
    assert store._client.head_object.call_count == 0
    assert store._client.put_object.call_args.kwargs["IfNoneMatch"] == "*"


def test_existing_object_is_not_overwritten():
    """A 412 means something is already at that key, so we decline the write."""
    store = _store()
    store._client.put_object.side_effect = _client_error(412, "PreconditionFailed")
    assert store.put_if_absent("landing", "k", b"body", "text/html") is False


def test_backend_without_conditional_writes_falls_back():
    """An older S3 must not break uploads; we check-then-write instead."""
    store = _store()
    attempts: list[dict] = []

    def put_object(**kwargs):
        attempts.append(kwargs)
        if "IfNoneMatch" in kwargs:
            raise _client_error(501, "NotImplemented")
        return {}

    store._client.put_object.side_effect = put_object
    store._client.head_object.side_effect = _client_error(404, "404")

    assert store.put_if_absent("landing", "k", b"body", "text/html") is True
    assert len(attempts) == 2  # the rejected conditional attempt, then the plain one
    assert store._supports_conditional_put is False

    # The capability is remembered, so the next upload does not retry the header.
    assert store.put_if_absent("landing", "k2", b"body", "text/html") is True
    assert len(attempts) == 3


def test_real_errors_are_not_swallowed():
    """A permissions problem must surface, not look like 'already there'."""
    store = _store()
    store._client.put_object.side_effect = _client_error(403, "AccessDenied")
    with pytest.raises(ClientError):
        store.put_if_absent("landing", "k", b"body", "text/html")


def test_transformed_writes_are_unconditional():
    """The derived bucket is rebuildable, so overwriting it is intentional."""
    store = _store()
    store.put("transformed", "labour_court/UDD242.html", b"body", "text/html")
    assert "IfNoneMatch" not in store._client.put_object.call_args.kwargs


def test_key_layouts_match_the_documented_scheme():
    landing = ObjectStore.landing_key("labour_court", "2024-01-01", "abc123", "udd242.html")
    assert landing == "labour_court/2024-01-01/abc123/udd242.html"
    assert ObjectStore.transformed_key("labour_court", "UDD242", "html") == (
        "labour_court/UDD242.html"
    )


def test_uri_round_trip():
    uri = ObjectStore.uri("landing", "labour_court/2024-01-01/abc/udd242.html")
    ref = parse_uri(uri)
    assert (ref.bucket, ref.key) == ("landing", "labour_court/2024-01-01/abc/udd242.html")
    with pytest.raises(ValueError):
        parse_uri("https://example.com/not-an-object")
