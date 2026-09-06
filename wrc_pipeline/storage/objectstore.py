"""Object storage adapter (MinIO locally, any S3-compatible service in production).

Why object storage and not the filesystem or the database
---------------------------------------------------------
Documents are blobs: opaque bytes that we never query on. Object storage scales
to billions of them, is reachable from every machine that runs a pipeline step,
and is the same API in every cloud. Storing them inside MongoDB would bloat the
database and slow down the metadata queries that actually matter.

We talk to MinIO through ``boto3``, i.e. the standard S3 API, so moving to real
AWS S3 is a change of endpoint in ``.env`` and nothing else.

Landing-zone rules
------------------
* Keys are **content-addressed**: they contain a short hash of the payload, so
  the same bytes always land on the same key and a repeated upload is a no-op.
* Nothing is ever deleted or overwritten. A changed document is written to a new
  key and the previous version stays reachable through the metadata history.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import boto3
from botocore.client import Config as BotoConfig
from botocore.exceptions import ClientError

from wrc_pipeline.logging_setup import get_logger

if TYPE_CHECKING:  # pragma: no cover - typing only
    from wrc_pipeline.config import Settings

logger = get_logger(__name__)


@dataclass(frozen=True)
class ObjectRef:
    """A parsed ``s3://bucket/key`` URI."""

    bucket: str
    key: str

    @property
    def uri(self) -> str:
        return f"s3://{self.bucket}/{self.key}"


def parse_uri(uri: str) -> ObjectRef:
    """Parse ``s3://bucket/key`` into its parts.

    Metadata stores the full URI (not just the key) so that a consumer can find
    the file without being told which bucket it lives in.
    """
    if not uri.startswith("s3://"):
        raise ValueError(f"Not an object-storage URI: {uri!r}")
    bucket, _, key = uri[len("s3://") :].partition("/")
    if not bucket or not key:
        raise ValueError(f"Malformed object-storage URI: {uri!r}")
    return ObjectRef(bucket=bucket, key=key)


class ObjectStore:
    """Thin wrapper over the S3 API with the few operations we need."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._client = boto3.client(
            "s3",
            endpoint_url=settings.s3_endpoint,
            aws_access_key_id=settings.s3_access_key,
            aws_secret_access_key=settings.s3_secret_key,
            region_name=settings.s3_region,
            # MinIO requires path-style addressing ("endpoint/bucket/key");
            # virtual-host style ("bucket.endpoint") only works on real S3.
            config=BotoConfig(s3={"addressing_style": "path"}, retries={"max_attempts": 5}),
        )
        # Set to False the first time a backend rejects the conditional-write
        # header, so we stop paying for a doomed attempt on every upload.
        self._supports_conditional_put = True

    # ------------------------------------------------------------------ #
    # Buckets                                                             #
    # ------------------------------------------------------------------ #
    def ensure_buckets(self) -> None:
        """Create the landing and transformed buckets if they do not exist.

        Compose already does this via the ``minio-init`` container; doing it in
        code as well means the pipeline works against a fresh S3 account too.
        """
        for bucket in (self._settings.s3_landing_bucket, self._settings.s3_transformed_bucket):
            self.ensure_bucket(bucket)

    def ensure_bucket(self, bucket: str) -> None:
        try:
            self._client.head_bucket(Bucket=bucket)
        except ClientError as exc:
            status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if status not in (403, 404):
                raise
            self._client.create_bucket(Bucket=bucket)
            logger.info("bucket_created", bucket=bucket)

    # ------------------------------------------------------------------ #
    # Objects                                                             #
    # ------------------------------------------------------------------ #
    def exists(self, bucket: str, key: str) -> bool:
        try:
            self._client.head_object(Bucket=bucket, Key=key)
            return True
        except ClientError as exc:
            if exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode") == 404:
                return False
            raise

    def put_if_absent(self, bucket: str, key: str, data: bytes, content_type: str) -> bool:
        """Upload ``data`` unless the key already exists.

        One round trip rather than two. ``If-None-Match: *`` tells the store to
        refuse the write if anything is already at that key, so the server
        enforces the landing zone's "never overwrite" rule; a HEAD-then-PUT pair
        would cost an extra request and could still race between the two calls.

        Returns:
            ``True`` if this call wrote the object, ``False`` if it was already
            there. Because keys are content-addressed, "already there" means
            "identical bytes", so declining the write loses nothing.
        """
        if self._supports_conditional_put:
            try:
                self._client.put_object(
                    Bucket=bucket,
                    Key=key,
                    Body=data,
                    ContentType=content_type,
                    IfNoneMatch="*",
                )
                return True
            except ClientError as exc:
                error = exc.response.get("Error", {}).get("Code")
                status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
                # 412: the precondition failed, i.e. the object is already there.
                if status == 412 or error in {"PreconditionFailed", "ConditionalRequestConflict"}:
                    return False
                if status not in (400, 501) and error not in {
                    "NotImplemented",
                    "InvalidRequest",
                    "InvalidArgument",
                }:
                    raise
                # An older S3 implementation that does not know the header. Fall
                # back permanently for this client and carry on.
                logger.info("conditional_put_unsupported", bucket=bucket)
                self._supports_conditional_put = False

        # Fallback: check first, then write. Two round trips, and technically
        # racy, but correct for any backend.
        if self.exists(bucket, key):
            return False
        self._client.put_object(Bucket=bucket, Key=key, Body=data, ContentType=content_type)
        return True

    def put(self, bucket: str, key: str, data: bytes, content_type: str) -> None:
        """Unconditional upload, used only for the transformed (derived) bucket.

        Overwriting is safe there: the output can always be rebuilt from the
        untouched landing zone.
        """
        self._client.put_object(Bucket=bucket, Key=key, Body=data, ContentType=content_type)

    def get(self, uri: str) -> bytes:
        """Download an object addressed by its ``s3://`` URI."""
        ref = parse_uri(uri)
        response = self._client.get_object(Bucket=ref.bucket, Key=ref.key)
        return response["Body"].read()

    def count_objects(self, bucket: str, prefix: str = "") -> int:
        """Count objects under a prefix (used by the verification command)."""
        paginator = self._client.get_paginator("list_objects_v2")
        total = 0
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            total += page.get("KeyCount", 0)
        return total

    # ------------------------------------------------------------------ #
    # Key layout                                                          #
    # ------------------------------------------------------------------ #
    @staticmethod
    def landing_key(body: str, partition_start: str, short_digest: str, filename: str) -> str:
        """``<body>/<partition start>/<hash>/<original filename>``.

        The body and partition prefixes make the bucket browsable and let a
        consumer list one unit cheaply; the hash segment makes the key unique per
        content version; the original filename is preserved because the landing
        zone stores things exactly as the source served them (renaming to
        ``identifier.ext`` is the transformation step's job).
        """
        return f"{body}/{partition_start}/{short_digest}/{filename}"

    @staticmethod
    def transformed_key(body: str, identifier: str, extension: str) -> str:
        """``<body>/<identifier>.<ext>`` as required by the assignment."""
        return f"{body}/{identifier}.{extension}"

    @staticmethod
    def uri(bucket: str, key: str) -> str:
        return f"s3://{bucket}/{key}"

    def ping(self) -> None:
        """Raise if the object store is unreachable (used by ``wrc verify``)."""
        self._client.list_buckets()
