"""MongoDB adapter: metadata, failures and run summaries.

Why a document database
-----------------------
Scraped metadata is semi-structured and differs per source: a WRC adjudication
has "Complainant/Respondent", a Labour Court determination has
"Chairman/Employer member". Adding fifty such sources to a rigid relational
schema means constant migrations; a document store absorbs the variation, keeps
nested structures such as the ``versions`` history natural, and shards
horizontally when the volume grows.

Idempotency lives here
----------------------
* a **unique index on ``identifier``** makes duplicates impossible at the
  database level - a stronger guarantee than "the code checks first", because it
  also holds when two partitions run in parallel;
* upserts use ``$setOnInsert`` for creation-time fields and ``$set`` for the
  rest, so re-running a range refreshes bookkeeping without inventing history.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from typing import TYPE_CHECKING, Any

from pymongo import ASCENDING, DESCENDING, MongoClient
from pymongo.collection import Collection

from wrc_pipeline.logging_setup import get_logger

if TYPE_CHECKING:  # pragma: no cover - typing only
    from wrc_pipeline.config import Settings

logger = get_logger(__name__)


def to_datetime(value: date | datetime) -> datetime:
    """Convert a ``date`` to a timezone-aware ``datetime``.

    BSON has no date-only type, so every date is stored as midnight UTC. Doing
    the conversion in one place keeps range queries consistent.
    """
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    return datetime(value.year, value.month, value.day, tzinfo=UTC)


def utcnow() -> datetime:
    return datetime.now(tz=UTC)


class MongoRepository:
    """All reads and writes against MongoDB."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        # pymongo connects lazily, so constructing this is cheap and safe inside
        # a Scrapy spider's __init__.
        self._client: MongoClient = MongoClient(settings.mongo_uri, tz_aware=True)
        self._db = self._client[settings.mongo_db]

    # ------------------------------------------------------------------ #
    # Collections                                                         #
    # ------------------------------------------------------------------ #
    @property
    def landing(self) -> Collection:
        return self._db[self._settings.mongo_landing_collection]

    @property
    def transformed(self) -> Collection:
        return self._db[self._settings.mongo_transformed_collection]

    @property
    def failures(self) -> Collection:
        return self._db[self._settings.mongo_failures_collection]

    @property
    def runs(self) -> Collection:
        return self._db[self._settings.mongo_runs_collection]

    # ------------------------------------------------------------------ #
    # Setup                                                               #
    # ------------------------------------------------------------------ #
    def ensure_indexes(self) -> None:
        """Create every index the pipeline relies on. Safe to call repeatedly."""
        # Unique: the database itself refuses a duplicate decision.
        self.landing.create_index([("identifier", ASCENDING)], unique=True, name="uniq_identifier")
        # The transformation selects by date range.
        self.landing.create_index([("published_date", DESCENDING)], name="published_date")
        # Reporting and re-driving a single unit.
        self.landing.create_index(
            [("body", ASCENDING), ("partition_date", ASCENDING)], name="body_partition"
        )
        self.landing.create_index([("file_hash", ASCENDING)], name="file_hash")

        self.transformed.create_index(
            [("identifier", ASCENDING)], unique=True, name="uniq_identifier"
        )
        self.transformed.create_index([("published_date", DESCENDING)], name="published_date")

        self.failures.create_index([("run_id", ASCENDING)], name="run_id")
        self.failures.create_index([("identifier", ASCENDING)], name="identifier")
        self.runs.create_index([("run_id", ASCENDING)], name="run_id")

    def ping(self) -> None:
        """Raise if the database is unreachable (used by ``wrc verify``)."""
        self._client.admin.command("ping")

    def close(self) -> None:
        self._client.close()

    # ------------------------------------------------------------------ #
    # Landing metadata                                                    #
    # ------------------------------------------------------------------ #
    def get_landing_many(self, identifiers: list[str]) -> dict[str, dict[str, Any]]:
        """Fetch the stored records for a batch of identifiers, keyed by identifier.

        The spider calls this once per listing page (ten identifiers) instead of
        once per document, which is the difference between one round trip and
        ten. The records it returns are what the item pipeline compares hashes
        against, so the batch also removes the per-document lookup entirely.
        """
        if not identifiers:
            return {}
        cursor = self.landing.find({"identifier": {"$in": identifiers}})
        return {doc["identifier"]: doc for doc in cursor}

    def upsert_landing(
        self,
        record: dict[str, Any],
        *,
        previous: dict[str, Any] | None,
        changed: bool,
    ) -> None:
        """Insert a new record, or update an existing one.

        Args:
            record: The metadata document (already serialised for BSON).
            previous: The stored version, if any.
            changed: ``True`` when the file hash differs from ``previous``.

        The landing zone is append-only, so a change never overwrites history:
        the superseded version is pushed onto ``versions`` and the object it
        points to stays in the bucket.
        """
        identifier = record["identifier"]
        now = utcnow()
        set_fields = {key: value for key, value in record.items() if key != "identifier"}
        set_fields["last_seen_at"] = now
        set_fields["last_run_id"] = record.get("run_id")

        update: dict[str, Any] = {
            "$set": set_fields,
            "$setOnInsert": {
                "identifier": identifier,
                "first_seen_at": now,
                "first_run_id": record.get("run_id"),
            },
        }
        if changed:
            set_fields["last_changed_at"] = now
            if previous is not None:
                update["$push"] = {
                    "versions": {
                        "file_hash": previous.get("file_hash"),
                        "file_path": previous.get("file_path"),
                        "file_size": previous.get("file_size"),
                        "scraped_at": previous.get("last_seen_at"),
                        "run_id": previous.get("last_run_id"),
                    }
                }
        self.landing.update_one({"identifier": identifier}, update, upsert=True)

    def touch_landing(self, identifier: str, run_id: str, partition: dict[str, Any]) -> None:
        """Mark an unchanged document as seen again by this run.

        Only bookkeeping fields are written; the payload metadata is untouched
        because nothing about the document changed. Partition fields are filled
        in only if they are missing, so re-scraping a record under a different
        window never rewrites the window that first produced it.
        """
        self.landing.update_one(
            {"identifier": identifier},
            {
                "$set": {"last_seen_at": utcnow(), "last_run_id": run_id},
                "$setOnInsert": partition,
            },
        )

    def _date_range_query(self, start: date, end: date, bodies: list[str] | None) -> dict[str, Any]:
        """Filter for landing records inside ``[start, end]``.

        Which date is used comes from configuration: the publication date by
        default, or the partition date when you want the transformation to line
        up exactly with the window that scraped the records.
        """
        query: dict[str, Any] = {
            self._settings.date_field: {"$gte": to_datetime(start), "$lte": to_datetime(end)},
            "scrape_status": "ok",
        }
        if bodies:
            query["body"] = {"$in": bodies}
        return query

    def iter_landing_by_date(
        self,
        start: date,
        end: date,
        bodies: list[str] | None = None,
        batch_size: int = 500,
    ) -> Any:
        """Stream landing records inside ``[start, end]``.

        A cursor with a bounded batch size, never ``list(...)``: at a thousand
        times this volume the result set would not fit in memory.
        """
        return self.landing.find(self._date_range_query(start, end, bodies)).batch_size(batch_size)

    def count_landing_by_date(self, start: date, end: date, bodies: list[str] | None = None) -> int:
        return self.landing.count_documents(self._date_range_query(start, end, bodies))

    # ------------------------------------------------------------------ #
    # Transformed metadata                                                #
    # ------------------------------------------------------------------ #
    def get_transformed(self, identifier: str) -> dict[str, Any] | None:
        return self.transformed.find_one({"identifier": identifier})

    def upsert_transformed(self, record: dict[str, Any]) -> None:
        identifier = record["identifier"]
        set_fields = {key: value for key, value in record.items() if key != "identifier"}
        self.transformed.update_one(
            {"identifier": identifier},
            {
                "$set": set_fields,
                "$setOnInsert": {"identifier": identifier, "first_transformed_at": utcnow()},
            },
            upsert=True,
        )

    # ------------------------------------------------------------------ #
    # Failures and runs                                                   #
    # ------------------------------------------------------------------ #
    def record_failure(
        self,
        *,
        run_id: str,
        stage: str,
        url: str,
        error_type: str,
        error_message: str,
        body: str | None = None,
        partition_label: str | None = None,
        identifier: str | None = None,
        http_status: int | None = None,
        attempts: int | None = None,
    ) -> None:
        """Persist one failed record with the reason it failed.

        The assignment asks that every document we could not scrape is logged
        with a reason. Logs answer that for a human; this collection answers it
        for a query ("show me everything run X missed") and for a re-drive job.
        """
        self.failures.insert_one(
            {
                "run_id": run_id,
                "stage": stage,
                "url": url,
                "identifier": identifier,
                "body": body,
                "partition_label": partition_label,
                "http_status": http_status,
                "error_type": error_type,
                "error_message": error_message[:2000],
                "attempts": attempts,
                "occurred_at": utcnow(),
            }
        )

    def count_failures(self, run_id: str) -> int:
        return self.failures.count_documents({"run_id": run_id})

    def record_run(self, document: dict[str, Any]) -> None:
        """Store a run summary (one per scrape unit, transform, or Dagster job)."""
        self.runs.insert_one({**document, "recorded_at": utcnow()})
