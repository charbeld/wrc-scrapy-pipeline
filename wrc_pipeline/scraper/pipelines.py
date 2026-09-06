"""Item pipeline: store the document, then store its metadata.

This is where idempotency is decided. For every scraped document we compare the
freshly computed hash with what MongoDB already holds:

===============  ==========================================================
Situation         Action
===============  ==========================================================
not seen before   upload the payload, insert the metadata record
hash differs      upload under a new content-addressed key, update the record
                  and push the previous version onto ``versions``
hash identical    upload nothing, touch ``last_seen_at`` only
===============  ==========================================================

Nothing in the landing zone is ever deleted or overwritten, which is what makes
the raw data safe to re-process forever.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from scrapy.exceptions import DropItem

from wrc_pipeline.hashing import short_hash
from wrc_pipeline.logging_setup import get_logger
from wrc_pipeline.storage.objectstore import ObjectStore

if TYPE_CHECKING:  # pragma: no cover - typing only
    from wrc_pipeline.scraper.items import DocumentItem

logger = get_logger(__name__)

#: Content types used when uploading, keyed by detected document type.
_CONTENT_TYPES = {
    "html": "text/html; charset=utf-8",
    "pdf": "application/pdf",
    "doc": "application/msword",
    "docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
}


class LandingPipeline:
    """Persist one item into the landing zone (object store + MongoDB)."""

    def process_item(self, item: DocumentItem, spider: Any) -> DocumentItem:
        counters = spider.counters_for(item.unit_key)

        # Guard against the same decision appearing twice inside one run (it can
        # be listed under two bodies, or on both sides of a partition boundary).
        if item.identifier in spider.seen_identifiers:
            counters.duplicates_in_run += 1
            logger.info(
                "document_duplicate_in_run",
                identifier=item.identifier,
                url=item.doc_url,
            )
            raise DropItem(f"duplicate identifier in this run: {item.identifier}")
        spider.seen_identifiers.add(item.identifier)

        try:
            return self._store(item, spider, counters)
        except DropItem:
            raise
        except Exception as exc:
            counters.failed += 1
            spider.repo.record_failure(
                run_id=item.run_id,
                stage="storage",
                url=item.doc_url,
                identifier=item.identifier,
                body=item.body,
                partition_label=item.partition_label,
                error_type=type(exc).__name__,
                error_message=str(exc),
            )
            logger.error(
                "document_failed",
                stage="storage",
                identifier=item.identifier,
                url=item.doc_url,
                error_type=type(exc).__name__,
                error_message=str(exc),
            )
            raise DropItem(f"storage failure for {item.identifier}") from exc

    def _store(self, item: DocumentItem, spider: Any, counters: Any) -> DocumentItem:
        # Fetched in one batched query when the listing page was parsed, so
        # there is no per-document round trip to MongoDB here.
        previous = item.previous_record
        is_new = previous is None
        changed = not is_new and previous.get("file_hash") != item.file_hash

        if not is_new and not changed:
            # Identical bytes: no upload, no new version, no metadata rewrite.
            spider.repo.touch_landing(item.identifier, item.run_id, item.partition_fields())
            counters.unchanged += 1
            logger.info(
                "document_unchanged",
                identifier=item.identifier,
                file_hash=item.file_hash,
                file_path=previous.get("file_path"),
            )
            item.payload = None
            item.previous_record = None
            return item

        settings = spider.app_settings
        bucket = settings.s3_landing_bucket
        key = ObjectStore.landing_key(
            body=item.body,
            partition_start=item.partition_start.isoformat(),
            short_digest=short_hash(item.file_hash or ""),
            filename=item.source_filename,
        )
        content_type = _CONTENT_TYPES.get(item.doc_type or "", "application/octet-stream")
        uploaded = spider.store.put_if_absent(bucket, key, item.payload or b"", content_type)

        record = item.metadata()
        record["file_path"] = ObjectStore.uri(bucket, key)
        spider.repo.upsert_landing(record, previous=previous, changed=changed)

        if is_new:
            counters.new += 1
        else:
            counters.changed += 1

        logger.info(
            "document_stored",
            identifier=item.identifier,
            status="new" if is_new else "changed",
            uploaded=uploaded,
            file_path=record["file_path"],
            file_hash=item.file_hash,
            file_size=item.file_size,
            doc_type=item.doc_type,
        )
        # Free the payload and the copy of the previous record: with hundreds of
        # documents in flight, keeping them referenced would grow memory for no
        # reason.
        item.payload = None
        item.previous_record = None
        return item
