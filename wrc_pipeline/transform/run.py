"""Run the transformation over a date range.

Reads landing metadata from MongoDB, pulls each file from the landing bucket,
cleans HTML documents (PDFs and Word files pass through untouched, as the
assignment requires), renames everything to ``identifier.ext``, writes it to the
transformed bucket and records the result in a second MongoDB collection.

Idempotency
-----------
A document is reprocessed only if the landing hash changed or the cleaning logic
changed (:data:`~wrc_pipeline.transform.html_clean.TRANSFORM_VERSION`). A second
run over the same range therefore does no work, which is what makes the step
safe to retry inside an orchestrator.

The landing zone is only ever read here; the derived bucket is the only thing
this stage writes.
"""

from __future__ import annotations

import time
from dataclasses import asdict, dataclass, field
from datetime import date
from typing import Any

from wrc_pipeline.config import Settings, get_settings
from wrc_pipeline.hashing import sha256_bytes
from wrc_pipeline.identifiers import file_extension_for
from wrc_pipeline.logging_setup import bind_run_context, get_logger
from wrc_pipeline.run_ids import new_run_id
from wrc_pipeline.storage.mongo import MongoRepository, utcnow
from wrc_pipeline.storage.objectstore import ObjectStore
from wrc_pipeline.transform.html_clean import TRANSFORM_VERSION, clean_html

logger = get_logger(__name__)

#: Content types for the transformed uploads.
_CONTENT_TYPES = {
    "html": "text/html; charset=utf-8",
    "pdf": "application/pdf",
    "doc": "application/msword",
    "docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
}


@dataclass
class TransformSummary:
    """Counters for one transformation run.

    Invariant: ``candidates == transformed + skipped_unchanged + failed``.
    """

    run_id: str
    start_date: str
    end_date: str
    bodies: list[str] = field(default_factory=list)
    candidates: int = 0
    transformed: int = 0
    skipped_unchanged: int = 0
    failed: int = 0
    elapsed_seconds: float = 0.0

    @property
    def reconciled(self) -> bool:
        return self.candidates == self.transformed + self.skipped_unchanged + self.failed

    def as_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["reconciled"] = self.reconciled
        data["kind"] = "transform"
        return data


def transform_range(
    start: date,
    end: date,
    bodies: list[str] | None = None,
    run_id: str | None = None,
    force: bool = False,
    settings: Settings | None = None,
) -> TransformSummary:
    """Transform every landing document published in ``[start, end]``.

    Args:
        start: First publication date to include (inclusive).
        end: Last publication date to include (inclusive).
        bodies: Restrict to these body slugs; ``None`` means all.
        run_id: Shared id when called from an orchestrated job.
        force: Reprocess even when nothing changed.
        settings: Injected in tests; defaults to the environment.

    Returns:
        The run's counters.
    """
    settings = settings or get_settings()
    run_id = run_id or new_run_id()
    bind_run_context(run_id=run_id)
    started = time.time()

    repo = MongoRepository(settings)
    store = ObjectStore(settings)
    repo.ensure_indexes()
    store.ensure_bucket(settings.s3_transformed_bucket)

    summary = TransformSummary(
        run_id=run_id,
        start_date=start.isoformat(),
        end_date=end.isoformat(),
        bodies=bodies or settings.body_slugs,
    )
    logger.info(
        "transform_started",
        start_date=summary.start_date,
        end_date=summary.end_date,
        bodies=summary.bodies,
        force=force,
        transform_version=TRANSFORM_VERSION,
    )

    try:
        # A cursor, never a list: the result set must not have to fit in memory.
        for record in repo.iter_landing_by_date(start, end, bodies):
            summary.candidates += 1
            try:
                _transform_one(record, repo, store, settings, run_id, force, summary)
            except Exception as exc:
                summary.failed += 1
                repo.record_failure(
                    run_id=run_id,
                    stage="transform",
                    url=record.get("file_path", ""),
                    identifier=record.get("identifier"),
                    body=record.get("body"),
                    partition_label=record.get("partition_label"),
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                )
                logger.error(
                    "document_transform_failed",
                    identifier=record.get("identifier"),
                    file_path=record.get("file_path"),
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                )
    finally:
        summary.elapsed_seconds = round(time.time() - started, 2)

    if not summary.reconciled:
        logger.error("reconciliation_failed", **summary.as_dict())
    logger.info("transform_summary", **summary.as_dict())

    try:
        repo.record_run(summary.as_dict())
    except Exception as exc:
        logger.error("run_summary_not_persisted", error=str(exc))
    repo.close()
    return summary


def _transform_one(
    record: dict[str, Any],
    repo: MongoRepository,
    store: ObjectStore,
    settings: Settings,
    run_id: str,
    force: bool,
    summary: TransformSummary,
) -> None:
    """Transform a single landing record."""
    identifier = record["identifier"]
    landing_hash = record.get("file_hash")

    existing = repo.get_transformed(identifier)
    if (
        not force
        and existing is not None
        and existing.get("landing_file_hash") == landing_hash
        and existing.get("transform_version") == TRANSFORM_VERSION
    ):
        summary.skipped_unchanged += 1
        logger.info(
            "document_transform_skipped",
            identifier=identifier,
            reason="unchanged",
            file_path=existing.get("file_path"),
        )
        return

    raw = store.get(record["file_path"])
    doc_type = record.get("doc_type") or "other"

    if doc_type == "html":
        payload, info = clean_html(raw, identifier, base_url=record.get("doc_url"))
        if info["content_root"] != "div.col-sm-9":
            logger.warning(
                "content_root_fallback",
                identifier=identifier,
                content_root=info["content_root"],
            )
        extracted = info["extracted"]
        if identifier.replace("-", "") not in _squash(payload.decode("utf-8", "ignore")):
            # The reference should always appear in the decision text; if it does
            # not, the extraction probably grabbed the wrong part of the page.
            logger.warning("identifier_not_in_content", identifier=identifier)
    else:
        # PDFs and Word documents are stored exactly as downloaded.
        payload, extracted = raw, None

    new_hash = sha256_bytes(payload)
    extension = file_extension_for(doc_type)
    bucket = settings.s3_transformed_bucket
    key = ObjectStore.transformed_key(record["body"], identifier, extension)
    store.put(bucket, key, payload, _CONTENT_TYPES.get(doc_type, "application/octet-stream"))

    transformed_record = {
        "identifier": identifier,
        "identifier_raw": record.get("identifier_raw"),
        "title": record.get("title"),
        "description": record.get("description"),
        "doc_title": record.get("doc_title"),
        "doc_heading": record.get("doc_heading"),
        "published_date": record.get("published_date"),
        "published_date_raw": record.get("published_date_raw"),
        "body": record.get("body"),
        "body_id": record.get("body_id"),
        "body_name": record.get("body_name"),
        "doc_url": record.get("doc_url"),
        "doc_type": doc_type,
        "partition_date": record.get("partition_date"),
        "partition_label": record.get("partition_label"),
        "source": record.get("source"),
        "landing_file_path": record.get("file_path"),
        "landing_file_hash": landing_hash,
        "file_path": ObjectStore.uri(bucket, key),
        "file_hash": new_hash,
        "file_size": len(payload),
        "extracted": extracted,
        "transform_version": TRANSFORM_VERSION,
        "transform_run_id": run_id,
        "transformed_at": utcnow(),
    }
    repo.upsert_transformed(transformed_record)
    summary.transformed += 1
    logger.info(
        "document_transformed",
        identifier=identifier,
        doc_type=doc_type,
        file_path=transformed_record["file_path"],
        file_hash=new_hash,
        file_size=len(payload),
        landing_file_hash=landing_hash,
    )


def _squash(text: str) -> str:
    """Upper-case text with spaces and hyphens removed, for tolerant matching."""
    return "".join(text.split()).replace("-", "").upper()
