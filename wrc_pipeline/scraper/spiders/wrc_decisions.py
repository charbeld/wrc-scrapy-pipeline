"""The spider: walks the search listing per (body, partition) and fetches documents.

How the site is queried
-----------------------
The advanced search is an ASP.NET WebForms page with an 800 KB ``__VIEWSTATE``,
but submitting the form only redirects to a plain GET URL. We call that URL
directly::

    /en/search/?decisions=1&from=01/01/2024&to=31/01/2024&body=3&pageNumber=1

That is the "fastest way to scrape without getting blocked" the assignment asks
for: no session, no cookies, no form round-trip, so every request is independent
and can run in parallel.

Page 1 reports "Shows 1 to 10 of N results". Because the page size is fixed at
ten, we compute the number of pages from N and request pages 2..N immediately
rather than following "next" fifty times in sequence.

Unit of work
------------
One unit is one (body, partition) pair. Counters, logs and the reconciliation
check are all per unit, so a failure is attributable and cheap to re-run.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import scrapy
from scrapy import signals
from scrapy.spidermiddlewares.httperror import HttpError

from wrc_pipeline.bodies import Body, resolve_bodies
from wrc_pipeline.config import get_settings
from wrc_pipeline.hashing import normalise_bytes, sha256_bytes, sniff_doc_type
from wrc_pipeline.identifiers import identifier_from_url, normalise_identifier
from wrc_pipeline.logging_setup import bind_run_context, configure_logging, get_logger
from wrc_pipeline.partitions import Partition, build_partitions
from wrc_pipeline.run_ids import new_run_id
from wrc_pipeline.scraper.items import DocumentItem
from wrc_pipeline.scraper.parsing import (
    find_attachment,
    parse_document_page,
    parse_listing_rows,
    parse_result_count,
    parse_site_date,
    total_pages,
)
from wrc_pipeline.storage.mongo import MongoRepository
from wrc_pipeline.storage.objectstore import ObjectStore

logger = get_logger(__name__)


@dataclass
class UnitCounters:
    """Reconciliation counters for one (body, partition) unit.

    The invariant that must always hold::

        records_listed == new + changed + unchanged + skipped_known
                          + failed + duplicates_in_run

    It is what proves the assignment's requirement: scrape all N records, or
    N - X with every one of the X recorded with its reason.
    """

    body: str
    partition_label: str
    partition_date: str
    records_found: int | None = None
    listing_pages_expected: int = 0
    listing_pages_fetched: int = 0
    records_listed: int = 0
    documents_requested: int = 0
    new: int = 0
    changed: int = 0
    unchanged: int = 0
    skipped_known: int = 0
    failed: int = 0
    duplicates_in_run: int = 0

    @property
    def accounted(self) -> int:
        return (
            self.new
            + self.changed
            + self.unchanged
            + self.skipped_known
            + self.failed
            + self.duplicates_in_run
        )

    @property
    def reconciled(self) -> bool:
        return self.records_listed == self.accounted

    def as_dict(self) -> dict[str, Any]:
        return {
            "body": self.body,
            "partition_label": self.partition_label,
            "partition_date": self.partition_date,
            "records_found": self.records_found,
            "listing_pages_expected": self.listing_pages_expected,
            "listing_pages_fetched": self.listing_pages_fetched,
            "records_listed": self.records_listed,
            "documents_requested": self.documents_requested,
            "new": self.new,
            "changed": self.changed,
            "unchanged": self.unchanged,
            "skipped_known": self.skipped_known,
            "failed": self.failed,
            "duplicates_in_run": self.duplicates_in_run,
            "accounted": self.accounted,
            "reconciled": self.reconciled,
        }


@dataclass
class Unit:
    """A body plus a time window: the spider's unit of work."""

    body: Body
    partition: Partition
    counters: UnitCounters = field(init=False)

    def __post_init__(self) -> None:
        self.counters = UnitCounters(
            body=self.body.slug,
            partition_label=self.partition.label,
            partition_date=self.partition.partition_date.isoformat(),
        )

    @property
    def key(self) -> str:
        return f"{self.body.slug}|{self.partition.label}"


class WrcDecisionsSpider(scrapy.Spider):
    """Scrapes decisions and determinations for a date range and set of bodies."""

    name = "wrc_decisions"

    @classmethod
    def from_crawler(cls, crawler: Any, *args: Any, **kwargs: Any) -> WrcDecisionsSpider:
        """Subscribe to the signal Scrapy raises when it discards a request.

        Scrapy's duplicate filter drops a repeated URL before it is ever
        fetched, without calling a callback or an errback. That is the right
        behaviour, but it happens silently: the document would be counted as
        requested and then never accounted for, and the reconciliation check
        would fail with no explanation. Listening for the signal lets the drop
        be counted and logged like any other outcome.
        """
        spider = super().from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.on_request_dropped, signal=signals.request_dropped)
        return spider

    def on_request_dropped(self, request: Any, spider: Any) -> None:
        """Count a request the duplicate filter discarded.

        The site paginates a live result set, so the same decision can appear on
        two different pages of one search and be listed twice.
        """
        unit_key = request.meta.get("unit_key")
        if unit_key is None or unit_key not in self.units:
            return
        unit = self.units[unit_key]
        item: DocumentItem | None = request.meta.get("item")
        if item is None:
            # A listing page, not a document. Those are sent with
            # dont_filter=True, so this should not happen; log it if it does.
            logger.warning("listing_request_dropped", url=request.url, body=unit.body.slug)
            return
        unit.counters.duplicates_in_run += 1
        logger.info(
            "document_duplicate_in_run",
            stage="dupefilter",
            identifier=item.identifier,
            url=request.url,
            body=unit.body.slug,
            partition_label=unit.partition.label,
        )

    def __init__(
        self,
        start_date: str | None = None,
        end_date: str | None = None,
        bodies: str | None = None,
        partition: str | None = None,
        refresh_policy: str | None = None,
        run_id: str | None = None,
        summary_suffix: str = "",
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """All arguments arrive as strings from ``scrapy crawl -a key=value``."""
        super().__init__(*args, **kwargs)
        self.app_settings = get_settings()

        self.start_date = date.fromisoformat(start_date or self.app_settings.start_date)
        self.end_date = date.fromisoformat(end_date or self.app_settings.end_date)
        self.partition_spec = partition or self.app_settings.partition
        self.refresh_policy = (refresh_policy or self.app_settings.refresh_policy).lower()
        if self.refresh_policy not in {"hash", "skip_known"}:
            raise ValueError(
                f"Unknown refresh policy {self.refresh_policy!r}; use 'hash' or 'skip_known'."
            )
        # A run id ties together the logs, the Mongo records, the failure rows
        # and the summary file of one execution. Dagster passes its own.
        self.run_id = run_id or new_run_id()
        self.summary_suffix = summary_suffix

        # Logging is configured here, before any request is made, so that even a
        # startup failure is captured as JSON.
        configure_logging(
            level=self.app_settings.log_level,
            json_output=self.app_settings.log_json,
            log_dir=self.app_settings.log_dir,
            run_id=self.run_id,
            file_suffix=summary_suffix,
        )
        bind_run_context(run_id=self.run_id)

        self.bodies = resolve_bodies(bodies, self.app_settings.body_slugs)
        self.partitions = build_partitions(self.start_date, self.end_date, self.partition_spec)
        self.units: dict[str, Unit] = {}
        for partition_window in self.partitions:
            for body in self.bodies:
                unit = Unit(body=body, partition=partition_window)
                self.units[unit.key] = unit

        # The spider owns the storage clients because both the item pipeline and
        # the spider's own error handlers need them. Both clients connect lazily.
        self.repo = MongoRepository(self.app_settings)
        self.store = ObjectStore(self.app_settings)
        self.seen_identifiers: set[str] = set()
        self._started_at = time.time()

    # ------------------------------------------------------------------ #
    # Setup                                                               #
    # ------------------------------------------------------------------ #
    def counters_for(self, unit_key: str) -> UnitCounters:
        return self.units[unit_key].counters

    def _listing_url(self, unit: Unit, page: int) -> str:
        params = {
            # Without decisions=1 the endpoint renders the empty search form.
            "decisions": "1",
            "from": unit.partition.start.strftime(self.app_settings.date_format_site),
            "to": unit.partition.end.strftime(self.app_settings.date_format_site),
            "body": unit.body.site_id,
            "pageNumber": str(page),
        }
        # safe="/" keeps the dates in the DD/MM/YYYY form the site itself emits.
        return f"{self.app_settings.search_url}?{urlencode(params, safe='/')}"

    def _listing_request(self, unit: Unit, page: int) -> scrapy.Request:
        return scrapy.Request(
            url=self._listing_url(unit, page),
            callback=self.parse_listing,
            errback=self.on_listing_error,
            meta={
                "unit_key": unit.key,
                "page": page,
                # Listing pages get their own concurrency budget: they are slow
                # server-side searches and collapse if hammered.
                "download_slot": "listing",
            },
            # Lower than documents: finish the documents already discovered
            # before opening more listing pages, which keeps memory bounded.
            priority=10,
            dont_filter=True,
        )

    def start_requests(self):
        """One request per unit to begin with; page 2..N follow from page 1."""
        self.repo.ensure_indexes()
        self.store.ensure_buckets()
        logger.info(
            "run_started",
            kind="scrape",
            start_date=self.start_date.isoformat(),
            end_date=self.end_date.isoformat(),
            partition=self.partition_spec,
            refresh_policy=self.refresh_policy,
            bodies=[body.slug for body in self.bodies],
            partitions=[p.label for p in self.partitions],
            units=len(self.units),
        )
        for unit in self.units.values():
            logger.info(
                "partition_started",
                body=unit.body.slug,
                body_name=unit.body.name,
                partition_label=unit.partition.label,
                partition_date=unit.partition.partition_date.isoformat(),
            )
            yield self._listing_request(unit, page=1)

    # ------------------------------------------------------------------ #
    # Listing                                                             #
    # ------------------------------------------------------------------ #
    def parse_listing(self, response: Any):
        unit = self.units[response.meta["unit_key"]]
        page = response.meta["page"]
        counters = unit.counters
        counters.listing_pages_fetched += 1

        if page == 1:
            count = parse_result_count(response.text)
            counters.records_found = count or 0
            counters.listing_pages_expected = total_pages(
                counters.records_found, self.app_settings.page_size
            )
            logger.info(
                "listing_page_parsed",
                body=unit.body.slug,
                partition_label=unit.partition.label,
                page=page,
                records_found=counters.records_found,
                pages_expected=counters.listing_pages_expected,
                url=response.url,
            )
            for next_page in range(2, counters.listing_pages_expected + 1):
                yield self._listing_request(unit, page=next_page)

        rows = parse_listing_rows(response)
        if page > 1:
            logger.info(
                "listing_page_parsed",
                body=unit.body.slug,
                partition_label=unit.partition.label,
                page=page,
                items_on_page=len(rows),
                url=response.url,
            )

        # One query per page, not one per document: fetch whatever we already
        # hold for these ten identifiers. The result decides both whether to
        # skip a document (skip_known) and, later in the item pipeline, whether
        # its content changed.
        candidates = [
            normalise_identifier(row["identifier_raw"]) for row in rows if row["identifier_raw"]
        ]
        existing = self.repo.get_landing_many([c for c in candidates if c])

        for row in rows:
            counters.records_listed += 1
            request = self._document_request(unit, row, page, response.url, existing)
            if request is not None:
                yield request

    def _document_request(
        self,
        unit: Unit,
        row: dict[str, Any],
        page: int,
        listing_url: str,
        existing: dict[str, dict[str, Any]],
    ) -> scrapy.Request | None:
        """Build the request for one listing row, or record why we cannot."""
        counters = unit.counters
        doc_url = row["doc_url"]
        identifier = normalise_identifier(row["identifier_raw"])

        if not doc_url:
            counters.failed += 1
            self._record_failure(
                unit=unit,
                stage="listing",
                url=listing_url,
                identifier=identifier or None,
                error_type="MissingDocumentLink",
                error_message=f"listing row on page {page} has no document link",
            )
            return None

        if not identifier:
            identifier = identifier_from_url(doc_url)
            logger.warning(
                "identifier_fallback",
                body=unit.body.slug,
                partition_label=unit.partition.label,
                url=doc_url,
                identifier=identifier,
                identifier_raw=row["identifier_raw"],
            )

        try:
            published = parse_site_date(row["published_date_raw"])
        except ValueError as exc:
            counters.failed += 1
            self._record_failure(
                unit=unit,
                stage="listing",
                url=doc_url,
                identifier=identifier,
                error_type="UnparsableDate",
                error_message=f"{row['published_date_raw']!r}: {exc}",
            )
            return None

        if self.refresh_policy == "skip_known" and identifier in existing:
            counters.skipped_known += 1
            logger.info(
                "document_skipped_known",
                body=unit.body.slug,
                partition_label=unit.partition.label,
                identifier=identifier,
                url=doc_url,
            )
            return None

        item = DocumentItem(
            identifier=identifier,
            identifier_raw=row["identifier_raw"],
            site_ref=row.get("site_ref", ""),
            title=row["title"],
            description=row["description"],
            published_date=published,
            published_date_raw=row["published_date_raw"],
            body=unit.body.slug,
            body_id=unit.body.site_id,
            body_name=unit.body.name,
            doc_url=doc_url,
            listing_url=listing_url,
            listing_page=page,
            partition_date=unit.partition.partition_date,
            partition_start=unit.partition.start,
            partition_end=unit.partition.end,
            partition_label=unit.partition.label,
            run_id=self.run_id,
            unit_key=unit.key,
            # Carried from the batched lookup above so the item pipeline does
            # not have to query MongoDB again for this document.
            previous_record=existing.get(identifier),
        )
        counters.documents_requested += 1
        return scrapy.Request(
            url=doc_url,
            callback=self.parse_document,
            errback=self.on_document_error,
            meta={"item": item, "unit_key": unit.key, "download_slot": "documents"},
            priority=20,
        )

    # ------------------------------------------------------------------ #
    # Documents                                                           #
    # ------------------------------------------------------------------ #
    def parse_document(self, response: Any):
        """Detect the type, normalise, hash, and hand the item to the pipeline.

        Called twice for the minority of records whose case page is only a
        download link: once for that page, and once for the file it points at.
        """
        item: DocumentItem = response.meta["item"]
        is_attachment = response.meta.get("is_attachment", False)
        content_type = response.headers.get("Content-Type", b"").decode("latin-1") or None
        doc_type = sniff_doc_type(content_type, response.body[:8], response.url)

        if doc_type == "html" and not is_attachment and self.app_settings.follow_attachments:
            attachment = find_attachment(response)
            if attachment:
                # The page holds no decision, only a link to the file that does.
                # Fetch that instead and store it byte for byte.
                item.attachment_url = attachment
                logger.info(
                    "attachment_followed",
                    identifier=item.identifier,
                    url=response.url,
                    attachment_url=attachment,
                    body=item.body,
                    partition_label=item.partition_label,
                )
                yield scrapy.Request(
                    url=attachment,
                    callback=self.parse_document,
                    errback=self.on_document_error,
                    meta={
                        "item": item,
                        "unit_key": item.unit_key,
                        "download_slot": "documents",
                        "is_attachment": True,
                    },
                    # Above documents, so a followed file finishes the record it
                    # belongs to rather than queueing behind fresh work.
                    priority=25,
                )
                return

        # PDFs and Word documents are stored byte-for-byte as served; only HTML
        # is normalised (the volatile render-time comment is removed).
        payload = normalise_bytes(response.body, doc_type)

        item.doc_type = doc_type
        item.content_type = content_type
        item.payload = payload
        item.file_size = len(payload)
        item.file_hash = sha256_bytes(payload)

        if doc_type == "html" and not is_attachment:
            extra = parse_document_page(response)
            item.doc_title = extra["doc_title"]
            item.doc_heading = extra["doc_heading"]
            item.related_urls = extra["related_urls"]
            if item.related_urls:
                # Never observed; logged so a future multi-page decision is
                # visible rather than silently half-scraped.
                logger.warning(
                    "related_pages_found",
                    identifier=item.identifier,
                    url=response.url,
                    related_urls=item.related_urls,
                )
        yield item

    # ------------------------------------------------------------------ #
    # Failures                                                            #
    # ------------------------------------------------------------------ #
    def _record_failure(
        self,
        *,
        unit: Unit,
        stage: str,
        url: str,
        error_type: str,
        error_message: str,
        identifier: str | None = None,
        http_status: int | None = None,
        attempts: int | None = None,
    ) -> None:
        """Log the failure and persist it, so no lost record is invisible."""
        self.repo.record_failure(
            run_id=self.run_id,
            stage=stage,
            url=url,
            identifier=identifier,
            body=unit.body.slug,
            partition_label=unit.partition.label,
            http_status=http_status,
            error_type=error_type,
            error_message=error_message,
            attempts=attempts,
        )
        logger.error(
            "listing_failed" if stage == "listing" else "document_failed",
            stage=stage,
            body=unit.body.slug,
            partition_label=unit.partition.label,
            identifier=identifier,
            url=url,
            http_status=http_status,
            error_type=error_type,
            error_message=error_message,
        )

    @staticmethod
    def _describe_failure(failure: Any) -> tuple[str, str, int | None]:
        """Turn a Twisted failure into (error_type, message, http_status)."""
        if failure.check(HttpError):
            response = failure.value.response
            return ("HttpError", f"HTTP {response.status} for {response.url}", response.status)
        return (failure.type.__name__, str(failure.value), None)

    def on_listing_error(self, failure: Any) -> None:
        """A listing page failed after all retries.

        This is more serious than a failed document: we cannot know which records
        that page held, so the unit is marked incomplete and the reconciliation
        check will flag it.
        """
        request = failure.request
        unit = self.units[request.meta["unit_key"]]
        error_type, message, status = self._describe_failure(failure)
        self._record_failure(
            unit=unit,
            stage="listing",
            url=request.url,
            error_type=error_type,
            error_message=message,
            http_status=status,
            attempts=request.meta.get("retry_times"),
        )

    def on_document_error(self, failure: Any) -> None:
        """A document failed after all retries: recorded, never fatal."""
        request = failure.request
        unit = self.units[request.meta["unit_key"]]
        item: DocumentItem | None = request.meta.get("item")
        unit.counters.failed += 1
        error_type, message, status = self._describe_failure(failure)
        self._record_failure(
            unit=unit,
            stage="document",
            url=request.url,
            identifier=item.identifier if item else None,
            error_type=error_type,
            error_message=message,
            http_status=status,
            attempts=request.meta.get("retry_times"),
        )

    # ------------------------------------------------------------------ #
    # Summary                                                             #
    # ------------------------------------------------------------------ #
    def closed(self, reason: str) -> None:
        """Emit the end-of-run summary and persist it."""
        elapsed = round(time.time() - self._started_at, 2)
        units = [unit.counters for unit in self.units.values()]

        for counters in units:
            if counters.records_found is None:
                logger.error(
                    "listing_incomplete",
                    body=counters.body,
                    partition_label=counters.partition_label,
                    reason="page 1 never parsed",
                )
            elif counters.records_listed < counters.records_found:
                logger.error(
                    "listing_incomplete",
                    body=counters.body,
                    partition_label=counters.partition_label,
                    records_found=counters.records_found,
                    records_listed=counters.records_listed,
                    pages_expected=counters.listing_pages_expected,
                    pages_fetched=counters.listing_pages_fetched,
                )
            if not counters.reconciled:
                logger.error(
                    "reconciliation_failed",
                    body=counters.body,
                    partition_label=counters.partition_label,
                    records_listed=counters.records_listed,
                    accounted=counters.accounted,
                )
            logger.info("partition_finished", **counters.as_dict())

        totals = {
            "records_found": sum(c.records_found or 0 for c in units),
            "records_listed": sum(c.records_listed for c in units),
            "new": sum(c.new for c in units),
            "changed": sum(c.changed for c in units),
            "unchanged": sum(c.unchanged for c in units),
            "skipped_known": sum(c.skipped_known for c in units),
            "failed": sum(c.failed for c in units),
            "duplicates_in_run": sum(c.duplicates_in_run for c in units),
        }
        totals["scraped"] = totals["new"] + totals["changed"] + totals["unchanged"]
        summary = {
            "run_id": self.run_id,
            "kind": "scrape",
            "reason": reason,
            "start_date": self.start_date.isoformat(),
            "end_date": self.end_date.isoformat(),
            "partition": self.partition_spec,
            "refresh_policy": self.refresh_policy,
            "bodies": [body.slug for body in self.bodies],
            "elapsed_seconds": elapsed,
            "units": [c.as_dict() for c in units],
            "totals": totals,
            "all_units_reconciled": all(c.reconciled for c in units),
        }
        logger.info("run_summary", **summary)

        try:
            self.repo.record_run(summary)
        except Exception as exc:
            # Bookkeeping must never turn a successful crawl into a failure.
            logger.error("run_summary_not_persisted", error=str(exc))

        # Written to disk so the orchestrator can read the result of a unit it
        # launched as a subprocess.
        log_dir = Path(self.app_settings.log_dir)
        log_dir.mkdir(parents=True, exist_ok=True)
        summary_path = log_dir / f"{self.run_id}{self.summary_suffix}.summary.json"
        summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")

        self.repo.close()
