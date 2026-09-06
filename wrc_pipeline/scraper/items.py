"""The item the spider produces: one scraped decision.

A dataclass rather than ``scrapy.Item`` because it gives real type hints, IDE
completion and a natural place for the small amount of behaviour we need
(turning itself into a MongoDB document). Scrapy supports dataclass items
natively through ``itemadapter``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Any

from wrc_pipeline.storage.mongo import to_datetime


@dataclass
class DocumentItem:
    """Metadata for one decision, plus the downloaded payload.

    Fields are filled in two stages: everything from the listing row is known
    when the item is created, and the payload fields are added once the document
    itself has been downloaded.
    """

    # --- from the search listing -----------------------------------------
    identifier: str
    identifier_raw: str
    #: The site's own "Ref no". Kept for traceability, never used as the key:
    #: for older imported records it is an internal number that collides across
    #: different decisions.
    site_ref: str
    title: str
    description: str
    published_date: date
    published_date_raw: str
    body: str
    body_id: str
    body_name: str
    doc_url: str
    listing_url: str
    listing_page: int
    partition_date: date
    partition_start: date
    partition_end: date
    partition_label: str
    run_id: str
    unit_key: str
    source: str = "workplacerelations.ie"

    # --- from the document page ------------------------------------------
    doc_type: str | None = None
    content_type: str | None = None
    file_size: int | None = None
    file_hash: str | None = None
    doc_title: str | None = None
    doc_heading: str | None = None
    #: Links to other ``/en/cases/`` pages found inside the decision text.
    #: Expected to be empty (each decision is a single page); populated and
    #: logged if the site ever changes, so multi-page decisions cannot pass
    #: unnoticed.
    related_urls: list[str] = field(default_factory=list)

    #: Set when the case page carried no decision text, only a link to the file
    #: that holds it. ``doc_url`` stays the case page, because that is the
    #: record's canonical address; this is where the bytes came from.
    attachment_url: str | None = None

    #: The bytes to store. Excluded from the metadata document.
    payload: bytes | None = None

    #: The record already stored for this identifier, or ``None`` if it is new.
    #: Looked up in one batched query per listing page rather than once per
    #: document, and carried on the item because the pipeline cannot see request
    #: metadata. Excluded from the metadata document.
    previous_record: dict[str, Any] | None = None

    def metadata(self) -> dict[str, Any]:
        """Serialise to a MongoDB document (without the payload).

        Dates become timezone-aware datetimes because BSON has no date type.
        """
        return {
            "identifier": self.identifier,
            "identifier_raw": self.identifier_raw,
            "site_ref": self.site_ref,
            "title": self.title,
            "description": self.description,
            "doc_title": self.doc_title,
            "doc_heading": self.doc_heading,
            "published_date": to_datetime(self.published_date),
            "published_date_raw": self.published_date_raw,
            "body": self.body,
            "body_id": self.body_id,
            "body_name": self.body_name,
            "doc_url": self.doc_url,
            "attachment_url": self.attachment_url,
            "listing_url": self.listing_url,
            "listing_page": self.listing_page,
            "doc_type": self.doc_type,
            "content_type": self.content_type,
            "file_size": self.file_size,
            "file_hash": self.file_hash,
            "related_urls": self.related_urls,
            "partition_date": to_datetime(self.partition_date),
            "partition_start": to_datetime(self.partition_start),
            "partition_end": to_datetime(self.partition_end),
            "partition_label": self.partition_label,
            "source": self.source,
            "run_id": self.run_id,
            "scrape_status": "ok",
        }

    @property
    def source_filename(self) -> str:
        """File name to store the payload under in the landing zone.

        The landing zone keeps the source's own name, so for a followed
        attachment that is the attachment's name, not the case page's.
        """
        url = self.attachment_url or self.doc_url
        return url.rsplit("/", 1)[-1] or f"{self.identifier}.{self.doc_type or 'bin'}"

    def partition_fields(self) -> dict[str, Any]:
        """Just the partition stamps, used when touching an unchanged record."""
        return {
            "partition_date": to_datetime(self.partition_date),
            "partition_start": to_datetime(self.partition_start),
            "partition_end": to_datetime(self.partition_end),
            "partition_label": self.partition_label,
        }
