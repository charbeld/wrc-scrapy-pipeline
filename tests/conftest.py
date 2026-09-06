"""Shared test fixtures.

The HTML files under ``tests/fixtures`` are real pages saved from the site, so
the parsers are tested against exactly what production sees, with no network
access and no Docker.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from scrapy.http import HtmlResponse, Request

FIXTURES = Path(__file__).parent / "fixtures"

BASE = "https://www.workplacerelations.ie"


def load_response(name: str, url: str) -> HtmlResponse:
    """Build a Scrapy response from a saved page, as the spider would see it."""
    body = (FIXTURES / name).read_bytes()
    return HtmlResponse(url=url, body=body, encoding="utf-8", request=Request(url=url))


@pytest.fixture
def listing_response() -> HtmlResponse:
    """Labour Court, 2024, page 1: 508 results, 10 rows."""
    return load_response(
        "listing_lc_2024_p1.html",
        f"{BASE}/en/search/?decisions=1&from=01/01/2024&to=31/12/2024&body=3&pageNumber=1",
    )


@pytest.fixture
def empty_listing_response() -> HtmlResponse:
    """Equality Tribunal, 2024: the body is defunct, so there are no results."""
    return load_response(
        "listing_empty.html",
        f"{BASE}/en/search/?decisions=1&from=01/01/2024&to=31/12/2024&body=1&pageNumber=1",
    )


@pytest.fixture
def adj_document() -> HtmlResponse:
    """A WRC adjudication decision page."""
    return load_response("doc_adj-00054447.html", f"{BASE}/en/cases/2025/january/adj-00054447.html")


@pytest.fixture
def labour_court_document() -> HtmlResponse:
    """A Labour Court determination page."""
    return load_response("doc_udd2449.html", f"{BASE}/en/cases/2024/december/udd2449.html")
