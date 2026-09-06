"""Pure parsing helpers for the search listing and the decision pages.

Kept separate from the spider so they can be unit-tested against saved HTML
fixtures, with no network and no Scrapy engine. If the site is redesigned, these
tests fail first and tell us exactly what changed.

Markup contract (verified against live pages, 2008-2025)
--------------------------------------------------------
Listing row::

    <li class="each-item clearfix">
      <h2 class="title" title="ADJ-00054447"><a href="/en/cases/...">ADJ-00054447</a></h2>
      <span class="date">27/06/2025</span>
      <p class="description" title="A v B">A v B</p>
      <span class="refNO">ADJ-00054447</span>
      <div class="col-sm-3 link"><a class="btn btn-primary" href="/en/cases/...">View Page</a></div>
    </li>

Decision page::

    <div class="col-sm-9">
      <h1 class="page-title">ADJ-00054447</h1>
      <div class="content"> ...decision text... </div>
    </div>
"""

from __future__ import annotations

import math
import re
from datetime import date, datetime
from typing import Any

#: "Shows 1 to 10 of 508 results" - absent when a search returns nothing.
_RESULT_COUNT = re.compile(r"of\s+([\d,]+)\s+results", re.IGNORECASE)


def parse_result_count(text: str) -> int | None:
    """Total number of results the site reports, or ``None`` if not shown.

    ``None`` and ``0`` mean the same thing operationally (an empty search), but
    they are distinguished so an unexpected markup change is visible instead of
    silently looking like "no results".
    """
    match = _RESULT_COUNT.search(text)
    if not match:
        return None
    return int(match.group(1).replace(",", ""))


def total_pages(count: int, page_size: int) -> int:
    """How many listing pages a result set spans.

    Computing this from the count on page 1 lets us request every remaining page
    in parallel instead of following "next" links one at a time.
    """
    if count <= 0:
        return 0
    return math.ceil(count / page_size)


def parse_site_date(value: str) -> date:
    """Parse the site's ``DD/MM/YYYY`` dates.

    Parsed explicitly rather than with a fuzzy date parser, which would read
    ``03/04/2024`` as 4 March instead of 3 April.
    """
    return datetime.strptime(value.strip(), "%d/%m/%Y").date()


def _first(values: list[str]) -> str:
    for value in values:
        cleaned = value.strip()
        if cleaned:
            return cleaned
    return ""


def parse_listing_rows(response: Any) -> list[dict[str, Any]]:
    """Extract the raw fields of every result row on a listing page.

    Returns dictionaries (not items) because the spider still has to add
    partition and run context. Missing fields come back empty so the caller can
    record a parsing failure with its reason instead of raising.
    """
    rows: list[dict[str, Any]] = []
    for node in response.css("li.each-item"):
        href = _first(
            [
                *node.css("div.link a.btn::attr(href)").getall(),
                *node.css("h2.title a::attr(href)").getall(),
                *node.css("p.fullpath::attr(title)").getall(),
            ]
        )
        rows.append(
            {
                # The title is the tribunal's own reference and is the reliable
                # identifier across every era. "Ref no" looks like the obvious
                # choice but is not: for Employment Appeals Tribunal records and
                # some early Equality Tribunal ones it holds an internal numeric
                # id, and that id is not unique - two different decisions in
                # December 2008 both carry 33397. Keying on it silently discards
                # one of them as a duplicate.
                "identifier_raw": _first(
                    [
                        *node.css("h2.title::attr(title)").getall(),
                        *node.css("h2.title a::text").getall(),
                        *node.css("span.refNO::text").getall(),
                    ]
                ),
                # Kept because it is the site's own key, and throwing away source
                # data is never free.
                "site_ref": _first(node.css("span.refNO::text").getall()),
                "title": _first(
                    [
                        *node.css("h2.title a::text").getall(),
                        *node.css("h2.title::attr(title)").getall(),
                    ]
                ),
                "description": _first(
                    [
                        *node.css("p.description::text").getall(),
                        *node.css("p.description::attr(title)").getall(),
                    ]
                ),
                "published_date_raw": _first(node.css("span.date::text").getall()),
                "doc_url": response.urljoin(href) if href else "",
            }
        )
    return rows


#: Extensions the site uses when a decision is published as a file rather than
#: as a web page.
_ATTACHMENT_SUFFIXES = (".pdf", ".doc", ".docx", ".rtf")

#: Site-wide documents linked from every page; never the decision itself.
_BOILERPLATE = ("cookie_policy", "decisions_information_guide")


def _content_column(response: Any) -> Any:
    """The column holding the case title and the decision, or ``None``."""
    for column in response.css("div.col-sm-9"):
        if column.css("h1.page-title"):
            return column
    return None


def find_attachment(response: Any) -> str | None:
    """Return the decision file a case page links to, if it is one of those pages.

    Employment Appeals Tribunal decisions published before 2013 are not web
    pages at all. The case page carries no decision text, only the reference,
    the file size and a Download link to a PDF; roughly 10,700 records are like
    this. For them the PDF *is* the document, so the spider follows this link
    and stores the file rather than an empty page.

    Boilerplate linked from every page (the cookie policy, the searching guide)
    is excluded, and only the content column is searched, so a link in the
    footer can never be mistaken for the decision.
    """
    column = _content_column(response)
    if column is None:
        return None
    for href in column.css("a::attr(href)").getall():
        target = href.strip()
        path, _, query = target.lower().partition("?")
        if not path.endswith(_ATTACHMENT_SUFFIXES):
            continue
        if any(marker in path for marker in _BOILERPLATE):
            continue
        # The same PDF is also referenced with ?type=pdfPreview to render a
        # thumbnail. That is an image of the first page, not the decision.
        if "preview" in query:
            continue
        return response.urljoin(target)
    return None


def parse_document_page(response: Any) -> dict[str, Any]:
    """Extra metadata taken from the decision page itself.

    * ``doc_title``  - the page ``<title>`` without the site suffix.
    * ``doc_heading`` - the first heading inside the decision text, e.g.
      "ADJUDICATION OFFICER DECISION".
    * ``related_urls`` - links to other case pages inside the decision text.
      Every sampled decision is a single page, so this is expected to be empty;
      capturing it means a future multi-page decision shows up in the logs
      instead of being silently truncated.
    """
    raw_title = _first(response.css("title::text").getall())
    doc_title = re.sub(r"\s*-\s*Workplace Relations Commission\s*$", "", raw_title).strip()

    content = response.css("div.content")
    heading = ""
    if content:
        for text in content[0].css("h1::text, h2::text").getall():
            cleaned = " ".join(text.split())
            if cleaned:
                heading = cleaned
                break

    related: list[str] = []
    if content:
        for href in content[0].css("a::attr(href)").getall():
            if "/cases/" in href.lower():
                related.append(response.urljoin(href))

    return {
        "doc_title": doc_title or None,
        "doc_heading": heading or None,
        "related_urls": sorted(set(related)),
    }
