"""Decisions published as a downloadable file rather than as a web page.

Employment Appeals Tribunal decisions from 2007 to 2012 - roughly 10,700 records
- do not have their text on the case page at all. The page shows the reference,
a file size and a Download link, and the decision itself is the PDF behind it.
The assignment requires those to be stored as they are, so the spider follows
the link and keeps the file.

These tests run against a real saved page, so a change to that layout fails here
rather than silently storing empty pages.
"""

from __future__ import annotations

from scrapy.http import HtmlResponse, Request

from tests.conftest import load_response
from wrc_pipeline.scraper.parsing import find_attachment, parse_document_page

STUB_URL = "https://www.workplacerelations.ie/en/cases/2008/december/ud1051_2007.html"


def _response(html: str, url: str = STUB_URL) -> HtmlResponse:
    return HtmlResponse(
        url=url, body=html.encode("utf-8"), encoding="utf-8", request=Request(url=url)
    )


def test_download_page_yields_the_pdf_link():
    response = load_response("doc_eat_pdf_stub.html", STUB_URL)
    assert find_attachment(response) == (
        "https://www.workplacerelations.ie"
        "/en/eat_import/2008/12/99d0088b-18e2-486b-8672-559fe840dd21.pdf"
    )


def test_download_page_really_has_no_decision_text():
    """The reason we follow the link: there is nothing else to store."""
    response = load_response("doc_eat_pdf_stub.html", STUB_URL)
    content = response.css("div.content").get() or ""
    text = " ".join(response.css("div.content *::text").getall()).strip()
    assert content != "", "the content div should exist"
    assert text == "", "if this page gains text, revisit whether to follow the link"


def test_ordinary_decision_pages_have_no_attachment():
    """A page whose decision is in the HTML must not trigger a second fetch."""
    for name, url in (
        ("doc_adj-00054447.html", "https://www.workplacerelations.ie/en/cases/x/adj.html"),
        ("doc_udd2449.html", "https://www.workplacerelations.ie/en/cases/x/udd2449.html"),
        ("doc_dec-e2014-097.html", "https://www.workplacerelations.ie/en/cases/x/dec.html"),
    ):
        assert find_attachment(load_response(name, url)) is None


def test_site_wide_boilerplate_is_not_mistaken_for_the_decision():
    """Every page links to the cookie policy and the searching guide as PDFs."""
    html = """
    <div class="col-sm-9"><h1 class="page-title">ADJ-1</h1>
      <div class="content">
        <a href="/en/privacy-policy/cookie_policy.pdf">Cookie policy</a>
        <a href="/en/Publications_Forms/Decisions_Information_Guide.pdf">Guide</a>
      </div>
    </div>"""
    assert find_attachment(_response(html)) is None


def test_preview_thumbnails_are_skipped():
    """The same PDF is linked with ?type=pdfPreview to render a thumbnail."""
    html = """
    <div class="col-sm-9"><h1 class="page-title">UD1/2007</h1>
      <div class="content">
        <a href="/en/eat_import/2008/12/abc.pdf?type=pdfPreview&amp;width=200">preview</a>
        <a href="/en/eat_import/2008/12/abc.pdf">Download</a>
      </div>
    </div>"""
    assert find_attachment(_response(html)).endswith("/abc.pdf")


def test_links_outside_the_content_column_are_ignored():
    """A PDF in the footer is not the decision."""
    html = """
    <footer><a href="/en/some/report.pdf">Annual report</a></footer>
    <div class="col-sm-9"><h1 class="page-title">ADJ-2</h1>
      <div class="content"><p>The decision text.</p></div>
    </div>"""
    assert find_attachment(_response(html)) is None


def test_word_documents_are_recognised_too():
    html = """
    <div class="col-sm-9"><h1 class="page-title">UD2/2007</h1>
      <div class="content"><a href="/en/eat_import/2009/01/x.doc">Download</a></div>
    </div>"""
    assert find_attachment(_response(html)).endswith("/x.doc")


def test_the_stub_page_still_yields_its_reference_as_a_title():
    """Metadata from the stub is kept even though its body is empty."""
    response = load_response("doc_eat_pdf_stub.html", STUB_URL)
    parsed = parse_document_page(response)
    assert parsed["doc_title"]
    assert "UD1051" in parsed["doc_title"].replace(" ", "")
