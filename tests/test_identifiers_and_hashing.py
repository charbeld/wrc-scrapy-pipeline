"""Identifier normalisation, hashing and document-type sniffing.

These two modules underpin deduplication: the identifier is the unique key and
the hash decides whether a document changed. A regression here would either
create duplicates or make every run look like everything changed.
"""

from __future__ import annotations

import pytest

from wrc_pipeline.hashing import (
    normalise_bytes,
    normalise_html_bytes,
    sha256_bytes,
    short_hash,
    sniff_doc_type,
)
from wrc_pipeline.identifiers import (
    file_extension_for,
    identifier_from_url,
    normalise_identifier,
)


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("ADJ-00054447", "ADJ-00054447"),
        ("IR - SC - 00002163", "IR-SC-00002163"),
        ("DEC-S2008- 123", "DEC-S2008-123"),  # stray space after the hyphen
        ("DEC-S2008-120 ", "DEC-S2008-120"),  # trailing space
        ("UD833/2015", "UD833-2015"),  # slash would fake a folder in object storage
        ("  udd2449  ", "UDD2449"),
        ("DEC-E2014-097", "DEC-E2014-097"),
        ("UD1075/2015 MN502/2015", "UD1075-2015-MN502-2015"),
    ],
)
def test_identifier_normalisation(raw, expected):
    assert normalise_identifier(raw) == expected


@pytest.mark.parametrize("raw", ["", None, "   ", "---"])
def test_unusable_identifiers_return_empty(raw):
    assert normalise_identifier(raw) == ""


def test_identifier_falls_back_to_the_url():
    url = "https://www.workplacerelations.ie/en/cases/2018/january/ud833_2015.html"
    assert identifier_from_url(url) == "UD833-2015"


def test_file_extension_mapping():
    assert file_extension_for("html") == "html"
    assert file_extension_for("pdf") == "pdf"
    assert file_extension_for("something-else") == "bin"


# --------------------------------------------------------------------------- #
# Hashing                                                                      #
# --------------------------------------------------------------------------- #
def test_elapsed_time_comment_is_ignored_by_the_hash():
    """The site stamps a different render time into every response.

    Without stripping it, every document would look modified on every run and
    the pipeline would re-upload the whole corpus each time.
    """
    first = b"<html><body>decision</body></html><!-- Elapsed time: 0.0469062 -->"
    second = b"<html><body>decision</body></html><!-- Elapsed time: 0.0312515 -->"

    assert first != second
    assert normalise_html_bytes(first) == normalise_html_bytes(second)
    assert sha256_bytes(normalise_html_bytes(first)) == sha256_bytes(normalise_html_bytes(second))


def test_cache_marker_comments_are_ignored_by_the_hash():
    """The site adds cache markers to some responses and not others.

    Found by the end-to-end test: two crawls minutes apart disagreed about all
    45 documents because one response carried these markers and the other did
    not. The decision text was identical.
    """
    newline = b"\n"
    without = b"<html><body>decision</body></html>" + newline + b"<!-- Elapsed time: 0.04 -->"
    with_markers = (
        b"<html><body>decision</body></html>"
        + newline
        + b"<!-- cached location --><!-- cached or not being index.aspx page -->"
        + newline
        + b"<!-- Elapsed time: 0.04 -->"
    )
    assert normalise_html_bytes(without) == normalise_html_bytes(with_markers)
    assert sha256_bytes(normalise_html_bytes(without)) == sha256_bytes(
        normalise_html_bytes(with_markers)
    )


def test_conditional_comments_are_preserved():
    """IE conditional comments wrap real tags, so removing them would damage
    the document. They are also stable between fetches, so they can stay."""
    body = b'<!--[if lt IE 9]><script src="shim.js"></script><![endif]--><p>text</p>'
    assert normalise_html_bytes(body) == body


def test_real_content_changes_still_change_the_hash():
    first = b"<html><body>decision</body></html><!-- Elapsed time: 0.04 -->"
    second = b"<html><body>amended decision</body></html><!-- Elapsed time: 0.04 -->"
    assert sha256_bytes(normalise_html_bytes(first)) != sha256_bytes(normalise_html_bytes(second))


def test_binary_payloads_are_hashed_untouched():
    """PDFs are stored exactly as served, so nothing may be rewritten."""
    payload = b"%PDF-1.7\n<!-- Elapsed time: 0.04 -->trailer"
    assert normalise_bytes(payload, "pdf") == payload
    assert normalise_bytes(payload, "html") != payload


def test_hash_format_and_short_form():
    digest = sha256_bytes(b"abc")
    assert digest.startswith("sha256:")
    assert len(digest) == len("sha256:") + 64
    assert short_hash(digest) == digest.split(":")[1][:12]


@pytest.mark.parametrize(
    ("content_type", "head", "url", "expected"),
    [
        ("text/html; charset=utf-8", b"<!DOCTYPE ", "https://x/a.html", "html"),
        # Magic bytes win over a wrong content type.
        ("text/html", b"%PDF-1.7ab", "https://x/a.html", "pdf"),
        ("application/pdf", b"%PDF-1.7ab", "https://x/a.pdf", "pdf"),
        ("application/octet-stream", b"PK\x03\x04abcd", "https://x/a", "docx"),
        ("application/octet-stream", b"\xd0\xcf\x11\xe0abcd", "https://x/a", "doc"),
        # Falls back to the URL suffix when nothing else identifies it.
        (None, b"random", "https://x/report.pdf", "pdf"),
        (None, b"random", "https://x/page", "other"),
    ],
)
def test_document_type_sniffing(content_type, head, url, expected):
    assert sniff_doc_type(content_type, head, url) == expected
