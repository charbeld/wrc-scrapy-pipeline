"""The HTML cleaner: keep the decision, drop the website.

Two things are asserted: that the chrome is gone and the substance stays, and
that the output is deterministic (the transformed file is hashed, so identical
input must give identical bytes).
"""

from __future__ import annotations

from tests.conftest import FIXTURES
from wrc_pipeline.hashing import sha256_bytes
from wrc_pipeline.transform.html_clean import TRANSFORM_VERSION, clean_html


def _clean(name: str, identifier: str):
    return clean_html((FIXTURES / name).read_bytes(), identifier)


def test_decision_content_survives_cleaning():
    payload, info = _clean("doc_adj-00054447.html", "ADJ-00054447")
    text = payload.decode("utf-8")

    assert info["content_root"] == "div.col-sm-9"
    assert "ADJUDICATION OFFICER DECISION" in text
    assert "ADJ-00054447" in text
    assert "Christian Nolan" in text  # complainant
    assert "Brennans Bakery" in text  # respondent
    assert "<table" in text  # the parties/complaints tables are structure, not chrome


def test_site_chrome_is_removed():
    payload, _ = _clean("doc_adj-00054447.html", "ADJ-00054447")
    text = payload.decode("utf-8").lower()

    for chrome in (
        "<footer",
        "<header",
        "<script",
        "<nav",
        "return to search",
        "cookie",
        "google",
        "linkedin",
        "sitemap",
    ):
        assert chrome not in text, f"chrome leaked into the output: {chrome}"


def test_output_is_a_standalone_document_titled_by_identifier():
    payload, _ = _clean("doc_udd2449.html", "UDD2449")
    text = payload.decode("utf-8")
    assert text.startswith("<!doctype html>")
    assert "<title>UDD2449</title>" in text
    assert text.rstrip().endswith("</html>")


def test_cleaning_is_deterministic():
    """Same input, same bytes: otherwise the hash would change on every run."""
    first, _ = _clean("doc_udd2449.html", "UDD2449")
    second, _ = _clean("doc_udd2449.html", "UDD2449")
    assert sha256_bytes(first) == sha256_bytes(second)


def test_cleaning_removes_the_page_overhead():
    """A short decision is mostly chrome, so the saving is large.

    A long determination keeps most of its bytes because they really are the
    decision, so only the direction is asserted there.
    """
    short_raw = (FIXTURES / "doc_adj-00054447.html").read_bytes()
    short_clean, _ = _clean("doc_adj-00054447.html", "ADJ-00054447")
    assert len(short_clean) < len(short_raw) / 2

    long_raw = (FIXTURES / "doc_udd2449.html").read_bytes()
    long_clean, _ = _clean("doc_udd2449.html", "UDD2449")
    assert len(long_clean) < len(long_raw)


def test_empty_tables_are_dropped():
    """The site's import leaves stray empty tables behind; they are noise."""
    payload, _ = _clean("doc_udd2449.html", "UDD2449")
    assert "<table><tbody></tbody></table>" not in payload.decode("utf-8")


def test_structured_fields_are_extracted():
    _, info = _clean("doc_adj-00054447.html", "ADJ-00054447")
    extracted = info["extracted"]

    assert extracted["text_length"] > 500
    assert extracted["language"] == "en"
    assert any("ADJUDICATION OFFICER" in heading.upper() for heading in extracted["headings"])
    assert extracted["parties"]


def test_older_and_other_body_pages_also_clean():
    """The layout has been stable since at least 2014; check both eras."""
    for name, identifier in (
        ("doc_dec-e2014-097.html", "DEC-E2014-097"),
        ("doc_ud833_2015.html", "UD833-2015"),
        ("doc_ir-sc-00001595.html", "IR-SC-00001595"),
        ("doc_udd2449.html", "UDD2449"),
    ):
        payload, info = _clean(name, identifier)
        assert info["content_root"] == "div.col-sm-9"
        assert len(payload) > 500


def test_transform_version_is_an_integer():
    """Bumping it forces already-transformed documents to be reprocessed."""
    assert isinstance(TRANSFORM_VERSION, int)
