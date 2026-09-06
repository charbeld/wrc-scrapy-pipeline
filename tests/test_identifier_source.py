"""Which field on a listing row is the decision's identifier.

"Ref no" looks like the obvious choice and is what the site labels it, but it is
not safe as a key. For Employment Appeals Tribunal records, and for some early
Equality Tribunal ones, it holds an internal numeric id, and that id is reused
across different decisions. Keying on it makes two distinct decisions look like
one and silently drops the second.

The title is the tribunal's own reference and never collided in any sample. The
tests below pin that decision to real saved pages from both eras.
"""

from __future__ import annotations

from tests.conftest import load_response
from wrc_pipeline.identifiers import normalise_identifier
from wrc_pipeline.scraper.parsing import parse_listing_rows

EAT_URL = (
    "https://www.workplacerelations.ie/en/search/"
    "?decisions=1&from=01/12/2008&to=31/12/2008&body=2&pageNumber=1"
)
MODERN_URL = (
    "https://www.workplacerelations.ie/en/search/"
    "?decisions=1&from=01/01/2024&to=31/12/2024&body=3&pageNumber=1"
)


def _rows(name: str, url: str):
    return parse_listing_rows(load_response(name, url))


def test_older_records_use_the_case_reference_not_the_internal_number():
    rows = _rows("listing_eat_2008_p1.html", EAT_URL)
    assert len(rows) == 10

    for row in rows:
        identifier = normalise_identifier(row["identifier_raw"])
        assert identifier, "every row must yield an identifier"
        assert not identifier.isdigit(), (
            f"{identifier} is the site's internal number, not a case reference"
        )
        # The site's own key is kept, just not used as the identifier.
        assert row["site_ref"].isdigit()


def test_the_internal_number_really_does_collide():
    """The reason the title is preferred, demonstrated on real data."""
    rows = _rows("listing_eat_2008_p1.html", EAT_URL)
    site_refs = [r["site_ref"] for r in rows]
    identifiers = [normalise_identifier(r["identifier_raw"]) for r in rows]

    assert len(set(site_refs)) < len(site_refs), "expected a colliding Ref no on this page"
    assert len(set(identifiers)) == len(identifiers), "identifiers must stay distinct"


def test_combined_references_survive_normalisation():
    """Older records cover several claims at once: 'UD1020/2007, WT341/2007'."""
    rows = _rows("listing_eat_2008_p1.html", EAT_URL)
    combined = [r for r in rows if "," in r["identifier_raw"]]
    assert combined, "expected at least one multi-claim reference on this page"
    for row in combined:
        identifier = normalise_identifier(row["identifier_raw"])
        assert "/" not in identifier and " " not in identifier
        assert identifier.startswith(row["identifier_raw"].split("/")[0].upper())


def test_modern_records_are_unaffected():
    """Where the two fields agree, the change makes no difference."""
    rows = _rows("listing_lc_2024_p1.html", MODERN_URL)
    for row in rows:
        assert normalise_identifier(row["identifier_raw"]) == normalise_identifier(row["site_ref"])
