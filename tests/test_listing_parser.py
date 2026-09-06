"""Parsing of the search listing, against saved pages.

If the site is redesigned these tests fail first and say exactly which selector
broke, instead of the pipeline quietly scraping zero records.
"""

from __future__ import annotations

import pytest

from wrc_pipeline.bodies import BODIES, get_body, resolve_bodies
from wrc_pipeline.identifiers import normalise_identifier
from wrc_pipeline.scraper.parsing import (
    parse_listing_rows,
    parse_result_count,
    parse_site_date,
    total_pages,
)


def test_result_count_is_read_from_the_page(listing_response):
    # "Shows 1 to 10 of 508 results" for the Labour Court in 2024.
    assert parse_result_count(listing_response.text) == 508


def test_empty_search_reports_no_count(empty_listing_response):
    """A defunct body in a recent year returns no results at all.

    The count element is absent entirely, which must read as "nothing found"
    rather than raising.
    """
    assert parse_result_count(empty_listing_response.text) is None
    assert parse_listing_rows(empty_listing_response) == []


def test_page_count_is_computed_from_the_total():
    """Page size is fixed at 10, so pages 2..N can be requested in parallel."""
    assert total_pages(508, 10) == 51
    assert total_pages(10, 10) == 1
    assert total_pages(11, 10) == 2
    assert total_pages(0, 10) == 0


def test_listing_rows_are_extracted(listing_response):
    rows = parse_listing_rows(listing_response)
    assert len(rows) == 10

    first = rows[0]
    assert first["identifier_raw"].strip() == "UDD2449"
    assert normalise_identifier(first["identifier_raw"]) == "UDD2449"
    assert first["published_date_raw"] == "20/12/2024"
    assert first["doc_url"] == (
        "https://www.workplacerelations.ie/en/cases/2024/december/udd2449.html"
    )
    assert first["description"]
    assert first["title"]


def test_every_row_has_the_fields_the_pipeline_needs(listing_response):
    for row in parse_listing_rows(listing_response):
        assert row["doc_url"].startswith("https://www.workplacerelations.ie/en/cases/")
        assert normalise_identifier(row["identifier_raw"])
        assert parse_site_date(row["published_date_raw"])


def test_dates_are_parsed_as_day_first():
    """03/04/2024 is 3 April, not 4 March: a fuzzy parser would get this wrong."""
    assert parse_site_date("03/04/2024").isoformat() == "2024-04-03"
    assert parse_site_date(" 30/06/2025 ").isoformat() == "2025-06-30"
    with pytest.raises(ValueError):
        parse_site_date("2024-04-03")


def test_body_registry_matches_the_site_filter_values():
    assert get_body("labour_court").site_id == "3"
    assert get_body("workplace_relations_commission").site_id == "15376"
    assert get_body("employment_appeals_tribunal").site_id == "2"
    assert get_body("equality_tribunal").site_id == "1"
    assert len(BODIES) == 4


def test_unknown_body_fails_loudly():
    with pytest.raises(KeyError, match="Unknown body"):
        get_body("supreme_court")


def test_resolve_bodies_accepts_csv_and_falls_back_to_defaults():
    assert [b.slug for b in resolve_bodies("labour_court", [])] == ["labour_court"]
    assert [b.slug for b in resolve_bodies(None, ["labour_court"])] == ["labour_court"]
