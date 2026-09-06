"""Partitioning is pure logic, so it is tested exhaustively.

The properties that matter operationally: windows must cover the whole range,
never overlap, and never leak outside the requested bounds. A gap would silently
lose records; an overlap would waste requests.
"""

from __future__ import annotations

from datetime import date, timedelta
from itertools import pairwise

import pytest

from wrc_pipeline.partitions import Partition, build_partitions


def test_monthly_partitions_span_calendar_months():
    partitions = build_partitions(date(2024, 1, 1), date(2024, 3, 31), "monthly")
    assert [(p.start, p.end) for p in partitions] == [
        (date(2024, 1, 1), date(2024, 1, 31)),
        (date(2024, 2, 1), date(2024, 2, 29)),  # 2024 is a leap year
        (date(2024, 3, 1), date(2024, 3, 31)),
    ]


def test_monthly_partitions_are_clipped_to_the_requested_range():
    partitions = build_partitions(date(2024, 1, 15), date(2024, 2, 10), "monthly")
    assert [(p.start, p.end) for p in partitions] == [
        (date(2024, 1, 15), date(2024, 1, 31)),
        (date(2024, 2, 1), date(2024, 2, 10)),
    ]


def test_monthly_partition_crossing_a_year_boundary():
    partitions = build_partitions(date(2024, 12, 1), date(2025, 1, 31), "monthly")
    assert [p.label for p in partitions] == [
        "2024-12-01_2024-12-31",
        "2025-01-01_2025-01-31",
    ]


def test_single_day_range_produces_one_partition():
    partitions = build_partitions(date(2025, 6, 30), date(2025, 6, 30), "monthly")
    assert partitions == [Partition(date(2025, 6, 30), date(2025, 6, 30))]


@pytest.mark.parametrize(
    ("spec", "expected_first", "expected_count"),
    [
        ("weekly", (date(2024, 1, 1), date(2024, 1, 7)), 5),
        ("daily", (date(2024, 1, 1), date(2024, 1, 1)), 31),
        ("10d", (date(2024, 1, 1), date(2024, 1, 10)), 4),
    ],
)
def test_fixed_length_specs(spec, expected_first, expected_count):
    partitions = build_partitions(date(2024, 1, 1), date(2024, 1, 31), spec)
    assert (partitions[0].start, partitions[0].end) == expected_first
    assert len(partitions) == expected_count


@pytest.mark.parametrize("spec", ["monthly", "weekly", "daily", "10d"])
def test_partitions_cover_the_range_without_gaps_or_overlaps(spec):
    start, end = date(2023, 11, 17), date(2024, 3, 5)
    partitions = build_partitions(start, end, spec)

    assert partitions[0].start == start
    assert partitions[-1].end == end
    for earlier, later in pairwise(partitions):
        assert earlier.end < later.start, "windows overlap"
        assert later.start == earlier.end + timedelta(days=1), "gap between windows"

    covered = sum((p.end - p.start).days + 1 for p in partitions)
    assert covered == (end - start).days + 1


def test_partition_date_is_the_window_start():
    partition = build_partitions(date(2024, 5, 3), date(2024, 5, 20), "monthly")[0]
    assert partition.partition_date == date(2024, 5, 3)
    assert partition.label == "2024-05-03_2024-05-20"


def test_start_after_end_is_rejected():
    with pytest.raises(ValueError, match="after end_date"):
        build_partitions(date(2024, 2, 1), date(2024, 1, 1), "monthly")


@pytest.mark.parametrize("spec", ["", "yearly", "0d", "monthlyish"])
def test_unknown_spec_is_rejected(spec):
    with pytest.raises(ValueError):
        build_partitions(date(2024, 1, 1), date(2024, 1, 31), spec)
