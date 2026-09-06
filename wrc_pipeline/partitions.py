"""Split a date range into the time windows the scraper iterates over.

Why partition at all
--------------------
The assignment requires iterating "on a time-period basis" between two dates and
stamping every record with a ``partition_date``. Beyond that requirement,
partitioning is what makes the pipeline operable:

* each unit of work is bounded (the busiest body publishes ~250-300 decisions a
  month, i.e. at most ~30 listing pages), so a failure is cheap to retry;
* units are independent, so an orchestrator can run several in parallel;
* progress and reconciliation are reported per partition rather than per run.

This module is deliberately free of I/O so it can be unit-tested exhaustively.
"""

from __future__ import annotations

import calendar
import re
from dataclasses import dataclass
from datetime import date, timedelta

#: ``10d`` style partition specification.
_DAYS_SPEC = re.compile(r"^(\d+)\s*d$", re.IGNORECASE)


@dataclass(frozen=True)
class Partition:
    """A closed date interval ``[start, end]`` (both bounds inclusive).

    Inclusive on both ends because the website's own ``from``/``to`` filter is
    inclusive: ``from=30/06/2025&to=30/06/2025`` returns that day's decisions.
    """

    start: date
    end: date

    @property
    def label(self) -> str:
        """Stable human-readable id, e.g. ``2024-01-01_2024-01-31``."""
        return f"{self.start.isoformat()}_{self.end.isoformat()}"

    @property
    def partition_date(self) -> date:
        """The value stored on every record: the window's first day."""
        return self.start


def _month_end(day: date) -> date:
    """Last calendar day of ``day``'s month."""
    return day.replace(day=calendar.monthrange(day.year, day.month)[1])


def _next_month_start(day: date) -> date:
    """First day of the month following ``day``'s month."""
    if day.month == 12:
        return date(day.year + 1, 1, 1)
    return date(day.year, day.month + 1, 1)


def build_partitions(start: date, end: date, spec: str = "monthly") -> list[Partition]:
    """Split ``[start, end]`` into contiguous, non-overlapping windows.

    Args:
        start: First day to scrape (inclusive).
        end: Last day to scrape (inclusive).
        spec: ``monthly``, ``weekly``, ``daily`` or ``<N>d`` (e.g. ``10d``).

    Returns:
        Windows in chronological order. The first and last are clipped to
        ``start``/``end``, so the union of the windows is exactly the input
        range with no gaps and no overlaps.

    Raises:
        ValueError: if ``start > end`` or the spec is not recognised.
    """
    if start > end:
        raise ValueError(f"start_date {start} is after end_date {end}")

    normalised = spec.strip().lower()
    if normalised == "monthly":
        return _build_monthly(start, end)

    if normalised == "weekly":
        step = 7
    elif normalised == "daily":
        step = 1
    else:
        match = _DAYS_SPEC.match(normalised)
        if not match or int(match.group(1)) < 1:
            raise ValueError(
                f"Unknown partition spec {spec!r}. Use 'monthly', 'weekly', 'daily' or '<N>d'."
            )
        step = int(match.group(1))
    return _build_fixed_length(start, end, step)


def _build_monthly(start: date, end: date) -> list[Partition]:
    """Calendar months clipped to the requested range."""
    partitions: list[Partition] = []
    cursor = start
    while cursor <= end:
        window_end = min(_month_end(cursor), end)
        partitions.append(Partition(start=cursor, end=window_end))
        cursor = _next_month_start(cursor)
    return partitions


def _build_fixed_length(start: date, end: date, days: int) -> list[Partition]:
    """Fixed-length windows counted from ``start``."""
    partitions: list[Partition] = []
    cursor = start
    while cursor <= end:
        window_end = min(cursor + timedelta(days=days - 1), end)
        partitions.append(Partition(start=cursor, end=window_end))
        cursor = window_end + timedelta(days=1)
    return partitions
