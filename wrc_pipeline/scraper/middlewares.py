"""Downloader middleware: retries with exponential backoff.

Scrapy's stock ``RetryMiddleware`` retries immediately. Retrying a struggling
server without pausing makes things worse, so this subclass waits
``base ** attempt`` seconds (capped) before re-issuing the request, and honours a
``Retry-After`` header when the server sends one with a 429.

The wait is scheduled on the Twisted reactor with ``deferLater`` rather than
``time.sleep``: the crawler is single-threaded and event-driven, so sleeping
would freeze every other in-flight request.
"""

from __future__ import annotations

import contextlib
from typing import Any

from scrapy.downloadermiddlewares.retry import RetryMiddleware
from twisted.internet import reactor
from twisted.internet.task import deferLater

from wrc_pipeline.logging_setup import get_logger

logger = get_logger(__name__)


def backoff_delay(attempt: int, base: float, cap: float) -> float:
    """Seconds to wait before retry number ``attempt`` (1-based).

    Pure function so the growth curve can be unit-tested without a reactor.
    """
    if attempt < 1:
        attempt = 1
    return float(min(base**attempt, cap))


class BackoffRetryMiddleware(RetryMiddleware):
    """``RetryMiddleware`` that spaces attempts out over time."""

    def __init__(self, settings: Any) -> None:
        super().__init__(settings)
        self.backoff_base = settings.getfloat("RETRY_BACKOFF_BASE", 2.0)
        self.backoff_cap = settings.getfloat("RETRY_BACKOFF_MAX", 60.0)

    def process_response(self, request, response, spider):
        """Remember a server-supplied ``Retry-After`` before the usual handling."""
        if response.status == 429:
            header = response.headers.get("Retry-After")
            if header:
                # Retry-After may also be an HTTP date; if it will not parse as
                # seconds we simply fall back to our own exponential backoff.
                with contextlib.suppress(ValueError):
                    request.meta["retry_after"] = float(header.decode("latin-1").strip())
        return super().process_response(request, response, spider)

    def _retry(self, request, reason, spider):
        new_request = super()._retry(request, reason, spider)
        if new_request is None:
            # Retries exhausted: Scrapy will propagate the failure to the
            # spider's errback, which records it with its reason.
            return None

        attempt = int(new_request.meta.get("retry_times", 1))
        delay = float(new_request.meta.pop("retry_after", 0.0)) or backoff_delay(
            attempt, self.backoff_base, self.backoff_cap
        )
        logger.debug(
            "request_retry_scheduled",
            url=request.url,
            attempt=attempt,
            delay_seconds=delay,
            reason=str(reason),
        )
        return self._delay(new_request, delay)

    def _delay(self, request, delay: float):
        """Return the request after ``delay`` seconds.

        Isolated in its own method so tests can substitute it without a reactor.
        """
        return deferLater(reactor, delay, lambda: request)
