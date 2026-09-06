"""Structured (JSON) logging for every component.

The assignment asks for machine-readable logs that always say which partition
and which body is being processed, how many records were found versus scraped,
which downloads failed with which error code, and a summary per run.

Two design points worth explaining
----------------------------------
1. **structlog context variables.** ``bind_run_context`` binds ``run_id``,
   ``body`` and ``partition_label`` once per unit of work; every event logged
   afterwards carries those fields automatically, so no call site has to repeat
   them and no event can forget them.
2. **One pipe for everything.** Scrapy, pymongo and botocore log through the
   standard library. We install ``structlog``'s ``ProcessorFormatter`` on the
   root logger so those records come out as the same JSON objects, rather than
   half the output being JSON and half plain text.

The module is named ``logging_setup`` rather than ``logging`` so it can never be
confused with the standard-library module.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Any

import structlog

#: Loggers that are useful at DEBUG but pure noise at INFO.
_NOISY_LOGGERS = (
    "scrapy.core.engine",
    "scrapy.core.scraper",
    "scrapy.downloadermiddlewares.retry",
    "scrapy.spidermiddlewares.httperror",
    "scrapy.statscollectors",
    "scrapy.extensions.logstats",
    "scrapy.middleware",
    "scrapy.addons",
    "scrapy.crawler",
    "scrapy.utils.log",
    "pymongo",
    "botocore",
    "boto3",
    "urllib3",
    "s3transfer",
    "filelock",
)

_CONFIGURED = False


def configure_logging(
    *,
    level: str = "INFO",
    json_output: bool = True,
    log_dir: Path | None = None,
    run_id: str | None = None,
    file_suffix: str = "",
) -> None:
    """Configure structlog and the standard library to emit one JSON stream.

    Args:
        level: Root log level (``INFO`` by default, ``DEBUG`` for diagnosis).
        json_output: ``True`` for JSON lines, ``False`` for a colourised console
            renderer while developing.
        log_dir: Directory for the run's ``.jsonl`` file. ``None`` disables the
            file handler (used by tests).
        run_id: Names the log file ``<run_id><file_suffix>.jsonl``.
        file_suffix: Extra suffix so parallel units of the same run do not write
            to the same file.
    """
    global _CONFIGURED

    timestamper = structlog.processors.TimeStamper(fmt="iso", utc=True)
    shared_processors: list[Any] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        timestamper,
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
    ]

    structlog.configure(
        processors=[
            *shared_processors,
            # Hands the event dict to the stdlib formatter below, which is what
            # lets Scrapy's records and ours share a single renderer.
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    renderer = (
        structlog.processors.JSONRenderer()
        if json_output
        else structlog.dev.ConsoleRenderer(colors=False)
    )
    console_formatter = structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=shared_processors,
        processors=[structlog.stdlib.ProcessorFormatter.remove_processors_meta, renderer],
    )
    # The file always gets JSON, even when the console is human-readable.
    file_formatter = structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=shared_processors,
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            structlog.processors.JSONRenderer(),
        ],
    )

    root = logging.getLogger()
    for handler in list(root.handlers):
        root.removeHandler(handler)

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(console_formatter)
    root.addHandler(stream_handler)

    if log_dir is not None and run_id:
        log_dir = Path(log_dir)
        log_dir.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(
            log_dir / f"{run_id}{file_suffix}.jsonl", encoding="utf-8"
        )
        file_handler.setFormatter(file_formatter)
        root.addHandler(file_handler)

    root.setLevel(level.upper())
    if level.upper() != "DEBUG":
        for name in _NOISY_LOGGERS:
            logging.getLogger(name).setLevel(logging.WARNING)

    _CONFIGURED = True


def bind_run_context(**kwargs: Any) -> None:
    """Attach fields to every subsequent log event in this context."""
    structlog.contextvars.bind_contextvars(**kwargs)


def unbind_run_context(*keys: str) -> None:
    """Remove previously bound fields."""
    structlog.contextvars.unbind_contextvars(*keys)


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    """Return a bound logger, configuring logging with defaults if needed."""
    if not _CONFIGURED:
        configure_logging()
    return structlog.get_logger(name)
