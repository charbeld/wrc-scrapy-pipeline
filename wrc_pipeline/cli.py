"""Command-line entry point: ``python -m wrc_pipeline <command>``.

Three commands:

``verify``     check that MongoDB and the object store are reachable and set up.
``scrape``     run the spider for a date range (thin wrapper over ``scrapy crawl``).
``transform``  run the transformation for a date range.

The orchestrated path is Dagster; this CLI exists so every stage can also be run
and debugged on its own.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from datetime import date

from wrc_pipeline.config import get_settings
from wrc_pipeline.logging_setup import configure_logging, get_logger
from wrc_pipeline.run_ids import new_run_id
from wrc_pipeline.storage.mongo import MongoRepository
from wrc_pipeline.storage.objectstore import ObjectStore
from wrc_pipeline.transform.run import transform_range

logger = get_logger(__name__)


def _add_range_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--start", help="First day to process, ISO format (YYYY-MM-DD).")
    parser.add_argument("--end", help="Last day to process, inclusive, ISO format.")
    parser.add_argument("--bodies", help="Comma-separated body slugs; default is all four.")
    parser.add_argument("--run-id", help="Reuse an existing run id (set by the orchestrator).")


def cmd_verify(_: argparse.Namespace) -> int:
    """Check connectivity and create indexes and buckets."""
    settings = get_settings()
    repo = MongoRepository(settings)
    store = ObjectStore(settings)

    repo.ping()
    repo.ensure_indexes()
    logger.info("mongo_ready", uri=_redact(settings.mongo_uri), database=settings.mongo_db)

    store.ping()
    store.ensure_buckets()
    logger.info(
        "object_store_ready",
        endpoint=settings.s3_endpoint,
        buckets=[settings.s3_landing_bucket, settings.s3_transformed_bucket],
    )

    logger.info(
        "verify_ok",
        landing_documents=repo.landing.estimated_document_count(),
        transformed_documents=repo.transformed.estimated_document_count(),
        landing_objects=store.count_objects(settings.s3_landing_bucket),
        transformed_objects=store.count_objects(settings.s3_transformed_bucket),
    )
    repo.close()
    return 0


def cmd_scrape(args: argparse.Namespace) -> int:
    """Run the spider as a child process.

    Scrapy owns a Twisted reactor that cannot be restarted inside a process, so
    invoking it as a subprocess keeps this CLI (and Dagster) free of that
    constraint.
    """
    settings = get_settings()
    command = [
        sys.executable,
        "-m",
        "scrapy",
        "crawl",
        "wrc_decisions",
        "-a",
        f"start_date={args.start or settings.start_date}",
        "-a",
        f"end_date={args.end or settings.end_date}",
        "-a",
        f"partition={args.partition or settings.partition}",
        "-a",
        f"refresh_policy={args.refresh_policy or settings.refresh_policy}",
        "-a",
        f"run_id={args.run_id or new_run_id()}",
    ]
    if args.bodies:
        command += ["-a", f"bodies={args.bodies}"]
    return subprocess.run(command, check=False).returncode


def cmd_transform(args: argparse.Namespace) -> int:
    settings = get_settings()
    start = date.fromisoformat(args.start or settings.start_date)
    end = date.fromisoformat(args.end or settings.end_date)
    bodies = [b.strip() for b in args.bodies.split(",")] if args.bodies else None
    summary = transform_range(
        start=start,
        end=end,
        bodies=bodies,
        run_id=args.run_id or new_run_id(),
        force=args.force,
        settings=settings,
    )
    return 0 if summary.reconciled and summary.failed == 0 else 1


def _redact(uri: str) -> str:
    """Hide the password in a connection string before it reaches a log."""
    if "@" not in uri or "//" not in uri:
        return uri
    scheme, _, rest = uri.partition("//")
    credentials, _, host = rest.partition("@")
    user = credentials.split(":", 1)[0]
    return f"{scheme}//{user}:***@{host}"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="wrc",
        description="Scrape and transform decisions from workplacerelations.ie.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("verify", help="Check storage connectivity and create indexes/buckets.")

    scrape = subparsers.add_parser("scrape", help="Run the spider for a date range.")
    _add_range_args(scrape)
    scrape.add_argument("--partition", help="monthly | weekly | daily | <N>d")
    scrape.add_argument("--refresh-policy", help="hash | skip_known")

    transform = subparsers.add_parser("transform", help="Transform landing data for a date range.")
    _add_range_args(transform)
    transform.add_argument(
        "--force",
        action="store_true",
        help="Reprocess documents even when nothing changed.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    settings = get_settings()
    configure_logging(
        level=settings.log_level,
        json_output=settings.log_json,
        log_dir=settings.log_dir,
        run_id=getattr(args, "run_id", None) or new_run_id(),
        file_suffix=f".{args.command}",
    )
    handlers = {"verify": cmd_verify, "scrape": cmd_scrape, "transform": cmd_transform}
    return handlers[args.command](args)


if __name__ == "__main__":
    raise SystemExit(main())
