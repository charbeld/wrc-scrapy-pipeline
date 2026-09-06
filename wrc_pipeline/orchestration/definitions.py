"""Dagster orchestration: ingestion and transformation as separate, ordered tasks.

The job
-------
::

    build_plan ──► build_units ─┬─► scrape_partition (unit 1) ─┐
                                ├─► scrape_partition (unit 2) ─┼─► collect_scrape_results
                                └─► scrape_partition (unit N) ─┘          │
                                                                          ▼
                                              verify_run ◄── transform_landing

Dependency handling is structural, not a convention: ``transform_landing`` takes
the output of ``collect_scrape_results`` as an input, so Dagster cannot start the
transformation until every scrape unit has finished successfully.

``build_units`` emits one dynamic output per (body, partition) pair, so the
number of parallel tasks follows the requested date range instead of being fixed
in the graph.

Why the spider runs in a subprocess
-----------------------------------
Scrapy drives a Twisted reactor, and a reactor cannot be started twice in one
process. Dagster may run several ops in the same process, so launching
``python -m scrapy crawl`` as a child process is the reliable pattern. The op
reads back the JSON summary the spider writes and turns it into its output.
"""

# NOTE: no `from __future__ import annotations` here on purpose. Dagster inspects
# the real annotation objects to build the run-config schema from PipelineConfig,
# and postponed (string) annotations make that resolution fail at import time.

import json
import os
import subprocess
import sys
from datetime import date
from pathlib import Path
from typing import Any

from dagster import (
    AssetMaterialization,
    Backoff,
    Config,
    Definitions,
    DynamicOut,
    DynamicOutput,
    Failure,
    MetadataValue,
    OpExecutionContext,
    Out,
    RetryPolicy,
    job,
    multiprocess_executor,
    op,
)

from wrc_pipeline.bodies import resolve_bodies
from wrc_pipeline.config import get_settings
from wrc_pipeline.partitions import build_partitions
from wrc_pipeline.transform.run import transform_range

#: Repository root (where scrapy.cfg lives); the spider subprocess runs here.
REPO_ROOT = Path(__file__).resolve().parents[2]


class PipelineConfig(Config):
    """Run configuration, with defaults taken from the environment.

    Exposed in the Dagster UI as a typed form, so an operator can launch a
    backfill without touching code or environment variables.
    """

    start_date: str = get_settings().start_date
    end_date: str = get_settings().end_date
    bodies: str = get_settings().bodies
    partition: str = get_settings().partition
    refresh_policy: str = get_settings().refresh_policy


@op(out=Out(dict), description="Validate the run configuration and expand it into a plan.")
def build_plan(context: OpExecutionContext, config: PipelineConfig) -> dict[str, Any]:
    settings = get_settings()
    start = date.fromisoformat(config.start_date)
    end = date.fromisoformat(config.end_date)
    bodies = resolve_bodies(config.bodies, settings.body_slugs)
    partitions = build_partitions(start, end, config.partition)

    plan = {
        # Every unit of this job shares the Dagster run id, so all logs, Mongo
        # records and summary files can be traced back to one execution.
        "run_id": context.run_id,
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "partition": config.partition,
        "refresh_policy": config.refresh_policy,
        "bodies": [body.slug for body in bodies],
        "partition_labels": [p.label for p in partitions],
        "n_units": len(bodies) * len(partitions),
    }
    context.log.info(f"Planned {plan['n_units']} units: {plan}")
    return plan


@op(
    out=DynamicOut(dict),
    description="Emit one dynamic output per (body, partition) unit of work.",
)
def build_units(plan: dict[str, Any]):
    settings = get_settings()
    start = date.fromisoformat(plan["start_date"])
    end = date.fromisoformat(plan["end_date"])
    for partition in build_partitions(start, end, plan["partition"]):
        for body in resolve_bodies(plan["bodies"], settings.body_slugs):
            # Mapping keys must be alphanumeric/underscore for Dagster.
            mapping_key = f"{body.slug}__{partition.label.replace('-', '_')}"
            yield DynamicOutput(
                value={
                    "run_id": plan["run_id"],
                    "body": body.slug,
                    "partition_start": partition.start.isoformat(),
                    "partition_end": partition.end.isoformat(),
                    "partition_label": partition.label,
                    "refresh_policy": plan["refresh_policy"],
                    "mapping_key": mapping_key,
                },
                mapping_key=mapping_key,
            )


@op(
    out=Out(dict),
    # A whole unit failing is usually transient (a slow listing page, a restart
    # of the site). Retry it before failing the job.
    retry_policy=RetryPolicy(max_retries=2, delay=30, backoff=Backoff.EXPONENTIAL),
    description="Scrape one (body, partition) unit with Scrapy.",
)
def scrape_partition(context: OpExecutionContext, unit: dict[str, Any]) -> dict[str, Any]:
    settings = get_settings()
    suffix = f".{unit['mapping_key']}"
    command = [
        sys.executable,
        "-m",
        "scrapy",
        "crawl",
        "wrc_decisions",
        "-a",
        f"start_date={unit['partition_start']}",
        "-a",
        f"end_date={unit['partition_end']}",
        "-a",
        f"bodies={unit['body']}",
        # The window is already one partition wide; 'daily' would re-split it.
        "-a",
        "partition=monthly",
        "-a",
        f"refresh_policy={unit['refresh_policy']}",
        "-a",
        f"run_id={unit['run_id']}",
        "-a",
        f"summary_suffix={suffix}",
    ]
    context.log.info(f"Running: {' '.join(command)}")
    completed = subprocess.run(
        command,
        cwd=REPO_ROOT,
        env={**os.environ, "PYTHONUTF8": "1"},
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    if completed.stdout:
        context.log.info(completed.stdout[-20000:])
    if completed.stderr:
        context.log.warning(completed.stderr[-20000:])

    summary_path = Path(settings.log_dir) / f"{unit['run_id']}{suffix}.summary.json"
    if completed.returncode != 0 or not summary_path.exists():
        raise Failure(
            description=(
                f"Scrapy failed for {unit['mapping_key']} "
                f"(exit code {completed.returncode}, summary written: {summary_path.exists()})"
            )
        )

    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    totals = summary["totals"]
    context.add_output_metadata(
        {
            "unit": MetadataValue.text(unit["mapping_key"]),
            "records_found": MetadataValue.int(totals["records_found"]),
            "scraped": MetadataValue.int(totals["scraped"]),
            "failed": MetadataValue.int(totals["failed"]),
        }
    )
    # A failed document is data, not an op failure: it is recorded with its
    # reason in MongoDB and surfaces in the reconciliation check at the end.
    return summary


@op(out=Out(dict), description="Aggregate every scrape unit into one run summary.")
def collect_scrape_results(
    context: OpExecutionContext,
    plan: dict[str, Any],
    results: list[dict[str, Any]],
) -> dict[str, Any]:
    keys = (
        "records_found",
        "records_listed",
        "new",
        "changed",
        "unchanged",
        "skipped_known",
        "failed",
        "duplicates_in_run",
        "scraped",
    )
    totals = {key: sum(result["totals"].get(key, 0) for result in results) for key in keys}
    per_body: dict[str, dict[str, int]] = {}
    units: list[dict[str, Any]] = []
    for result in results:
        for unit in result["units"]:
            units.append(unit)
            bucket = per_body.setdefault(
                unit["body"], {"records_found": 0, "scraped": 0, "failed": 0}
            )
            bucket["records_found"] += unit["records_found"] or 0
            bucket["scraped"] += unit["new"] + unit["changed"] + unit["unchanged"]
            bucket["failed"] += unit["failed"]

    summary = {
        "run_id": plan["run_id"],
        "kind": "scrape_job",
        "start_date": plan["start_date"],
        "end_date": plan["end_date"],
        "partition": plan["partition"],
        "bodies": plan["bodies"],
        "units": units,
        "per_body": per_body,
        "totals": totals,
        "all_units_reconciled": all(unit["reconciled"] for unit in units),
    }
    context.log.info(json.dumps(summary["totals"], indent=2))
    context.log_event(
        AssetMaterialization(
            asset_key="landing_documents",
            description="Raw documents and metadata in the landing zone.",
            metadata={
                "records_found": MetadataValue.int(totals["records_found"]),
                "scraped": MetadataValue.int(totals["scraped"]),
                "failed": MetadataValue.int(totals["failed"]),
                "units": MetadataValue.int(len(units)),
            },
        )
    )
    return summary


@op(out=Out(dict), description="Clean and republish the landing data (runs after all scraping).")
def transform_landing(
    context: OpExecutionContext,
    plan: dict[str, Any],
    scrape_summary: dict[str, Any],
) -> dict[str, Any]:
    # `scrape_summary` is an explicit input purely to order the graph: the
    # transformation must not start until every scrape unit has completed.
    del scrape_summary
    summary = transform_range(
        start=date.fromisoformat(plan["start_date"]),
        end=date.fromisoformat(plan["end_date"]),
        bodies=plan["bodies"],
        run_id=plan["run_id"],
    ).as_dict()
    context.log.info(json.dumps(summary, indent=2))
    context.log_event(
        AssetMaterialization(
            asset_key="transformed_documents",
            description="Cleaned documents renamed to identifier.ext.",
            metadata={
                "candidates": MetadataValue.int(summary["candidates"]),
                "transformed": MetadataValue.int(summary["transformed"]),
                "skipped_unchanged": MetadataValue.int(summary["skipped_unchanged"]),
                "failed": MetadataValue.int(summary["failed"]),
            },
        )
    )
    return summary


@op(description="Fail the run if the counters do not reconcile.")
def verify_run(
    context: OpExecutionContext,
    scrape_summary: dict[str, Any],
    transform_summary: dict[str, Any],
) -> None:
    problems: list[str] = []

    for unit in scrape_summary["units"]:
        if not unit["reconciled"]:
            problems.append(
                f"{unit['body']} {unit['partition_label']}: listed {unit['records_listed']} "
                f"but accounted for {unit['accounted']}"
            )
        found = unit["records_found"]
        scraped = unit["new"] + unit["changed"] + unit["unchanged"] + unit["skipped_known"]
        if found is not None and found != scraped + unit["failed"] + unit["duplicates_in_run"]:
            problems.append(
                f"{unit['body']} {unit['partition_label']}: site reported {found} records, "
                f"pipeline accounted for {scraped + unit['failed'] + unit['duplicates_in_run']}"
            )

    if not transform_summary["reconciled"]:
        problems.append(
            f"transform: {transform_summary['candidates']} candidates but "
            f"{transform_summary['transformed']} transformed + "
            f"{transform_summary['skipped_unchanged']} skipped + "
            f"{transform_summary['failed']} failed"
        )

    context.log.info(
        json.dumps(
            {
                "event": "pipeline_summary",
                "run_id": scrape_summary["run_id"],
                "scrape_totals": scrape_summary["totals"],
                "per_body": scrape_summary["per_body"],
                "transform": {
                    key: transform_summary[key]
                    for key in ("candidates", "transformed", "skipped_unchanged", "failed")
                },
                "problems": problems,
            },
            indent=2,
        )
    )
    if problems:
        raise Failure(description="Reconciliation failed:\n" + "\n".join(problems))


@op(out=Out(dict), description="Placeholder so the transform-only job shares one graph shape.")
def _empty_scrape_summary() -> dict[str, Any]:
    return {"units": [], "totals": {}}


# The executor runs ops in separate processes; max_concurrent bounds how many
# scrape units hit the website at once (each is itself a concurrent crawler).
_EXECUTOR = multiprocess_executor.configured(
    {"max_concurrent": get_settings().dagster_max_parallel_units}
)


@job(
    executor_def=_EXECUTOR,
    description="Scrape the landing zone, then transform it. The full pipeline.",
)
def wrc_ingest_and_transform() -> None:
    plan = build_plan()
    results = build_units(plan).map(scrape_partition).collect()
    scrape_summary = collect_scrape_results(plan, results)
    verify_run(scrape_summary, transform_landing(plan, scrape_summary))


@job(executor_def=_EXECUTOR, description="Ingestion only: scrape into the landing zone.")
def wrc_ingest_only() -> None:
    plan = build_plan()
    results = build_units(plan).map(scrape_partition).collect()
    collect_scrape_results(plan, results)


@job(description="Transformation only: rebuild the derived zone from the landing zone.")
def wrc_transform_only() -> None:
    plan = build_plan()
    transform_landing(plan, _empty_scrape_summary())


defs = Definitions(
    jobs=[wrc_ingest_and_transform, wrc_ingest_only, wrc_transform_only],
)
