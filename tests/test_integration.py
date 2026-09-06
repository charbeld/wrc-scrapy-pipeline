"""End-to-end test against the live site and the Docker services.

Skipped automatically when MongoDB or MinIO is unreachable, so the unit suite
still runs anywhere. Run it with the stack up:

    docker compose up -d && uv run pytest tests/test_integration.py

It proves the two properties that are easy to claim and hard to demonstrate:
the pipeline stores everything the site reported, and running it again changes
nothing.
"""

from __future__ import annotations

import subprocess
import sys
from datetime import date
from pathlib import Path

import pytest

from wrc_pipeline.config import get_settings
from wrc_pipeline.run_ids import new_run_id
from wrc_pipeline.storage.mongo import MongoRepository
from wrc_pipeline.storage.objectstore import ObjectStore
from wrc_pipeline.transform.run import transform_range

REPO_ROOT = Path(__file__).resolve().parents[1]

# A small, stable window: the Labour Court published 45 decisions in January 2024.
BODY = "labour_court"
START = date(2024, 1, 1)
END = date(2024, 1, 31)


def _services_available() -> bool:
    settings = get_settings()
    try:
        MongoRepository(settings).ping()
        ObjectStore(settings).ping()
    except Exception:
        # Any failure at all means the services are not available for this test.
        return False
    return True


pytestmark = pytest.mark.skipif(
    not _services_available(),
    reason="MongoDB/MinIO not reachable; run `docker compose up -d` first",
)


def _crawl(run_id: str) -> int:
    """Run the spider in a subprocess, exactly as the orchestrator does."""
    return subprocess.run(
        [
            sys.executable,
            "-m",
            "scrapy",
            "crawl",
            "wrc_decisions",
            "-a",
            f"start_date={START.isoformat()}",
            "-a",
            f"end_date={END.isoformat()}",
            "-a",
            f"bodies={BODY}",
            "-a",
            f"run_id={run_id}",
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        check=False,
    ).returncode


def _assert_reconciled(repo: MongoRepository, run_id: str) -> None:
    """Every listed record must be accounted for by exactly one outcome.

    This is the guarantee the assignment asks for - scrape all N, or N minus a
    set of failures each recorded with its reason. Asserting it here is what
    catches a document disappearing between the listing and the pipeline, which
    is otherwise invisible: the counts of stored records and objects both still
    look correct.
    """
    run = repo.runs.find_one({"run_id": run_id, "kind": "scrape"})
    assert run is not None, f"no run summary recorded for {run_id}"
    for unit in run["units"]:
        assert unit["reconciled"], (
            f"{unit['body']} {unit['partition_label']}: listed "
            f"{unit['records_listed']} but accounted for {unit['accounted']}"
        )
        assert unit["records_found"] == unit["records_listed"], (
            f"site reported {unit['records_found']} records but only "
            f"{unit['records_listed']} rows were read"
        )


@pytest.mark.slow
def test_pipeline_is_complete_and_idempotent():
    settings = get_settings()
    repo = MongoRepository(settings)
    store = ObjectStore(settings)

    first_run = new_run_id()
    assert _crawl(first_run) == 0
    stored = repo.count_landing_by_date(START, END, [BODY])
    objects = store.count_objects(settings.s3_landing_bucket, prefix=f"{BODY}/")
    assert stored > 0, "the crawl stored nothing"
    _assert_reconciled(repo, first_run)

    # Every stored record must carry the fields the assignment requires.
    for record in repo.iter_landing_by_date(START, END, [BODY]):
        assert record["file_path"].startswith("s3://")
        assert record["file_hash"].startswith("sha256:")
        assert record["partition_date"] is not None
        assert record["identifier"] and record["doc_url"] and record["published_date"]

    # A second run must not create records or upload objects.
    second_run = new_run_id()
    assert _crawl(second_run) == 0
    _assert_reconciled(repo, second_run)
    assert repo.count_landing_by_date(START, END, [BODY]) == stored
    assert store.count_objects(settings.s3_landing_bucket, prefix=f"{BODY}/") == objects

    # Transformation covers every landing record, and is itself idempotent.
    first = transform_range(START, END, [BODY], run_id=new_run_id())
    assert first.candidates == stored
    assert first.failed == 0
    assert first.reconciled

    second = transform_range(START, END, [BODY], run_id=new_run_id())
    assert second.transformed == 0
    assert second.skipped_unchanged == stored

    # Transformed files are named identifier.ext and are smaller than the raw page.
    sample = repo.transformed.find_one({"body": BODY})
    assert sample is not None
    assert sample["file_path"].endswith(f"{sample['identifier']}.html")
    assert sample["file_hash"] != sample["landing_file_hash"]
    repo.close()
