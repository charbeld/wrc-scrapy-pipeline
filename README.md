# WRC Scrapy Pipeline

Scrapes decisions and determinations from
[workplacerelations.ie](https://www.workplacerelations.ie/en/search/?advance=true) with
Scrapy, stores the documents in object storage and their metadata in MongoDB, then
publishes a cleaned copy of every document into a second bucket and collection. Dagster
orchestrates ingestion and transformation as two dependent tasks. The pipeline is
idempotent: running it twice over the same dates creates no duplicate records and
re-uploads nothing.

```
workplacerelations.ie ──► Scrapy spider ──┬──► MinIO  landing      ──┐
   (GET search endpoint)   (per body ×    └──► Mongo  landing_documents
                            per month)                               │  transform
                                                                     ▼
                                           MinIO transformed ◄── BeautifulSoup cleaner
                                           Mongo transformed_documents
```

Design rationale is in [ARCHITECTURE.md](ARCHITECTURE.md).

## Prerequisites

* [Docker Desktop](https://www.docker.com/products/docker-desktop/) (running)
* [uv](https://docs.astral.sh/uv/) - installs Python 3.12 and the dependencies for you
* Git

## Setup

```bash
cp .env.example .env
docker compose up -d
uv sync --all-groups
uv run python -m wrc_pipeline verify
```

`verify` pings MongoDB and MinIO, creates the indexes and the two buckets, and prints how
much data is currently stored. If it succeeds, everything else will run.

## Running the pipeline

### With Dagster (the orchestrated path)

```bash
export DAGSTER_HOME="$PWD/dagster_home"   # Windows PowerShell: $env:DAGSTER_HOME="$PWD\dagster_home"
uv run dagster dev
```

Open <http://localhost:3000>, choose the `wrc_ingest_and_transform` job, click
**Launchpad**, edit the dates in the run configuration and launch. Headless:

```bash
uv run dagster job execute -m wrc_pipeline.orchestration.definitions -j wrc_ingest_and_transform -c run_config.yaml
```

Three jobs are defined: `wrc_ingest_and_transform` (the full pipeline),
`wrc_ingest_only` and `wrc_transform_only`.

### From the command line

```bash
uv run python -m wrc_pipeline scrape --start 2024-01-01 --end 2024-03-31 --partition monthly
uv run python -m wrc_pipeline transform --start 2024-01-01 --end 2024-03-31
```

Or call Scrapy directly, which is what both of the above ultimately do:

```bash
uv run scrapy crawl wrc_decisions -a start_date=2024-01-01 -a end_date=2024-01-31 -a bodies=labour_court
```

Spider arguments: `start_date`, `end_date`, `bodies`, `partition`, `refresh_policy`,
`run_id`, `summary_suffix`. All are optional and fall back to `.env`.

## Configuration

Everything is an environment variable (prefix `WRC_`), read once into a validated settings
object. There are no hardcoded connection strings, paths or tuning values in the code.
`.env.example` documents every key; the ones worth knowing:

| Variable | Default | Meaning |
| --- | --- | --- |
| `WRC_START_DATE` / `WRC_END_DATE` | `2024-01-01` / `2024-03-31` | Default date range |
| `WRC_PARTITION` | `monthly` | `monthly`, `weekly`, `daily` or `<N>d` |
| `WRC_BODIES` | all four | Which bodies to scrape |
| `WRC_REFRESH_POLICY` | `hash` | `hash` re-fetches to detect edits; `skip_known` skips known identifiers |
| `WRC_CONCURRENT_REQUESTS` | `16` | Overall Scrapy concurrency |
| `WRC_LISTING_CONCURRENCY` | `8` | Separate budget for slow listing pages |
| `WRC_RETRY_TIMES` | `5` | Retries per request, with exponential backoff |
| `WRC_MONGO_URI` | `mongodb://wrc:...@localhost:27017` | Metadata store |
| `WRC_S3_ENDPOINT` | `http://localhost:9000` | Object store (MinIO locally, S3 in production) |
| `WRC_S3_LANDING_BUCKET` / `WRC_S3_TRANSFORMED_BUCKET` | `landing` / `transformed` | The two zones |
| `WRC_DAGSTER_MAX_PARALLEL_UNITS` | `2` | How many units Dagster runs at once |

## Data model

**MongoDB** (database `wrc`):

| Collection | Contents |
| --- | --- |
| `landing_documents` | One record per decision: identifier, title, description, published date, body, document URL, `partition_date`, `file_path`, `file_hash`, and a `versions` array of superseded versions. Unique index on `identifier`. |
| `transformed_documents` | The cleaned copy: new `file_path`, new `file_hash`, `landing_file_hash`, `transform_version` and extracted fields (parties, acts, officer, hearing date). |
| `scrape_failures` | One row per record that could not be scraped, with URL, stage, error type, HTTP status and reason. |
| `pipeline_runs` | One summary per run with all counters. |

**Object storage**:

| Bucket | Key layout | Rules |
| --- | --- | --- |
| `landing` | `<body>/<partition start>/<hash>/<original filename>` | Append-only. Never deleted or overwritten. Keys are content-addressed, so identical bytes reuse the same key. |
| `transformed` | `<body>/<identifier>.<ext>` | Derived data; rebuilt from landing at any time. |

## Results of the reference run

Four bodies, 1 January to 31 March 2024, monthly partitions: 12 units, run through
`wrc_ingest_and_transform`.

Run from empty: `docker compose down -v && docker compose up -d`, then the job.

| Metric | Value |
| --- | --- |
| Records the site reported (`records_found`) | 895 |
| Listing rows parsed (`records_listed`) | 895 |
| Stored as new | 893 |
| Duplicate listings collapsed | 2 |
| Failed | 0 |
| Unique decisions in `landing_documents` | 893 |
| Objects in the `landing` bucket | 893 |
| Documents transformed | 893 |
| Objects in the `transformed` bucket | 893 |
| Rows in `scrape_failures` | 0 |

Per body: Labour Court 127, Workplace Relations Commission 766, Employment Appeals Tribunal
and Equality Tribunal 0 (both were folded into the other two in 2015, so a 2024 window has
nothing to return). Every unit reconciled and `verify_run` reported no problems.

The two duplicates are real: the site lists some decisions twice inside one result set, and
its own total counts them twice. They are collapsed on the unique identifier and counted as
`duplicates_in_run`, so the difference between 895 and 893 is explained in the summary
rather than looking like two lost records.

### The PDF path, demonstrated

The 2024 range is entirely web pages. To exercise the other kind of record, one month of
Employment Appeals Tribunal decisions was scraped as well:

```bash
uv run scrapy crawl wrc_decisions -a start_date=2008-12-01 -a end_date=2008-12-31 -a bodies=employment_appeals_tribunal
uv run python -m wrc_pipeline transform --start 2008-12-01 --end 2008-12-31
```

87 found, 87 stored, 0 failed. Those case pages hold no decision text, so the spider followed
each Download link and stored the PDF. Taking one record through both zones:

| | |
| --- | --- |
| identifier | `UD1020-2007-WT341-2007` (from the case title) |
| `site_ref` | `31482` (the site's own number, kept but not used as the key) |
| `doc_url` | the case page |
| `attachment_url` | `/en/eat_import/2008/12/674e1a97-...pdf` |
| landing | `s3://landing/employment_appeals_tribunal/2008-12-01/16f1f6d0b98f/674e1a97-...pdf` |
| transformed | `s3://transformed/employment_appeals_tribunal/UD1020-2007-WT341-2007.pdf` |

The transformed bytes are identical to the landing bytes: PDFs are renamed but never altered,
as the brief requires. Re-running both stages reported 0 new and 87 skipped.

Combined store after both ranges: **980 documents and 980 objects in each zone**, 893 HTML and
87 PDF, no rows in `scrape_failures`.

## Idempotency

Re-running a date range does the following, and nothing else:

* **Unchanged document** (hash matches): no download stored, no new object, no metadata
  rewrite; only `last_seen_at` is touched. Logged as `document_unchanged`.
* **Changed document**: uploaded under a new content-addressed key, the previous version is
  pushed onto `versions`, and the old object stays in place.
* **New document**: uploaded and inserted.

A duplicate is impossible regardless: `identifier` carries a unique index. The
transformation applies the same rule using `landing_file_hash` plus `transform_version`.

Demonstrate it:

```bash
uv run python -m wrc_pipeline scrape --start 2024-01-01 --end 2024-01-31 --bodies labour_court
uv run python -m wrc_pipeline scrape --start 2024-01-01 --end 2024-01-31 --bodies labour_court
```

The second run reports `"new": 0, "changed": 0, "unchanged": 45`.

The same holds for the whole pipeline. Running `wrc_ingest_and_transform` a second time over
the reference range reported `new: 0, changed: 0, unchanged: 893` and
`transformed: 0, skipped_unchanged: 893`, with documents and objects both still at 893 in
each zone and no rows in `scrape_failures`. Both figures come from a run started with the
volumes destroyed, so nothing carried over from earlier work.

## Logging

Structured JSON, one object per line, to stdout and to `logs/<run_id>.jsonl`. Every event
carries the `run_id`, and events inside a unit also carry `body` and `partition_label`.

| Event | When |
| --- | --- |
| `run_started`, `partition_started` | Start of a run and of each unit |
| `listing_page_parsed` | A search page was read (`records_found` on page 1) |
| `document_stored` | A document was uploaded (`new` or `changed`) |
| `document_unchanged`, `document_skipped_known` | Nothing to do for this document |
| `document_failed`, `listing_failed` | With URL, HTTP status or error type, and reason |
| `listing_incomplete`, `reconciliation_failed` | Counters do not add up |
| `partition_finished` | Per-unit counters |
| `run_summary` | End-of-run totals |
| `transform_started`, `document_transformed`, `document_transform_skipped`, `document_transform_failed`, `transform_summary` | Transformation |

Each run also writes `logs/<run_id>.summary.json`. Useful queries:

```bash
jq 'select(.event=="partition_finished")' logs/<run_id>.jsonl
jq 'select(.event=="document_failed") | {url, http_status, error_type}' logs/<run_id>.jsonl
```

Every unit reports `records_found` (what the site said), `records_listed`, and the split
into `new`, `changed`, `unchanged`, `skipped_known`, `failed` and `duplicates_in_run`.
These must add up; `verify_run` fails the Dagster job if they do not. That is how the
pipeline proves it scraped everything, or everything minus a set of failures each recorded
with its reason.

## Inspecting the results

MinIO console: <http://localhost:9001> (credentials from `.env`).

```bash
docker exec -it wrc-mongo mongosh -u wrc -p wrc_password --authenticationDatabase admin
```

```javascript
use wrc
db.landing_documents.countDocuments()
db.landing_documents.findOne({ identifier: "UDD242" })
db.landing_documents.aggregate([{ $group: { _id: "$body", n: { $sum: 1 } } }])
db.scrape_failures.find({ run_id: "<run id>" })
db.pipeline_runs.find().sort({ recorded_at: -1 }).limit(1)
```

## Tests

```bash
uv run pytest                          # 67 unit tests, no network, no Docker
uv run pytest -m slow                   # end-to-end, needs Docker and the live site
uv run ruff check . && uv run ruff format --check .
```

Unit tests run the parsers against real pages saved in `tests/fixtures/`, so a site
redesign fails a test instead of silently scraping nothing. The integration test crawls one
month, re-crawls it and asserts nothing changed, then transforms twice with the same check.

## How the site behaves, and what the pipeline does about it

Each of these was measured against the live site, not assumed.

**Two kinds of decision.** Most are web pages. But roughly 10,700 Employment Appeals
Tribunal records from 2007 to 2012 are not: the case page carries no decision text at all,
only a reference, a file size and a Download link. For those the PDF *is* the decision, so
the spider follows the link and stores the file byte for byte. `doc_type` records which kind
each is, and `attachment_url` records where a followed file came from.

**"Ref no" is not the identifier.** It is the obvious candidate and the site labels it that
way, but for those same older records it holds an internal number that is reused across
different decisions - two December 2008 cases both carry `33397`. Keying on it silently
discards one of them. The identifier comes from the case title, which never collided in any
sample; the site's own reference is kept as `site_ref`.

**Page size is fixed at ten** and the site ignores any page-size parameter (thirteen
spellings tried). A large result set therefore costs one request per ten records. The count
on page 1 is what lets pages 2..N be fetched in parallel instead of sequentially.

**Change detection needs the fetch.** Case pages send no `ETag` or `Last-Modified`, and
conditional requests are ignored, so the only way to know whether a decision was edited is
to download and compare hashes. `WRC_REFRESH_POLICY=skip_known` trades that guarantee for
speed. The PDFs do carry both headers, which is a cheap optimisation still on the table.

**Responses vary in ways that are not content.** Pages carry a render-time comment, and
cache markers that appear only when the response came from cache. All comments without
markup of their own are stripped before hashing, otherwise every document looks edited on
every run.

**The same decision can be listed twice** within one search, because the site paginates a
live result set. Those are collapsed on the identifier and reported as `duplicates_in_run`,
including the ones Scrapy's duplicate filter discards before they reach the pipeline.

**Two bodies are historical.** The Equality Tribunal and the Employment Appeals Tribunal were
folded into the WRC and the Labour Court, so recent partitions for them return zero records.
That is correct, not a failure.

**The transformation slices on publication date** by default; set
`WRC_TRANSFORM_DATE_FIELD=partition_date` to line it up with the window that scraped the
records instead.

## Resetting

```bash
docker compose down        # stop containers, keep the data
docker compose down -v     # DESTRUCTIVE: also deletes the Mongo and MinIO volumes
```
