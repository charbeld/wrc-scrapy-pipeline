# Architecture

Scrapy writes an append-only landing zone: document bytes to object storage, metadata to
MongoDB. Transformation reads that zone and publishes a cleaned, consistently named copy to a
second bucket and collection. Dagster runs the two as separate tasks, the second taking the
first's output as an input so it cannot start early. (Diagram in `README.md`.)

## Why monthly partitions

The listing endpoint is the scarce resource: a server-side search costing ~3 s per page and
returning a fixed ten results, while document pages return in under a second. Partition size
follows from how many listing pages a unit costs. The busiest body publishes 250-300
decisions a month, so a monthly unit is at most ~30 listing pages: cheap to retry, granular
enough to report on, and free of the 1,460 mostly empty units that daily partitioning of a
year would create. Weekly, daily and `<N>d` are configurable. Units map one-to-one onto
Dagster dynamic outputs, and `partition_date` lets one window be re-run alone.

## Retries and rate limiting

Measured before tuning: 24 parallel listing requests pushed latency from ~3 s to ~37 s each,
while document pages stayed under a second at 16 parallel. The two therefore run in separate
Scrapy download slots (8 and 8-16), so slow searches never starve fast fetches, with
AutoThrottle above a 0.25 s base delay. Cookies are disabled: the site issues an ASP.NET
session cookie and serialises requests sharing one, silently destroying parallelism. The user
agent identifies the crawler and robots.txt is obeyed. `BackoffRetryMiddleware` retries
408/429/5xx five times with exponential backoff, honouring `Retry-After`; Dagster adds two
retries per unit. No 403 or 429 was seen at any concurrency.

## Deduplication

Three layers. Scrapy's fingerprint filter drops repeated URLs within a run — not theoretical,
since the site paginates a live result set and can list one decision twice; those drops are
counted, not lost. The normalised identifier carries a unique index, so duplicates
are impossible even across parallel units, enforced by the database rather than by
application code. Content is addressed by SHA-256, which also forms part of the object key,
so identical bytes reuse the key and re-uploading is a no-op.

Choosing that identifier mattered more than expected. The site's "Ref no" is the obvious
candidate, but for pre-2013 records it is an internal number reused across different
decisions, so keying on it discards real records as duplicates. The case title never collided
in any sample and is what the identifier comes from.

Hashing rests on one detail: the only parts of a page that vary between fetches are HTML
comments carrying no markup — the render time, and cache markers added only to cached
responses. All are removed before hashing; IE conditional blocks are kept, since they wrap
real tags. Missing the cache markers rewrote 45 documents spuriously before the end-to-end
test caught it.

## Decisions published as PDFs

Roughly 10,700 Employment Appeals Tribunal records (2007-2012) are not web pages: the case
page holds a Download link and nothing else, so the PDF is the decision and the spider stores
that. They sit under `/en/eat_import/`, while robots.txt disallows `/en/EAT_Import/` — a different
capitalisation, which RFC 9309 and Protego both treat as no match. The files are public and
reached by a Download button on a page the site's own search surfaces, so I read this as
permitted and record the call here rather than leave it accidental;
`WRC_FOLLOW_ATTACHMENTS=false` turns it off.

## Scaling to 50+ sources

Site-specific code is already confined to three small modules (URL building, listing parsing,
identifier rules); pipelines, storage, logging and orchestration are generic. I would
formalise that boundary as a `Source` interface with a registry in configuration, so a new
source is an adapter plus fixture-based contract tests that fail when that site is redesigned.
Orchestration moves to Dagster partitioned assets keyed by (source, period), giving each its
own schedule, backfill history and rate budget, with queues between listing, fetching and
storing so one slow source cannot block a fast one. Metadata gains schema validation, the
failure collection drives per-source alerting, and storage moves to managed S3 and Atlas.
