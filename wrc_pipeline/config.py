"""Single source of configuration for the whole pipeline.

Why this module exists
----------------------
The assignment requires that every connection string, storage path, partition
size and scraping parameter is configurable without touching code. We follow the
twelve-factor rule: configuration lives in the environment, code reads it once
into a validated object.

`pydantic-settings` gives us validation for free: a typo such as
``WRC_RETRY_TIMES=five`` fails at startup with a clear message instead of
crashing an hour into a run.

Everything is read from environment variables prefixed with ``WRC_``; a local
``.env`` file is loaded automatically (see ``.env.example``).
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Typed view over the environment. Instantiate via :func:`get_settings`."""

    model_config = SettingsConfigDict(
        env_prefix="WRC_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    # --- Target website ---------------------------------------------------
    base_url: str = "https://www.workplacerelations.ie"
    search_path: str = "/en/search/"
    #: The site renders exactly 10 results per page and ignores any page-size
    #: parameter. We only use this to compute how many pages a result set has.
    page_size: int = 10
    #: Comma-separated body slugs; parsed by :meth:`body_slugs`.
    bodies: str = (
        "employment_appeals_tribunal,equality_tribunal,labour_court,workplace_relations_commission"
    )
    #: The site's date filter format (``31/12/2024``).
    date_format_site: str = "%d/%m/%Y"

    # --- Run defaults -----------------------------------------------------
    start_date: str = "2024-01-01"
    end_date: str = "2024-03-31"
    partition: str = "monthly"
    #: ``hash`` re-fetches known documents to detect edits; ``skip_known`` skips
    #: them entirely (faster incremental runs, cannot see edits).
    refresh_policy: str = "hash"
    #: Follow a case page's Download link when the page holds no decision text.
    #: Needed for the ~10,700 Employment Appeals Tribunal records whose decision
    #: exists only as a PDF; set false to store the (empty) case page instead.
    follow_attachments: bool = True

    # --- Scrapy tuning ----------------------------------------------------
    user_agent: str = "wrc-scrapy-pipeline/1.0 (+https://github.com/charbeld/wrc-scrapy-pipeline)"
    concurrent_requests: int = 16
    concurrent_requests_per_domain: int = 8
    listing_concurrency: int = 8
    download_delay: float = 0.25
    autothrottle_enabled: bool = True
    autothrottle_start_delay: float = 0.5
    autothrottle_max_delay: float = 10.0
    autothrottle_target_concurrency: float = 6.0
    retry_times: int = 5
    retry_http_codes: str = "408,429,500,502,503,504,522,524"
    retry_backoff_base: float = 2.0
    retry_backoff_max: float = 60.0
    download_timeout: int = 90
    robotstxt_obey: bool = True
    httpcache_enabled: bool = False
    httpcache_expiration_secs: int = 86400

    # --- MongoDB ----------------------------------------------------------
    mongo_uri: str = "mongodb://wrc:wrc_password@localhost:27017/?authSource=admin"
    mongo_db: str = "wrc"
    mongo_landing_collection: str = "landing_documents"
    mongo_transformed_collection: str = "transformed_documents"
    mongo_failures_collection: str = "scrape_failures"
    mongo_runs_collection: str = "pipeline_runs"
    #: Which date the transformation selects on. ``published_date`` is the date
    #: the decision was published, which is what a consumer usually means;
    #: ``partition_date`` is the window that scraped it, which lines the two
    #: stages up exactly when you re-run one window.
    transform_date_field: str = "published_date"

    # --- Object storage ---------------------------------------------------
    s3_endpoint: str = "http://localhost:9000"
    s3_access_key: str = "minioadmin"
    s3_secret_key: str = "minioadmin"
    s3_region: str = "us-east-1"
    s3_secure: bool = False
    s3_landing_bucket: str = "landing"
    s3_transformed_bucket: str = "transformed"

    # --- Logging ----------------------------------------------------------
    log_level: str = "INFO"
    log_dir: Path = Path("./logs")
    log_json: bool = True

    # --- Orchestration ----------------------------------------------------
    dagster_max_parallel_units: int = 2

    # ------------------------------------------------------------------ #
    # Derived values                                                      #
    # ------------------------------------------------------------------ #
    @property
    def body_slugs(self) -> list[str]:
        """``WRC_BODIES`` parsed into a list.

        Kept as a comma-separated string in the environment (rather than a JSON
        list) because that is what a human types in a ``.env`` file.
        """
        return [slug.strip() for slug in self.bodies.split(",") if slug.strip()]

    @property
    def retry_codes(self) -> list[int]:
        """``WRC_RETRY_HTTP_CODES`` parsed into a list of integers."""
        return [int(code) for code in self.retry_http_codes.split(",") if code.strip()]

    @property
    def date_field(self) -> str:
        """Validated name of the date field the transformation slices on."""
        allowed = {"published_date", "partition_date"}
        if self.transform_date_field not in allowed:
            raise ValueError(
                f"WRC_TRANSFORM_DATE_FIELD must be one of {sorted(allowed)}, "
                f"got {self.transform_date_field!r}"
            )
        return self.transform_date_field

    @property
    def search_url(self) -> str:
        """Absolute URL of the search endpoint."""
        return f"{self.base_url.rstrip('/')}{self.search_path}"

    def scrapy_settings(self) -> dict[str, object]:
        """Map our settings onto Scrapy's setting names.

        Keeping this mapping in one place is what lets ``scraper/settings.py``
        contain no literals at all.
        """
        return {
            "BOT_NAME": "wrc_pipeline",
            "SPIDER_MODULES": ["wrc_pipeline.scraper.spiders"],
            "NEWSPIDER_MODULE": "wrc_pipeline.scraper.spiders",
            "USER_AGENT": self.user_agent,
            "ROBOTSTXT_OBEY": self.robotstxt_obey,
            # The site sets an ASP.NET session cookie, and ASP.NET serialises
            # requests that share a session. Disabling cookies keeps every
            # request independent and therefore parallelisable.
            "COOKIES_ENABLED": False,
            "CONCURRENT_REQUESTS": self.concurrent_requests,
            "CONCURRENT_REQUESTS_PER_DOMAIN": self.concurrent_requests_per_domain,
            "DOWNLOAD_DELAY": self.download_delay,
            "DOWNLOAD_TIMEOUT": self.download_timeout,
            # Two named slots with different concurrency: listing pages are slow
            # server-side searches that degrade above ~8 parallel requests, while
            # document pages stay fast at 16. Requests choose their slot via
            # ``meta["download_slot"]``.
            "DOWNLOAD_SLOTS": {
                "listing": {
                    "concurrency": self.listing_concurrency,
                    "delay": self.download_delay,
                },
                "documents": {
                    "concurrency": self.concurrent_requests_per_domain,
                    "delay": self.download_delay,
                },
            },
            "AUTOTHROTTLE_ENABLED": self.autothrottle_enabled,
            "AUTOTHROTTLE_START_DELAY": self.autothrottle_start_delay,
            "AUTOTHROTTLE_MAX_DELAY": self.autothrottle_max_delay,
            "AUTOTHROTTLE_TARGET_CONCURRENCY": self.autothrottle_target_concurrency,
            "RETRY_ENABLED": True,
            "RETRY_TIMES": self.retry_times,
            "RETRY_HTTP_CODES": self.retry_codes,
            # Custom settings read by BackoffRetryMiddleware.
            "RETRY_BACKOFF_BASE": self.retry_backoff_base,
            "RETRY_BACKOFF_MAX": self.retry_backoff_max,
            "HTTPCACHE_ENABLED": self.httpcache_enabled,
            "HTTPCACHE_EXPIRATION_SECS": self.httpcache_expiration_secs,
            "HTTPCACHE_DIR": "httpcache",
            "TELNETCONSOLE_ENABLED": False,
            "REQUEST_FINGERPRINTER_IMPLEMENTATION": "2.7",
            "FEED_EXPORT_ENCODING": "utf-8",
            # Explicit reactor: Scrapy 2.13 defaults to asyncio, but naming it
            # avoids surprises on Windows where the Proactor loop is unusable.
            "TWISTED_REACTOR": "twisted.internet.asyncioreactor.AsyncioSelectorReactor",
            "DOWNLOADER_MIDDLEWARES": {
                # Replaces Scrapy's RetryMiddleware with one that waits
                # exponentially longer between attempts.
                "scrapy.downloadermiddlewares.retry.RetryMiddleware": None,
                "wrc_pipeline.scraper.middlewares.BackoffRetryMiddleware": 550,
            },
            "ITEM_PIPELINES": {
                "wrc_pipeline.scraper.pipelines.LandingPipeline": 300,
            },
            # Scrapy's own logging is routed through our structlog JSON handler
            # (see wrc_pipeline.logging_setup), so we disable its handler setup.
            "LOG_ENABLED": False,
        }


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return the process-wide settings object (parsed once)."""
    return Settings()
