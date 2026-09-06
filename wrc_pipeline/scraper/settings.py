"""Scrapy settings module.

Deliberately empty of literals: every value comes from :class:`~wrc_pipeline.config.Settings`,
which reads the environment. This is what makes "no hardcoded values" true for
the crawler as well as for the rest of the pipeline.

Scrapy reads the upper-case module attributes, so we inject them into the module
namespace.
"""

from wrc_pipeline.config import get_settings

globals().update(get_settings().scrapy_settings())
