"""Registry of the four decision-making bodies offered by the site's filter.

The numeric ids are the ``value`` attributes of the "Body" checkboxes in the
advanced-search form (``ctl00$ContentPlaceHolder_Main$CB2$CB2_n``) and are what
the search endpoint expects in its ``body`` query parameter.

The endpoint accepts exactly one body per request (repeated or comma-joined
values are ignored), which is why the spider loops over bodies instead of
querying them together.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Body:
    """One decision-making body.

    Attributes:
        slug: Stable machine name used in configuration, object keys and Mongo.
        site_id: Value for the site's ``body`` query parameter.
        name: Human-readable name as displayed on the website.
    """

    slug: str
    site_id: str
    name: str


#: All bodies, keyed by slug. Two of them are defunct (the Equality Tribunal and
#: the Employment Appeals Tribunal were folded into the WRC and the Labour Court
#: in 2015), so recent partitions legitimately return zero records for them.
BODIES: dict[str, Body] = {
    "employment_appeals_tribunal": Body(
        slug="employment_appeals_tribunal",
        site_id="2",
        name="Employment Appeals Tribunal",
    ),
    "equality_tribunal": Body(
        slug="equality_tribunal",
        site_id="1",
        name="Equality Tribunal",
    ),
    "labour_court": Body(
        slug="labour_court",
        site_id="3",
        name="Labour Court",
    ),
    "workplace_relations_commission": Body(
        slug="workplace_relations_commission",
        site_id="15376",
        name="Workplace Relations Commission",
    ),
}


def get_body(slug: str) -> Body:
    """Look up a body by slug.

    Raises:
        KeyError: if the slug is not one of the four known bodies. Failing loudly
            here beats silently scraping nothing.
    """
    try:
        return BODIES[slug]
    except KeyError:
        raise KeyError(
            f"Unknown body {slug!r}. Known bodies: {', '.join(sorted(BODIES))}"
        ) from None


def resolve_bodies(bodies: str | list[str] | None, default: list[str]) -> list[Body]:
    """Turn a comma-separated string or list of slugs into :class:`Body` objects.

    Args:
        bodies: User input (CLI argument, Dagster config) or ``None``.
        default: Slugs to use when ``bodies`` is empty.
    """
    if not bodies:
        slugs = list(default)
    elif isinstance(bodies, str):
        slugs = [part.strip() for part in bodies.split(",") if part.strip()]
    else:
        slugs = list(bodies)
    return [get_body(slug) for slug in slugs]
