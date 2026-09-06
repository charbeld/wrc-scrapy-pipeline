"""Normalisation of the decision reference ("identifier").

The identifier is our **business key**: it is the unique index in MongoDB, the
name of the transformed file (``identifier.ext``) and the value a human quotes
when asking for a decision. It therefore has to be stable.

The site is inconsistent about whitespace and separators:

===========================  ==================
As published (``span.refNO``) Normalised
===========================  ==================
``ADJ-00054447``              ``ADJ-00054447``
``IR - SC - 00002163``        ``IR-SC-00002163``
``DEC-S2008- 123``            ``DEC-S2008-123``
``DEC-S2008-120 ``            ``DEC-S2008-120``
``UD833/2015``                ``UD833-2015``
===========================  ==================

The raw value is kept alongside as ``identifier_raw`` so nothing is lost.
"""

from __future__ import annotations

import re
from urllib.parse import urlparse

#: Characters we accept in a normalised identifier.
_VALID = re.compile(r"^[A-Z0-9][A-Z0-9.\-]*$")
_WHITESPACE = re.compile(r"\s+")
_AROUND_DASH = re.compile(r"\s*-\s*")
_MULTI_DASH = re.compile(r"-{2,}")
_UNSAFE = re.compile(r"[^A-Za-z0-9.\-]+")


def normalise_identifier(raw: str | None) -> str:
    """Return a stable, filesystem-safe identifier.

    Returns an empty string when the input has no usable content; callers treat
    that as a parsing failure and fall back to :func:`identifier_from_url`.
    """
    if not raw:
        return ""
    value = _WHITESPACE.sub(" ", raw).strip()
    # "/" is a separator in older Employment Appeals Tribunal references
    # (UD833/2015) and would create a fake folder level in object storage.
    value = value.replace("/", "-")
    # Collapse "A - B" and "A- B" into "A-B" before touching the rest.
    value = _AROUND_DASH.sub("-", value)
    value = _UNSAFE.sub("-", value)
    value = _MULTI_DASH.sub("-", value).strip("-.")
    value = value.upper()
    return value if _VALID.match(value) else ""


def identifier_from_url(url: str) -> str:
    """Fallback identifier derived from the document URL's file name.

    Used only when the listing row has no usable reference number; the event is
    logged as ``identifier_fallback`` so it is visible rather than silent.
    """
    stem = urlparse(url).path.rsplit("/", 1)[-1]
    stem = stem.rsplit(".", 1)[0]
    return normalise_identifier(stem) or "UNKNOWN"


def file_extension_for(doc_type: str) -> str:
    """Map a detected document type to the extension used when renaming files."""
    return {"html": "html", "pdf": "pdf", "doc": "doc", "docx": "docx"}.get(doc_type, "bin")
