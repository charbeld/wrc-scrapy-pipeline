"""Content hashing and document-type detection.

The hash is the backbone of idempotency: it is stored on every metadata record,
it names the object in the landing zone, and comparing it against the stored
value is how a re-run decides between "unchanged", "changed" and "new".

One site-specific detail matters enormously
-------------------------------------------
Every page ends with an HTML comment holding the server's render time::

    <!-- Elapsed time: 0.0469062 -->

That number differs on every single fetch. Hashing the raw bytes would make
every document look modified on every run, which would defeat the "do not
re-download unchanged files" requirement and grow the version history forever.
We therefore strip that comment before hashing and before storing. Verified: two
fetches of the same page are byte-identical afterwards.
"""

from __future__ import annotations

import hashlib
import re

#: HTML comments that contain no markup of their own, plus any whitespace that
#: follows them.
#:
#: Every part of these pages that varies between two fetches of the same
#: decision is a comment of this shape:
#:
#:   <!-- Elapsed time: 0.0469062 -->                    the server's render time
#:   <!-- cached location -->                            present only when the
#:   <!-- cached or not being index.aspx page -->         response came from cache
#:
#: The trailing ``\s*`` matters: without it, removing a comment that sat alone on
#: its own line would leave a blank line behind, and the two variants still would
#: not match.
#:
#: Comments that do contain markup are left alone. Those are the Internet
#: Explorer conditional blocks, which wrap real ``<html>`` and ``<script>`` tags;
#: deleting them would damage the document structure. They are also stable.
_VOLATILE_COMMENT = re.compile(rb"<!--[^<]*-->\s*")

#: Leading bytes that identify a binary document format.
_MAGIC: tuple[tuple[bytes, str], ...] = (
    (b"%PDF-", "pdf"),
    (b"PK\x03\x04", "docx"),  # OOXML is a zip archive
    (b"\xd0\xcf\x11\xe0", "doc"),  # legacy OLE compound file
    (b"{\\rtf", "doc"),
)


def normalise_html_bytes(body: bytes) -> bytes:
    """Remove the comments that vary between two fetches of the same page.

    Without this the hash changes on every fetch, so every document looks
    modified on every run: no idempotency, an endless version history, and the
    whole corpus re-uploaded each time.
    """
    return _VOLATILE_COMMENT.sub(b"", body)


def normalise_bytes(body: bytes, doc_type: str) -> bytes:
    """Normalise a payload before hashing/storing.

    HTML has its volatile comments removed; binary formats (PDF/DOC/DOCX) are
    stored exactly as served, as the assignment requires.
    """
    return normalise_html_bytes(body) if doc_type == "html" else body


def sha256_bytes(body: bytes) -> str:
    """Return ``sha256:<hex>`` for ``body``.

    The algorithm prefix documents which function produced the digest, so the
    stored value stays meaningful if we ever migrate.
    """
    return f"sha256:{hashlib.sha256(body).hexdigest()}"


def short_hash(file_hash: str, length: int = 12) -> str:
    """Short form of a hash, used inside object keys to make them unique."""
    return file_hash.split(":", 1)[-1][:length]


def sniff_doc_type(content_type: str | None, head: bytes, url: str) -> str:
    """Detect the document type from magic bytes, then headers, then the URL.

    Magic bytes come first because a misconfigured server can serve a PDF as
    ``text/html``; the bytes never lie.
    """
    for signature, doc_type in _MAGIC:
        if head.startswith(signature):
            return doc_type

    ctype = (content_type or "").split(";", 1)[0].strip().lower()
    by_content_type = {
        "text/html": "html",
        "application/xhtml+xml": "html",
        "application/pdf": "pdf",
        "application/msword": "doc",
        "application/rtf": "doc",
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document": "docx",
    }
    if ctype in by_content_type:
        return by_content_type[ctype]

    suffix = url.rsplit("?", 1)[0].rsplit(".", 1)[-1].lower()
    if suffix in {"html", "htm"}:
        return "html"
    if suffix in {"pdf", "doc", "docx"}:
        return suffix
    return "other"
