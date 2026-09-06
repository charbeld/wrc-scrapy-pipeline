"""Strip a decision page down to the decision itself.

The landing zone holds the page exactly as the site served it: menus, cookie
banner, logos, footer, and the decision text. Downstream consumers want the last
part only, so this module keeps the content root and throws the chrome away.

Structure of a decision page (verified 2008-2025)::

    <div class="col-sm-9">            <-- content root
        <h1 class="page-title">ADJ-00054447</h1>
        <div class="content"> ...headings, paragraphs, tables... </div>
    </div>

Determinism matters
-------------------
The output is hashed, so the same input must always produce the same bytes:
attributes are stripped to a fixed whitelist, whitespace is normalised, and the
document is serialised without pretty-printing.

Bump :data:`TRANSFORM_VERSION` whenever the cleaning logic changes; the runner
uses it to decide that already-transformed documents must be reprocessed.
"""

from __future__ import annotations

import re
from typing import Any

from bs4 import BeautifulSoup, NavigableString, Tag

#: Version of the cleaning logic; stored on every transformed record.
TRANSFORM_VERSION = 1

#: Tags that never carry decision content.
_DROP_TAGS = ("script", "style", "noscript", "iframe", "form", "button", "input", "select", "img")

#: Class fragments that mark site chrome inside the content area.
_DROP_CLASS_FRAGMENTS = ("hidden-print", "no-print", "binder", "social", "return-to-search")

#: Attributes worth keeping; everything else (class, style, width) is noise.
_KEEP_ATTRS = {"href", "colspan", "rowspan"}

_WHITESPACE = re.compile(r"\s+")
# The non-breaking space is intentional: the site's imported HTML is full of
# paragraphs whose only content is &nbsp;, and those count as empty.
_BLANK_TEXT = re.compile(r"^[\s ]*$")  # noqa: RUF001


def _looks_blank(tag: Tag) -> bool:
    """True when a tag holds neither text nor a meaningful child element."""
    if tag.find(["table", "img", "br", "a", "li"]):
        return False
    return bool(_BLANK_TEXT.match(tag.get_text() or ""))


def _find_content_root(soup: BeautifulSoup) -> tuple[Tag | None, str]:
    """Locate the decision content, reporting which selector matched.

    The fallbacks exist so a redesigned page still produces something usable;
    the caller logs the fallback so the degradation is visible.
    """
    for candidate in soup.select("div.col-sm-9"):
        if candidate.find("h1", class_="page-title") and candidate.find("div", class_="content"):
            return candidate, "div.col-sm-9"

    content = soup.find("div", class_="content")
    if content:
        return content, "div.content"

    main = soup.find("main")
    if main:
        return main, "main"

    return (soup.body, "body") if soup.body else (None, "none")


def _class_names(tag: Tag) -> str:
    """The tag's classes as one lower-case string (bs4 may return str or list)."""
    value = tag.get("class") or []
    if isinstance(value, str):
        value = value.split()
    return " ".join(str(part) for part in value).lower()


def _scrub(root: Tag, base_url: str | None) -> None:
    """Remove chrome and normalise attributes, in place."""
    for tag in root.find_all(list(_DROP_TAGS)):
        tag.decompose()

    for tag in root.find_all(True):
        # A parent may already have been removed by an earlier iteration.
        if tag.decomposed:
            continue
        if any(fragment in _class_names(tag) for fragment in _DROP_CLASS_FRAGMENTS):
            tag.decompose()

    for tag in root.find_all(True):
        if tag.decomposed:
            continue
        attrs = {}
        for name, value in tag.attrs.items():
            if name not in _KEEP_ATTRS:
                continue
            if name == "href" and base_url and isinstance(value, str):
                value = _absolutise(base_url, value)
            attrs[name] = value
        tag.attrs = attrs

    # Collapse runs of whitespace so formatting differences cannot change the hash.
    for text in root.find_all(string=True):
        if isinstance(text, NavigableString):
            collapsed = _WHITESPACE.sub(" ", str(text))
            if collapsed != str(text):
                text.replace_with(collapsed)

    # Drop the empty paragraphs, headings and tables that the site's own import
    # process leaves behind. Empty <td> cells are deliberately kept: removing
    # them would shift the remaining cells and corrupt the parties tables.
    for tag in root.find_all(["p", "h1", "h2", "h3", "div", "span", "table"]):
        if not tag.decomposed and _looks_blank(tag):
            tag.decompose()


def _absolutise(base_url: str, href: str) -> str:
    from urllib.parse import urljoin

    return urljoin(base_url, href)


def _table_lookup(root: Tag, label: str) -> str | None:
    """Value of the cell to the right of a cell whose text starts with ``label``."""
    for cell in root.find_all(["td", "th"]):
        text = " ".join(cell.get_text(" ", strip=True).split()).rstrip(":")
        if text.lower().startswith(label.lower()):
            sibling = cell.find_next_sibling(["td", "th"])
            if sibling:
                value = " ".join(sibling.get_text(" ", strip=True).split())
                if value:
                    return value
    return None


def _extract_fields(root: Tag) -> dict[str, Any]:
    """Pull a few structured values out of the decision.

    Not required by the assignment, but it is the cheap data-quality win the
    brief invites: consumers can filter by act or officer without re-parsing the
    HTML. Every lookup is defensive; a missing value is simply ``None``.
    """
    text = root.get_text(" ", strip=True)
    fields: dict[str, Any] = {
        "text_length": len(text),
        "language": "en",
    }

    headings = [
        " ".join(node.get_text(" ", strip=True).split())
        for node in root.find_all(["h1", "h2"])
        if node.get_text(strip=True)
    ]
    fields["headings"] = headings[:10]

    for key, label in (
        ("adjudication_officer", "Workplace Relations Commission Adjudication Officer"),
        ("date_of_hearing", "Date of Hearing"),
        ("chairman", "Chairman"),
        ("complainant", "Complainant"),
        ("respondent", "Respondent"),
    ):
        try:
            fields[key] = _table_lookup(root, label)
        except Exception:
            fields[key] = None

    # "Parties" row: the two cells after the label are complainant and respondent.
    parties: list[str] = []
    for cell in root.find_all(["td", "th"]):
        label = " ".join(cell.get_text(" ", strip=True).split()).lower()
        if label in {"parties", "anonymised parties"}:
            for sibling in cell.find_next_siblings(["td", "th"]):
                value = " ".join(sibling.get_text(" ", strip=True).split())
                if value:
                    parties.append(value)
            if parties:
                break
    fields["parties"] = parties or None

    acts = []
    for cell in root.find_all(["td", "th"]):
        value = " ".join(cell.get_text(" ", strip=True).split())
        if "Act" in value and len(value) > 20 and value not in acts:
            acts.append(value)
    fields["acts"] = acts[:10] or None
    return fields


def clean_html(
    raw: bytes,
    identifier: str,
    base_url: str | None = None,
) -> tuple[bytes, dict[str, Any]]:
    """Return the cleaned HTML document and the fields extracted from it.

    Args:
        raw: The stored landing-zone bytes.
        identifier: Used as the ``<title>`` of the produced document.
        base_url: Makes any surviving link absolute.

    Returns:
        ``(html_bytes, info)`` where ``info`` carries ``content_root`` (which
        selector matched) and ``extracted`` (the structured fields).
    """
    soup = BeautifulSoup(raw, "lxml")
    root, selector = _find_content_root(soup)
    if root is None:
        raise ValueError(f"No content could be located in the document for {identifier}")

    _scrub(root, base_url)
    extracted = _extract_fields(root)

    inner = root.decode_contents().strip()
    document = (
        "<!doctype html>\n"
        '<html lang="en">\n'
        "<head>\n"
        '<meta charset="utf-8">\n'
        f"<title>{identifier}</title>\n"
        "</head>\n"
        "<body>\n"
        f"{inner}\n"
        "</body>\n"
        "</html>\n"
    )
    return document.encode("utf-8"), {"content_root": selector, "extracted": extracted}
