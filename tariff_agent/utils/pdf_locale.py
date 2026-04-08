"""SEDAR-style PDF filenames: English vs French.

Many issuers publish paired English and French documents. This pipeline indexes and
runs Docling/LLM on **English** filings only; French counterparts are expected and ignored.
"""

from __future__ import annotations

import re

# ASCII hyphen or en/em dash before the language token (matches typical SEDAR+ downloads).
_DASH = r"[-\u2013—]"

ENGLISH_FILING_PDF_RE = re.compile(
    rf"^(?P<fd>\d{{4}}-\d{{2}}-\d{{2}})_(?P<desc>.+?)\s*{_DASH}\s*English\.pdf$",
    re.I,
)
FRENCH_FILING_PDF_RE = re.compile(
    rf"^(?P<fd>\d{{4}}-\d{{2}}-\d{{2}})_(?P<desc>.+?)\s*{_DASH}\s*French\.pdf$",
    re.I,
)


def is_english_filing_pdf(filename: str) -> bool:
    return bool(ENGLISH_FILING_PDF_RE.match(filename))


def is_french_filing_pdf(filename: str) -> bool:
    return bool(FRENCH_FILING_PDF_RE.match(filename))
