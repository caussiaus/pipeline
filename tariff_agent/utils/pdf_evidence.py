"""Render PDF pages with highlighted evidence regions (Docling bboxes or text search)."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from docling_core.types.doc.base import BoundingBox, CoordOrigin


def _bbox_dict_to_fitz_rect(d: dict[str, Any], page_height: float) -> tuple[float, float, float, float]:
    """Return (x0, y0, x1, y1) in PyMuPDF top-left coordinates."""
    co_raw = d.get("coord_origin", "TOPLEFT")
    try:
        co = CoordOrigin(str(co_raw))
    except ValueError:
        co = CoordOrigin.TOPLEFT
    bb = BoundingBox(
        l=float(d["l"]),
        t=float(d["t"]),
        r=float(d["r"]),
        b=float(d["b"]),
        coord_origin=co,
    )
    if bb.coord_origin == CoordOrigin.BOTTOMLEFT:
        bb = bb.to_top_left_origin(page_height)
    return (bb.l, bb.t, bb.r, bb.b)


def parse_source_bboxes_json(raw: str | None) -> list[dict[str, Any]]:
    if not raw or str(raw).strip() in ("", "[]", "nan"):
        return []
    try:
        v = json.loads(str(raw))
        return v if isinstance(v, list) else []
    except json.JSONDecodeError:
        return []


def bboxes_on_page(boxes: list[dict[str, Any]], page_no_1based: int, page_height: float) -> list[tuple[float, float, float, float]]:
    """Filter boxes for this 1-based page and convert to fitz-aligned (x0,y0,x1,y1)."""
    out: list[tuple[float, float, float, float]] = []
    for d in boxes:
        try:
            if int(d.get("page_no", 0)) != page_no_1based:
                continue
            out.append(_bbox_dict_to_fitz_rect(d, page_height))
        except (KeyError, TypeError, ValueError):
            continue
    return out


_WHITESPACE_RE = re.compile(r"\s+")


def _snippet_for_search(text: str, max_len: int = 240) -> str:
    t = _WHITESPACE_RE.sub(" ", (text or "").strip())
    if len(t) <= max_len:
        return t
    return t[: max_len - 3] + "..."


def search_quads_for_snippet(page: Any, snippet: str) -> list[tuple[float, float, float, float]]:
    """Return rectangles (x0,y0,x1,y1) from PyMuPDF search_for, shrinking query if needed."""
    if not snippet:
        return []
    fitz = _fitz_mod()
    rects: list[Any] = []
    for attempt in (snippet, _snippet_for_search(snippet, 120), _snippet_for_search(snippet, 60)):
        if len(attempt) < 12:
            break
        try:
            rects = page.search_for(attempt, quads=False)
        except Exception:
            rects = []
        if rects:
            break
    return [(r.x0, r.y0, r.x1, r.y1) for r in rects]


def _fitz_mod() -> Any:
    try:
        import fitz  # PyMuPDF
    except ImportError as e:
        raise ImportError(
            "PyMuPDF is required for PDF evidence rendering. Install: pip install 'pymupdf>=1.24,<2'"
        ) from e
    return fitz


def render_page_highlight_png(
    pdf_path: str | Path,
    page_no_1based: int,
    *,
    highlight_rects: list[tuple[float, float, float, float]] | None = None,
    text_snippet: str = "",
    zoom: float = 1.75,
) -> bytes:
    """Render one PDF page as PNG with red outline rectangles (evidence)."""
    fitz = _fitz_mod()
    path = Path(pdf_path)
    if not path.is_file():
        raise FileNotFoundError(path)

    doc = fitz.open(str(path))
    try:
        idx = max(0, int(page_no_1based) - 1)
        if idx >= len(doc):
            raise ValueError(f"Page {page_no_1based} out of range ({len(doc)} pages)")
        page = doc[idx]
        h = float(page.rect.height)
        rects = list(highlight_rects or [])
        if not rects and text_snippet.strip():
            rects = search_quads_for_snippet(page, _snippet_for_search(text_snippet))
        shape = page.new_shape()
        for x0, y0, x1, y1 in rects:
            shape.draw_rect(fitz.Rect(x0, y0, x1, y1))
        shape.finish(color=(1, 0, 0), width=1.2)
        shape.commit()

        mat = fitz.Matrix(zoom, zoom)
        pix = page.get_pixmap(matrix=mat, alpha=False)
        return pix.tobytes("png")
    finally:
        doc.close()
