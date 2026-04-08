from __future__ import annotations

import json
import re
import uuid
from pathlib import Path
from typing import Any

import pandas as pd
from docling_core.types.doc.document import DoclingDocument, SectionHeaderItem, TableItem, TextItem
from tqdm import tqdm

from tariff_agent.prompts.chunk_prompt import keyword_terms
from tariff_agent.state import ChunkRecord, normalize_filing_type
from tariff_agent.utils.config import Settings, get_settings
from tariff_agent.utils.meta_normalize import clean_meta_str
from tariff_agent.utils.sector_meta import enrich_with_sector

# Absolute upper bound for a single chunk body in characters (~1500 estimated tokens, ~2500 real BPE tokens).
# Applied as a last-resort split when a single text block exceeds this — typically from headerless docs.
_HARD_SPLIT_MAX_CHARS = 6_000


def estimate_tokens(text: str) -> int:
    return max(1, int(len(text.split()) / 0.75))


def split_prefix_by_estimated_tokens(text: str, max_tokens: int) -> tuple[str, str]:
    """Split ``text`` into (head, tail) where ``estimate_tokens(head) <= max_tokens`` and head is maximal."""
    if max_tokens < 1:
        return "", text
    t = text
    if not t.strip():
        return "", ""
    if estimate_tokens(t) <= max_tokens:
        return t, ""
    lo, hi = 0, len(t)
    while lo < hi:
        mid = (lo + hi + 1) // 2
        if estimate_tokens(t[:mid]) <= max_tokens:
            lo = mid
        else:
            hi = mid - 1
    br = t[:lo].rfind("\n")
    if br != -1 and br > int(lo * 0.85):
        lo = br + 1
    head = t[:lo].rstrip()
    tail = t[lo:].lstrip()
    if tail and not head:
        n = 1
        while n <= len(t) and estimate_tokens(t[:n]) <= max_tokens:
            n += 1
        cut = max(1, n - 1)
        head, tail = t[:cut].rstrip(), t[cut:].lstrip()
    return head, tail


def truncate_text_to_estimated_tokens(text: str, max_tokens: int, *, note: str = "[... truncated for model context limit ...]") -> str:
    """Coarse token cap for LLM prompts (``estimate_tokens`` understates some tokenizers; keep margin in Settings)."""
    if max_tokens < 1 or estimate_tokens(text) <= max_tokens:
        return text
    head, _tail = split_prefix_by_estimated_tokens(text, max(1, max_tokens - estimate_tokens(note) - 10))
    if not head:
        return note
    return head.rstrip() + "\n\n" + note


def _hard_split(text: str, max_chars: int = _HARD_SPLIT_MAX_CHARS) -> list[str]:
    """Last-resort paragraph split for headerless documents that produced one massive text block.

    Splits on blank lines first; if a single paragraph still exceeds ``max_chars``, splits on
    sentence boundaries (period/newline), then hard-truncates at the character limit.
    """
    if len(text) <= max_chars:
        return [text]
    results: list[str] = []
    buf: list[str] = []
    size = 0
    for para in re.split(r"\n{2,}", text):
        if not para.strip():
            continue
        if size + len(para) > max_chars and buf:
            results.append("\n\n".join(buf))
            buf, size = [], 0
        if len(para) > max_chars:
            # paragraph itself is huge — split on sentence boundaries
            sentences = re.split(r"(?<=[.!?])\s+|\n", para)
            for sent in sentences:
                if not sent.strip():
                    continue
                if size + len(sent) > max_chars and buf:
                    results.append("\n\n".join(buf))
                    buf, size = [], 0
                if len(sent) > max_chars:
                    # absolute hard cap: slice into max_chars pieces
                    for i in range(0, len(sent), max_chars):
                        results.append(sent[i : i + max_chars])
                else:
                    buf.append(sent)
                    size += len(sent)
        else:
            buf.append(para)
            size += len(para)
    if buf:
        results.append("\n\n".join(buf))
    return [r for r in results if r.strip()] or [text[:max_chars]]


def _page_for_item(item: TextItem | TableItem) -> int:
    prov = getattr(item, "prov", None) or []
    if not prov:
        return 0
    return int(prov[0].page_no)


def _prov_boxes_from_item(item: Any) -> list[dict[str, Any]]:
    """Serialize Docling provenance bboxes for later PDF overlay (one item may have multiple prov records)."""
    prov = getattr(item, "prov", None) or []
    out: list[dict[str, Any]] = []
    for p in prov:
        try:
            bb = p.bbox
            origin = bb.coord_origin
            co = origin.value if hasattr(origin, "value") else str(origin)
            out.append({
                "page_no": int(p.page_no),
                "l": float(bb.l),
                "t": float(bb.t),
                "r": float(bb.r),
                "b": float(bb.b),
                "coord_origin": co,
            })
        except Exception:
            continue
    return out


def _merge_source_bboxes(buf_boxes: list[list[dict[str, Any]]]) -> str:
    flat: list[dict[str, Any]] = []
    for lst in buf_boxes:
        flat.extend(lst)
    return json.dumps(flat, ensure_ascii=False) if flat else ""


def _emit_chunk(
    buffer: list[str],
    buf_pages: list[int],
    buf_boxes: list[list[dict[str, Any]]],
    *,
    filing_row: pd.Series,
    section_path: str,
) -> ChunkRecord | None:
    if not buffer:
        return None
    chunk_text = "\n".join(buffer).strip()
    if not chunk_text:
        return None
    ps = [p for p in buf_pages if p]
    p0 = min(ps) if ps else 0
    p1 = max(ps) if ps else 0
    kterms = keyword_terms(chunk_text)
    row_d = filing_row if isinstance(filing_row, dict) else filing_row.to_dict()
    return ChunkRecord(
        chunk_id=str(uuid.uuid4()),
        filing_id=str(row_d["filing_id"]),
        profile_id=clean_meta_str(row_d.get("profile_id")),
        ticker=clean_meta_str(row_d.get("ticker")) or "unknown",
        filing_type=normalize_filing_type(row_d["filing_type"]),
        filing_date=clean_meta_str(row_d.get("filing_date")),
        section_path=section_path,
        page_start=p0,
        page_end=p1,
        text=chunk_text,
        num_tokens=estimate_tokens(chunk_text),
        keyword_hit=bool(kterms),
        keyword_hit_terms=kterms,
        source_bboxes_json=_merge_source_bboxes(buf_boxes),
        naics_sector=str(row_d.get("naics_sector") or "unknown"),
        mechanism=str(row_d.get("mechanism") or "minimal_no_vector"),
        exposure_vector=str(row_d.get("exposure_vector") or ""),
        cap_earnings=int(row_d.get("cap_earnings") or 3),
        cap_supply_chain=int(row_d.get("cap_supply_chain") or 3),
        cap_macro=int(row_d.get("cap_macro") or 3),
    )


def _flush_with_split(
    buffer: list[str],
    buf_pages: list[int],
    buf_boxes: list[list[dict[str, Any]]],
    *,
    filing_row: pd.Series,
    section_path: str,
    target_tokens: int,
) -> tuple[list[ChunkRecord], list[str], list[int], list[list[dict[str, Any]]]]:
    chunks: list[ChunkRecord] = []
    b, bp, bb = buffer, buf_pages, buf_boxes
    while b:
        joined = "\n".join(b).strip()
        if not joined:
            break
        if estimate_tokens(joined) <= target_tokens:
            c = _emit_chunk(b, bp, bb, filing_row=filing_row, section_path=section_path)
            if c:
                chunks.append(c)
            b, bp, bb = [], [], []
            break
        if len(b) == 1:
            joined_one = "\n".join(b).strip()
            if estimate_tokens(joined_one) <= target_tokens:
                c = _emit_chunk(b, bp, bb, filing_row=filing_row, section_path=section_path)
                if c:
                    chunks.append(c)
                b, bp, bb = [], [], []
                break
            head, tail = split_prefix_by_estimated_tokens(joined_one, target_tokens)
            c = _emit_chunk([head], bp, bb, filing_row=filing_row, section_path=section_path)
            if c:
                chunks.append(c)
            b, bp, bb = ([tail] if tail else []), bp, bb
            if not tail:
                break
            continue
        # Emit all complete lines except the last overflowing paragraph
        c = _emit_chunk(b[:-1], bp[:-1], bb[:-1], filing_row=filing_row, section_path=section_path)
        if c:
            chunks.append(c)
        b, bp, bb = [b[-1]], [bp[-1]], [bb[-1]]
    return chunks, b, bp, bb


def chunk_from_docling_document(doc: DoclingDocument, filing_row: pd.Series, target_tokens: int) -> list[ChunkRecord]:
    section_titles: list[str] = []
    chunks: list[ChunkRecord] = []
    buffer: list[str] = []
    buf_pages: list[int] = []
    buf_boxes: list[list[dict[str, Any]]] = []

    def section_path() -> str:
        return "/".join(section_titles) if section_titles else "ROOT"

    for item, _depth in doc.iterate_items(with_groups=True):
        if isinstance(item, SectionHeaderItem):
            level = max(1, int(item.level or 1))
            title = (item.text or "").strip() or (getattr(item, "orig", None) or "").strip()
            section_titles = section_titles[: level - 1]
            if title:
                section_titles.append(title)
            if buffer:
                new_c, buffer, buf_pages, buf_boxes = _flush_with_split(
                    buffer,
                    buf_pages,
                    buf_boxes,
                    filing_row=filing_row,
                    section_path=section_path(),
                    target_tokens=target_tokens,
                )
                chunks.extend(new_c)
            continue

        if isinstance(item, TextItem):
            body = (item.text or "").strip()
            if not body:
                continue
            page = _page_for_item(item)
            buffer.append(body)
            buf_pages.append(page)
            buf_boxes.append(_prov_boxes_from_item(item))
            new_c, buffer, buf_pages, buf_boxes = _flush_with_split(
                buffer,
                buf_pages,
                buf_boxes,
                filing_row=filing_row,
                section_path=section_path(),
                target_tokens=target_tokens,
            )
            chunks.extend(new_c)
            continue

        if isinstance(item, TableItem):
            try:
                md = item.export_to_markdown(doc=doc)
            except Exception:
                md = ""
            md = (md or "").strip()
            if not md:
                continue
            page = int(item.prov[0].page_no) if getattr(item, "prov", None) else 0
            buffer.append(md)
            buf_pages.append(page)
            buf_boxes.append(_prov_boxes_from_item(item))
            new_c, buffer, buf_pages, buf_boxes = _flush_with_split(
                buffer,
                buf_pages,
                buf_boxes,
                filing_row=filing_row,
                section_path=section_path(),
                target_tokens=target_tokens,
            )
            chunks.extend(new_c)

    if buffer:
        new_c, buffer, buf_pages, buf_boxes = _flush_with_split(
            buffer,
            buf_pages,
            buf_boxes,
            filing_row=filing_row,
            section_path=section_path(),
            target_tokens=target_tokens,
        )
        chunks.extend(new_c)

    # Hard-split any chunk that still exceeds the character ceiling (headerless / degenerate docs).
    chunks = _apply_hard_split_to_oversized(chunks, filing_row)
    return chunks


def _apply_hard_split_to_oversized(chunks: list[ChunkRecord], filing_row: Any) -> list[ChunkRecord]:
    """Post-process: any chunk whose text body exceeds ``_HARD_SPLIT_MAX_CHARS`` is split further."""
    out: list[ChunkRecord] = []
    for c in chunks:
        if len(c.text) <= _HARD_SPLIT_MAX_CHARS:
            out.append(c)
            continue
        pieces = _hard_split(c.text)
        for piece in pieces:
            piece = piece.strip()
            if not piece:
                continue
            kterms = keyword_terms(piece)
            out.append(
                ChunkRecord(
                    chunk_id=str(uuid.uuid4()),
                    filing_id=c.filing_id,
                    profile_id=c.profile_id,
                    ticker=c.ticker,
                    filing_type=c.filing_type,
                    filing_date=c.filing_date,
                    section_path=c.section_path,
                    page_start=c.page_start,
                    page_end=c.page_end,
                    text=piece,
                    num_tokens=estimate_tokens(piece),
                    keyword_hit=bool(kterms),
                    keyword_hit_terms=kterms,
                    source_bboxes_json="",
                    naics_sector=c.naics_sector,
                    mechanism=c.mechanism,
                    exposure_vector=c.exposure_vector,
                    cap_earnings=c.cap_earnings,
                    cap_supply_chain=c.cap_supply_chain,
                    cap_macro=c.cap_macro,
                )
            )
    return out


def chunk_from_fallback_json(data: dict[str, Any], filing_row: pd.Series, target_tokens: int) -> list[ChunkRecord]:
    chunks: list[ChunkRecord] = []
    for section in data.get("sections", []):
        path = str(section.get("path", "ROOT"))
        texts = section.get("texts", [])
        buffer: list[str] = []
        buf_pages: list[int] = []
        buf_boxes: list[list[dict[str, Any]]] = []
        for t in texts:
            content = str(t.get("text", "")).strip()
            if not content:
                continue
            page = int(t.get("page_no", t.get("page", 0)) or 0)
            buffer.append(content)
            buf_pages.append(page)
            buf_boxes.append([])
            new_c, buffer, buf_pages, buf_boxes = _flush_with_split(
                buffer,
                buf_pages,
                buf_boxes,
                filing_row=filing_row,
                section_path=path,
                target_tokens=target_tokens,
            )
            chunks.extend(new_c)
        if buffer:
            new_c, buffer, buf_pages, buf_boxes = _flush_with_split(
                buffer,
                buf_pages,
                buf_boxes,
                filing_row=filing_row,
                section_path=path,
                target_tokens=target_tokens,
            )
            chunks.extend(new_c)
    chunks = _apply_hard_split_to_oversized(chunks, filing_row)
    return chunks


def chunk_document_path(doc_json_path: Path, filing_row: pd.Series, target_tokens: int) -> list[ChunkRecord]:
    raw = json.loads(doc_json_path.read_text(encoding="utf-8"))
    if raw.get("fallback"):
        return chunk_from_fallback_json(raw, filing_row, target_tokens)
    doc = DoclingDocument.model_validate(raw)
    return chunk_from_docling_document(doc, filing_row, target_tokens)


def run_chunking(settings: Settings | None = None, *, force: bool = False) -> pd.DataFrame:
    settings = settings or get_settings()
    filings = pd.read_csv(settings.resolve(settings.filings_index_path))
    if settings.sedar_master_issuers_path.strip():
        filings = enrich_with_sector(filings, settings.sedar_master_issuers_path)
    parse_idx = pd.read_csv(settings.resolve(settings.parse_index_csv))
    ok = parse_idx[parse_idx["parse_status"].str.startswith("OK", na=False)]
    merged = filings.merge(ok[["filing_id", "local_path_docling"]], on="filing_id")

    out_parquet = settings.resolve(settings.chunks_parquet)
    if not force and settings.skip_chunk_if_exists and out_parquet.is_file():
        return pd.read_parquet(out_parquet)

    all_records: list[dict[str, Any]] = []
    target = settings.chunk_target_tokens

    for _, row in tqdm(merged.iterrows(), total=len(merged), desc="chunking"):
        doc_json_path = Path(str(row["local_path_docling"]))
        if not doc_json_path.is_file():
            continue
        chunks = chunk_document_path(doc_json_path, row, target)
        all_records.extend(c.model_dump() for c in chunks)

    out_parquet.parent.mkdir(parents=True, exist_ok=True)
    df_chunks = pd.DataFrame(all_records)
    if df_chunks.empty:
        df_chunks = pd.DataFrame(
            columns=[f for f in ChunkRecord.model_fields.keys()],
        )
    df_chunks.to_parquet(out_parquet, index=False)
    return df_chunks
