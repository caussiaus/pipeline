from __future__ import annotations

from tariff_agent.utils.config import ensure_hf_hub_env_for_process

ensure_hf_hub_env_for_process()

import gc
import json
import logging
from pathlib import Path
from typing import Any

import pandas as pd
import torch
from docling.datamodel.accelerator_options import AcceleratorOptions
from docling.datamodel.base_models import InputFormat
from docling.datamodel.pipeline_options import (
    PdfPipelineOptions,
    TableFormerMode,
    TableStructureOptions,
)
from docling.document_converter import DocumentConverter, PdfFormatOption
from docling.pipeline.standard_pdf_pipeline import StandardPdfPipeline
from tqdm import tqdm

from tariff_agent.utils.config import Settings, get_settings

logger = logging.getLogger(__name__)


def build_docling_converter(settings: Settings | None = None) -> DocumentConverter:
    s = settings or get_settings()
    opts = PdfPipelineOptions()
    opts.do_ocr = s.docling_do_ocr
    opts.do_table_structure = s.docling_do_table_structure
    opts.ocr_batch_size = s.docling_ocr_batch_size
    opts.layout_batch_size = s.docling_layout_batch_size
    opts.table_batch_size = s.docling_table_batch_size
    if s.docling_do_table_structure:
        ts = opts.table_structure_options
        if isinstance(ts, TableStructureOptions) and s.docling_table_former_mode == "fast":
            opts.table_structure_options = ts.model_copy(update={"mode": TableFormerMode.FAST})
    lo = opts.layout_options
    if hasattr(lo, "model_copy"):
        opts.layout_options = lo.model_copy(update={"skip_cell_assignment": s.docling_skip_cell_assignment})
    opts.accelerator_options = AcceleratorOptions(
        device=s.docling_device,
        num_threads=s.docling_num_threads,
    )
    logger.info(
        "Docling device=%s cuda=%s ocr=%s tables=%s table_mode=%s ocr_batch=%s layout_batch=%s table_batch=%s",
        s.docling_device,
        torch.cuda.is_available(),
        s.docling_do_ocr,
        s.docling_do_table_structure,
        s.docling_table_former_mode,
        s.docling_ocr_batch_size,
        s.docling_layout_batch_size,
        s.docling_table_batch_size,
    )
    return DocumentConverter(
        format_options={
            InputFormat.PDF: PdfFormatOption(
                pipeline_cls=StandardPdfPipeline,
                pipeline_options=opts,
            )
        },
    )


def normalize_path(path: str) -> str:
    return path.replace("\\", "/")


def resolve_pdf_path(local_path: str, settings: Settings) -> Path:
    raw = normalize_path(local_path.strip())
    p = Path(raw)
    if p.is_absolute():
        return p
    root = settings.resolve(settings.filings_pdf_root) if settings.filings_pdf_root else settings.project_root
    return (root / p).resolve()


def _pypdf_fallback_text(pdf_path: Path) -> str:
    """Return all text as a single string (used for quick char-count checks)."""
    from pypdf import PdfReader

    reader = PdfReader(str(pdf_path))
    parts: list[str] = []
    for page in reader.pages:
        try:
            t = page.extract_text() or ""
        except Exception:
            t = ""
        parts.append(t)
    return "\n\n".join(parts).strip()


def _pypdf_fallback_pages(pdf_path: Path) -> list[tuple[int, str]]:
    """Return per-page text as (1-indexed page_no, text) pairs, preserving real page numbers."""
    from pypdf import PdfReader

    reader = PdfReader(str(pdf_path))
    pages: list[tuple[int, str]] = []
    for i, page in enumerate(reader.pages, start=1):
        try:
            t = (page.extract_text() or "").strip()
        except Exception:
            t = ""
        if t:
            pages.append((i, t))
    return pages


def _write_minimal_docling_json(
    content: str | list[tuple[int, str]], out_path: Path
) -> None:
    """Write the minimal fallback JSON consumed by chunk_from_fallback_json.

    ``content`` is either:
    - a plain string (all text, page_no will be 0 — avoid for large multi-page docs)
    - a list of (page_no, text) tuples from _pypdf_fallback_pages (preserves real page numbers)
    """
    if isinstance(content, str):
        texts: list[dict[str, Any]] = [{"text": content, "page_no": 0}]
    else:
        texts = [{"text": t, "page_no": pg} for pg, t in content if t.strip()]

    payload: dict[str, Any] = {
        "fallback": True,
        "sections": [
            {
                "path": "FALLBACK_EXTRACT",
                "texts": texts,
            }
        ],
    }
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


# PDF size thresholds for converter selection.
# Table-structure analysis (TableFormer) is the main VRAM/RAM spike for large PDFs.
_LEAN_THRESHOLD_MB = 4      # PDFs above this use a lean converter (no table structure)
_PYPDF_THRESHOLD_MB = 10    # PDFs above this try PyPDF first; scanned ones get OCR lean
_INCREMENTAL_WRITE_EVERY = 5  # flush parse_index.csv every N real (non-skipped) files
_MIN_TEXT_CHARS = 500        # below this, treat PDF as scanned and enable OCR


def _build_lean_converter(settings: Settings) -> DocumentConverter:
    """Minimal Docling converter: layout only, no table structure, no OCR. Safe for large PDFs."""
    opts = PdfPipelineOptions()
    opts.do_ocr = False
    opts.do_table_structure = False   # main memory hog for large PDFs
    opts.layout_batch_size = max(1, settings.docling_layout_batch_size // 2)
    opts.table_batch_size = 1
    opts.accelerator_options = AcceleratorOptions(
        device=settings.docling_device,
        num_threads=max(2, settings.docling_num_threads // 2),
    )
    logger.info("Building lean Docling converter (no table structure, no OCR) for large PDFs")
    return DocumentConverter(
        format_options={
            InputFormat.PDF: PdfFormatOption(
                pipeline_cls=StandardPdfPipeline,
                pipeline_options=opts,
            )
        },
    )


def _build_ocr_lean_converter(settings: Settings) -> DocumentConverter:
    """OCR-enabled lean Docling converter for scanned PDFs. No table structure, minimal batches."""
    opts = PdfPipelineOptions()
    opts.do_ocr = True
    opts.do_table_structure = False
    opts.ocr_batch_size = 1           # conservative to avoid OOM on large scanned pages
    opts.layout_batch_size = max(1, settings.docling_layout_batch_size // 2)
    opts.table_batch_size = 1
    opts.accelerator_options = AcceleratorOptions(
        device=settings.docling_device,
        num_threads=max(2, settings.docling_num_threads // 2),
    )
    logger.info("Building OCR lean Docling converter for scanned PDFs")
    return DocumentConverter(
        format_options={
            InputFormat.PDF: PdfFormatOption(
                pipeline_cls=StandardPdfPipeline,
                pipeline_options=opts,
            )
        },
    )


def _docling_json_char_count(out_path: Path) -> int:
    """Return total text character count from a saved Docling JSON, or 0 on error."""
    try:
        data = json.loads(out_path.read_text(encoding="utf-8"))
        # Real Docling JSON: texts array at top level
        if "texts" in data:
            return sum(len(item.get("text", "")) for item in data["texts"])
        # Fallback JSON: sections/texts structure
        if data.get("fallback"):
            return sum(
                len(t.get("text", ""))
                for s in data.get("sections", [])
                for t in s.get("texts", [])
            )
        return 0
    except Exception:
        return 0


def run_docling_on_filings(settings: Settings | None = None, *, force: bool = False) -> pd.DataFrame:
    settings = settings or get_settings()
    index_path = settings.resolve(settings.filings_index_path)
    doc_dir = settings.resolve(settings.doc_json_dir)
    doc_dir.mkdir(parents=True, exist_ok=True)

    out_idx = settings.resolve(settings.parse_index_csv)
    out_idx.parent.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(index_path)

    # Sort: process small PDFs first — ensures maximum files complete before hitting
    # large ones that may spike memory usage.  Absolute-path files always come last.
    pdf_root = settings.resolve(settings.filings_pdf_root) if settings.filings_pdf_root else settings.project_root

    def _pdf_size_bytes(row: Any) -> int:
        p = resolve_pdf_path(str(row["local_path"]), settings)
        try:
            return p.stat().st_size
        except OSError:
            return 0

    df = df.copy()
    df["_pdf_bytes"] = df.apply(_pdf_size_bytes, axis=1)
    df = df.sort_values("_pdf_bytes").reset_index(drop=True)

    # Load any incremental records from a previous partial run
    existing_records: dict[str, dict[str, Any]] = {}
    if out_idx.exists():
        try:
            prev = pd.read_csv(out_idx)
            for _, r in prev.iterrows():
                existing_records[str(r["filing_id"])] = r.to_dict()
            logger.info("Loaded %d prior parse records from %s", len(existing_records), out_idx)
        except Exception as exc:
            logger.warning("Could not load prior parse index: %s", exc)

    conv_full: DocumentConverter | None = None
    conv_lean: DocumentConverter | None = None
    conv_ocr_lean: DocumentConverter | None = None
    records: list[dict[str, Any]] = []
    new_since_flush = 0
    converts_since_reset = 0

    def _flush(final: bool = False) -> None:
        out_df = pd.DataFrame(records)
        out_df.to_csv(out_idx, index=False)
        if final:
            logger.info("parse_index.csv written: %d records", len(records))

    for _, row in tqdm(df.iterrows(), total=len(df), desc="docling"):
        filing_id = str(row["filing_id"])
        out_path = doc_dir / f"{filing_id}.json"
        pdf_path = resolve_pdf_path(str(row["local_path"]), settings)
        pdf_bytes: int = int(row.get("_pdf_bytes", 0))
        pdf_mb = pdf_bytes / 1_048_576

        # Already done in a prior run: use the recorded status
        if filing_id in existing_records and out_path.is_file():
            records.append(existing_records[filing_id])
            continue

        if not force and settings.skip_parse_if_exists and out_path.is_file():
            records.append(
                {
                    "filing_id": filing_id,
                    "local_path_pdf": str(pdf_path),
                    "local_path_docling": str(out_path),
                    "parse_status": "OK_SKIPPED",
                }
            )
            continue

        if not pdf_path.is_file():
            records.append(
                {
                    "filing_id": filing_id,
                    "local_path_pdf": str(pdf_path),
                    "local_path_docling": "",
                    "parse_status": "PDF_MISSING",
                }
            )
            continue

        status: str
        # Very large PDFs: try PyPDF first (fast, zero VRAM).
        # If the PDF is scanned (empty text), fall back to OCR-lean Docling.
        if pdf_mb >= _PYPDF_THRESHOLD_MB:
            logger.info(
                "PDF %.1fMB >= %dMB threshold — trying PyPDF for %s",
                pdf_mb, _PYPDF_THRESHOLD_MB, filing_id,
            )
            try:
                pages = _pypdf_fallback_pages(pdf_path)
                text = "\n\n".join(t for _, t in pages)
            except Exception as e_fb:
                pages, text = [], ""
                logger.warning("PyPDF failed for %s: %s", filing_id, e_fb)

            if len(text) >= _MIN_TEXT_CHARS:
                _write_minimal_docling_json(pages, out_path)  # per-page → real page_no
                status = "OK_FALLBACK"
            else:
                # Likely a scanned PDF — OCR is the only way to get text
                logger.info(
                    "%.1fMB PDF appears scanned (%d chars from PyPDF) — running OCR lean Docling for %s",
                    pdf_mb, len(text), filing_id,
                )
                if conv_ocr_lean is None:
                    conv_ocr_lean = _build_ocr_lean_converter(settings)
                try:
                    result = conv_ocr_lean.convert(str(pdf_path))
                    result.document.save_as_json(out_path)
                    status = "OK_OCR"
                except Exception as e_ocr:
                    logger.error("OCR lean Docling also failed for %s: %s", filing_id, e_ocr)
                    status = f"ERROR:{e_ocr}"
        else:
            # Medium PDFs (4-10MB): lean converter (no table structure, no OCR)
            # Small PDFs (<4MB): full converter
            if pdf_mb >= _LEAN_THRESHOLD_MB:
                if conv_lean is None:
                    conv_lean = _build_lean_converter(settings)
                conv = conv_lean
            else:
                if conv_full is None:
                    conv_full = build_docling_converter(settings)
                conv = conv_full

            try:
                result = conv.convert(str(pdf_path))
                result.document.save_as_json(out_path)
                # Detect silent scanned failure: Docling ran but extracted no text
                char_count = _docling_json_char_count(out_path)
                if char_count < _MIN_TEXT_CHARS:
                    logger.info(
                        "Docling produced minimal text (%d chars) for %s (%.1fMB) — retrying with OCR",
                        char_count, filing_id, pdf_mb,
                    )
                    if conv_ocr_lean is None:
                        conv_ocr_lean = _build_ocr_lean_converter(settings)
                    try:
                        result2 = conv_ocr_lean.convert(str(pdf_path))
                        result2.document.save_as_json(out_path)
                        status = "OK_OCR"
                    except Exception as e_ocr2:
                        logger.warning("OCR retry also failed for %s: %s", filing_id, e_ocr2)
                        status = "OK"  # keep original (may be legitimately short)
                else:
                    status = "OK"
            except Exception as e:
                logger.warning("Docling failed for %s (%.1fMB): %s; trying pypdf fallback", filing_id, pdf_mb, e)
                try:
                    pages = _pypdf_fallback_pages(pdf_path)
                    text = "\n\n".join(t for _, t in pages)
                    if len(text) >= _MIN_TEXT_CHARS:
                        _write_minimal_docling_json(pages, out_path)
                        status = "OK_FALLBACK"
                    else:
                        status = f"ERROR:{e}"
                except Exception as e2:
                    status = f"ERROR:{e}|FALLBACK:{e2}"

        records.append(
            {
                "filing_id": filing_id,
                "local_path_pdf": str(pdf_path),
                "local_path_docling": str(out_path) if status.startswith("OK") else "",
                "parse_status": status,
            }
        )

        # GPU / native stack hygiene — exit code -11 (SIGSEGV) sometimes tracks VRAM pressure.
        if status.startswith("OK"):
            converts_since_reset += 1
            if settings.docling_cuda_empty_cache_each_pdf and torch.cuda.is_available():
                try:
                    torch.cuda.empty_cache()
                except Exception:
                    pass
            gc.collect()
            reset_every = int(settings.docling_converter_reset_every)
            if reset_every > 0 and converts_since_reset >= reset_every:
                conv_full = conv_lean = conv_ocr_lean = None
                converts_since_reset = 0
                if torch.cuda.is_available():
                    try:
                        torch.cuda.empty_cache()
                    except Exception:
                        pass
                gc.collect()
                logger.info(
                    "Docling converters reset (%s PDFs; DOCLING_CONVERTER_RESET_EVERY)",
                    reset_every,
                )

        new_since_flush += 1
        if new_since_flush >= _INCREMENTAL_WRITE_EVERY:
            _flush()
            new_since_flush = 0

    _flush(final=True)
    return pd.read_csv(out_idx)
