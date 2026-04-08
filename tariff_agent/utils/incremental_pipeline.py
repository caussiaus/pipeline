"""Parse/chunk incremental steps for a subset of filing_ids (per-company watcher runs)."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import pandas as pd
from tqdm import tqdm

from tariff_agent.state import ChunkLLMOutput, ChunkRecord
from tariff_agent.utils.chunking import chunk_document_path
from tariff_agent.utils.config import Settings, get_settings
from tariff_agent.utils.docling_pipeline import (
    _pypdf_fallback_text,
    _write_minimal_docling_json,
    build_docling_converter,
    resolve_pdf_path,
)

logger = logging.getLogger(__name__)


def merge_filings_index_rows(
    rows: list[dict[str, Any]],
    settings: Settings | None = None,
) -> pd.DataFrame:
    settings = settings or get_settings()
    path = settings.resolve(settings.filings_index_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    new_df = pd.DataFrame(rows)
    if new_df.empty:
        return pd.read_csv(path) if path.is_file() else new_df
    if "filing_id" not in new_df.columns:
        raise ValueError("merge_filings_index_rows requires filing_id")
    new_df = new_df.copy()
    new_df["filing_id"] = new_df["filing_id"].astype(str)
    if path.is_file():
        old = pd.read_csv(path)
        old["filing_id"] = old["filing_id"].astype(str)
        old = old[~old["filing_id"].isin(set(new_df["filing_id"]))]
        merged = pd.concat([old, new_df], ignore_index=True)
    else:
        merged = new_df
    merged.to_csv(path, index=False)
    return merged


def run_docling_for_filings(
    filing_ids: set[str],
    settings: Settings | None = None,
    *,
    force: bool = False,
) -> pd.DataFrame:
    settings = settings or get_settings()
    want = {str(x) for x in filing_ids}
    index_path = settings.resolve(settings.filings_index_path)
    doc_dir = settings.resolve(settings.doc_json_dir)
    doc_dir.mkdir(parents=True, exist_ok=True)
    df = pd.read_csv(index_path)
    df["filing_id"] = df["filing_id"].astype(str)
    subset = df[df["filing_id"].isin(want)]
    out_idx = settings.resolve(settings.parse_index_csv)
    out_idx.parent.mkdir(parents=True, exist_ok=True)
    if out_idx.is_file():
        prev = pd.read_csv(out_idx)
        prev["filing_id"] = prev["filing_id"].astype(str)
        prev = prev[~prev["filing_id"].isin(subset["filing_id"])]
    else:
        prev = pd.DataFrame()

    conv = build_docling_converter(settings)
    records: list[dict[str, Any]] = []
    for _, row in tqdm(subset.iterrows(), total=len(subset), desc="docling(subset)"):
        filing_id = str(row["filing_id"])
        out_path = doc_dir / f"{filing_id}.json"
        pdf_path = resolve_pdf_path(str(row["local_path"]), settings)
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
        try:
            result = conv.convert(str(pdf_path))
            result.document.save_as_json(out_path)
            status = "OK"
        except Exception as e:
            logger.warning("Docling failed for %s: %s; trying pypdf fallback", filing_id, e)
            try:
                text = _pypdf_fallback_text(pdf_path)
                if not text:
                    status = f"ERROR:{e}"
                else:
                    _write_minimal_docling_json(text, out_path)
                    status = "OK_FALLBACK"
            except Exception as e2:
                status = f"ERROR:{e}|FALLBACK:{e2}"
        records.append(
            {
                "filing_id": filing_id,
                "local_path_pdf": str(pdf_path),
                "local_path_docling": str(out_path) if str(status).startswith("OK") else "",
                "parse_status": status,
            }
        )
    new_df = pd.DataFrame(records)
    merged = pd.concat([prev, new_df], ignore_index=True) if len(prev) else new_df
    merged.to_csv(out_idx, index=False)
    return merged


def run_chunking_for_filings(
    filing_ids: set[str],
    settings: Settings | None = None,
    *,
    force: bool = False,
) -> pd.DataFrame:
    settings = settings or get_settings()
    want = {str(x) for x in filing_ids}
    filings = pd.read_csv(settings.resolve(settings.filings_index_path))
    filings["filing_id"] = filings["filing_id"].astype(str)
    parse_idx = pd.read_csv(settings.resolve(settings.parse_index_csv))
    parse_idx["filing_id"] = parse_idx["filing_id"].astype(str)
    ok = parse_idx[parse_idx["parse_status"].astype(str).str.startswith("OK", na=False)]
    merged = filings.merge(ok[["filing_id", "local_path_docling"]], on="filing_id")
    subset = merged[merged["filing_id"].isin(want)]
    out_parquet = settings.resolve(settings.chunks_parquet)
    out_parquet.parent.mkdir(parents=True, exist_ok=True)
    target = settings.chunk_target_tokens

    existing = pd.DataFrame()
    if out_parquet.is_file():
        existing = pd.read_parquet(out_parquet)
        if "filing_id" in existing.columns:
            existing = existing[~existing["filing_id"].astype(str).isin(want)]

    llm_p = settings.resolve(settings.chunks_llm_parquet)
    if llm_p.is_file():
        llm_df = pd.read_parquet(llm_p)
        if "filing_id" in llm_df.columns:
            llm_df = llm_df[~llm_df["filing_id"].astype(str).isin(want)]
            llm_p.parent.mkdir(parents=True, exist_ok=True)
            if llm_df.empty:
                pd.DataFrame(
                    columns=[f for f in ChunkLLMOutput.model_fields.keys()],
                ).to_parquet(llm_p, index=False)
            else:
                llm_df.to_parquet(llm_p, index=False)

    if not force and settings.skip_chunk_if_exists and subset.empty and out_parquet.is_file():
        return pd.read_parquet(out_parquet)

    all_records: list[dict[str, Any]] = []
    for _, row in tqdm(subset.iterrows(), total=len(subset), desc="chunking(subset)"):
        doc_json_path = Path(str(row["local_path_docling"]))
        if not doc_json_path.is_file():
            continue
        chunks = chunk_document_path(doc_json_path, row, target)
        all_records.extend(c.model_dump() for c in chunks)

    new_chunks = pd.DataFrame(all_records)
    if new_chunks.empty and existing.empty:
        cols = [f for f in ChunkRecord.model_fields.keys()]
        final = pd.DataFrame(columns=cols)
    elif new_chunks.empty:
        final = existing
    elif existing.empty:
        final = new_chunks
    else:
        final = pd.concat([existing, new_chunks], ignore_index=True)

    final.to_parquet(out_parquet, index=False)
    return final
