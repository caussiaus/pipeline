"""Targeted extraction node.

Supports two modes of operation:

  design (sample) mode  — run on a small subset of tickers chosen by the user
                          so they can validate field definitions before committing
                          to a full-corpus run. Uses the interactive vLLM profile.

  full-corpus mode      — run on all filings. Uses the batch vLLM profile.

Each extracted row carries per-field evidence spans (quote + chunk_id + page) so
every cell can answer: which row is this, what evidence supports it, who overrode it.
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
from typing import Any

import orjson
import pandas as pd

from tariff_agent.dataset_graph.state import CellRecord, DatasetState, EvidenceSpan, SchemaColumn
from tariff_agent.prompts.dataset_prompt import (
    EXTRACTION_SYSTEM_PROMPT,
    build_dynamic_json_schema,
    build_extraction_user_prompt,
)
from tariff_agent.utils.config import get_settings
from tariff_agent.utils.vllm_router import get_profile, make_async_client, profile_for_workload

logger = logging.getLogger(__name__)

_FENCE_RE = re.compile(r"^\s*```(?:json)?\s*|\s*```\s*$", re.I | re.M)
_THINK_RE = re.compile(r"<think>.*?</think>|<redacted_thinking>.*?</redacted_thinking>", re.I | re.DOTALL)
_JSON_OBJ_RE = re.compile(r"\{.*\}", re.DOTALL)


def _strip(s: str) -> str:
    s = _THINK_RE.sub("", s).strip()
    return _FENCE_RE.sub("", s).strip()


def _parse_json(content: str) -> dict[str, Any]:
    raw = _strip(content)
    try:
        return orjson.loads(raw)
    except Exception:
        m = _JSON_OBJ_RE.search(raw)
        if m:
            return orjson.loads(m.group())
        raise


def _null_evidence() -> dict[str, Any]:
    return {"quote": None, "chunk_id": None, "page_start": None, "page_end": None, "section_path": None}


def _default_row(
    filing_meta: dict[str, Any],
    columns: list[SchemaColumn],
    *,
    error: str = "",
) -> dict[str, Any]:
    row: dict[str, Any] = {
        "filing_id": filing_meta.get("filing_id", ""),
        "ticker": filing_meta.get("ticker", ""),
        "issuer_name": filing_meta.get("issuer_name", ""),
        "profile_number": filing_meta.get("profile_number", ""),
        "filing_date": filing_meta.get("filing_date", ""),
        "naics_sector": filing_meta.get("naics_sector", ""),
        "mechanism": filing_meta.get("mechanism", ""),
        "_extraction_error": error,
    }
    for col in columns:
        row[col["name"]] = col.get("default")
        row[f"{col['name']}_evidence_quote"] = None
        row[f"{col['name']}_evidence_pages"] = None
        row[f"{col['name']}_evidence_section"] = None
    return row


def _cells_from_row(
    row: dict[str, Any],
    columns: list[SchemaColumn],
) -> list[CellRecord]:
    """Convert a flat row dict into structured CellRecord objects."""
    cells: list[CellRecord] = []
    fid = row.get("filing_id", "")
    for col in columns:
        ev_q = row.get(f"{col['name']}_evidence_quote")
        ev_p = row.get(f"{col['name']}_evidence_pages", "")
        ev_s = row.get(f"{col['name']}_evidence_section")
        # Parse page range back out
        p0, p1 = 0, 0
        if ev_p and ev_p != "None":
            parts = str(ev_p).split("-")
            try:
                p0 = int(parts[0])
                p1 = int(parts[1]) if len(parts) > 1 else p0
            except (ValueError, IndexError):
                pass
        evidence: EvidenceSpan = {
            "quote": ev_q,
            "chunk_id": None,
            "page_start": p0 or None,
            "page_end": p1 or None,
            "section_path": ev_s,
            "relevance": "direct" if ev_q else "indirect",
        }
        cells.append(CellRecord(
            row_id=fid,
            field_name=col["name"],
            proposed_value=row.get(col["name"]),
            evidence=evidence if ev_q else None,
            decision="proposed",
        ))
    return cells


async def _extract_one(
    client,
    sem: asyncio.Semaphore,
    cfg,
    profile,
    filing_meta: dict[str, Any],
    evidence_blocks: list[dict[str, Any]],
    columns: list[SchemaColumn],
    json_schema: dict,
    *,
    all_chunks: int,
    keyword_hits: int,
    pass1_pos: int,
    extraction_mode: str,
) -> dict[str, Any]:
    user_prompt = build_extraction_user_prompt(
        columns=columns,
        filing_meta=filing_meta,
        evidence_blocks=evidence_blocks,
        all_chunks_count=all_chunks,
        keyword_hit_count=keyword_hits,
        pass1_positive_count=pass1_pos,
        extraction_mode=extraction_mode,
    )

    extra: dict[str, Any] = profile.extra_body(
        guided_json=json_schema if cfg.use_guided_decoding else None
    )

    for attempt in range(3):
        try:
            async with sem:
                resp = await client.chat.completions.create(
                    model=cfg.vllm_model_name,
                    messages=[
                        {"role": "system", "content": EXTRACTION_SYSTEM_PROMPT},
                        {"role": "user", "content": user_prompt},
                    ],
                    temperature=profile.temperature,
                    max_tokens=profile.max_tokens,
                    top_p=profile.top_p,
                    extra_body=extra if extra else None,
                    response_format={"type": "json_object"} if not cfg.use_guided_decoding else None,
                )
            raw = resp.choices[0].message.content or ""
            data = _parse_json(raw)
        except Exception as exc:
            if attempt == 2:
                logger.warning("extraction failed for %s: %s", filing_meta.get("filing_id"), exc)
                row = _default_row(filing_meta, columns, error=str(exc))
                row.update({"_all_chunks": all_chunks, "_keyword_hits": keyword_hits,
                            "_pass1_positive": pass1_pos, "_evidence_blocks_used": 0})
                return row
            await asyncio.sleep(2 ** attempt)
            continue

        # Build flat row: identity + field values + flattened evidence
        row = _default_row(filing_meta, columns)
        for col in columns:
            name = col["name"]
            if name in data:
                row[name] = data[name]
            # Unpack companion evidence object
            ev_key = f"{name}_evidence"
            ev = data.get(ev_key) or {}
            if isinstance(ev, dict):
                row[f"{name}_evidence_quote"] = ev.get("quote")
                p0, p1 = ev.get("page_start"), ev.get("page_end")
                row[f"{name}_evidence_pages"] = f"{p0}-{p1}" if p0 is not None else None
                row[f"{name}_evidence_section"] = ev.get("section_path")

        row["_all_chunks"] = all_chunks
        row["_keyword_hits"] = keyword_hits
        row["_pass1_positive"] = pass1_pos
        row["_evidence_blocks_used"] = len(evidence_blocks)
        return row

    return _default_row(filing_meta, columns, error="max retries")


def _build_evidence_blocks(
    filing_id: str,
    chunks: pd.DataFrame,
    chunks_llm: pd.DataFrame,
) -> tuple[list[dict], int, int, int]:
    """Return (evidence_blocks, total_chunks, keyword_hits, pass1_positive).

    keyword_hit lives in chunks.parquet; mentions_tariffs in chunks_llm.parquet.
    Text and page provenance come from chunks.parquet.
    """
    f_chunks = chunks[chunks["filing_id"] == filing_id] if not chunks.empty else pd.DataFrame()
    f_llm = chunks_llm[chunks_llm["filing_id"] == filing_id] if not chunks_llm.empty else pd.DataFrame()

    total = len(f_chunks)

    kw_hits = 0
    if not f_chunks.empty and "keyword_hit" in f_chunks.columns:
        kw_hits = int(f_chunks["keyword_hit"].astype(str).str.lower().isin(["true", "1"]).sum())

    pos_ids: set[str] = set()
    if not f_llm.empty and "mentions_tariffs" in f_llm.columns:
        pos_mask = f_llm["mentions_tariffs"].astype(str).str.lower().isin(["true", "1"])
        pos_ids = set(f_llm.loc[pos_mask, "chunk_id"].astype(str).tolist())

    pass1_pos = len(pos_ids)

    blocks: list[dict] = []
    if pos_ids and not f_chunks.empty and "chunk_id" in f_chunks.columns:
        pos_chunks = f_chunks[f_chunks["chunk_id"].astype(str).isin(pos_ids)]
        for _, row in pos_chunks.iterrows():
            blocks.append({
                "chunk_id": str(row.get("chunk_id", "")),
                "text": str(row.get("text", "")),
                "section_path": str(row.get("section_path", "")),
                "page_start": row.get("page_start", 0),
                "page_end": row.get("page_end", 0),
            })

    return blocks, total, kw_hits, pass1_pos


def _load_corpus_data(
    settings,
    state: DatasetState | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    st = state or {}
    idx_path = st.get("corpus_index_csv") or str(settings.resolve(settings.filings_index_path))
    chunks_path = st.get("corpus_chunks_parquet") or str(settings.resolve(settings.chunks_parquet))
    llm_path = st.get("corpus_chunks_llm_parquet") or str(settings.resolve(settings.chunks_llm_parquet))

    idx = pd.read_csv(idx_path, dtype=str)
    try:
        chunks = pd.read_parquet(chunks_path)
    except Exception:
        chunks = pd.DataFrame(
            columns=[
                "filing_id", "chunk_id", "text", "section_path",
                "page_start", "page_end", "keyword_hit",
            ]
        )
    try:
        chunks_llm = pd.read_parquet(llm_path)
    except Exception:
        chunks_llm = pd.DataFrame(columns=["filing_id", "chunk_id", "mentions_tariffs"])
    return idx, chunks, chunks_llm


async def _run_extraction(
    cfg,
    idx: pd.DataFrame,
    chunks: pd.DataFrame,
    chunks_llm: pd.DataFrame,
    columns: list[SchemaColumn],
    extraction_mode: str,
    profile_name: str,
) -> list[dict[str, Any]]:
    profile = get_profile(profile_name, cfg)  # type: ignore[arg-type]
    client = make_async_client(profile, cfg)
    sem = asyncio.Semaphore(profile.max_concurrent_requests)
    json_schema = build_dynamic_json_schema(columns, with_evidence=True)

    tasks = []
    for _, row in idx.iterrows():
        meta = row.to_dict()
        fid = str(meta.get("filing_id") or meta.get("doc_id", ""))
        blocks, total, kw, pos = _build_evidence_blocks(fid, chunks, chunks_llm)
        tasks.append(
            _extract_one(
                client, sem, cfg, profile, meta, blocks, columns, json_schema,
                all_chunks=total, keyword_hits=kw, pass1_pos=pos,
                extraction_mode=extraction_mode,
            )
        )

    logger.info("extraction_node: %d async extractions [profile=%s]", len(tasks), profile_name)
    return list(await asyncio.gather(*tasks, return_exceptions=False))


def extraction_node(state: DatasetState) -> DatasetState:
    """Run targeted extraction.

    If state['use_sample'] is True, restrict to state['sample_tickers'] and
    use the interactive vLLM profile (lower concurrency, tunable temperature).
    Otherwise run on the full corpus with the batch profile.
    """
    cfg = get_settings()
    columns = state.get("proposed_columns", [])
    if not columns:
        return {**state, "error": "No columns defined — run schema_node first"}

    idx, chunks, chunks_llm = _load_corpus_data(cfg, state)

    # ── Sample mode ──────────────────────────────────────────────────────
    use_sample = state.get("use_sample", False)
    sample_tickers = state.get("sample_tickers") or []
    full_idx = idx
    if use_sample and sample_tickers:
        if "ticker" in full_idx.columns:
            idx = full_idx[full_idx["ticker"].isin(sample_tickers)].copy()
        elif "issuer_name" in full_idx.columns:
            def _issuer_match(row: dict) -> bool:
                name = str(row.get("issuer_name", "")).lower()
                for t in sample_tickers:
                    frag = t.lower().replace("tsx:", "").replace("nyse:", "").strip("_")
                    if frag and frag in name:
                        return True
                return False
            idx = full_idx[full_idx.apply(_issuer_match, axis=1)].copy()
        else:
            idx = full_idx.copy()
        logger.info("extraction_node: sample mode — %d filings for filter %s", len(idx), sample_tickers)
        if idx.empty:
            if len(full_idx) <= 50:
                idx = full_idx.copy()
                logger.info(
                    "extraction_node: sample filter empty; using full small corpus (%d rows)",
                    len(idx),
                )
            elif "ticker" not in full_idx.columns:
                idx = full_idx.copy()
                logger.info(
                    "extraction_node: no ticker column; sample filter ignored (%d rows)",
                    len(idx),
                )
            else:
                return {**state, "error": f"No rows matched sample filter (tickers/names): {sample_tickers}"}

    n_rows = len(idx)
    profile_name = "interactive" if use_sample else profile_for_workload(n_rows)
    extraction_mode = state.get("extraction_mode") or "direct"

    logger.info(
        "extraction_node: %d filings | profile=%s | mode=%s",
        n_rows, profile_name, extraction_mode,
    )

    rows = asyncio.run(
        _run_extraction(cfg, idx, chunks, chunks_llm, columns, extraction_mode, profile_name)
    )

    cells: list[CellRecord] = []
    for row in rows:
        cells.extend(_cells_from_row(row, columns))

    return {
        **state,
        "rows": rows,
        "cells": cells,
        "extraction_done": True,
        "error": "",
    }
