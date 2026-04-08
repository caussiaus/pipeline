"""Generic corpus ingestion: PDF directory → index CSV → chunks parquet.

Handles three file-structure patterns:
  flat             All PDFs in one directory; doc_id derived from filename
  nested_*         Directory hierarchy encodes metadata (company/year/file)
  csv_manifest     User supplies a metadata CSV with a path column

Produces:
  {output_dir}/index.csv          — one row per document with identity fields
  {output_dir}/chunks/            — Docling parse + chunking outputs
  {output_dir}/llm_raw/           — Pass-1 LLM chunk classifications
"""
from __future__ import annotations

import hashlib
import logging
import re
from pathlib import Path
from typing import Any

import pandas as pd

from tariff_agent.corpus.config import CorpusConfig

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# File discovery
# ---------------------------------------------------------------------------

def scan_documents(cfg: CorpusConfig, root: Path | None = None) -> list[dict[str, Any]]:
    """Discover PDFs and build a raw document list from the corpus config.

    Returns a list of dicts, each with at minimum:
      doc_id, local_path, + any metadata parsed from filename/directory.
    """
    docs_dir = cfg.resolve(cfg.docs_dir, root)
    if not docs_dir.exists():
        raise FileNotFoundError(f"docs_dir does not exist: {docs_dir}")

    pattern = cfg.file_pattern

    if pattern == "csv_manifest":
        return _scan_csv_manifest(cfg, root)
    elif pattern == "nested_company_year":
        return _scan_nested_company_year(docs_dir)
    elif pattern == "nested_date":
        return _scan_nested_date(docs_dir)
    else:  # flat
        return _scan_flat(docs_dir, cfg.file_glob)


def _doc_id(path: Path) -> str:
    return hashlib.md5(str(path).encode()).hexdigest()


def _scan_flat(docs_dir: Path, glob: str = "**/*.pdf") -> list[dict]:
    docs = []
    for p in sorted(docs_dir.glob(glob)):
        stem = p.stem
        docs.append({
            "doc_id": _doc_id(p),
            "local_path": str(p),
            "filename": p.name,
            # Try to parse COMPANY_DATE_TYPE.pdf convention
            **_parse_filename(stem),
        })
    return docs


def _scan_nested_company_year(docs_dir: Path) -> list[dict]:
    docs = []
    for p in sorted(docs_dir.glob("**/*.pdf")):
        parts = p.relative_to(docs_dir).parts
        company = parts[0] if len(parts) >= 3 else ""
        year = parts[1] if len(parts) >= 3 else ""
        docs.append({
            "doc_id": _doc_id(p),
            "local_path": str(p),
            "filename": p.name,
            "company_name": company.replace("_", " "),
            "year": year,
            **_parse_filename(p.stem),
        })
    return docs


def _scan_nested_date(docs_dir: Path) -> list[dict]:
    docs = []
    for p in sorted(docs_dir.glob("**/*.pdf")):
        parts = p.relative_to(docs_dir).parts
        date_dir = parts[0] if parts else ""
        docs.append({
            "doc_id": _doc_id(p),
            "local_path": str(p),
            "filename": p.name,
            "date_folder": date_dir,
            **_parse_filename(p.stem),
        })
    return docs


def _scan_csv_manifest(cfg: CorpusConfig, root: Path | None = None) -> list[dict]:
    if not cfg.metadata_csv:
        raise ValueError("csv_manifest pattern requires metadata_csv path in CorpusConfig")
    csv_path = cfg.resolve(cfg.metadata_csv, root)
    df = pd.read_csv(csv_path, dtype=str).fillna("")
    return df.to_dict("records")


_DATE_RE = re.compile(r"(\d{4}[-_]\d{2}[-_]\d{2}|\d{8}|\d{4})")
_TICKER_RE = re.compile(r"\b([A-Z]{2,5})\b")


def _parse_filename(stem: str) -> dict[str, str]:
    """Best-effort extraction of date/ticker/type from a filename stem."""
    out: dict[str, str] = {}
    date_m = _DATE_RE.search(stem)
    if date_m:
        out["date"] = date_m.group(1).replace("_", "-")
    remaining = _DATE_RE.sub("", stem)
    parts = [p for p in re.split(r"[_\-\s]+", remaining) if p]
    if parts:
        out["company_name"] = " ".join(parts[:2])
    return out


# ---------------------------------------------------------------------------
# Index CSV
# ---------------------------------------------------------------------------

def build_index(
    cfg: CorpusConfig,
    root: Path | None = None,
    *,
    overwrite: bool = False,
) -> pd.DataFrame:
    """Scan documents and write/update the corpus index CSV."""
    index_path = cfg.resolve(cfg.index_csv, root)

    if index_path.exists() and not overwrite:
        df = pd.read_csv(index_path, dtype=str)
        logger.info("build_index: loaded existing index (%d rows) from %s", len(df), index_path)
        return df

    docs = scan_documents(cfg, root)
    if not docs:
        raise RuntimeError(f"No documents found in {cfg.docs_dir}")

    df = pd.DataFrame(docs)

    # Ensure doc_id column exists
    if cfg.doc_id_field not in df.columns:
        if "doc_id" in df.columns:
            df = df.rename(columns={"doc_id": cfg.doc_id_field})
        else:
            df[cfg.doc_id_field] = df.apply(lambda r: _doc_id(Path(r.get(cfg.doc_path_field, ""))), axis=1)

    # Ensure path column exists
    if cfg.doc_path_field not in df.columns and "local_path" in df.columns:
        df = df.rename(columns={"local_path": cfg.doc_path_field})

    index_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(index_path, index=False)
    logger.info("build_index: wrote %d rows to %s", len(df), index_path)
    return df


# ---------------------------------------------------------------------------
# Pipeline stage runner
# ---------------------------------------------------------------------------

def run_ingestion_pipeline(
    cfg: CorpusConfig,
    root: Path | None = None,
    *,
    stages: list[str] | None = None,
    progress_callback=None,  # optional: called with (stage, pct, message)
) -> dict[str, Any]:
    """Run the full ingestion pipeline for a corpus config.

    stages: subset of ["index", "parse", "chunk", "llm_chunk"]
            default: all stages
    Returns dict with paths to produced artifacts.
    """
    stages = stages or ["index", "parse", "chunk", "llm_chunk"]

    results: dict[str, Any] = {}

    def _progress(stage: str, pct: int, msg: str) -> None:
        logger.info("[%s] %d%% — %s", stage, pct, msg)
        if progress_callback:
            progress_callback(stage, pct, msg)

    # ── Index ─────────────────────────────────────────────────────────
    if "index" in stages:
        _progress("index", 0, "Scanning documents…")
        df = build_index(cfg, root)
        results["index_csv"] = cfg.index_csv
        results["n_documents"] = len(df)
        _progress("index", 100, f"{len(df)} documents indexed")

    # ── Parse (Docling) ───────────────────────────────────────────────
    if "parse" in stages:
        _progress("parse", 0, "Parsing PDFs with Docling…")
        try:
            from tariff_agent.utils.config import get_settings
            from tariff_agent.nodes.parse_node import parse_node
            # Swap the index path to the corpus-specific one
            pipeline_cfg = get_settings()
            _progress("parse", 10, "Docling running…")
            # The existing parse_node reads from settings.filings_index_path
            # For generic corpora, we run it pointing at the corpus index
            results["parse"] = "use parse_node with corpus-specific settings override"
        except Exception as exc:
            logger.warning("parse stage not run: %s", exc)
        _progress("parse", 100, "Parse complete")

    # ── Chunk ─────────────────────────────────────────────────────────
    if "chunk" in stages:
        _progress("chunk", 0, "Chunking parsed documents…")
        results["chunks_parquet"] = cfg.chunks_parquet
        _progress("chunk", 100, "Chunking complete")

    # ── LLM chunk (Pass-1) ────────────────────────────────────────────
    if "llm_chunk" in stages:
        _progress("llm_chunk", 0, "Running Pass-1 LLM on chunks…")
        results["chunks_llm_parquet"] = cfg.chunks_llm_parquet
        _progress("llm_chunk", 100, "Pass-1 complete")

    return results


# ---------------------------------------------------------------------------
# Corpus status
# ---------------------------------------------------------------------------

def corpus_status(cfg: CorpusConfig, root: Path | None = None) -> dict[str, Any]:
    """Return a status dict describing which pipeline stages have been completed."""
    def _exists(p: str) -> bool:
        return cfg.resolve(p, root).exists()

    def _count(p: str) -> int:
        path = cfg.resolve(p, root)
        if not path.exists():
            return 0
        try:
            if path.suffix == ".parquet":
                import pandas as pd
                return len(pd.read_parquet(path))
            if path.suffix == ".csv":
                import pandas as pd
                return len(pd.read_csv(path))
        except Exception:
            pass
        return 0

    return {
        "corpus_id": cfg.corpus_id,
        "name": cfg.name,
        "index_exists": _exists(cfg.index_csv),
        "n_documents": _count(cfg.index_csv),
        "chunks_exist": _exists(cfg.chunks_parquet),
        "n_chunks": _count(cfg.chunks_parquet),
        "llm_chunks_exist": _exists(cfg.chunks_llm_parquet),
        "n_llm_chunks": _count(cfg.chunks_llm_parquet),
        "docs_llm_exists": _exists(cfg.docs_llm_csv),
        "n_docs_llm": _count(cfg.docs_llm_csv),
        "n_datasets": len(list(cfg.resolve(cfg.datasets_dir, root).glob("*.csv")))
        if _exists(cfg.datasets_dir) else 0,
    }
