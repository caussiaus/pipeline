"""Apply corpus-specific paths to environment variables before ``get_settings()``."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from tariff_agent.corpus.config import CorpusConfig

_ENV_MAP = (
    ("FILINGS_INDEX_PATH", "index_csv"),
    ("FILINGS_PDF_ROOT", "filings_pdf_root_env"),
    ("DOC_JSON_DIR", "doc_json_dir"),
    ("PARSE_INDEX_CSV", "parse_index_csv"),
    ("CHUNKS_PARQUET", "chunks_parquet"),
    ("CHUNKS_LLM_PARQUET", "chunks_llm_parquet"),
    ("FILINGS_LLM_PARQUET", "filings_llm_parquet"),
    ("FILINGS_LLM_CSV", "docs_llm_csv"),  # Settings field is filings_llm_csv; CorpusConfig uses docs_llm_csv
    ("DATASETS_DIR", "datasets_dir"),
    ("CONSISTENCY_REPORT_CSV", "consistency_report_csv"),
)


def corpus_settings_overrides(cfg: CorpusConfig, project_root: Path) -> dict[str, str]:
    """Map env var name → string path for this corpus (absolute where helpful)."""
    root = project_root

    def R(p: str) -> str:
        return str(cfg.resolve(p, root))

    # Optional: SEDAR-style corpus uses relative paths + PDF root
    pdf_root = getattr(cfg, "filings_pdf_root_env", "") or ""

    out: dict[str, str] = {
        "FILINGS_INDEX_PATH": R(cfg.index_csv),
        "FILINGS_PDF_ROOT": str(normalize_pdf_root(pdf_root, root)) if pdf_root else "",
        "DOC_JSON_DIR": R(cfg.doc_json_dir),
        "PARSE_INDEX_CSV": R(cfg.parse_index_csv),
        "CHUNKS_PARQUET": R(cfg.chunks_parquet),
        "CHUNKS_LLM_PARQUET": R(cfg.chunks_llm_parquet),
        "FILINGS_LLM_PARQUET": R(cfg.filings_llm_parquet),
        "FILINGS_LLM_CSV": R(cfg.docs_llm_csv),
        "DATASETS_DIR": R(cfg.datasets_dir),
        "CONSISTENCY_REPORT_CSV": R(cfg.consistency_report_csv),
    }
    return out


def normalize_pdf_root(pdf_root: str, project_root: Path) -> Path:
    p = Path(pdf_root.replace("\\", "/"))
    if p.is_absolute():
        return p
    return (project_root / p).resolve()


def apply_corpus_env(cfg: CorpusConfig, project_root: Path) -> dict[str, str]:
    """Set ``os.environ`` for pipeline stages. Returns the dict applied."""
    overrides = corpus_settings_overrides(cfg, project_root)
    for k, v in overrides.items():
        os.environ[k] = v
    return overrides


def clear_corpus_env(overrides: dict[str, str] | None) -> None:
    """Remove keys previously applied (optional cleanup)."""
    if not overrides:
        return
    for k in overrides:
        os.environ.pop(k, None)
