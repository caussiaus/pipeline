"""Assess whether the tariff pipeline has reached a logical end state.

Criteria match the repo mandate: Docling parse index complete (no ERROR rows), section-aware
chunks, Pass-1 / Pass-2 LLM artifacts, issuer×fiscal_year aggregation, and optional
human-review CSV. Used by ``scripts/pipeline_supervisor.py`` for self-healing runs.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from tariff_agent.utils.config import Settings, get_settings


def _parse_status_terminal_ok(status: object) -> bool:
    """OK / OK_SKIPPED / OK_FALLBACK / PDF_MISSING are terminal; ERROR:* is not."""
    s = str(status).strip()
    if not s:
        return False
    if s == "PDF_MISSING":
        return True
    return s.startswith("OK")


def _parse_status_is_error(status: object) -> bool:
    s = str(status).strip()
    return bool(s) and not _parse_status_terminal_ok(s)


@dataclass
class CompletionReport:
    ok: bool
    n_filings: int
    parse_index_exists: bool
    n_parse_rows: int
    n_parse_errors: int
    n_parse_missing_filings: int
    parse_complete: bool
    chunks_parquet_exists: bool
    n_chunks: int
    chunks_llm_exists: bool
    n_chunks_llm: int
    filings_llm_exists: bool
    n_filings_llm: int
    filings_llm_aligned: bool
    issuer_year_exists: bool
    n_issuer_year: int
    review_exists: bool
    messages: list[str]

    def fingerprint(self) -> str:
        """Stable string for stuck detection across supervisor attempts."""
        return (
            f"e{self.n_parse_errors}:f{self.n_filings}:p{self.n_parse_rows}:"
            f"c{self.n_chunks}:cl{self.n_chunks_llm}:fl{self.n_filings_llm}:iy{self.n_issuer_year}"
        )

    def to_jsonable(self) -> dict[str, Any]:
        d = asdict(self)
        return d


def assess_pipeline_completion(
    settings: Settings | None = None,
    *,
    require_review_csv: bool = False,
) -> CompletionReport:
    """Return a structured report; ``ok`` is True when the pipeline reached the defined end state."""
    settings = settings or get_settings()
    root = settings.project_root
    messages: list[str] = []

    filings_path = settings.resolve(settings.filings_index_path)
    if not filings_path.is_file():
        return CompletionReport(
            ok=False,
            n_filings=0,
            parse_index_exists=False,
            n_parse_rows=0,
            n_parse_errors=0,
            n_parse_missing_filings=0,
            parse_complete=False,
            chunks_parquet_exists=False,
            n_chunks=0,
            chunks_llm_exists=False,
            n_chunks_llm=0,
            filings_llm_exists=False,
            n_filings_llm=0,
            filings_llm_aligned=False,
            issuer_year_exists=False,
            n_issuer_year=0,
            review_exists=False,
            messages=[f"Missing filings index: {filings_path}"],
        )

    filings = pd.read_csv(filings_path)
    n_filings = len(filings)
    if n_filings == 0:
        return CompletionReport(
            ok=True,
            n_filings=0,
            parse_index_exists=False,
            n_parse_rows=0,
            n_parse_errors=0,
            n_parse_missing_filings=0,
            parse_complete=True,
            chunks_parquet_exists=False,
            n_chunks=0,
            chunks_llm_exists=False,
            n_chunks_llm=0,
            filings_llm_exists=False,
            n_filings_llm=0,
            filings_llm_aligned=True,
            issuer_year_exists=False,
            n_issuer_year=0,
            review_exists=False,
            messages=["filings_index is empty — nothing to process (vacuous complete)."],
        )

    parse_path = settings.resolve(settings.parse_index_csv)
    parse_index_exists = parse_path.is_file()
    n_parse_rows = 0
    n_parse_errors = 0
    n_parse_missing = 0
    parse_complete = False

    if not parse_index_exists:
        messages.append(f"Parse index not found: {parse_path}")
        if n_filings:
            n_parse_missing = n_filings
    else:
        parse_df = pd.read_csv(parse_path)
        n_parse_rows = len(parse_df)
        if "parse_status" in parse_df.columns:
            n_parse_errors = int(parse_df["parse_status"].map(_parse_status_is_error).sum())
        f_ids = set(filings["filing_id"].astype(str))
        p_ids = set(parse_df["filing_id"].astype(str)) if "filing_id" in parse_df.columns else set()
        n_parse_missing = len(f_ids - p_ids)
        if n_parse_missing:
            messages.append(f"{n_parse_missing} filing_id(s) in index missing from parse output")
        if n_parse_errors:
            messages.append(f"{n_parse_errors} parse row(s) with ERROR or non-terminal status")
        if n_parse_rows != n_filings:
            messages.append(f"Parse row count {n_parse_rows} != filings count {n_filings}")
        parse_complete = (
            n_parse_errors == 0
            and n_parse_missing == 0
            and n_parse_rows == n_filings
            and n_filings > 0
        )

    chunks_path = settings.resolve(settings.chunks_parquet)
    chunks_parquet_exists = chunks_path.is_file()
    n_chunks = 0
    if chunks_parquet_exists:
        try:
            n_chunks = len(pd.read_parquet(chunks_path))
        except Exception as e:
            chunks_parquet_exists = False
            messages.append(f"Could not read chunks parquet: {e}")
    else:
        messages.append(f"Missing chunks parquet: {chunks_path}")

    cl_path = settings.resolve(settings.chunks_llm_parquet)
    chunks_llm_exists = cl_path.is_file()
    n_chunks_llm = 0
    if chunks_llm_exists:
        try:
            n_chunks_llm = len(pd.read_parquet(cl_path))
        except Exception as e:
            chunks_llm_exists = False
            messages.append(f"Could not read chunks_llm parquet: {e}")
    else:
        messages.append(f"Missing chunks_llm parquet: {cl_path}")

    fl_csv = settings.resolve(settings.filings_llm_csv)
    filings_llm_exists = fl_csv.is_file()
    n_filings_llm = 0
    filings_llm_aligned = False
    if filings_llm_exists:
        try:
            fl_df = pd.read_csv(fl_csv)
            n_filings_llm = len(fl_df)
            filings_llm_aligned = n_filings > 0 and n_filings_llm == n_filings
            if n_filings > 0 and not filings_llm_aligned:
                messages.append(f"filings_llm.csv rows {n_filings_llm} != filings {n_filings}")
        except Exception as e:
            filings_llm_exists = False
            messages.append(f"Could not read filings_llm.csv: {e}")
    else:
        messages.append(f"Missing filings_llm.csv: {fl_csv}")

    iy_path = settings.resolve(settings.issuer_year_csv)
    issuer_year_exists = iy_path.is_file()
    n_issuer_year = 0
    if issuer_year_exists:
        try:
            n_issuer_year = len(pd.read_csv(iy_path))
        except Exception as e:
            issuer_year_exists = False
            messages.append(f"Could not read issuer_year CSV: {e}")
    else:
        messages.append(f"Missing issuer_year CSV: {iy_path}")

    rev_path = settings.resolve(settings.review_csv)
    review_exists = rev_path.is_file()
    if require_review_csv and not review_exists:
        messages.append(f"Missing review CSV (required): {rev_path}")

    ok = (
        parse_complete
        and chunks_parquet_exists
        and chunks_llm_exists
        and filings_llm_exists
        and filings_llm_aligned
        and issuer_year_exists
        and (not require_review_csv or review_exists)
    )

    if ok:
        messages = [f"Pipeline complete ({n_filings} filings, {n_issuer_year} issuer-year rows)."]

    return CompletionReport(
        ok=ok,
        n_filings=n_filings,
        parse_index_exists=parse_index_exists,
        n_parse_rows=n_parse_rows,
        n_parse_errors=n_parse_errors,
        n_parse_missing_filings=n_parse_missing,
        parse_complete=parse_complete,
        chunks_parquet_exists=chunks_parquet_exists,
        n_chunks=n_chunks,
        chunks_llm_exists=chunks_llm_exists,
        n_chunks_llm=n_chunks_llm,
        filings_llm_exists=filings_llm_exists,
        n_filings_llm=n_filings_llm,
        filings_llm_aligned=filings_llm_aligned,
        issuer_year_exists=issuer_year_exists,
        n_issuer_year=n_issuer_year,
        review_exists=review_exists,
        messages=messages,
    )


def write_report_json(report: CompletionReport, path: str | Path) -> None:
    import json

    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    payload = report.to_jsonable()
    p.write_text(json.dumps(payload, indent=2), encoding="utf-8")
