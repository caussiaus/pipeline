from __future__ import annotations

from typing import Any

import orjson
import pandas as pd

from tariff_agent.utils.config import Settings, get_settings


def _safe_ticker(t: str) -> str:
    t = (t or "unknown").strip() or "unknown"
    return "".join(c if c.isalnum() or c in "._-" else "_" for c in t)[:120]


def write_company_filing_json_artifacts(
    records: list[dict[str, Any]],
    settings: Settings | None = None,
) -> None:
    """Write one JSON file per filing under output/companies/{ticker}/."""
    settings = settings or get_settings()
    root = settings.resolve(settings.companies_output_dir)
    for rec in records:
        ticker = _safe_ticker(str(rec.get("ticker", "")))
        fid = str(rec.get("filing_id", "unknown"))
        d = root / ticker
        d.mkdir(parents=True, exist_ok=True)
        path = d / f"{fid}_filing_llm.json"
        path.write_bytes(orjson.dumps(rec, option=orjson.OPT_INDENT_2, default=str))


def df_filings_for_csv(df: pd.DataFrame) -> pd.DataFrame:
    """Serialize nested columns for CSV."""
    out = df.copy()

    def _cell(x: Any) -> str:
        if x is None or (isinstance(x, float) and pd.isna(x)):
            return ""
        return orjson.dumps(x, default=str).decode()

    for col in ("key_quotes", "specific_tariff_programs"):
        if col in out.columns:
            out[col] = out[col].apply(_cell)
    return out
