from __future__ import annotations

from typing import Any, Sequence

import pandas as pd

from tariff_agent.utils.config import Settings, get_settings


def _as_list(val: Any) -> list:
    """Coerce a parquet/series cell (list, ndarray, None, NaN) to a plain list.

    Avoid ``val or []`` — numpy arrays raise ValueError on truthiness.
    """
    if val is None:
        return []
    if isinstance(val, float) and pd.isna(val):
        return []
    if hasattr(val, "tolist") and not isinstance(val, (str, bytes, dict)):
        try:
            val = val.tolist()
        except TypeError:
            return []
    return val if isinstance(val, list) else []


def _iter_evidence_rows(
    m: pd.DataFrame,
    col: str,
    category: str,
    type_key: str,
) -> list[dict[str, Any]]:
    if col not in m.columns:
        return []
    rows: list[dict[str, Any]] = []
    for _, r in m.iterrows():
        raw = _as_list(r.get(col))
        if not raw:
            continue
        for item in raw:
            quote = ""
            st = ""
            if isinstance(item, dict):
                quote = str(item.get("quote", "")).strip()
                st = str(item.get(type_key, item.get("signal_type", ""))).strip()
            elif isinstance(item, str):
                quote = item.strip()
            if not quote:
                continue
            rows.append(
                {
                    "chunk_id": r.get("chunk_id", ""),
                    "filing_id": r.get("filing_id", ""),
                    "ticker": r.get("ticker", ""),
                    "filing_type": r.get("filing_type", ""),
                    "filing_date": r.get("filing_date", ""),
                    "section_path": r.get("section_path", ""),
                    "page_start": r.get("page_start", ""),
                    "page_end": r.get("page_end", ""),
                    "label_category": category,
                    "signal_type": st,
                    "supporting_quote": quote,
                    "model_prediction": str(r.get("mentions_tariffs", "")),
                    "human_label_correct": "",
                    "corrected_label_value": "",
                    "human_comment": "",
                    "confirmed": "",
                }
            )
    return rows


def _explode_str_list(m: pd.DataFrame, col: str, category: str) -> pd.DataFrame:
    if col not in m.columns:
        return pd.DataFrame()
    rows: list[dict[str, Any]] = []
    for _, r in m.iterrows():
        spans = _as_list(r.get(col))
        if not spans:
            continue
        for sp in spans:
            if not str(sp).strip():
                continue
            rows.append(
                {
                    "chunk_id": r.get("chunk_id", ""),
                    "filing_id": r.get("filing_id", ""),
                    "ticker": r.get("ticker", ""),
                    "filing_type": r.get("filing_type", ""),
                    "filing_date": r.get("filing_date", ""),
                    "section_path": r.get("section_path", ""),
                    "page_start": r.get("page_start", ""),
                    "page_end": r.get("page_end", ""),
                    "label_category": category,
                    "signal_type": "",
                    "supporting_quote": str(sp),
                    "model_prediction": str(r.get("mentions_tariffs", "")),
                    "human_label_correct": "",
                    "corrected_label_value": "",
                    "human_comment": "",
                    "confirmed": "",
                }
            )
    return pd.DataFrame(rows)


def build_review_table(
    ticker_filter: Sequence[str] | None = None,
    settings: Settings | None = None,
) -> pd.DataFrame:
    settings = settings or get_settings()
    chunks = pd.read_parquet(settings.resolve(settings.chunks_parquet))
    llm = pd.read_parquet(settings.resolve(settings.chunks_llm_parquet))
    m = chunks.merge(llm, on=["chunk_id", "filing_id"], how="inner")

    if ticker_filter:
        tickers = {t.upper() for t in ticker_filter}
        m = m[m["ticker"].str.upper().isin(tickers)]

    m_flag = m[m["mentions_tariffs"] == True]  # noqa: E712

    row_dicts: list[dict[str, Any]] = []
    row_dicts.extend(_iter_evidence_rows(m_flag, "earnings_evidence", "earnings", "signal_type"))
    row_dicts.extend(_iter_evidence_rows(m_flag, "supply_chain_evidence", "supply_chain", "chain_type"))
    row_dicts.extend(_iter_evidence_rows(m_flag, "macro_evidence", "macro", "macro_type"))
    parts = [
        pd.DataFrame(row_dicts),
        _explode_str_list(m_flag, "other_tariff_mentions", "other"),
    ]
    df = pd.concat([p for p in parts if not p.empty], ignore_index=True)

    out = settings.resolve(settings.review_csv)
    out.parent.mkdir(parents=True, exist_ok=True)
    if df.empty:
        df = pd.DataFrame(
            columns=[
                "chunk_id",
                "filing_id",
                "ticker",
                "filing_type",
                "filing_date",
                "section_path",
                "page_start",
                "page_end",
                "label_category",
                "signal_type",
                "supporting_quote",
                "model_prediction",
                "human_label_correct",
                "corrected_label_value",
                "human_comment",
                "confirmed",
            ]
        )
    df.to_csv(out, index=False)
    return df
