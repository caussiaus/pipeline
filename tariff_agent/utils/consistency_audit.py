"""Cross-field business consistency checks for Pass-2 filing rows.

Forward-looking statement (FLS) bias
-------------------------------------
MD&A documents routinely contain a boilerplate "Forward-Looking Statements" (FLS)
section that *lists* tariffs as a risk factor with no reference to the company's
actual historical or current-period experience.  If the LLM only saw tariff mentions
in those sections the resulting signal is prospective risk disclosure, not confirmed
exposure — the two should not be conflated in quant factor construction.

Rules added here:
  fls_only_tariff_signals     (error)  — all key_quotes come from FLS/boilerplate sections;
                                          has_tariff_discussion should be False or at least
                                          has its scores zeroed before use in factor models.
  fls_majority_tariff_signals (warn)   — >50% of key_quotes from FLS sections.
  fls_signal_type_unclear     (warn)   — every key_quote has signal_type == UNCLEAR,
                                          often symptomatic of FLS-only exposure.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from tariff_agent.utils.config import Settings, get_settings
from tariff_agent.utils.meta_normalize import clean_meta_str


import re as _re

# Section path patterns that indicate a forward-looking statement boilerplate section.
_FLS_PATTERNS = _re.compile(
    r"forward.looking|cautionary|safe.harbour|safe.harbor|"
    r"forward looking|note regarding|risk factor|table of contents|"
    r"introduction|preamble|general risk",
    _re.I,
)

def _is_fls_section(section_path: str) -> bool:
    return bool(_FLS_PATTERNS.search(str(section_path or "")))


def _parse_key_quotes(raw: Any) -> list[dict]:
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return []
    if isinstance(raw, list):
        return raw
    s = str(raw).strip()
    if not s or s in ("[]", "nan"):
        return []
    try:
        v = json.loads(s)
        return v if isinstance(v, list) else []
    except json.JSONDecodeError:
        return []


def _parse_key_quotes_len(raw: Any) -> int:
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return 0
    if isinstance(raw, list):
        return len(raw)
    s = str(raw).strip()
    if not s or s in ("[]", "nan"):
        return 0
    try:
        v = json.loads(s)
        return len(v) if isinstance(v, list) else 0
    except json.JSONDecodeError:
        return -1  # malformed


def _row_bool(val: Any) -> bool:
    if val is True:
        return True
    if val is False:
        return False
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return False
    return str(val).strip().lower() in ("true", "1", "1.0")


def _score_sum(row: pd.Series) -> int:
    t = 0
    for c in ("earnings_tariff_score", "supply_chain_tariff_score", "macro_tariff_score"):
        if c not in row.index:
            continue
        try:
            t += int(float(row[c]))
        except (TypeError, ValueError):
            pass
    return t


def apply_index_identity(
    doc: pd.DataFrame, idx: pd.DataFrame, *, id_cols: list[str] | None = None
) -> pd.DataFrame:
    """Overwrite identity columns in ``doc`` from ``idx`` (authoritative), then normalize.

    Always includes ``profile_number`` (9-digit SEDAR ID) in the propagated columns.
    """
    id_cols = id_cols or [
        "profile_id", "profile_number", "ticker", "issuer_name", "filing_type", "filing_date"
    ]
    out = doc.copy()
    if "filing_id" not in out.columns:
        return out
    idx_i = idx.drop_duplicates("filing_id").set_index("filing_id")
    for col in id_cols:
        if col not in idx.columns:
            continue
        out[col] = out["filing_id"].astype(str).map(idx_i[col]).map(clean_meta_str)
    return out


def load_pass1_any_by_filing(settings: Settings) -> dict[str, bool] | None:
    path = settings.resolve(settings.chunks_llm_parquet)
    if not path.is_file():
        return None
    try:
        ch = pd.read_parquet(path, columns=["filing_id", "mentions_tariffs"])
    except Exception:
        return None
    if "mentions_tariffs" not in ch.columns:
        return None
    g = ch.groupby("filing_id")["mentions_tariffs"].any()
    return {str(k): bool(v) for k, v in g.items()}


def evaluate_filing_consistency(
    doc: pd.DataFrame,
    *,
    pass1_any: dict[str, bool] | None = None,
) -> pd.DataFrame:
    """One row per filing: rule codes, severities, counts.

    Rules are independent; a row may match several. ``qc_rules`` is ``|``-separated.
    """
    rows: list[dict[str, Any]] = []

    default_snippet = "no tariff-related passages detected"

    for _, r in doc.iterrows():
        fid = str(r.get("filing_id", ""))
        issues: list[tuple[str, str]] = []  # (code, severity)

        pid = clean_meta_str(r.get("profile_id"))
        if not pid:
            issues.append(("missing_profile_id", "warn"))

        ht = _row_bool(r.get("has_tariff_discussion"))
        ss = _score_sum(r)
        if (not ht) and ss > 0:
            issues.append(("has_tariff_false_but_scores_positive", "error"))
        if ht and ss == 0:
            issues.append(("has_tariff_true_but_all_scores_zero", "warn"))

        dq = str(r.get("disclosure_quality", "") or "").upper()
        if "BOILERPLATE" in dq and ss > 0:
            issues.append(("boilerplate_disclosure_with_positive_scores", "warn"))

        kql = _parse_key_quotes_len(r.get("key_quotes"))
        if kql < 0:
            issues.append(("key_quotes_json_invalid", "error"))
        summary = str(r.get("doc_summary_sentence", "") or "").lower()
        if default_snippet in summary and kql > 0:
            issues.append(("default_no_tariff_summary_but_key_quotes", "error"))
        if ht and kql == 0:
            issues.append(("has_tariff_no_key_quotes", "warn"))

        qi = _row_bool(r.get("quantified_impact"))
        qit = clean_meta_str(r.get("quantified_impact_text"))
        if qi and not qit:
            issues.append(("quantified_flag_without_text", "warn"))

        mf = _row_bool(r.get("mitigation_flag"))
        ms = clean_meta_str(r.get("mitigation_summary"))
        if mf and not ms:
            issues.append(("mitigation_flag_without_summary", "warn"))

        fd = r.get("filing_date")
        fy = r.get("fiscal_year")
        try:
            y_fd = pd.to_datetime(fd, errors="coerce")
            y_fd = int(y_fd.year) if pd.notna(y_fd) else None
            y_fy = int(float(fy)) if fy is not None and str(fy).strip() not in ("", "nan") else None
            if y_fd is not None and y_fy is not None and abs(y_fy - y_fd) > 1:
                issues.append(("fiscal_year_far_from_filing_date_year", "warn"))
        except (TypeError, ValueError):
            pass

        td = str(r.get("tariff_direction", "") or "").upper()
        try:
            es = int(float(r.get("earnings_tariff_score", 0)))
        except (TypeError, ValueError):
            es = 0
        if td in ("NONE", "") and es > 0:
            issues.append(("tariff_direction_none_but_earnings_score_positive", "warn"))

        if pass1_any is not None:
            p1 = pass1_any.get(fid, False)
            if p1 and not ht:
                issues.append(("pass1_chunk_positive_pass2_no_discussion", "info"))
            if ht and not p1:
                issues.append(("pass2_discussion_without_pass1_chunk_flag", "error"))

        # ── Forward-looking statement bias ────────────────────────────────
        quotes = _parse_key_quotes(r.get("key_quotes"))
        if quotes:
            n_fls = sum(1 for q in quotes if _is_fls_section(q.get("section_path", "")))
            fls_ratio = n_fls / len(quotes)
            if fls_ratio == 1.0:
                issues.append(("fls_only_tariff_signals", "error"))
            elif fls_ratio > 0.5:
                issues.append(("fls_majority_tariff_signals", "warn"))

            all_unclear = all(
                str(q.get("signal_type", "")).upper() in ("UNCLEAR", "")
                for q in quotes
            )
            if all_unclear and ht:
                issues.append(("fls_signal_type_all_unclear", "warn"))

        sev_order = {"error": 3, "warn": 2, "info": 1}
        max_sev = "none"
        for _, sev in issues:
            if sev_order.get(sev, 0) > sev_order.get(max_sev, 0):
                max_sev = sev

        fls_codes = {c for c, _ in issues if "fls" in c}
        fls_error = any(s == "error" for c, s in issues if "fls" in c)

        codes = "|".join(sorted({c for c, _ in issues})) if issues else ""
        rows.append(
            {
                "filing_id": fid,
                "qc_rule_count": len(issues),
                "qc_rules": codes,
                "qc_max_severity": max_sev,
                "qc_error_count": sum(1 for _, s in issues if s == "error"),
                "qc_warn_count": sum(1 for _, s in issues if s == "warn"),
                "qc_info_count": sum(1 for _, s in issues if s == "info"),
                # FLS-bias convenience columns
                "fls_bias": "|".join(sorted(fls_codes)) if fls_codes else "",
                "fls_only": fls_error,  # True = all quotes from FLS sections; scores unreliable
            }
        )

    return pd.DataFrame(rows)


def write_consistency_report(
    doc: pd.DataFrame,
    settings: Settings | None = None,
    *,
    pass1_any: dict[str, bool] | None = None,
) -> Path:
    settings = settings or get_settings()
    out = settings.resolve(settings.consistency_report_csv)
    out.parent.mkdir(parents=True, exist_ok=True)
    qc = evaluate_filing_consistency(doc, pass1_any=pass1_any)
    qc.to_csv(out, index=False)
    return out
