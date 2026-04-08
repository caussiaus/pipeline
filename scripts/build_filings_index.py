#!/usr/bin/env python3
"""Scan FILINGS_PDF_ROOT and write filings_index.csv (Prateek: use ticker_filing_alignment_report slug map)."""
from __future__ import annotations

import argparse
import hashlib
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd

from tariff_agent.utils.config import get_settings
from tariff_agent.utils.pdf_locale import ENGLISH_FILING_PDF_RE, is_french_filing_pdf


def infer_filing_type(desc: str) -> str:
    d = desc.lower()
    if "interim" in d or "quarterly" in d:
        return "MDA_INTERIM"
    if "management" in d or "md&a" in d:
        return "MDA_ANNUAL"
    if "annual information" in d or "information form" in d:
        return "AIF"
    if "annual report" in d:
        return "OTHER"
    if "financial statement" in d:
        return "MDA_ANNUAL"
    return "OTHER"


def _ticker_rank(t: str) -> tuple[int, str]:
    t = str(t).strip()
    u = t.upper()
    if u.startswith("TSX:"):
        return (0, t)
    if u.startswith("NYSE:"):
        return (1, t)
    if u.startswith("NASDAQ:"):
        return (2, t)
    return (3, t)


def load_slug_primary_ticker(alignment_path: Path) -> dict[str, str]:
    df = pd.read_csv(alignment_path, dtype=str)
    bucket: dict[str, list[str]] = {}
    for _, r in df.iterrows():
        sl = str(r["expected_slug"]).strip()
        t = str(r["ticker"]).strip()
        bucket.setdefault(sl, []).append(t)
    return {sl: sorted(ts, key=_ticker_rank)[0] for sl, ts in bucket.items()}


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument(
        "--alignment",
        type=Path,
        default=None,
        help="ticker_filing_alignment_report.csv (default: TICKERS_PATH parent)",
    )
    args = p.parse_args()
    settings = get_settings()
    pdf_root = settings.resolve(settings.filings_pdf_root)
    tickers_path = settings.resolve(settings.tickers_path)
    out_path = settings.resolve(settings.filings_index_path)
    alignment = args.alignment or (tickers_path.parent / "ticker_filing_alignment_report.csv")
    if not alignment.is_file():
        print(f"Missing alignment report: {alignment}", file=sys.stderr)
        return 1
    if not pdf_root.is_dir():
        print(f"Missing PDF root: {pdf_root}", file=sys.stderr)
        return 1

    slug_to_ticker = load_slug_primary_ticker(alignment)
    tdf = pd.read_csv(tickers_path, dtype=str).drop_duplicates(subset=["ticker"], keep="first")
    tdf["ticker"] = tdf["ticker"].str.strip()
    tdf.set_index("ticker", inplace=True)

    rows: list[dict[str, str]] = []
    unknown_slug = 0
    skipped_french = 0
    pdf_root_r = pdf_root.resolve()
    for pdf in sorted(pdf_root.rglob("*.pdf")):
        if is_french_filing_pdf(pdf.name):
            skipped_french += 1
            continue
        m = ENGLISH_FILING_PDF_RE.match(pdf.name)
        if not m:
            continue
        try:
            rel = pdf.resolve().relative_to(pdf_root_r)
        except ValueError:
            continue
        slug = rel.parts[0]
        ticker = slug_to_ticker.get(slug)
        if not ticker:
            unknown_slug += 1
            continue
        if ticker not in tdf.index:
            print(f"warn: ticker {ticker} missing from tickers CSV", file=sys.stderr)
            continue
        pr = tdf.loc[ticker]
        profile_id = "" if pd.isna(pr["profile_id"]) else str(pr["profile_id"]).strip()
        issuer_name = "" if pd.isna(pr["issuer_name"]) else str(pr["issuer_name"])
        issuer_name = " ".join(issuer_name.split())[:800]
        fd = m.group("fd")
        desc = m.group("desc")
        filing_id = hashlib.md5(str(pdf.resolve()).encode()).hexdigest()
        rows.append(
            {
                "filing_id": filing_id,
                "local_path": rel.as_posix(),
                "profile_id": profile_id,
                "ticker": ticker,
                "issuer_name": issuer_name,
                "filing_type": infer_filing_type(desc),
                "filing_date": fd,
            }
        )

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_df = pd.DataFrame(rows)
    out_df.sort_values(["ticker", "filing_date", "local_path"], inplace=True)
    out_df.to_csv(out_path, index=False)
    empty_pid = (out_df["profile_id"].fillna("") == "").sum()
    print(f"Wrote {len(out_df)} English rows -> {out_path}")
    if skipped_french:
        print(f"skipped {skipped_french} French PDFs (English-only index; expected alongside English)")
    if unknown_slug:
        print(f"warn: {unknown_slug} English PDFs skipped (folder slug not in alignment report)")
    print(f"rows with empty profile_id: {int(empty_pid)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
