#!/usr/bin/env python3
"""Summarize and spot-check Docling JSON (and optional parse index + filings index).

Examples:
  python3 scripts/inspect_parse_outputs.py
  python3 scripts/inspect_parse_outputs.py --sample 3 --verbose
  python3 scripts/inspect_parse_outputs.py --filing-id b5e953fd83655825bd4709f429601f94
"""
from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path

# Repo imports after env (optional .env for paths)
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from tariff_agent.utils.config import ensure_hf_hub_env_for_process, get_settings

ensure_hf_hub_env_for_process()


def _loads(path: Path) -> dict:
    try:
        import orjson

        return orjson.loads(path.read_bytes())
    except ImportError:
        return json.loads(path.read_text(encoding="utf-8"))


def analyze_docling_json(path: Path) -> dict:
    raw = _loads(path)
    if raw.get("fallback"):
        sections = raw.get("sections") or []
        n_text_blobs = sum(len(s.get("texts") or []) for s in sections)
        chars = 0
        for s in sections:
            for t in s.get("texts") or []:
                chars += len(str(t.get("text") or ""))
        return {
            "kind": "fallback",
            "path": path,
            "filing_id": path.stem,
            "name": "pypdf_fallback",
            "n_sections": len(sections),
            "n_text_items": n_text_blobs,
            "n_tables": 0,
            "n_pages_est": 0,
            "chars": chars,
            "label_counts": {},
            "section_headers": [],
        }

    texts = raw.get("texts") or []
    tables = raw.get("tables") or []
    pages = raw.get("pages") or {}
    label_counts: Counter[str] = Counter()
    chars = 0
    headers: list[str] = []
    for t in texts:
        lab = str(t.get("label") or "?")
        label_counts[lab] += 1
        tx = str(t.get("text") or t.get("orig") or "")
        chars += len(tx)
        if lab == "section_header":
            s = tx.strip()
            if s:
                headers.append(s[:300])

    origin = raw.get("origin") or {}
    return {
        "kind": "docling",
        "path": path,
        "filing_id": path.stem,
        "name": str(origin.get("filename") or raw.get("name") or ""),
        "n_sections": 0,
        "n_text_items": len(texts),
        "n_tables": len(tables),
        "n_pages_est": len(pages),
        "chars": chars,
        "label_counts": dict(label_counts),
        "section_headers": headers[:25],
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--sample", type=int, default=0, help="Show N detailed previews (first N filings)")
    ap.add_argument("--verbose", action="store_true", help="Include section header lists in summary table")
    ap.add_argument("--filing-id", default="", help="Inspect one filing id (JSON stem)")
    args = ap.parse_args()

    settings = get_settings()
    doc_dir: Path = settings.resolve(settings.doc_json_dir)
    if not doc_dir.is_dir():
        print(f"No directory: {doc_dir}", file=sys.stderr)
        return 1

    paths = sorted(doc_dir.glob("*.json"))
    if not paths:
        print(f"No JSON files in {doc_dir}", file=sys.stderr)
        return 1

    filings_by_id: dict[str, dict] = {}
    idx_path = settings.resolve(settings.filings_index_path)
    if idx_path.is_file():
        import pandas as pd

        df = pd.read_csv(idx_path)
        for _, row in df.iterrows():
            filings_by_id[str(row["filing_id"])] = row.to_dict()

    parse_by_id: dict[str, str] = {}
    parse_csv = settings.resolve(settings.parse_index_csv)
    if parse_csv.is_file():
        import pandas as pd

        pdf = pd.read_csv(parse_csv)
        if "filing_id" in pdf.columns and "parse_status" in pdf.columns:
            for _, row in pdf.iterrows():
                parse_by_id[str(row["filing_id"])] = str(row["parse_status"])

    if args.filing_id.strip():
        stem = args.filing_id.strip()
        p = doc_dir / f"{stem}.json"
        if not p.is_file():
            print(f"Missing {p}", file=sys.stderr)
            return 1
        paths = [p]

    kinds = Counter()
    total_chars = 0
    rows: list[dict] = []

    for p in paths:
        info = analyze_docling_json(p)
        kinds[info["kind"]] += 1
        total_chars += info["chars"]
        meta = filings_by_id.get(info["filing_id"])
        ticker = str(meta.get("ticker", "")) if meta else ""
        local_pdf = str(meta.get("local_path", "")) if meta else ""
        status = parse_by_id.get(info["filing_id"], "")
        rows.append({**info, "ticker": ticker, "local_pdf": local_pdf, "parse_status": status})

    print(f"Directory: {doc_dir}")
    print(f"Files: {len(paths)}")
    print(f"Kinds: {dict(kinds)}  (docling = full layout; fallback = PyPDF-only minimal JSON)")
    print(f"Total extracted chars (sum over texts): {total_chars:,}")
    if parse_by_id:
        print(f"Parse index: {parse_csv} ({len(parse_by_id)} rows)")
    else:
        print(f"Parse index: not found yet ({parse_csv}) — written when a full parse run completes")
    print()

    # Compact table
    for info in rows[:500]:
        lc = info["label_counts"]
        lbl_short = ",".join(f"{k}:{v}" for k, v in sorted(lc.items())[:6]) if lc else "-"
        if len(lc) > 6:
            lbl_short += ",…"
        ps = info["parse_status"] or "-"
        print(
            f"{info['filing_id'][:12]}…  {info['kind']:<8}  pages~{info['n_pages_est']:<4}  "
            f"texts={info['n_text_items']:<5}  tables={info['n_tables']:<4}  chars={info['chars']:,}  "
            f"ticker={info['ticker'] or '-'}  parse={ps}"
        )
        print(f"    labels: {lbl_short}")
        if args.verbose and info["section_headers"]:
            for h in info["section_headers"][:8]:
                print(f"    § {h[:120]}{'…' if len(h) >= 120 else ''}")
        print(f"    file: {info['name'] or info['path'].name}")
    if len(rows) > 500:
        print(f"… truncated listing ({len(rows)} total); narrow with --filing-id or smaller corpus")

    n = args.sample
    if n > 0:
        print("\n--- Sample previews (first text payloads) ---\n")
        for info in rows[:n]:
            raw = _loads(Path(info["path"]))
            print("=" * 72)
            print(info["filing_id"], info.get("ticker"), info["name"])
            if raw.get("fallback"):
                for sec in (raw.get("sections") or [])[:2]:
                    for t in (sec.get("texts") or [])[:2]:
                        tx = str(t.get("text") or "")[:1200]
                        print(tx + ("…" if len(tx) >= 1200 else ""))
            else:
                shown = 0
                for t in raw.get("texts") or []:
                    if str(t.get("label")) != "text":
                        continue
                    tx = str(t.get("text") or "")[:900]
                    if not tx.strip():
                        continue
                    print(tx + ("…" if len(tx) >= 900 else ""))
                    shown += 1
                    if shown >= 3:
                        break
            print()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
