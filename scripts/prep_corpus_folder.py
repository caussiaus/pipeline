"""Prepare a flat corpus folder from one or more PDF source trees.

For each source dir:
  - Walk all *.pdf recursively
  - Language-detect first page text (skip non-English PDFs)
  - MD5-deduplicate by file content
  - Copy surviving files to --out-dir, naming them {filing_id}.pdf
  - Write a companion CSV: filing_id, original_path, company_name, detected_lang, status

Usage::

    python3 scripts/prep_corpus_folder.py \\
        --src "/mnt/c/Users/casey/.../TSX_2023_ESGReports" \\
        --src "/mnt/c/Users/casey/.../ESG Reports 2024" \\
        --src "/mnt/c/Users/casey/.../filings" \\
        --out-dir output/corpus_flat_pdfs \\
        --manifest output/corpus_flat_pdfs/manifest.csv \\
        --workers 4

Flags:
  --skip-lang     Skip language detection entirely (faster; keep all PDFs)
  --lang-sample   Characters of first-page text to sample for lang detect (default 800)
  --min-bytes     Skip PDF files smaller than this (default 1024)
"""
from __future__ import annotations

import argparse
import hashlib
import logging
import re
import shutil
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from tariff_agent.corpus.paths import normalize_host_path

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_YEAR_RE = re.compile(r"(20[12]\d)")
_SLUG_RE = re.compile(r"[^a-zA-Z0-9]+")


def _md5(path: Path, chunk_size: int = 1 << 18) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        while True:
            buf = f.read(chunk_size)
            if not buf:
                break
            h.update(buf)
    return h.hexdigest()


def _first_page_text(path: Path, max_chars: int = 800) -> str:
    try:
        from pypdf import PdfReader
        r = PdfReader(str(path))
        if not r.pages:
            return ""
        return (r.pages[0].extract_text() or "")[:max_chars]
    except Exception:
        return ""


_COMMON_FRENCH = {"de", "du", "les", "et", "en", "que", "qui", "une", "des", "dans", "par", "est", "avec", "pour", "sur"}
_COMMON_ENGLISH = {"the", "and", "of", "to", "in", "is", "for", "this", "that", "with", "are", "have", "has", "its"}
_COMMON_SPANISH = {"de", "la", "el", "en", "que", "es", "un", "una", "con", "por", "se", "los", "del", "las"}


def _detect_lang_heuristic(text: str) -> str:
    """Heuristic: return 'en'|'fr'|'es'|'unknown' without external deps."""
    words = {w.lower() for w in re.findall(r"[a-zA-Zàâéèêëîïôùûüç]{3,}", text)}
    if not words:
        return "unknown"
    en = len(words & _COMMON_ENGLISH)
    fr = len(words & _COMMON_FRENCH)
    es = len(words & _COMMON_SPANISH)
    best = max((en, "en"), (fr, "fr"), (es, "es"), key=lambda t: t[0])
    return best[1] if best[0] > 1 else "unknown"


def _detect_lang(text: str) -> str:
    if not text.strip():
        return "unknown"
    try:
        from langdetect import detect
        return detect(text)
    except Exception:
        return _detect_lang_heuristic(text)


def _issuer_from_path(p: Path, root: Path) -> str:
    try:
        parts = p.relative_to(root).parts
        if len(parts) >= 2:
            return str(parts[0]).replace("_", " ").replace("-", " ").strip()
    except ValueError:
        pass
    return p.stem.replace("_", " ").replace("-", " ")[:80]


def _year_from_path(p: Path) -> str:
    for part in p.parts:
        m = _YEAR_RE.search(part)
        if m:
            return m.group(1)
    return ""


def _process_one(
    pdf: Path,
    root: Path,
    out_dir: Path,
    skip_lang: bool,
    lang_sample: int,
    min_bytes: int,
) -> dict:
    row: dict = {
        "original_path": str(pdf),
        "company_name": _issuer_from_path(pdf, root),
        "year_hint": _year_from_path(pdf),
        "detected_lang": "unknown",
        "filing_id": "",
        "dest_path": "",
        "status": "OK",
    }

    try:
        size = pdf.stat().st_size
    except OSError as e:
        row["status"] = f"STAT_ERROR:{e}"
        return row

    if size < min_bytes:
        row["status"] = "TOO_SMALL"
        return row

    row["filing_id"] = _md5(pdf)

    if not skip_lang:
        text = _first_page_text(pdf, max_chars=lang_sample)
        lang = _detect_lang(text)
        row["detected_lang"] = lang
        if lang not in ("en", "unknown", ""):
            row["status"] = f"SKIP_LANG:{lang}"
            return row

    dest = out_dir / f"{row['filing_id']}.pdf"
    if dest.exists():
        row["dest_path"] = str(dest)
        row["status"] = "DEDUPE_SKIP"
        return row

    try:
        shutil.copy2(str(pdf), str(dest))
        row["dest_path"] = str(dest)
    except Exception as e:
        row["status"] = f"COPY_ERROR:{e}"

    return row


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--src", action="append", required=True, metavar="DIR",
                    help="Source PDF dir (Windows or POSIX, repeatable)")
    ap.add_argument("--out-dir", required=True, help="Output flat folder")
    ap.add_argument("--manifest", default="", help="Output manifest CSV path (default: <out-dir>/manifest.csv)")
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--skip-lang", action="store_true", help="Skip language detection, keep all PDFs")
    ap.add_argument("--lang-sample", type=int, default=800)
    ap.add_argument("--min-bytes", type=int, default=1024)
    args = ap.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = Path(args.manifest) if args.manifest else out_dir / "manifest.csv"

    pdfs: list[tuple[Path, Path]] = []  # (pdf, root)
    for src_raw in args.src:
        root = normalize_host_path(src_raw)
        if not root.is_dir():
            logger.warning("Source dir not found: %s", root)
            continue
        for p in sorted(root.rglob("*.pdf")):
            pdfs.append((p, root))

    logger.info("Found %d PDFs across %d source dirs", len(pdfs), len(args.src))

    rows: list[dict] = []
    seen_ids: set[str] = set()

    def _safe_process(pdf: Path, root: Path) -> dict:
        return _process_one(pdf, root, out_dir,
                            skip_lang=args.skip_lang,
                            lang_sample=args.lang_sample,
                            min_bytes=args.min_bytes)

    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futs = {ex.submit(_safe_process, p, r): (p, r) for p, r in pdfs}
        done = 0
        for fut in as_completed(futs):
            done += 1
            try:
                row = fut.result()
            except Exception as e:
                p, _ = futs[fut]
                row = {"original_path": str(p), "status": f"EXCEPTION:{e}", "filing_id": ""}
            fid = row.get("filing_id", "")
            if fid and fid in seen_ids and row.get("status") == "OK":
                row["status"] = "DEDUPE_SKIP"
            if fid:
                seen_ids.add(fid)
            rows.append(row)
            if done % 50 == 0:
                logger.info("  %d / %d processed", done, len(pdfs))

    df = pd.DataFrame(rows)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(manifest_path, index=False)

    ok = (df["status"] == "OK").sum()
    deduped = (df["status"] == "DEDUPE_SKIP").sum()
    lang_skipped = df["status"].str.startswith("SKIP_LANG").sum()
    errors = df[~df["status"].isin(("OK", "DEDUPE_SKIP", "TOO_SMALL"))
                & ~df["status"].str.startswith("SKIP_LANG")].shape[0]

    print(f"\n{'─'*60}")
    print(f"  Total PDFs scanned:   {len(pdfs)}")
    print(f"  Copied (English):     {ok}")
    print(f"  Deduped (same MD5):   {deduped}")
    print(f"  Skipped (language):   {lang_skipped}")
    print(f"  Errors:               {errors}")
    print(f"  Manifest:             {manifest_path}")
    print(f"{'─'*60}")


if __name__ == "__main__":
    main()
