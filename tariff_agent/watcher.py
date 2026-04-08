"""Watch PDF root, debounce per company slug, merge filings index, run incremental pipeline."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import logging
import re
from collections import defaultdict
from pathlib import Path
from typing import Any

import pandas as pd
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

from tariff_agent.utils.aggregate import build_issuer_year_table
from tariff_agent.utils.async_llm_client import run_llm_on_chunks, run_llm_on_docs
from tariff_agent.utils.config import Settings, get_settings
from tariff_agent.utils.incremental_pipeline import (
    merge_filings_index_rows,
    run_chunking_for_filings,
    run_docling_for_filings,
)
from tariff_agent.utils.vllm_lifecycle import maybe_start_vllm_after_parse
from tariff_agent.utils.pdf_locale import ENGLISH_FILING_PDF_RE, is_french_filing_pdf

logger = logging.getLogger(__name__)

_company_async_locks: dict[str, asyncio.Lock] = {}


def issuer_slug(issuer_name: str) -> str:
    s = issuer_name.lower()
    s = s.replace("&", "and").replace(".", "").replace(",", "").replace("'", "")
    s = re.sub(r"[^a-z0-9]+", "_", s).strip("_")
    return s


def infer_filing_type(category: str, desc: str) -> str:
    d, _c = desc.lower(), category.lower().replace("-", " ")
    if "annual information" in d or "information form" in d:
        return "AIF"
    if "material change" in d:
        return "MATERIAL_CHANGE_REPORT"
    if "news" in d or "press release" in d:
        return "NEWS_RELEASE"
    if "interim" in d or "quarterly" in d:
        return "MDA_INTERIM"
    if "management" in d and "analysis" in d:
        return "MDA_ANNUAL"
    if "annual report" in d:
        return "OTHER"
    if "financial statement" in d:
        return "MDA_ANNUAL"
    return "OTHER"


def load_company_slug_map(settings: Settings) -> dict[str, tuple[str, str, str]]:
    p = settings.resolve(settings.tickers_path)
    if not p.is_file():
        raise FileNotFoundError(f"missing tickers CSV: {p}")
    df = pd.read_csv(p)
    colmap = {str(c).lower(): c for c in df.columns}
    tcol = colmap.get("ticker") or df.columns[0]
    pcol = colmap.get("profile_id") or colmap.get("issuer_profile_id")
    if not pcol:
        raise ValueError(f"{p}: need profile_id column; got {list(df.columns)}")
    ncol = None
    for key in ("issuer_name", "issuer", "company_name", "name"):
        if key in colmap:
            ncol = colmap[key]
            break
    if ncol is None:
        raise ValueError(f"{p}: need issuer_name / issuer column; got {list(df.columns)}")
    out: dict[str, tuple[str, str, str]] = {}
    for _, row in df.iterrows():
        ticker = str(row[tcol]).strip()
        profile_id = str(row[pcol]).strip()
        name = str(row[ncol]).strip()
        sl = issuer_slug(name)
        if sl in out and out[sl][0] != ticker:
            logger.warning("duplicate slug %s (keeping %s, also %s)", sl, out[sl][0], ticker)
        out[sl] = (ticker, profile_id, name)
    return out


def filing_rows_for_company_slug(
    slug: str,
    pdf_root: Path,
    slug_map: dict[str, tuple[str, str, str]],
) -> list[dict[str, Any]]:
    if slug not in slug_map:
        logger.warning("[watcher] unknown slug %r (not in %s)", slug, pdf_root)
        return []
    ticker, profile_id, issuer_name = slug_map[slug]
    base = pdf_root / slug
    if not base.is_dir():
        return []
    rows: list[dict[str, Any]] = []
    root = pdf_root.resolve()
    for pdf_path in base.rglob("*.pdf"):
        try:
            rel = pdf_path.resolve().relative_to(root)
        except ValueError:
            continue
        if len(rel.parts) < 3:
            logger.warning("[watcher] skip %s (expected slug/category/file)", rel.as_posix())
            continue
        category = rel.parts[1]
        fname = rel.parts[-1]
        if is_french_filing_pdf(fname):
            logger.debug("[watcher] skip French PDF (English-only pipeline): %s", rel.as_posix())
            continue
        m = ENGLISH_FILING_PDF_RE.match(fname)
        if not m:
            logger.debug("[watcher] skip non-English / non-indexed PDF: %s", rel.as_posix())
            continue
        filing_date = m.group("fd")
        local_posix = rel.as_posix()
        filing_id = hashlib.md5(str(pdf_path.resolve()).encode()).hexdigest()
        rows.append(
            {
                "filing_id": filing_id,
                "local_path": local_posix,
                "profile_id": profile_id,
                "ticker": ticker,
                "issuer_name": issuer_name,
                "filing_type": infer_filing_type(category, m.group("desc")),
                "filing_date": filing_date,
            }
        )
    return rows


def run_company_pipeline_sync(slug: str, n_seen: int, settings: Settings) -> None:
    pdf_root = (
        settings.resolve(settings.filings_pdf_root)
        if settings.filings_pdf_root
        else settings.project_root / "data" / "pdfs"
    )
    slug_map = load_company_slug_map(settings)
    rows = filing_rows_for_company_slug(slug, pdf_root, slug_map)
    if not rows:
        logger.info("[watcher] nothing to index for %s", slug)
        return
    logger.info(
        "[watcher] firing worker for %s (%s queue events, %s pdf rows)",
        slug,
        n_seen,
        len(rows),
    )
    merge_filings_index_rows(rows, settings)
    fids = {str(r["filing_id"]) for r in rows}
    run_docling_for_filings(fids, settings, force=False)
    maybe_start_vllm_after_parse(settings)
    run_chunking_for_filings(fids, settings, force=False)
    run_llm_on_chunks(settings, force=False)
    doc_csv = settings.resolve(settings.filings_llm_csv)
    if doc_csv.is_file():
        run_llm_on_docs(settings, force=False, update_filing_ids=fids)
    else:
        logger.warning("[watcher] no filings_llm.csv — running full Pass-2 (seed once, then incremental)")
        run_llm_on_docs(settings, force=False)
    build_issuer_year_table(settings, force=True)


class SlugDebouncer:
    def __init__(self, delay_sec: float, on_fire):
        self.delay_sec = delay_sec
        self.on_fire = on_fire
        self._timers: dict[str, asyncio.TimerHandle] = {}
        self._counts: dict[str, int] = defaultdict(int)

    def touch(self, slug: str) -> None:
        self._counts[slug] += 1
        if slug in self._timers:
            self._timers[slug].cancel()
        loop = asyncio.get_running_loop()

        def _fire() -> None:
            self._timers.pop(slug, None)
            n = self._counts.pop(slug, 0)
            asyncio.create_task(self.on_fire(slug, n))

        self._timers[slug] = loop.call_later(self.delay_sec, _fire)


class _PdfHandler(FileSystemEventHandler):
    def __init__(self, pdf_root: Path, enqueue):
        super().__init__()
        self.pdf_root = pdf_root.resolve()
        self.enqueue = enqueue

    def on_created(self, event) -> None:  # type: ignore[no-untyped-def]
        if event.is_directory:
            return
        path = Path(str(event.src_path))
        if path.suffix.lower() != ".pdf":
            return
        if is_french_filing_pdf(path.name):
            logger.debug("[watcher] ignore French PDF (debounce not triggered): %s", path.name)
            return
        if not ENGLISH_FILING_PDF_RE.match(path.name):
            return
        try:
            rel = path.resolve().relative_to(self.pdf_root)
        except ValueError:
            return
        if not rel.parts:
            return
        slug = rel.parts[0]
        self.enqueue(slug, path)


async def _consume(queue: asyncio.Queue, debouncer: SlugDebouncer, settle_sec: float) -> None:
    while True:
        try:
            slug, _path_s = await asyncio.wait_for(queue.get(), timeout=settle_sec)
        except asyncio.TimeoutError:
            logger.debug("[watcher] idle (%.0fs settle)", settle_sec)
            continue
        debouncer.touch(str(slug))


async def _async_main(*, watch: Path, debounce: float, settle: float) -> None:
    settings = get_settings()

    async def work(slug: str, n: int) -> None:
        lock = _company_async_locks.setdefault(slug, asyncio.Lock())
        async with lock:
            await asyncio.to_thread(run_company_pipeline_sync, slug, n, settings)

    debouncer = SlugDebouncer(debounce, work)
    queue: asyncio.Queue = asyncio.Queue()

    def enqueue(slug: str, path: Path) -> None:
        logger.info("[watcher] new PDF detected: %s → company=%s", path.name, slug)
        asyncio.get_running_loop().call_soon_threadsafe(queue.put_nowait, (slug, str(path)))

    handler = _PdfHandler(watch, enqueue)
    obs = Observer()
    obs.schedule(handler, str(watch.resolve()), recursive=True)
    obs.start()
    logger.info(
        "[watcher] watching %s (debounce=%ss settle=%ss); English PDFs only — French ignored",
        watch.resolve(),
        debounce,
        settle,
    )
    try:
        await _consume(queue, debouncer, settle)
    finally:
        obs.stop()
        obs.join(timeout=8.0)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    p = argparse.ArgumentParser(description="Watch data/pdfs and run incremental tariff pipeline per company.")
    p.add_argument(
        "--watch",
        default="",
        help="PDF root (default: FILINGS_PDF_ROOT or data/pdfs under project)",
    )
    p.add_argument("--debounce", type=float, default=15.0, help="Quiet period per slug before worker runs")
    p.add_argument(
        "--settle",
        type=float,
        default=30.0,
        help="Queue wait logging timeout (idle log); does not stop watcher",
    )
    args = p.parse_args()
    settings = get_settings()
    if args.watch:
        root = Path(args.watch).expanduser()
    elif settings.filings_pdf_root:
        root = settings.resolve(settings.filings_pdf_root)
    else:
        root = settings.project_root / "data" / "pdfs"
    root.mkdir(parents=True, exist_ok=True)
    asyncio.run(_async_main(watch=root, debounce=args.debounce, settle=args.settle))


if __name__ == "__main__":
    main()
