#!/usr/bin/env python3
"""Run parse → chunk → (optional) LLM stages for the active corpus.

Uses :func:`tariff_agent.corpus.runtime.apply_corpus_env` so outputs go to
``output/{corpus_id}/`` and never overwrite another corpus.

Examples::

    # TSX 2023 — after indexes exist under data/metadata/
    python scripts/run_corpus_pipeline.py --corpus tsx_esg_2023 --stage parse
    python scripts/run_corpus_pipeline.py --corpus tsx_esg_2023 --stage chunk
    python scripts/run_corpus_pipeline.py --corpus tsx_esg_2023 --stage llm_chunk

    # SEDAR with prateek filings root (relative paths in filings_index.csv)
    python scripts/run_corpus_pipeline.py --corpus sedar_prateek_filings --stage parse

    # Custom YAML
    python scripts/run_corpus_pipeline.py --config output/corpus_configs/my_corpus.yaml --stage all

Environment is set **before** pipeline imports reload settings.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def _load_corpus(corpus: str, config: str | None, project_root: Path):
    from tariff_agent.corpus.config import CorpusConfig
    if config:
        return CorpusConfig.from_yaml(Path(config).expanduser())
    key = corpus.strip().lower().replace("-", "_")
    if key in ("sedar_tariff", "sedar", ""):
        return CorpusConfig.sedar_default(project_root)
    if key in ("sedar_prateek", "sedar_prateek_filings", "prateek"):
        return CorpusConfig.sedar_prateek_filings(project_root)
    if key in ("tsx_esg_2023", "tsx2023", "tsx_23"):
        return CorpusConfig.tsx_esg_2023(project_root)
    if key in ("tsx_esg_2024", "tsx2024", "tsx_24"):
        return CorpusConfig.tsx_esg_2024(project_root)
    if key in ("pdf_agents", "pdf_agents_esg"):
        return CorpusConfig.pdf_agents_esg_default(project_root)
    raise SystemExit(f"Unknown --corpus {corpus!r}; use --config path/to/corpus.yaml")


def _write_trial_index(corpus_cfg, project_root: Path, n: int) -> Path | None:
    """Write a temporary index CSV containing only the first *n* smallest PDFs.

    Returns the path to the trial index CSV, or None if the corpus index doesn't exist.
    The trial index is placed beside the real one with a ``_trial`` suffix.
    """
    import os
    import pandas as pd
    from tariff_agent.corpus.runtime import apply_corpus_env

    apply_corpus_env(corpus_cfg, project_root)
    idx_path = Path(os.environ.get("FILINGS_INDEX_PATH", ""))
    if not idx_path.is_file():
        return None
    df = pd.read_csv(idx_path, dtype=str)

    # Sort by PDF size (small first) so fast-parse docs are chosen
    pdf_root_env = os.environ.get("FILINGS_PDF_ROOT", "")
    def _size(row):
        lp = str(row.get("local_path", ""))
        p = Path(lp.replace("\\", "/"))
        if not p.is_absolute() and pdf_root_env:
            p = Path(pdf_root_env) / p
        try:
            return p.stat().st_size
        except OSError:
            return 999_999_999
    df = df.copy()
    df["_sz"] = df.apply(_size, axis=1)
    df = df.sort_values("_sz").head(n).drop(columns=["_sz"])

    trial_path = idx_path.with_name(idx_path.stem + f"_trial{n}.csv")
    df.to_csv(trial_path, index=False)
    print(f"  Trial index: {trial_path} ({len(df)} rows)")
    return trial_path


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--corpus", default="tsx_esg_2023",
                   help="Preset: sedar_tariff | sedar_prateek_filings | tsx_esg_2023 | tsx_esg_2024 | pdf_agents_esg")
    p.add_argument("--config", default="", help="YAML corpus config (overrides --corpus)")
    p.add_argument("--stage", choices=("parse", "chunk", "llm_chunk", "llm_doc", "all"),
                   default="parse")
    p.add_argument("--no-skip", action="store_true",
                   help="Set SKIP_* env vars to recompute even if outputs exist.")
    p.add_argument("--trial-n", type=int, default=0,
                   help="Process only the N smallest PDFs (trial/sample run). 0 = full corpus.")
    args = p.parse_args()

    project_root = Path(__file__).resolve().parents[1]
    corpus_cfg = _load_corpus(args.corpus, args.config or None, project_root)

    from tariff_agent.corpus.runtime import apply_corpus_env
    from tariff_agent.utils.config import ensure_hf_hub_env_for_process

    ensure_hf_hub_env_for_process()
    applied = apply_corpus_env(corpus_cfg, project_root)
    print("Corpus:", corpus_cfg.name, f"({corpus_cfg.corpus_id})")
    for k in sorted(applied):
        print(f"  {k}={applied[k]}")

    # Trial mode: swap index to a small subset so Docling/chunking only touch N files
    if args.trial_n > 0:
        import os
        trial_path = _write_trial_index(corpus_cfg, project_root, args.trial_n)
        if trial_path:
            os.environ["FILINGS_INDEX_PATH"] = str(trial_path)
            print(f"  [trial] FILINGS_INDEX_PATH → {trial_path}")

    if args.no_skip:
        for k in (
            "SKIP_PARSE_IF_EXISTS",
            "SKIP_CHUNK_IF_EXISTS",
            "SKIP_LLM_CHUNK_IF_EXISTS",
            "SKIP_LLM_DOC_IF_EXISTS",
        ):
            import os
            os.environ[k] = "0"

    # Import after env is set
    from tariff_agent.utils.docling_pipeline import run_docling_on_filings
    from tariff_agent.utils.chunking import run_chunking
    from tariff_agent.utils.llm_client import run_llm_on_chunks
    from tariff_agent.utils.doc_level import run_doc_level
    from tariff_agent.utils.vllm_lifecycle import maybe_start_vllm_after_parse

    if args.stage in ("parse", "all"):
        run_docling_on_filings(force=args.no_skip)
        maybe_start_vllm_after_parse()
    if args.stage in ("chunk", "all"):
        run_chunking(force=args.no_skip)
    if args.stage in ("llm_chunk", "all"):
        run_llm_on_chunks(force=args.no_skip)
    if args.stage in ("llm_doc", "all"):
        run_doc_level(force=args.no_skip)

    print("Done:", args.stage)


if __name__ == "__main__":
    main()
