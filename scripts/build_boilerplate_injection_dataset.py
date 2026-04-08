#!/usr/bin/env python3
"""Build a Parquet/CSV of chunks with injected boilerplate for Pass-1 robustness testing.

Reads ``output/chunks/chunks.parquet`` (configurable via .env). For each sampled row,
adds ``text_injected``, ``injection_phrases`` (JSON array), and keyword-gate flags.

Example:
  python3 scripts/build_boilerplate_injection_dataset.py --sample 200 --seed 42
  python3 scripts/build_boilerplate_injection_dataset.py --output output/eval/boilerplate_chunks.parquet
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tariff_agent.utils.boilerplate_noise import (
    injection_triggers_keyword_gate,
    inject_boilerplate,
)
from tariff_agent.utils.config import ensure_hf_hub_env_for_process, get_settings

ensure_hf_hub_env_for_process()


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--sample", type=int, default=100, help="Number of chunks to sample.")
    ap.add_argument("--seed", type=int, default=0, help="RNG seed.")
    ap.add_argument(
        "--output",
        default="",
        help="Output parquet path (default: output/eval/boilerplate_injected_chunks.parquet).",
    )
    ap.add_argument("--n-phrases", type=int, default=2, help="Boilerplate sentences per chunk.")
    ap.add_argument(
        "--mode",
        choices=("append", "prepend", "interleave"),
        default="append",
        help="Injection style (see boilerplate_noise.inject_boilerplate).",
    )
    args = ap.parse_args()

    import random

    import pandas as pd

    settings = get_settings()
    src = settings.resolve(settings.chunks_parquet)
    if not src.is_file():
        print(f"Missing chunks parquet: {src}", file=sys.stderr)
        return 1

    out = Path(
        args.output or str(settings.resolve("output/eval/boilerplate_injected_chunks.parquet"))
    )
    out.parent.mkdir(parents=True, exist_ok=True)

    df = pd.read_parquet(src)
    if df.empty:
        print("chunks.parquet is empty.", file=sys.stderr)
        return 1

    n = min(max(1, args.sample), len(df))
    rng = random.Random(args.seed)
    sample = df.sample(n=n, random_state=args.seed)

    rows: list[dict] = []
    for _, r in sample.iterrows():
        text = str(r.get("text", "") or "")
        section = str(r.get("section_path", "") or "")
        hit_before = injection_triggers_keyword_gate(text)
        new_text, phrases = inject_boilerplate(
            text,
            section_path=section,
            n_phrases=args.n_phrases,
            mode=args.mode,
            rng=rng,
        )
        hit_after = injection_triggers_keyword_gate(new_text)
        row = r.to_dict()
        row["text_orig"] = text
        row["text_injected"] = new_text
        row["injection_phrases_json"] = json.dumps(phrases, ensure_ascii=False)
        row["keyword_gate_before"] = hit_before
        row["keyword_gate_after"] = hit_after
        rows.append(row)

    out_df = pd.DataFrame(rows)
    out_df.to_parquet(out, index=False)
    csv_path = out.with_suffix(".csv")
    out_df.to_csv(csv_path, index=False)
    print(f"Wrote {len(out_df)} rows to {out}")
    print(f"CSV copy: {csv_path}")
    na = int(out_df["keyword_gate_after"].sum())
    nb = int(out_df["keyword_gate_before"].sum())
    print(f"keyword_gate_before: {nb}/{len(out_df)}  keyword_gate_after: {na}/{len(out_df)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
