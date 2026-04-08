#!/usr/bin/env python3
"""Build a supervised fine-tuning (SFT) dataset from pipeline outputs.

Produces conversation pairs (system + user → assistant) for both passes:

Pass-1 pairs  (chunk → extraction JSON)
  Positive: keyword_hit=True, mentions_tariffs=True, high-confidence
  Hard negative: keyword_hit=True, mentions_tariffs=False (model correctly rejected)

Pass-2 pairs  (filing context → synthesis JSON)
  Gold: has_tariff_discussion=True, no QC errors, key_quotes non-empty
  Rejection: fls_only=True → corrected to has_tariff_discussion=False

Outputs:
  output/sft/pass1_sft.parquet  + .jsonl
  output/sft/pass2_sft.parquet  + .jsonl
  output/sft/summary.txt
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import pandas as pd

from tariff_agent.utils.config import ensure_hf_hub_env_for_process, get_settings
from tariff_agent.prompts.chunk_prompt import CHUNK_SYSTEM_PROMPT, build_chunk_user_prompt
from tariff_agent.prompts.doc_prompt import DOC_SYSTEM_PROMPT, build_doc_user_prompt
from tariff_agent.state import ChunkRecord, normalize_filing_type

ensure_hf_hub_env_for_process()


def _jsonl_write(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False, default=str) + "\n")


def build_pass1_pairs(
    chunks: pd.DataFrame,
    chunks_llm: pd.DataFrame,
    *,
    max_pos: int,
    max_neg: int,
    seed: int,
) -> list[dict]:
    """Build (system, user, assistant) triples from Pass-1 chunk outputs."""
    merged = chunks.merge(
        chunks_llm[["chunk_id", "mentions_tariffs", "earnings_impact_present",
                     "macro_risk_present", "supply_chain_risk_present", "llm_skipped"]],
        on="chunk_id", how="inner",
    )
    merged = merged[merged["llm_skipped"] == False]  # noqa: E712
    positives = merged[merged["mentions_tariffs"] == True].sample(  # noqa: E712
        n=min(max_pos, (merged["mentions_tariffs"] == True).sum()), random_state=seed
    )
    negatives = merged[
        (merged["mentions_tariffs"] == False) & merged["keyword_hit"]  # noqa: E712
    ].sample(
        n=min(max_neg, ((merged["mentions_tariffs"] == False) & merged["keyword_hit"]).sum()),
        random_state=seed,
    )

    pairs = []
    for label, subset in [("positive", positives), ("hard_negative", negatives)]:
        for _, row in subset.iterrows():
            sector_profile = {
                "naics_sector": row.get("naics_sector", "unknown"),
                "mechanism": row.get("mechanism", "minimal_no_vector"),
                "exposure_vector": row.get("exposure_vector", ""),
                "cap_earnings": row.get("cap_earnings", 3),
                "cap_supply_chain": row.get("cap_supply_chain", 3),
                "cap_macro": row.get("cap_macro", 3),
            }
            assistant = {
                "mentions_tariffs": bool(row.get("mentions_tariffs", False)),
                "earnings_impact_present": bool(row.get("earnings_impact_present", False)),
                "macro_risk_present": bool(row.get("macro_risk_present", False)),
                "supply_chain_risk_present": bool(row.get("supply_chain_risk_present", False)),
            }
            chunk_rec = ChunkRecord(
                chunk_id=str(row.get("chunk_id", "")),
                filing_id=str(row.get("filing_id", "")),
                profile_id=str(row.get("profile_id", "") or ""),
                ticker=str(row.get("ticker", "") or "unknown"),
                filing_type=normalize_filing_type(row.get("filing_type", "OTHER")),
                filing_date=str(row.get("filing_date", "") or ""),
                section_path=str(row.get("section_path", "") or ""),
                page_start=int(row.get("page_start", 0) or 0),
                page_end=int(row.get("page_end", 0) or 0),
                text=str(row.get("text", "") or ""),
                num_tokens=int(row.get("num_tokens", 0) or 0),
                keyword_hit=bool(row.get("keyword_hit", False)),
                keyword_hit_terms=list(row["keyword_hit_terms"]) if isinstance(row.get("keyword_hit_terms"), (list, tuple)) else [],
                naics_sector=str(row.get("naics_sector", "unknown") or "unknown"),
                mechanism=str(row.get("mechanism", "minimal_no_vector") or "minimal_no_vector"),
                exposure_vector=str(row.get("exposure_vector", "") or ""),
                cap_earnings=int(float(row["cap_earnings"]) if pd.notna(row.get("cap_earnings")) else 3),
                cap_supply_chain=int(float(row["cap_supply_chain"]) if pd.notna(row.get("cap_supply_chain")) else 3),
                cap_macro=int(float(row["cap_macro"]) if pd.notna(row.get("cap_macro")) else 3),
            )
            pairs.append({
                "split": label,
                "chunk_id": str(row["chunk_id"]),
                "filing_id": str(row["filing_id"]),
                "messages": [
                    {"role": "system", "content": CHUNK_SYSTEM_PROMPT},
                    {"role": "user", "content": build_chunk_user_prompt(chunk_rec)},
                    {"role": "assistant", "content": json.dumps(assistant, ensure_ascii=False)},
                ],
            })
    return pairs


def build_pass2_pairs(
    filings_llm: pd.DataFrame,
    chunks: pd.DataFrame,
    chunks_llm: pd.DataFrame,
    qc: pd.DataFrame,
    *,
    max_gold: int,
    seed: int,
) -> list[dict]:
    """Build (system, user, assistant) triples from Pass-2 doc outputs.

    Gold rows: has_tariff_discussion=True, QC max_severity != error, key_quotes non-empty.
    Correction rows: fls_only=True (ground truth should be False, scores zeroed).
    """
    merged_llm = filings_llm.merge(qc[["filing_id", "fls_only", "qc_max_severity"]], on="filing_id", how="left")
    merged_chunks = chunks.merge(
        chunks_llm[["chunk_id", "filing_id", "mentions_tariffs"]], on=["chunk_id", "filing_id"], how="left"
    )

    gold_mask = (
        (merged_llm["has_tariff_discussion"].astype(str).str.lower() == "true")
        & (merged_llm["qc_max_severity"].astype(str).str.lower().isin(["none", "warn"]))
        & (merged_llm["key_quotes"].astype(str).str.len() > 4)
        & (merged_llm["fls_only"].astype(str).str.lower() != "true")
    )
    gold = merged_llm[gold_mask].sample(n=min(max_gold, gold_mask.sum()), random_state=seed)

    fls_mask = merged_llm["fls_only"].astype(str).str.lower() == "true"
    fls_rows = merged_llm[fls_mask]

    pairs = []

    def _chunk_rows_for(fid: str) -> list[dict]:
        pos_chunks = merged_chunks[
            (merged_chunks["filing_id"] == fid) & (merged_chunks["mentions_tariffs"] == True)  # noqa: E712
        ]
        return pos_chunks.to_dict(orient="records")

    for label, subset, correct_fn in [
        ("gold", gold, None),
        ("fls_correction", fls_rows, lambda r: _zeroed(r)),
    ]:
        for _, row in subset.iterrows():
            fid = str(row["filing_id"])
            chunk_rows = _chunk_rows_for(fid)
            if not chunk_rows:
                continue
            assistant_raw = row.to_dict()
            if correct_fn is not None:
                assistant_raw = correct_fn(assistant_raw)

            assistant = {
                "has_tariff_discussion": bool(str(assistant_raw.get("has_tariff_discussion", "false")).lower() == "true"),
                "tariff_direction": str(assistant_raw.get("tariff_direction", "NONE")),
                "earnings_tariff_score": int(float(assistant_raw.get("earnings_tariff_score", 0) or 0)),
                "supply_chain_tariff_score": int(float(assistant_raw.get("supply_chain_tariff_score", 0) or 0)),
                "macro_tariff_score": int(float(assistant_raw.get("macro_tariff_score", 0) or 0)),
                "disclosure_quality": str(assistant_raw.get("disclosure_quality", "BOILERPLATE")),
                "doc_summary_sentence": str(assistant_raw.get("doc_summary_sentence", "")),
            }
            try:
                filing_meta = {
                    "filing_id": fid,
                    "filing_date": str(row.get("filing_date", "")),
                    "filing_type": str(row.get("filing_type", "")),
                    "ticker": str(row.get("ticker", "")),
                    "issuer_name": str(row.get("issuer_name", "")),
                    "naics_sector": str(row.get("naics_sector", "")),
                    "mechanism": str(row.get("mechanism", "")),
                    "exposure_vector": str(row.get("exposure_vector", "")),
                }
                user_prompt = build_doc_user_prompt(filing_meta, chunk_rows)
            except Exception:
                continue

            pairs.append({
                "split": label,
                "filing_id": fid,
                "messages": [
                    {"role": "system", "content": DOC_SYSTEM_PROMPT},
                    {"role": "user", "content": user_prompt},
                    {"role": "assistant", "content": json.dumps(assistant, ensure_ascii=False)},
                ],
            })
    return pairs


def _zeroed(r: dict) -> dict:
    """Correction: FLS-only filing should have no scores and has_tariff=False."""
    out = dict(r)
    out["has_tariff_discussion"] = False
    out["tariff_direction"] = "NONE"
    out["earnings_tariff_score"] = 0
    out["supply_chain_tariff_score"] = 0
    out["macro_tariff_score"] = 0
    out["disclosure_quality"] = "BOILERPLATE"
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--pass1-pos",  type=int, default=500, help="Max Pass-1 positive pairs.")
    ap.add_argument("--pass1-neg",  type=int, default=500, help="Max Pass-1 hard-negative pairs.")
    ap.add_argument("--pass2-gold", type=int, default=200, help="Max Pass-2 gold pairs.")
    ap.add_argument("--seed",       type=int, default=42)
    ap.add_argument("--out-dir",    default="", help="Output dir (default: output/sft/).")
    args = ap.parse_args()

    s = get_settings()
    out_dir = Path(args.out_dir or str(s.resolve("output/sft")))
    out_dir.mkdir(parents=True, exist_ok=True)

    print("Loading parquet inputs …")
    chunks     = pd.read_parquet(s.resolve(s.chunks_parquet))
    chunks_llm = pd.read_parquet(s.resolve(s.chunks_llm_parquet))
    filings    = pd.read_csv(s.resolve(s.filings_llm_csv), dtype=str)
    qc_path    = s.resolve(s.consistency_report_csv)
    qc         = pd.read_csv(qc_path, dtype=str) if qc_path.is_file() else pd.DataFrame(
        columns=["filing_id", "fls_only", "qc_max_severity"]
    )

    print("Building Pass-1 SFT pairs …")
    p1 = build_pass1_pairs(chunks, chunks_llm, max_pos=args.pass1_pos,
                           max_neg=args.pass1_neg, seed=args.seed)
    p1_df = pd.DataFrame(p1)
    p1_df.to_parquet(out_dir / "pass1_sft.parquet", index=False)
    _jsonl_write(out_dir / "pass1_sft.jsonl", p1)

    print("Building Pass-2 SFT pairs …")
    p2 = build_pass2_pairs(filings, chunks, chunks_llm, qc,
                           max_gold=args.pass2_gold, seed=args.seed)
    p2_df = pd.DataFrame(p2)
    p2_df.to_parquet(out_dir / "pass2_sft.parquet", index=False)
    _jsonl_write(out_dir / "pass2_sft.jsonl", p2)

    p1_pos = sum(1 for r in p1 if r["split"] == "positive")
    p1_neg = sum(1 for r in p1 if r["split"] == "hard_negative")
    p2_gold = sum(1 for r in p2 if r["split"] == "gold")
    p2_fls  = sum(1 for r in p2 if r["split"] == "fls_correction")

    summary = (
        f"Pass-1: {p1_pos} positive + {p1_neg} hard-negative = {len(p1)} pairs\n"
        f"Pass-2: {p2_gold} gold + {p2_fls} FLS-correction = {len(p2)} pairs\n"
        f"Total:  {len(p1) + len(p2)} training examples\n"
        f"Output: {out_dir}\n"
    )
    (out_dir / "summary.txt").write_text(summary)
    print(summary)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
