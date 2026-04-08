#!/usr/bin/env python3
from __future__ import annotations
"""Self-healing supervisor: rerun ``run_pipeline.py --stage all`` until completion criteria hold.

Completion matches the research pipeline end state (parse index, chunks, Pass-1/2 LLM, issuer-year
aggregation). Optional: regenerate ``review_ready.csv`` after success.

Per-ticker incremental runs: prefer ``python -m tariff_agent.watcher`` — filtering the global
``filings_index`` in ``run_pipeline`` would desync ``docling_parse_index`` row counts vs completion
checks unless we add merge/incremental index updates (not done here).
"""
_SUPERVISOR_EPILOG = """
examples:
  python3 scripts/pipeline_supervisor.py --status
  python3 scripts/pipeline_supervisor.py --max-attempts 0 --sleep-sec 120
  python3 scripts/pipeline_supervisor.py --wait-vllm --vllm-wait-sec 900
  python3 scripts/pipeline_supervisor.py --jitter 0.2 --no-require-review
  python3 scripts/pipeline_supervisor.py --state-json output/my_supervisor_state.json
"""

import argparse
import json
import logging
import os
import random
import subprocess
import sys
import time
from pathlib import Path

# Repo root on sys.path
_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from tariff_agent.utils.config import ensure_hf_hub_env_for_process, get_settings
from tariff_agent.utils.pipeline_completion import assess_pipeline_completion, write_report_json  # noqa: E402
from tariff_agent.utils.vllm_lifecycle import vllm_http_reachable, wait_for_vllm_http_soft  # noqa: E402

ensure_hf_hub_env_for_process()
logger = logging.getLogger("pipeline_supervisor")


def _run_pipeline(
    *,
    root: Path,
    thread_id: str,
    no_skip: bool,
) -> int:
    cmd = [
        sys.executable,
        "-u",
        str(root / "run_pipeline.py"),
        "--stage",
        "all",
        "--thread-id",
        thread_id,
    ]
    if no_skip:
        cmd.append("--no-skip")
    logger.info("Running: %s", " ".join(cmd))
    proc = subprocess.run(cmd, cwd=str(root))
    return int(proc.returncode)


def _only_review_missing(report) -> bool:
    return (
        report.parse_complete
        and report.chunks_parquet_exists
        and report.chunks_llm_exists
        and report.filings_llm_exists
        and report.filings_llm_aligned
        and report.issuer_year_exists
        and not report.review_exists
    )


def _build_review(settings) -> None:
    from tariff_agent.utils.human_review import build_review_table

    build_review_table(settings=settings)
    logger.info("Wrote review table: %s", settings.resolve(settings.review_csv))


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    ap = argparse.ArgumentParser(description=__doc__, epilog=_SUPERVISOR_EPILOG, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--status", action="store_true", help="Print completion report and exit.")
    ap.add_argument("--max-attempts", type=int, default=50, help="0 = unlimited attempts.")
    ap.add_argument("--sleep-sec", type=float, default=90.0, help="Backoff between attempts after failure.")
    ap.add_argument(
        "--sleep-cap-sec",
        type=float,
        default=900.0,
        help="Maximum sleep after exponential backoff.",
    )
    ap.add_argument("--thread-id", default="supervisor", help="LangGraph thread id for run_pipeline.")
    ap.add_argument("--no-skip", action="store_true", help="Forward --no-skip to run_pipeline.")
    ap.add_argument(
        "--no-require-review",
        action="store_true",
        help="Do not require output/human_review/review_ready.csv for completion (default is to require it).",
    )
    ap.add_argument(
        "--state-json",
        default="",
        help="Write last CompletionReport JSON here (default: output/pipeline_supervisor_state.json).",
    )
    ap.add_argument(
        "--jitter",
        type=float,
        default=0.15,
        help="Sleep jitter ratio ± around base sleep (0 disables). Default 0.15 → ~0.85–1.15×.",
    )
    ap.add_argument(
        "--wait-vllm",
        action="store_true",
        help="Before each pipeline attempt, wait up to --vllm-wait-sec for GET /v1/models (also env PIPELINE_SUPERVISOR_WAIT_VLLM=1).",
    )
    ap.add_argument(
        "--vllm-wait-sec",
        type=float,
        default=600.0,
        help="Max seconds to wait for vLLM when --wait-vllm or PIPELINE_SUPERVISOR_WAIT_VLLM is set.",
    )
    args = ap.parse_args()
    require_review = not bool(args.no_require_review)
    wait_vllm = bool(args.wait_vllm) or os.environ.get("PIPELINE_SUPERVISOR_WAIT_VLLM", "").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )

    settings = get_settings()
    root = settings.project_root
    state_path = Path(args.state_json or (root / "output/pipeline_supervisor_state.json"))

    report = assess_pipeline_completion(settings, require_review_csv=require_review)
    write_report_json(report, state_path)
    if args.status:
        print(json.dumps(report.to_jsonable(), indent=2))
        return 0 if report.ok else 1

    if report.ok:
        logger.info("Already complete: %s", "; ".join(report.messages))
        return 0

    if require_review and _only_review_missing(report):
        logger.info("Structured outputs present; building missing review table only.")
        try:
            _build_review(settings)
        except Exception as e:
            logger.exception("build_review_table failed: %s", e)
            return 1
        report = assess_pipeline_completion(settings, require_review_csv=require_review)
        write_report_json(report, state_path)
        if report.ok:
            return 0

    logger.info("Incomplete: %s", "; ".join(report.messages))

    attempt = 0
    consecutive_stuck = 0
    last_fp: str | None = None
    sleep_sec = max(5.0, float(args.sleep_sec))
    jitter_r = random.Random()
    jitter_ratio = max(0.0, float(args.jitter))

    while True:
        attempt += 1
        if args.max_attempts and attempt > args.max_attempts:
            logger.error("Exceeded max attempts (%s); still incomplete.", args.max_attempts)
            return 3

        if wait_vllm:
            logger.info(
                "Waiting for vLLM (up to %ss) before attempt %s…",
                int(args.vllm_wait_sec),
                attempt,
            )
            if wait_for_vllm_http_soft(settings, total_timeout_sec=args.vllm_wait_sec, poll_sec=15.0):
                logger.info("vLLM reachable: %s", settings.vllm_base_url.rstrip("/"))
            else:
                logger.warning(
                    "vLLM still not reachable after %ss (%s); running pipeline anyway (parse does not need vLLM).",
                    int(args.vllm_wait_sec),
                    settings.vllm_base_url.rstrip("/"),
                )
        else:
            v_ok = vllm_http_reachable(settings)
            logger.info(
                "vLLM probe (%s/models): %s",
                settings.vllm_base_url.rstrip("/"),
                "OK" if v_ok else "not reachable",
            )

        logger.info("Supervisor attempt %s starting…", attempt)
        code = _run_pipeline(root=root, thread_id=args.thread_id, no_skip=args.no_skip)
        if code != 0:
            logger.warning("run_pipeline exited %s", code)

        if require_review and code == 0:
            try:
                _build_review(settings)
            except Exception as e:
                logger.exception("build_review_table failed: %s", e)

        report = assess_pipeline_completion(settings, require_review_csv=require_review)
        write_report_json(report, state_path)
        fp = report.fingerprint()
        if fp == last_fp and code != 0:
            consecutive_stuck += 1
        else:
            consecutive_stuck = 0
        last_fp = fp

        if consecutive_stuck >= 3:
            logger.error(
                "Stuck: completion fingerprint unchanged after 3 failed attempts (%s). "
                "Fix errors (parse/vLLM/disk) and re-run.",
                fp,
            )
            return 2

        if report.ok:
            logger.info("Pipeline complete: %s", "; ".join(report.messages))
            return 0

        if jitter_ratio > 0:
            factor = 1.0 - jitter_ratio + (2.0 * jitter_ratio * jitter_r.random())
        else:
            factor = 1.0
        sleep_actual = min(float(args.sleep_cap_sec), sleep_sec * factor)
        logger.warning(
            "Still incomplete (attempt %s): %s — sleeping %.0fs",
            attempt,
            "; ".join(report.messages),
            sleep_actual,
        )
        time.sleep(sleep_actual)
        sleep_sec = min(float(args.sleep_cap_sec), sleep_sec * 1.35)


if __name__ == "__main__":
    raise SystemExit(main())
