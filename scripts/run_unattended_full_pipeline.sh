#!/usr/bin/env bash
# Full pipeline: Docling → (systemd start thomas-vllm if PIPELINE_VLLM_SYSTEMD_UNIT set) →
# chunk → Pass-1 vLLM → Pass-2 vLLM → aggregate → review_ready.csv
#
# GPU: Docling uses GPU during parse; parse_node then starts vLLM and waits for /v1/models before chunk.
# Unattended systemd often needs NOPASSWD for `systemctl start thomas-vllm`, or start Thomas manually after Docling.
# If vLLM is down, llm_chunk / llm_doc block until GET /v1/models succeeds (or timeout → RuntimeError).
#
# Usage:
#   chmod +x scripts/run_unattended_full_pipeline.sh
#   ./scripts/run_unattended_full_pipeline.sh
#   ./scripts/run_unattended_full_pipeline.sh --no-skip
#   nohup ./scripts/run_unattended_full_pipeline.sh >/dev/null 2>&1 &
set -euo pipefail
set -o pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

LOG="${PIPELINE_LOG:-$ROOT/output/pipeline_unattended.log}"
mkdir -p "$(dirname "$LOG")"
export PYTHONUNBUFFERED=1

if [[ -f .venv/bin/activate ]]; then
  # shellcheck source=/dev/null
  source .venv/bin/activate
fi

_run() {
  echo "=== $(date -Is) unattended full pipeline (cwd=$ROOT) ==="
  echo "Log file: $LOG"

  python3 -u run_pipeline.py --stage all --thread-id "${PIPELINE_THREAD_ID:-unattended}" "$@"

  echo "=== $(date -Is) build_review_table ==="
  python3 -u -c "
from tariff_agent.utils.config import ensure_hf_hub_env_for_process
ensure_hf_hub_env_for_process()
from tariff_agent.utils.human_review import build_review_table
from tariff_agent.utils.config import get_settings
s = get_settings()
build_review_table(settings=s)
print('wrote', s.resolve(s.review_csv))
"

  echo "=== $(date -Is) finished ==="
  echo "Artifacts (paths from .env):"
  echo "  output/docling_parse_index.csv, output/docling_json/"
  echo "  output/chunks/chunks.parquet"
  echo "  output/llm_raw/chunks_llm.parquet"
  echo "  output/csv/filings_llm.csv, output/llm_docs/filings_llm.parquet"
  echo "  output/csv/issuer_year_tariff_signals.csv"
  echo "  output/companies/*/*_filing_llm.json"
  echo "  output/human_review/review_ready.csv"
}

_run "$@" 2>&1 | tee -a "$LOG"
exit "${PIPESTATUS[0]}"
