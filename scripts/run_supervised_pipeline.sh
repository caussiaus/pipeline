#!/usr/bin/env bash
# Self-healing loop: rerun the full LangGraph pipeline until completion criteria are met
# (parse index, chunks, Pass-1/2 LLM, issuer-year CSV). Writes output/pipeline_supervisor_state.json.
#
# Usage:
#   ./scripts/run_supervised_pipeline.sh
#   ./scripts/run_supervised_pipeline.sh -- --max-attempts 0
#   PIPELINE_SUPERVISOR_WAIT_VLLM=1 ./scripts/run_supervised_pipeline.sh -- --vllm-wait-sec 900
#   ./scripts/run_supervised_pipeline.sh -- --no-require-review
#   nohup ./scripts/run_supervised_pipeline.sh >> output/supervised.outer.log 2>&1 &
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

export PYTHONUNBUFFERED=1

LOG="${PIPELINE_SUPERVISOR_LOG:-$ROOT/output/pipeline_supervised.log}"
mkdir -p "$(dirname "$LOG")"

if [[ -f .venv/bin/activate ]]; then
  # shellcheck source=/dev/null
  source .venv/bin/activate
fi

echo "=== $(date -Is) supervised pipeline (log=$LOG) ===" | tee -a "$LOG"
python3 -u "$ROOT/scripts/pipeline_supervisor.py" "$@" 2>&1 | tee -a "$LOG"
exit "${PIPESTATUS[0]}"
