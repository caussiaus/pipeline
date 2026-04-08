#!/usr/bin/env bash
# Stop all repo pipeline Python processes (supervisor + run_pipeline).
# Run this in YOUR terminal (WSL), not relying on an IDE sandbox, so PIDs match your session.
set -u

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
echo "=== tariff-sedar-pipeline: processes before ==="
pgrep -af 'pipeline_supervisor\.py|run_pipeline\.py' || echo "(none)"

_stop() {
  local sig=$1
  pkill "-${sig}" -f "${ROOT}/scripts/pipeline_supervisor.py" 2>/dev/null || true
  pkill "-${sig}" -f "${ROOT}/run_pipeline.py" 2>/dev/null || true
  # Fallback if cwd differs but command line still matches
  pkill "-${sig}" -f 'scripts/pipeline_supervisor.py' 2>/dev/null || true
  pkill "-${sig}" -f '[r]un_pipeline.py' 2>/dev/null || true
}

_stop TERM
sleep 2
_stop TERM
sleep 2
_stop KILL

echo "=== processes after ==="
pgrep -af 'pipeline_supervisor\.py|run_pipeline\.py' || echo "(none — pipeline stopped)"

echo "Note: vLLM (vllm serve) is separate — this script does not stop it."
echo "Docling runs inside run_pipeline; there is no separate 'docling' OS process."
