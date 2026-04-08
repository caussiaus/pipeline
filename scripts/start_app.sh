#!/usr/bin/env bash
# start_app.sh — launch Streamlit + Cloudflare Tunnel for data.caseyjussaume.com
# Usage:  bash scripts/start_app.sh [--no-tunnel]
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
TUNNEL_ID="59b0eaff-353c-4ae3-945c-3f0d5a56ff64"
STREAMLIT_PORT=8501
LOG_DIR="$PROJECT_ROOT/logs"
mkdir -p "$LOG_DIR"

# ── load .env ───────────────────────────────────────────────────────────────
if [[ -f "$PROJECT_ROOT/.env" ]]; then
  set -a; source "$PROJECT_ROOT/.env"; set +a
fi

# ── activate venv if present ────────────────────────────────────────────────
if [[ -f "$PROJECT_ROOT/.venv/bin/activate" ]]; then
  source "$PROJECT_ROOT/.venv/bin/activate"
fi

echo "[start_app] Starting Streamlit on port $STREAMLIT_PORT …"
streamlit run "$PROJECT_ROOT/app.py" \
  --server.port "$STREAMLIT_PORT" \
  --server.headless true \
  --server.enableCORS false \
  --server.enableXsrfProtection true \
  > "$LOG_DIR/streamlit.log" 2>&1 &
STREAMLIT_PID=$!
echo "[start_app] Streamlit PID=$STREAMLIT_PID"

# ── wait for Streamlit to be ready ──────────────────────────────────────────
echo "[start_app] Waiting for Streamlit …"
for i in $(seq 1 30); do
  if curl -sf "http://localhost:$STREAMLIT_PORT/_stcore/health" > /dev/null 2>&1; then
    echo "[start_app] Streamlit is up."
    break
  fi
  sleep 1
done

if [[ "${1:-}" == "--no-tunnel" ]]; then
  echo "[start_app] --no-tunnel flag set; skipping cloudflared."
  echo "[start_app] App: http://localhost:$STREAMLIT_PORT"
  wait $STREAMLIT_PID
  exit 0
fi

# ── start Cloudflare Tunnel ─────────────────────────────────────────────────
TUNNEL_TOKEN=$(cloudflared tunnel token "$TUNNEL_ID" 2>/dev/null | head -1)
echo "[start_app] Starting Cloudflare Tunnel → data.caseyjussaume.com …"
cloudflared tunnel run --token "$TUNNEL_TOKEN" \
  > "$LOG_DIR/cloudflared.log" 2>&1 &
CF_PID=$!
echo "[start_app] cloudflared PID=$CF_PID"

echo ""
echo "  ✓ App live at  https://data.caseyjussaume.com"
echo "  ✓ Local:       http://localhost:$STREAMLIT_PORT"
echo "  Logs:          $LOG_DIR/streamlit.log"
echo "           $LOG_DIR/cloudflared.log"
echo ""
echo "  Stop with:  kill $STREAMLIT_PID $CF_PID"
echo ""

# keep script alive; both processes die together on Ctrl-C
trap "kill $STREAMLIT_PID $CF_PID 2>/dev/null; exit 0" INT TERM
wait $STREAMLIT_PID
kill $CF_PID 2>/dev/null || true
