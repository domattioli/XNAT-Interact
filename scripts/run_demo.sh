#!/usr/bin/env bash
# run_demo.sh — Launch the guided XNAT-Interact demo.
# Requires Docker Desktop running on macOS (for the real-XNAT path).
# Usage:
#   bash scripts/run_demo.sh           # real XNAT via Docker (default)
#   bash scripts/run_demo.sh --fake    # local fake archive, no Docker needed

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

FAKE_MODE=0
for arg in "$@"; do
  case "$arg" in
    --fake) FAKE_MODE=1 ;;
  esac
done

export XNAT_DEMO_MODE=1

# --- Python environment sanity: a venv whose interpreter was deleted
# (e.g. Homebrew/Xcode Python upgrade) fails later with the cryptic
# "bad interpreter: .../python3.9: No such file or directory".
# Detect it up front and say what to do.
if ! python3 -c "import sys" >/dev/null 2>&1; then
  echo "[run_demo.sh] ERROR: your Python environment is broken (interpreter missing)."
  echo "  This usually means the virtualenv points at a Python that was removed."
  echo "  Fix:"
  echo "    deactivate 2>/dev/null; rm -rf .venv"
  echo "    python3 -m venv .venv && source .venv/bin/activate"
  echo "    pip install -r requirements.txt"
  exit 1
fi
if ! python3 -c "import streamlit" >/dev/null 2>&1; then
  echo "[run_demo.sh] ERROR: streamlit is not importable in this environment."
  echo "  Activate your venv and install dependencies first:"
  echo "    source .venv/bin/activate && pip install -r requirements.txt"
  echo "  (If activation itself errors with 'bad interpreter', rebuild the venv:"
  echo "   rm -rf .venv && python3 -m venv .venv && source .venv/bin/activate)"
  exit 1
fi

if [ "$FAKE_MODE" -eq 1 ]; then
  echo "[run_demo.sh] --fake flag set → using local FakeXNAT archive (no Docker needed)"
  echo "[run_demo.sh] Starting guided app..."
  cd "$REPO_ROOT"
  exec streamlit run streamlit_guided.py
fi

# --- Default: real XNAT via Docker ---
echo "[run_demo.sh] Checking Docker daemon..."
if ! docker info >/dev/null 2>&1; then
  echo ""
  echo "[run_demo.sh] WARNING: Docker daemon not running."
  echo "  To start the real XNAT locally, run:"
  echo "    docker compose -f tests/integration/xnat_local/docker-compose.yml up -d"
  echo "  Then re-run this script, or start with --fake to use local archive."
  echo ""
  echo "[run_demo.sh] Falling back to --fake mode..."
  cd "$REPO_ROOT"
  exec streamlit run streamlit_guided.py
fi

echo "[run_demo.sh] Starting XNAT via Docker Compose..."
docker compose -f "$REPO_ROOT/tests/integration/xnat_local/docker-compose.yml" up -d

echo "[run_demo.sh] Waiting for XNAT to be ready (this may take ~3 minutes)..."
MAX_WAIT=240
ELAPSED=0
INTERVAL=5
while true; do
  HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -u admin:admin \
    "http://localhost:8080/data/JSESSION" 2>/dev/null || echo "000")
  if [ "$HTTP_CODE" = "200" ]; then
    echo "[run_demo.sh] XNAT is ready (HTTP 200) after ${ELAPSED}s."
    break
  fi
  if [ "$ELAPSED" -ge "$MAX_WAIT" ]; then
    echo "[run_demo.sh] Timeout waiting for XNAT after ${MAX_WAIT}s. Falling back to --fake mode..."
    cd "$REPO_ROOT"
    exec streamlit run streamlit_guided.py
  fi
  printf "  [%3ds] XNAT not ready yet (HTTP %s), waiting...\n" "$ELAPSED" "$HTTP_CODE"
  sleep "$INTERVAL"
  ELAPSED=$((ELAPSED + INTERVAL))
done

export XNAT_SERVER_URL=http://localhost:8080
export XNAT_PROJECT_NAME=DEMO_UI
export XNAT_USERNAME=admin
export XNAT_PASSWORD=admin
export XNAT_IDENTITY_SALT=64656d6f6f6e6c79646f6e6f747275737474686973696e70726f64756374696f6e

echo "[run_demo.sh] Starting guided app with real XNAT at $XNAT_SERVER_URL..."
cd "$REPO_ROOT"
exec streamlit run streamlit_guided.py
