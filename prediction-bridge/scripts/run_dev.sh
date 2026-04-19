#!/usr/bin/env bash
# Local dev launcher. Usage: ./scripts/run_dev.sh [port]
set -euo pipefail

ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

PORT="${1:-8080}"
export PREDICTION_BRIDGE_CONFIG_PATH="${PREDICTION_BRIDGE_CONFIG_PATH:-$ROOT/config/config.yaml}"

if [[ ! -f "$PREDICTION_BRIDGE_CONFIG_PATH" ]]; then
  echo "config file not found at $PREDICTION_BRIDGE_CONFIG_PATH" >&2
  echo "copy config/config.example.yaml and edit it before running" >&2
  exit 1
fi

if [[ ! -d "$ROOT/.venv" ]]; then
  python3 -m venv "$ROOT/.venv"
fi
# shellcheck disable=SC1091
source "$ROOT/.venv/bin/activate"
pip install -q -r requirements.txt

exec uvicorn app.main:app --host 0.0.0.0 --port "$PORT" --reload
