#!/usr/bin/env bash
# One-click launcher for macOS/Linux — sets up a local venv and runs the deploy script.
# Usage: ./deploy/deploy.sh deploy/clients/acme.json [--skip-infra] [--yes]
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
VENV_DIR="$REPO_ROOT/.deploy_venv"

if [ $# -lt 1 ]; then
  echo "Usage: $0 <client-config.json> [extra one_click_deploy.py args...]"
  exit 1
fi
CLIENT_CONFIG="$1"; shift || true

PY=python3
command -v "$PY" >/dev/null 2>&1 || { echo "python3 not found — install Python 3.9+ first."; exit 1; }

if [ ! -d "$VENV_DIR" ]; then
  echo "Creating virtual environment at $VENV_DIR ..."
  "$PY" -m venv "$VENV_DIR"
fi
source "$VENV_DIR/bin/activate"

echo "Installing dependencies ..."
pip install --quiet --upgrade pip
pip install --quiet -r "$REPO_ROOT/requirements.txt"

echo "Running one-click deploy ..."
python "$SCRIPT_DIR/one_click_deploy.py" --client-config "$CLIENT_CONFIG" "$@"
