#!/usr/bin/env bash
# Sport Coach — start the Node backend on Linux/macOS (full monorepo checkout).
#
# Usage (from repo root): bash src/scripts/start-linux.sh
# Or:    chmod +x src/scripts/start-linux.sh && ./src/scripts/start-linux.sh
#
# Env:
#   PORT — listen port (platform default or unset → backend uses dev fallback 3000; optional in src/backend/.env)
#   SPORT_COACH_STATIC_ROOT — override web root (default: src/, sibling of backend/)
#
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SRC_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
BACKEND_DIR="$SRC_ROOT/backend"

if [[ ! -f "$BACKEND_DIR/package.json" ]]; then
  echo "error: expected backend at $BACKEND_DIR (run this script from the Sport Coach repo)." >&2
  exit 1
fi

export SPORT_COACH_STATIC_ROOT="${SPORT_COACH_STATIC_ROOT:-$SRC_ROOT}"

if [[ -f package-lock.json ]]; then
  (cd "$BACKEND_DIR" && npm ci)
else
  (cd "$BACKEND_DIR" && npm install)
fi

# Production: compile TypeScript from repo root (`npm run build`); skip if dist already exists.
if [[ ! -f "$BACKEND_DIR/dist/server.js" ]]; then
  (cd "$REPO_ROOT" && npm run build)
fi

cd "$BACKEND_DIR"
exec npm start
