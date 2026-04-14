#!/usr/bin/env bash
# Sport Coach — start the Node backend on Linux/macOS (full monorepo checkout).
#
# Usage: from anywhere — bash scripts/start-linux.sh
# Or:    chmod +x scripts/start-linux.sh && ./scripts/start-linux.sh
#
# Env:
#   PORT — listen port (default from backend/.env or 3000)
#   SPORT_COACH_STATIC_ROOT — override web root (default: repo root, sibling of backend/)
#
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BACKEND_DIR="$REPO_ROOT/backend"

if [[ ! -f "$BACKEND_DIR/package.json" ]]; then
  echo "error: expected backend at $BACKEND_DIR (run this script from the Sport Coach repo)." >&2
  exit 1
fi

export SPORT_COACH_STATIC_ROOT="${SPORT_COACH_STATIC_ROOT:-$REPO_ROOT}"

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
