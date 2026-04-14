#!/usr/bin/env bash
# Sport Coach — start when the service root is only the backend folder (e.g. Zeabur root = backend/).
#
# Usage: chmod +x start-linux.sh && ./start-linux.sh
#
# Static HTML (`main.html`, `js/`, …) must live under SPORT_COACH_STATIC_ROOT. By default that is
# this directory — copy or sync the repo web root here for production, or set:
#   SPORT_COACH_STATIC_ROOT=/path/to/full/repo   (parent folder containing main.html + backend/)
#
# Data paths (data_club/, data/) are always resolved from this backend directory.
#
set -euo pipefail
BACKEND_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$BACKEND_ROOT"

export SPORT_COACH_STATIC_ROOT="${SPORT_COACH_STATIC_ROOT:-$BACKEND_ROOT}"

if [[ -f package-lock.json ]]; then
  npm ci
else
  npm install
fi

exec npm start
