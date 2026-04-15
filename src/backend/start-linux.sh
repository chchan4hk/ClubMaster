#!/usr/bin/env bash
# Sport Coach — start when the service root is only src/backend (e.g. Zeabur root = src/backend).
#
# Usage: chmod +x start-linux.sh && ./start-linux.sh
#
# Static HTML (`main.html`, `js/`, …) must live under SPORT_COACH_STATIC_ROOT. Default: parent folder
# (src/) so main.html sits next to backend/. Override if you use a different layout.
#
# Data paths (data_club/, data/) are always resolved from this backend directory.
#
set -euo pipefail
BACKEND_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$BACKEND_ROOT"
SRC_PARENT="$(cd "$BACKEND_ROOT/.." && pwd)"

export SPORT_COACH_STATIC_ROOT="${SPORT_COACH_STATIC_ROOT:-$SRC_PARENT}"

if [[ -f package-lock.json ]]; then
  npm ci
else
  npm install
fi

exec npm start
