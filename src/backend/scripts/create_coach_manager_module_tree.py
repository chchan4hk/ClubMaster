#!/usr/bin/env python3
"""
Create folder hierarchy for the nine Coach Manager dashboard functions.

Layout (under backend/ — shared app storage, not copied per club with data_club/Src):

  backend/coach_manager_modules/
    club_master/
      data/
      imports/
      exports/
      attachments/
    coach_information/
    student_information/
    lesson_reservation/
    prize_list/
    lesson_payment_status/
    coach_salary/
    revenue/
    upcoming_events/

Run from anywhere:

  python backend/scripts/create_coach_manager_module_tree.py

Or from the backend directory:

  python scripts/create_coach_manager_module_tree.py

Re-running is safe: only missing directories and .gitkeep files are added.
"""

from __future__ import annotations

import sys
from pathlib import Path

# backend/ (parent of scripts/)
BACKEND_ROOT = Path(__file__).resolve().parent.parent
MODULE_ROOT = BACKEND_ROOT / "coach_manager_modules"

# Slug matches coming_soon.html?m=... query keys (hyphens -> underscores)
MODULES: list[tuple[str, str]] = [
    ("club_master", "Club Master"),
    ("coach_information", "Coach Information"),
    ("student_information", "Student Information"),
    ("lesson_reservation", "Lesson Reservation"),
    ("prize_list", "Prize List"),
    ("lesson_payment_status", "Lesson Payment Status"),
    ("coach_salary", "Coach Salary"),
    ("revenue", "Revenue"),
    ("upcoming_events", "Upcoming Events"),
]

SUBDIRS = ("data", "imports", "exports", "attachments")


def touch_gitkeep(d: Path) -> None:
    marker = d / ".gitkeep"
    if not marker.exists():
        marker.write_text("", encoding="utf-8")


def main() -> int:
    if not BACKEND_ROOT.is_dir():
        print(f"ERROR: backend root not found: {BACKEND_ROOT}", file=sys.stderr)
        return 1

    created_dirs = 0

    for slug, _ in MODULES:
        mod = MODULE_ROOT / slug
        for name in SUBDIRS:
            sub = mod / name
            if not sub.is_dir():
                sub.mkdir(parents=True, exist_ok=True)
                created_dirs += 1
            touch_gitkeep(sub)

    print(f"Module root: {MODULE_ROOT}")
    print(f"Modules: {len(MODULES)}")
    print(f"New leaf directories created this run: {created_dirs}")
    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
