import type { CoachCsvRow } from "./coachListCsv";
import { loadCoaches } from "./coachListCsv";

export function normCoachLabel(s: string): string {
  return String(s ?? "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ");
}

export function findCoachRosterRow(
  clubId: string,
  coachJwtSub: string,
): CoachCsvRow | null {
  const id = coachJwtSub.trim();
  if (!id) {
    return null;
  }
  return (
    loadCoaches(clubId).find(
      (c) => c.coachId.trim().toUpperCase() === id.toUpperCase(),
    ) ?? null
  );
}

/**
 * CSV cell (student_coach, VerifiedBy, Coach Name) matches roster full_name, CoachID, or login username.
 */
export function csvCoachFieldMatchesLoggedCoach(
  cellValue: string,
  coachRow: CoachCsvRow,
  jwtUsername: string,
): boolean {
  const n = normCoachLabel(cellValue);
  if (!n) {
    return false;
  }
  const candidates = [coachRow.coachName, coachRow.coachId, jwtUsername]
    .map(normCoachLabel)
    .filter((x) => x.length > 0);
  return candidates.includes(n);
}

function headerIdIndex(headers: string[], aliases: string[]): number {
  const norm = (h: string) =>
    h.replace(/^\uFEFF/, "").trim().toLowerCase().replace(/\s+/g, "");
  const want = new Set(aliases.map((a) => norm(a)));
  return headers.findIndex((h) => want.has(norm(h)));
}

export function filterRawRowsByIdColumn(
  raw: { headers: string[]; rows: string[][] },
  idHeaderAliases: string[],
  keepIds: Set<string>,
): { headers: string[]; rows: string[][] } {
  const idIdx = headerIdIndex(raw.headers, idHeaderAliases);
  if (idIdx < 0) {
    return { ...raw, rows: [] };
  }
  const rows = raw.rows.filter((r) =>
    keepIds.has(String(r[idIdx] ?? "").trim().toUpperCase()),
  );
  return { ...raw, rows };
}
