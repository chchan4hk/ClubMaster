import type { CoachCsvRow } from "./coachListCsv";
import { coachIdsEqual, coachLoginUidMatchesRosterCoachId } from "./coachListCsv";
import { isMongoConfigured } from "./db/DBConnection";
import { findUserLoginDocumentByUid } from "./userLoginCollectionMongo";
import { loadCoachesPreferred } from "./coachListMongo";

export function normCoachLabel(s: string): string {
  return String(s ?? "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ");
}

/**
 * Roster row for the signed-in coach within `clubId`.
 * JWT `sub` is usually the coach **login uid** (`userLogin.uid`), while `UserList_Coach` uses roster
 * `coach_id` (e.g. `C000004` or `{club}-C00001`). Also resolves via Mongo `userLogin.coach_id` /
 * `full_name` when ids differ.
 */
export async function findCoachRosterRow(
  clubId: string,
  coachJwtSub: string,
): Promise<CoachCsvRow | null> {
  const id = coachJwtSub.trim();
  if (!id) {
    return null;
  }
  const roster = await loadCoachesPreferred(clubId);
  const byJwt = roster.find((c) =>
    coachLoginUidMatchesRosterCoachId(clubId, c.coachId, id),
  );
  if (byJwt) {
    return byJwt;
  }
  const byPlainId = roster.find((c) => coachIdsEqual(c.coachId, id));
  if (byPlainId) {
    return byPlainId;
  }
  if (isMongoConfigured()) {
    const doc = await findUserLoginDocumentByUid(id);
    const ut = String(doc?.usertype ?? "").trim();
    if (doc && ut.toLowerCase() === "coach") {
      const rosterKey = String(doc.coach_id ?? "").trim();
      if (rosterKey) {
        const byCoachId = roster.find(
          (c) =>
            coachIdsEqual(c.coachId, rosterKey) ||
            coachLoginUidMatchesRosterCoachId(clubId, c.coachId, rosterKey),
        );
        if (byCoachId) {
          return byCoachId;
        }
      }
      const fn = String(doc.full_name ?? "").trim();
      if (fn) {
        const byName = roster.find(
          (c) => normCoachLabel(c.coachName) === normCoachLabel(fn),
        );
        if (byName) {
          return byName;
        }
      }
    }
  }
  return null;
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
