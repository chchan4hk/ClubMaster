import type { PrizeCsvRow } from "./prizeListJson";
import { isValidClubFolderId } from "./coachListCsv";
import { loadStudents, normalizeStudentIdInput } from "./studentListCsv";
import type { StudentCsvRow } from "./studentListCsv";

/**
 * Replace each prize's `studentName` with `full_name` from the club student roster
 * (MongoDB `UserList_Student` in {@link resolveUserListRosterDatabaseName}, default
 * `ClubMaster_DB`, when configured; else `UserList_Student.json`). Roster rows are
 * loaded per prize using that row's **ClubID** (`club_id` in Mongo) when it is a valid
 * folder id, otherwise the session club folder id. The prize cell must match a roster
 * `student_id` (or normalized id) or `full_name` (case-insensitive). Unmatched values
 * are left unchanged.
 */
function rosterClubFolderForPrize(
  prize: PrizeCsvRow,
  sessionClubFolderUid: string,
): string {
  const fromRow = (prize.clubId || "").replace(/^\uFEFF/, "").trim();
  if (fromRow && isValidClubFolderId(fromRow)) {
    return fromRow;
  }
  return sessionClubFolderUid.replace(/^\uFEFF/, "").trim();
}

type NameMaps = {
  byId: Map<string, string>;
  byName: Map<string, string>;
};

function buildNameMaps(roster: StudentCsvRow[]): NameMaps {
  const byId = new Map<string, string>();
  const byName = new Map<string, string>();

  for (const s of roster) {
    const displayName = (s.studentName || "").trim();
    if (!displayName) {
      continue;
    }
    const sid = s.studentId.replace(/^\uFEFF/, "").trim();
    if (sid) {
      byId.set(sid.toUpperCase(), displayName);
      const norm = normalizeStudentIdInput(sid);
      if (norm) {
        byId.set(norm.toUpperCase(), displayName);
      }
    }
    const nk = displayName.toLowerCase();
    if (!byName.has(nk)) {
      byName.set(nk, displayName);
    }
  }
  return { byId, byName };
}

function resolveDisplayName(raw: string, maps: NameMaps): string {
  const t = raw.replace(/^\uFEFF/, "").trim();
  if (!t) {
    return "";
  }
  const directId = maps.byId.get(t.toUpperCase());
  if (directId) {
    return directId;
  }
  const normPrize = normalizeStudentIdInput(t);
  if (normPrize) {
    const hit = maps.byId.get(normPrize.toUpperCase());
    if (hit) {
      return hit;
    }
  }
  const nameHit = maps.byName.get(t.toLowerCase());
  if (nameHit) {
    return nameHit;
  }
  return "";
}

export async function enrichPrizeStudentNamesFromStudentRoster(
  clubFolderUid: string,
  prizes: PrizeCsvRow[],
): Promise<PrizeCsvRow[]> {
  if (!prizes.length) {
    return prizes;
  }
  const session = clubFolderUid.replace(/^\uFEFF/, "").trim();
  const distinctClubs = new Set<string>();
  for (const p of prizes) {
    const cid = rosterClubFolderForPrize(p, session);
    if (cid && isValidClubFolderId(cid)) {
      distinctClubs.add(cid);
    }
  }
  if (!distinctClubs.size) {
    return prizes;
  }

  const mapsByClub = new Map<string, NameMaps>();
  for (const cid of distinctClubs) {
    let roster: StudentCsvRow[] = [];
    try {
      roster = await loadStudents(cid);
    } catch {
      roster = [];
    }
    mapsByClub.set(cid.toUpperCase(), buildNameMaps(roster));
  }

  return prizes.map((p) => {
    const raw = (p.studentName || "").trim();
    if (!raw) {
      return p;
    }
    const clubKey = rosterClubFolderForPrize(p, session).toUpperCase();
    const maps = mapsByClub.get(clubKey);
    if (!maps) {
      return p;
    }
    const resolved = resolveDisplayName(raw, maps);
    if (!resolved || resolved === p.studentName) {
      return p;
    }
    return { ...p, studentName: resolved };
  });
}
