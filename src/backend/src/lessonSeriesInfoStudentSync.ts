/**
 * Keep MongoDB `LessonSeriesInfo.studentList` in sync when students reserve or cancel
 * (Mongo `LessonReserveList` or `LessonReserveList.json` is the source of truth for bookings).
 */
import {
  getLessonSeriesInfoCollection,
  isMongoConfigured,
} from "./db/DBConnection";
import { loadMeProfileFromUserLoginMongo } from "./userLoginCollectionMongo";
import { lessonIdsEqual, resolveLessonFileClubId } from "./lessonListCsv";
import { loadStudents } from "./studentListCsv";

export function escapeRegexClubIdSegment(s: string): string {
  return s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

/** Normalize Mongo `studentList` (string, array of strings, or missing) to a clean list. */
export function normalizeLessonSeriesStudentListToArray(raw: unknown): string[] {
  if (raw == null) {
    return [];
  }
  if (Array.isArray(raw)) {
    return raw
      .map((x) => String(x ?? "").trim())
      .filter((s) => s.length > 0);
  }
  const s = String(raw).trim();
  if (!s) {
    return [];
  }
  return s
    .split(/[\n\r,;，、]+/)
    .map((p) => p.trim())
    .filter((p) => p.length > 0);
}

/** Flat string for APIs / CSV-style payloads (coach UI, HTML tables). */
export function formatLessonSeriesStudentListForApi(raw: unknown): string {
  return normalizeLessonSeriesStudentListToArray(raw).join(", ");
}

export function lessonSeriesStudentListMatchesRoster(
  studentList: unknown,
  tokens: string[],
): boolean {
  const parts = normalizeLessonSeriesStudentListToArray(studentList);
  const hay = parts.join(",").toUpperCase();
  if (!hay || tokens.length === 0) {
    return false;
  }
  return tokens.some((t) => {
    const x = String(t).trim();
    return x.length > 0 && hay.includes(x.toUpperCase());
  });
}

export async function resolveStudentLessonSeriesMatchTokens(
  studentId: string,
  clubId: string,
): Promise<string[]> {
  const out = new Set<string>();
  const id = studentId.trim();
  if (id) {
    out.add(id);
  }
  if (isMongoConfigured()) {
    try {
      const me = await loadMeProfileFromUserLoginMongo(id, "Student");
      const fn = String(me?.studentLogin?.full_name ?? "").trim();
      if (fn && fn !== "—") {
        out.add(fn);
      }
    } catch {
      /* ignore */
    }
  }
  try {
    const fileClub = resolveLessonFileClubId(clubId);
    for (const s of await loadStudents(fileClub)) {
      if (s.studentId.trim().toUpperCase() === id.toUpperCase()) {
        const n = String(s.studentName ?? "").trim();
        if (n) {
          out.add(n);
        }
      }
    }
  } catch {
    /* ignore */
  }
  return Array.from(out).filter((x) => x.length > 0);
}

function stripStudentIdentifiersFromLessonSeriesStudentListArray(
  rosterRaw: unknown,
  tokens: string[],
): string[] {
  const cleanTokens = Array.from(
    new Set(
      tokens
        .map((t) => String(t ?? "").trim())
        .filter((t) => t.length > 0),
    ),
  ).sort((a, b) => b.length - a.length);
  if (!cleanTokens.length) {
    return normalizeLessonSeriesStudentListToArray(rosterRaw);
  }
  const parts = normalizeLessonSeriesStudentListToArray(rosterRaw);
  return parts.filter((seg) => {
    const su = seg.toUpperCase();
    return !cleanTokens.some((tu0) => {
      const tu = String(tu0).trim().toUpperCase();
      if (!tu) {
        return false;
      }
      return su.includes(tu);
    });
  });
}

/**
 * After a reservation is removed, strip this student’s id / display name from `studentList`
 * on every `LessonSeriesInfo` session row for that lesson in this club.
 */
export async function removeStudentFromLessonSeriesForLessonMongo(opts: {
  clubId: string;
  lessonCanonicalId: string;
  studentId: string;
}): Promise<number> {
  const clubNorm = opts.clubId.trim();
  const lessonCanon = opts.lessonCanonicalId.trim();
  const sid = opts.studentId.trim();
  if (!clubNorm || !lessonCanon || !sid || !isMongoConfigured()) {
    return 0;
  }
  const tokens = await resolveStudentLessonSeriesMatchTokens(sid, clubNorm);
  if (!tokens.length) {
    return 0;
  }
  const coll = await getLessonSeriesInfoCollection();
  const clubRe = new RegExp(`^${escapeRegexClubIdSegment(clubNorm)}$`, "i");
  const docs = await coll.find({ ClubID: clubRe }).toArray();
  const targets = docs.filter((d) =>
    lessonIdsEqual(String(d.lessonId ?? ""), lessonCanon),
  );
  const today = new Date().toISOString().slice(0, 10);
  let updated = 0;
  for (const d of targets) {
    if (!d._id) {
      continue;
    }
    const prevArr = normalizeLessonSeriesStudentListToArray(d.studentList);
    const nextArr = stripStudentIdentifiersFromLessonSeriesStudentListArray(
      d.studentList,
      tokens,
    );
    if (
      nextArr.length === prevArr.length &&
      nextArr.every((v, i) => v === prevArr[i])
    ) {
      continue;
    }
    await coll.updateOne(
      { _id: d._id },
      { $set: { studentList: nextArr, lastUpdatedDate: today } },
    );
    updated += 1;
  }
  return updated;
}

/**
 * After a student reserves (file row added), append their display name to `studentList` on
 * every `LessonSeriesInfo` session row for that lesson, unless they are already listed.
 */
export async function appendStudentToLessonSeriesForLessonMongo(opts: {
  clubId: string;
  lessonCanonicalId: string;
  studentId: string;
  displayName: string;
}): Promise<number> {
  const clubNorm = opts.clubId.trim();
  const lessonCanon = opts.lessonCanonicalId.trim();
  const sid = opts.studentId.trim();
  const displayRaw = String(opts.displayName ?? "").trim();
  const display = displayRaw && displayRaw !== "—" ? displayRaw : sid;
  if (!clubNorm || !lessonCanon || !sid || !display || !isMongoConfigured()) {
    return 0;
  }
  const tokens = await resolveStudentLessonSeriesMatchTokens(sid, clubNorm);
  if (!tokens.length) {
    return 0;
  }
  const coll = await getLessonSeriesInfoCollection();
  const clubRe = new RegExp(`^${escapeRegexClubIdSegment(clubNorm)}$`, "i");
  const docs = await coll.find({ ClubID: clubRe }).toArray();
  const targets = docs.filter((d) =>
    lessonIdsEqual(String(d.lessonId ?? ""), lessonCanon),
  );
  const today = new Date().toISOString().slice(0, 10);
  let updated = 0;
  for (const d of targets) {
    if (!d._id) {
      continue;
    }
    const prevArr = normalizeLessonSeriesStudentListToArray(d.studentList);
    if (lessonSeriesStudentListMatchesRoster(prevArr, tokens)) {
      continue;
    }
    const displayTrim = display.trim();
    if (
      prevArr.some(
        (e) => e.trim().toUpperCase() === displayTrim.toUpperCase(),
      )
    ) {
      continue;
    }
    const nextArr = [...prevArr, displayTrim];
    await coll.updateOne(
      { _id: d._id },
      { $set: { studentList: nextArr, lastUpdatedDate: today } },
    );
    updated += 1;
  }
  return updated;
}
