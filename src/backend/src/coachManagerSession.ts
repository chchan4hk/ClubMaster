import fs from "node:fs";
import type { Request } from "express";
import { findUserByUid, type CsvUser } from "./userlistCsv";
import { isMongoConfigured } from "./db/DBConnection";
import {
  findMainUserByUidMongo,
  userLoginCsvReadFallbackEnabled,
} from "./userListMongo";
import { clubDataDir, isValidClubFolderId } from "./coachListCsv";
import { findClubUidForCoachIdPreferred } from "./coachListMongo";
import {
  loadStudents,
  resolveStudentClubSession,
  studentIdsEqual,
  type StudentClubSessionResult,
} from "./studentListCsv";
import { clubInfoFirstRowObject } from "./clubInfoJson";

/**
 * Coach scoped APIs: prefer JWT `club_folder_uid` from sign-in over `findClubUidForCoachId`,
 * which uses the first `data_club/*` folder that lists this CoachID (wrong when the ID is reused across clubs).
 */
export async function resolveClubFolderUidForCoachRequest(
  req: Request,
): Promise<string | null> {
  const coachId = String(req.user?.sub ?? "").trim();
  if (!coachId) {
    return null;
  }
  const fromJwt = String(req.user?.club_folder_uid ?? "").trim();
  if (fromJwt && isValidClubFolderId(fromJwt)) {
    // Coach Mongo login sets `club_folder_uid` from userLogin `club_id` / folder; JWT is
    // server-signed — use it for PrizeList / LessonList APIs even when roster `coach_id`
    // differs from login `uid` (`sub`).
    return fromJwt;
  }
  return findClubUidForCoachIdPreferred(coachId);
}

/**
 * Student scoped APIs: prefer JWT `club_folder_uid` when it matches an ACTIVE roster row,
 * else `resolveStudentClubSession` (role-login folder / first index hit).
 */
export async function resolveStudentClubSessionFromRequest(
  req: Request,
): Promise<StudentClubSessionResult> {
  const studentId = String(req.user?.sub ?? "").trim();
  if (!studentId) {
    return { ok: false, error: "Invalid session." };
  }
  const fromJwt = String(req.user?.club_folder_uid ?? "").trim();
  if (fromJwt && isValidClubFolderId(fromJwt)) {
    const roster = await loadStudents(fromJwt);
    const stu = roster.find((s) => studentIdsEqual(s.studentId, studentId));
    if (stu && stu.status.toUpperCase() === "ACTIVE") {
      return { ok: true, clubId: fromJwt, rosterRow: stu };
    }
  }
  return resolveStudentClubSession(studentId);
}

export type CoachManagerClubContext =
  | { ok: true; clubId: string; clubName: string }
  | { ok: false; status: number; error: string };

/**
 * Coach Manager row for a club folder UID (`data_club/{uid}/` = JWT `sub`).
 * Tries `userLogin.csv` first, then Mongo `userLogin` when configured.
 */
export async function findCoachManagerUserRowForClubUid(
  clubId: string,
): Promise<CsvUser | null> {
  const id = String(clubId ?? "").trim();
  if (!id) {
    return null;
  }
  if (isMongoConfigured()) {
    try {
      const fromMongo = await findMainUserByUidMongo(id);
      if (fromMongo && fromMongo.role === "CoachManager") {
        return fromMongo;
      }
    } catch {
      /* Mongo unavailable */
    }
    if (userLoginCsvReadFallbackEnabled()) {
      const fromCsv = findUserByUid(id);
      if (fromCsv && fromCsv.role === "CoachManager") {
        return fromCsv;
      }
    }
    return null;
  }
  const fromCsv = findUserByUid(id);
  if (fromCsv && fromCsv.role === "CoachManager") {
    return fromCsv;
  }
  return null;
}

export type ClubFolderRoleContext =
  | { ok: true; clubName: string }
  | { ok: false; status: number; error: string };

/**
 * Club display + folder gate for Coach/Student scoped APIs: prefer Coach Manager row in
 * userLogin; if missing (Mongo/CSV drift) but `data_club/{clubId}/` exists, use ClubInfo.json
 * or the folder id so roster-only clubs still work.
 */
export async function resolveClubFolderRoleContextAsync(
  clubId: string,
  notFoundToken: "lesson" | "prize" | "student" | "coach" | "payment",
): Promise<ClubFolderRoleContext> {
  const id = String(clubId ?? "").trim();
  const notFoundMsg =
    notFoundToken === "lesson"
      ? "Invalid club for lesson access."
      : notFoundToken === "prize"
        ? "Invalid club for prize access."
        : notFoundToken === "student"
          ? "Invalid club for student access."
          : notFoundToken === "payment"
            ? "Invalid club for payment access."
            : "Invalid club for coach access.";
  if (!id || !isValidClubFolderId(id)) {
    return { ok: false, status: 403, error: notFoundMsg };
  }
  const row = await findCoachManagerUserRowForClubUid(id);
  if (row?.role === "CoachManager") {
    const clubName = (row.clubName && row.clubName.trim()) || "";
    if (clubName && clubName !== "—") {
      return { ok: true, clubName };
    }
  }
  const dir = clubDataDir(id);
  if (!(dir && fs.existsSync(dir))) {
    return { ok: false, status: 403, error: notFoundMsg };
  }
  try {
    const o = clubInfoFirstRowObject(id);
    const n = String(o.Club_name ?? o.club_name ?? "").trim();
    if (n && n !== "—") {
      return { ok: true, clubName: n };
    }
  } catch {
    /* ClubInfo missing or unreadable */
  }
  return { ok: true, clubName: id };
}

/** Session context for routes that require a Coach Manager folder + club name. */
export async function coachManagerClubContextAsync(
  req: Request,
): Promise<CoachManagerClubContext> {
  const clubId = String(req.user?.sub ?? "").trim();
  if (!clubId || !isValidClubFolderId(clubId)) {
    return { ok: false, status: 403, error: "Invalid club session." };
  }
  const row = await findCoachManagerUserRowForClubUid(clubId);
  if (!row) {
    return { ok: false, status: 403, error: "Coach Manager access only." };
  }
  const clubName = (row.clubName && row.clubName.trim()) || "";
  if (!clubName || clubName === "—") {
    return {
      ok: false,
      status: 400,
      error: "Your account has no club name; contact an administrator.",
    };
  }
  return { ok: true, clubId, clubName };
}
