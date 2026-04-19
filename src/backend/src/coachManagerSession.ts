import type { Request } from "express";
import { findUserByUid, type CsvUser } from "./userlistCsv";
import { isMongoConfigured } from "./db/DBConnection";
import { findMainUserByUidMongo } from "./userListMongo";
import { isValidClubFolderId } from "./coachListCsv";

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
  const fromCsv = findUserByUid(id);
  if (fromCsv && fromCsv.role === "CoachManager") {
    return fromCsv;
  }
  if (!isMongoConfigured()) {
    return null;
  }
  try {
    const fromMongo = await findMainUserByUidMongo(id);
    if (fromMongo && fromMongo.role === "CoachManager") {
      return fromMongo;
    }
  } catch {
    /* Mongo unavailable */
  }
  return null;
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
