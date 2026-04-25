import { Router, type Request } from "express";
import { requireAuth, requireRole } from "../middleware/requireAuth";
import {
  resolveClubFolderRoleContextAsync,
  resolveClubFolderUidForCoachRequest,
} from "../coachManagerSession";
import {
  coachIdsEqual,
  coachLoginUidMatchesRosterCoachId,
  isValidClubFolderId,
} from "../coachListCsv";
import { findCoachRosterRow } from "../coachSelfFilter";
import { loadCoachesPreferred } from "../coachListMongo";
import { resolveLessonFileClubId } from "../lessonListCsv";
import { clubInfoFirstRowObject } from "../clubInfoJson";
import { clubCurrencyFromCountry } from "../countryCurrency";
import { buildCoachSalaryTableRows } from "../coachSalaryJson";
import {
  isCoachSalaryMongoAvailable,
  loadCoachSalaryDocumentFromMongo,
  loadCoachSalaryRowsForClubAndCoachKeysFromMongo,
  saveCoachSalaryDocumentMongo,
} from "../coachSalaryMongo";

/**
 * Coach login: salary rows from MongoDB `ClubMaster_DB.CoachManager` scoped by club folder id
 * (`ClubID`) and coach id (JWT `sub` and/or roster `coach_id`).
 */
async function coachSalaryPaymentContextAsync(
  req: Request,
): Promise<
  | {
      ok: true;
      coachId: string;
      rosterCoachId: string;
      clubId: string;
      clubName: string;
    }
  | { ok: false; status: number; error: string }
> {
  if (String(req.user?.role ?? "") !== "Coach") {
    return { ok: false, status: 403, error: "Coach access only." };
  }
  const coachId = String(req.user?.sub ?? "").trim();
  if (!coachId) {
    return { ok: false, status: 403, error: "Invalid session." };
  }
  const clubId = await resolveClubFolderUidForCoachRequest(req);
  if (!clubId || !isValidClubFolderId(clubId)) {
    return {
      ok: false,
      status: 403,
      error: "No club roster found for this coach account.",
    };
  }
  const crow = await findCoachRosterRow(clubId, coachId);
  if (!crow) {
    return { ok: false, status: 403, error: "Coach not in club roster." };
  }
  const rosterCoachId = crow.coachId.replace(/^\uFEFF/, "").trim();
  const folderCtx = await resolveClubFolderRoleContextAsync(clubId, "coach");
  if (!folderCtx.ok) {
    return { ok: false, status: folderCtx.status, error: folderCtx.error };
  }
  const clubName = folderCtx.clubName;
  return { ok: true, coachId, rosterCoachId, clubId, clubName };
}

function coachSalaryCoachKeys(ctx: {
  coachId: string;
  rosterCoachId: string;
}): string[] {
  return [
    ...new Set(
      [ctx.coachId, ctx.rosterCoachId]
        .map((s) => String(s ?? "").trim())
        .filter((s) => s.length > 0),
    ),
  ];
}

function salaryRowBelongsToCoachKeys(
  coachIdOnRow: string,
  clubId: string,
  keys: string[],
): boolean {
  const rid = String(coachIdOnRow ?? "").trim();
  if (!rid) {
    return false;
  }
  for (const k of keys) {
    if (coachIdsEqual(rid, k)) {
      return true;
    }
    if (coachLoginUidMatchesRosterCoachId(clubId, rid, k)) {
      return true;
    }
    if (coachLoginUidMatchesRosterCoachId(clubId, k, rid)) {
      return true;
    }
  }
  return false;
}

export function createCoachSalaryPaymentRouter(): Router {
  const r = Router();
  r.use(requireAuth);

  r.get("/", requireRole("Coach"), async (req, res) => {
    const ctx = await coachSalaryPaymentContextAsync(req);
    if (!ctx.ok) {
      res.status(ctx.status).json({ ok: false, error: ctx.error });
      return;
    }
    if (!isCoachSalaryMongoAvailable()) {
      res.status(503).json({
        ok: false,
        error:
          "Coach salary requires MongoDB (collection ClubMaster_DB.CoachManager). Configure MONGODB_URI / MONGO_URI.",
      });
      return;
    }
    const fileClub = resolveLessonFileClubId(ctx.clubId);
    const rosterCoaches = await loadCoachesPreferred(fileClub);
    const keys = coachSalaryCoachKeys(ctx);
    const salaryDoc = await loadCoachSalaryRowsForClubAndCoachKeysFromMongo(
      ctx.clubId,
      keys,
    );
    let rows: Awaited<ReturnType<typeof buildCoachSalaryTableRows>> = [];
    try {
      rows = await buildCoachSalaryTableRows(fileClub, {
        rosterCoaches,
        salaryDoc,
      });
    } catch {
      rows = [];
    }
    let clubCountry = "";
    try {
      const fields = clubInfoFirstRowObject(fileClub);
      clubCountry = String(fields.country ?? "").trim();
    } catch {
      clubCountry = "";
    }
    const { currencyCode, currencySymbol } =
      clubCurrencyFromCountry(clubCountry);
    res.json({
      ok: true,
      coachId: ctx.coachId,
      clubId: ctx.clubId,
      clubName: ctx.clubName,
      lessonStorageClubId: fileClub,
      clubCountry,
      currencyCode,
      currencySymbol,
      salaryStorage: "mongodb",
      rows,
    });
  });

  /**
   * Coach confirms receipt for their own salary row only (sets Payment_Confirm = true).
   */
  r.post("/confirm", requireRole("Coach"), async (req, res) => {
    const ctx = await coachSalaryPaymentContextAsync(req);
    if (!ctx.ok) {
      res.status(ctx.status).json({ ok: false, error: ctx.error });
      return;
    }
    if (!isCoachSalaryMongoAvailable()) {
      res.status(503).json({
        ok: false,
        error:
          "Coach salary requires MongoDB (collection ClubMaster_DB.CoachManager). Configure MONGODB_URI / MONGO_URI.",
      });
      return;
    }
    const body = req.body as Record<string, unknown> | null;
    const sid = String(body?.CoachSalaryID ?? body?.coachSalaryId ?? "").trim();
    if (!sid) {
      res.status(400).json({ ok: false, error: "CoachSalaryID is required." });
      return;
    }
    const now = new Date().toISOString();
    const keys = coachSalaryCoachKeys(ctx);
    try {
      const doc = await loadCoachSalaryDocumentFromMongo(ctx.clubId);
      let found = false;
      for (let i = 0; i < doc.coachSalaries.length; i++) {
        const row = doc.coachSalaries[i]!;
        if (row.CoachSalaryID.trim() !== sid) {
          continue;
        }
        if (
          !salaryRowBelongsToCoachKeys(row.coach_id, ctx.clubId, keys)
        ) {
          res.status(403).json({
            ok: false,
            error: "This salary record does not belong to your coach account.",
          });
          return;
        }
        row.Payment_Confirm = true;
        row.lastUpdatedDate = now;
        doc.coachSalaries[i] = row;
        found = true;
        break;
      }
      if (!found) {
        res.status(404).json({ ok: false, error: "Salary record not found." });
        return;
      }
      await saveCoachSalaryDocumentMongo(ctx.clubId, doc);
      res.json({ ok: true, CoachSalaryID: sid, lastUpdatedDate: now });
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      res.status(400).json({ ok: false, error: msg });
    }
  });

  return r;
}
