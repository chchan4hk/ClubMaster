import { Router, type Request } from "express";
import { requireAuth, requireRole } from "../middleware/requireAuth";
import { findCoachManagerUserRowForClubUid } from "../coachManagerSession";
import {
  findClubUidForCoachId,
  isValidClubFolderId,
  loadCoaches,
} from "../coachListCsv";
import { resolveLessonFileClubId } from "../lessonListCsv";
import { clubInfoFirstRowObject } from "../clubInfoJson";
import { clubCurrencyFromCountry } from "../countryCurrency";
import {
  buildCoachSalaryTableRows,
  loadCoachSalaryDocument,
  saveCoachSalaryDocument,
} from "../coachSalaryJson";

/**
 * Coach login (JWT sub = CoachID): read-only salary rows from CoachSalary.json for that coach only.
 */
async function coachSalaryPaymentContextAsync(
  req: Request,
):
  Promise<
    | { ok: true; coachId: string; clubId: string; clubName: string }
    | { ok: false; status: number; error: string }
  > {
  if (String(req.user?.role ?? "") !== "Coach") {
    return { ok: false, status: 403, error: "Coach access only." };
  }
  const coachId = String(req.user?.sub ?? "").trim();
  if (!coachId) {
    return { ok: false, status: 403, error: "Invalid session." };
  }
  const clubId = findClubUidForCoachId(coachId);
  if (!clubId || !isValidClubFolderId(clubId)) {
    return {
      ok: false,
      status: 403,
      error: "No club roster found for this coach account.",
    };
  }
  const managerRow = await findCoachManagerUserRowForClubUid(clubId);
  if (!managerRow || managerRow.role !== "CoachManager") {
    return { ok: false, status: 403, error: "Club folder is not available." };
  }
  const inRoster = loadCoaches(clubId).some(
    (c) =>
      c.coachId.replace(/^\uFEFF/, "").trim().toUpperCase() ===
      coachId.toUpperCase(),
  );
  if (!inRoster) {
    return { ok: false, status: 403, error: "Coach not in club roster." };
  }
  const clubName = (managerRow.clubName && managerRow.clubName.trim()) || "";
  return { ok: true, coachId, clubId, clubName };
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
    const fileClub = resolveLessonFileClubId(ctx.clubId);
    let rows: ReturnType<typeof buildCoachSalaryTableRows> = [];
    try {
      rows = buildCoachSalaryTableRows(fileClub, {
        onlyCoachId: ctx.coachId,
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
    const body = req.body as Record<string, unknown> | null;
    const sid = String(body?.CoachSalaryID ?? body?.coachSalaryId ?? "").trim();
    if (!sid) {
      res.status(400).json({ ok: false, error: "CoachSalaryID is required." });
      return;
    }
    const fileClub = resolveLessonFileClubId(ctx.clubId);
    const now = new Date().toISOString();
    try {
      const doc = loadCoachSalaryDocument(fileClub);
      let found = false;
      for (let i = 0; i < doc.coachSalaries.length; i++) {
        const row = doc.coachSalaries[i]!;
        if (row.CoachSalaryID.trim() !== sid) {
          continue;
        }
        if (
          row.coach_id.trim().toUpperCase() !== ctx.coachId.toUpperCase()
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
      saveCoachSalaryDocument(fileClub, doc);
      res.json({ ok: true, CoachSalaryID: sid, lastUpdatedDate: now });
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      res.status(400).json({ ok: false, error: msg });
    }
  });

  return r;
}
