import { Router, type Request } from "express";
import { requireAuth, requireRole } from "../middleware/requireAuth";
import { coachManagerClubContextAsync } from "../coachManagerSession";
import { resolveLessonFileClubId } from "../lessonListCsv";
import { clubInfoFirstRowObject } from "../clubInfoJson";
import { clubCurrencyFromCountry } from "../countryCurrency";
import {
  applyLessonFeeAllocations,
  buildCoachSalaryTableRows,
  buildLessonFeeAllocationRows,
  loadCoachSalaryDocument,
  normalizeCoachSalaryPaymentMethod,
  normalizeCoachSalaryPaymentStatus,
  saveCoachSalaryDocument,
} from "../coachSalaryJson";

/**
 * Coach Manager: read/update CoachSalary.json under data_club.
 * Mounted at `/api/coach-manager/lessons/coach-salary-data` (see coachManagerLessonRoutes).
 */
export function createCoachManagerSalaryRouter(): Router {
  const r = Router();
  r.use(requireAuth);

  r.get("/", requireRole("CoachManager"), async (req, res) => {
    const ctx = await coachManagerClubContextAsync(req);
    if (!ctx.ok) {
      res.status(ctx.status).json({ ok: false, error: ctx.error });
      return;
    }
    const qClub = String(req.query?.clubId ?? "").trim();
    if (qClub && qClub !== ctx.clubId) {
      res.status(403).json({
        ok: false,
        error: "clubId does not match your club folder.",
      });
      return;
    }
    const fileClub = resolveLessonFileClubId(ctx.clubId);
    try {
      let rows: ReturnType<typeof buildCoachSalaryTableRows> = [];
      try {
        rows = buildCoachSalaryTableRows(fileClub);
      } catch {
        rows = [];
      }
      const allocationRows = buildLessonFeeAllocationRows(fileClub);
      let clubCountry = "";
      try {
        const fields = clubInfoFirstRowObject(fileClub);
        clubCountry = String(fields.country ?? "").trim();
      } catch {
        clubCountry = "";
      }
      const { currencyCode, currencySymbol } =
        clubCurrencyFromCountry(clubCountry);
      const base = "/backend";
      res.json({
        ok: true,
        clubId: ctx.clubId,
        clubName: ctx.clubName,
        lessonStorageClubId: fileClub,
        clubCountry,
        currencyCode,
        currencySymbol,
        rows,
        allocationRows,
        integration: {
          coachInformationUrl: `${base}/coach_manager_modules/coach_information/coach-information.html`,
          lessonReservationUrl: `${base}/lesson_modules/lesson_reservation/lesson-reservation.html`,
          lessonPaymentStatusUrl: `${base}/payment_modules/lesson_payment_status/lesson-payment-status.html`,
        },
      });
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      res.status(500).json({ ok: false, error: msg });
    }
  });

  r.post("/fee-allocation", requireRole("CoachManager"), async (req, res) => {
    const ctx = await coachManagerClubContextAsync(req);
    if (!ctx.ok) {
      res.status(ctx.status).json({ ok: false, error: ctx.error });
      return;
    }
    const fileClub = resolveLessonFileClubId(ctx.clubId);
    const body = req.body as Record<string, unknown> | null;
    const items: { lessonId: string; feeAllocation: number }[] = [];
    const lessonsRaw = body?.lessons;
    if (Array.isArray(lessonsRaw)) {
      for (const x of lessonsRaw) {
        if (x && typeof x === "object") {
          const o = x as Record<string, unknown>;
          const lid = String(o.lessonId ?? "").trim();
          const fa = Number(o.feeAllocation ?? o.fee_allocation);
          if (lid && Number.isFinite(fa)) {
            items.push({ lessonId: lid, feeAllocation: fa });
          }
        }
      }
    } else if (body) {
      const lid = String(body.lessonId ?? "").trim();
      const fa = Number(body.feeAllocation ?? body.fee_allocation);
      if (lid && Number.isFinite(fa)) {
        items.push({ lessonId: lid, feeAllocation: fa });
      }
    }
    if (!items.length) {
      res.status(400).json({
        ok: false,
        error: "Missing lessonId or feeAllocation (send { lessonId, feeAllocation } or { lessons: [...] }).",
      });
      return;
    }
    try {
      const summary = applyLessonFeeAllocations(
        fileClub,
        ctx.clubId,
        ctx.clubName,
        items,
      );
      const allocationRows = buildLessonFeeAllocationRows(fileClub);
      const rows = buildCoachSalaryTableRows(fileClub);
      res.json({
        ok: true,
        ...summary,
        allocationRows,
        rows,
      });
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      res.status(400).json({ ok: false, error: msg });
    }
  });

  r.post("/update", requireRole("CoachManager"), async (req, res) => {
    const ctx = await coachManagerClubContextAsync(req);
    if (!ctx.ok) {
      res.status(ctx.status).json({ ok: false, error: ctx.error });
      return;
    }
    const fileClub = resolveLessonFileClubId(ctx.clubId);
    const body = req.body as Record<string, unknown> | null;
    const updatesRaw = body?.updates;
    const patches: Record<string, unknown>[] = [];
    if (Array.isArray(updatesRaw)) {
      for (const x of updatesRaw) {
        if (x && typeof x === "object") {
          patches.push(x as Record<string, unknown>);
        }
      }
    } else if (body && typeof body === "object") {
      patches.push(body);
    }

    if (!patches.length) {
      res.status(400).json({ ok: false, error: "Missing update body." });
      return;
    }

    const now = new Date().toISOString();

    try {
      const doc = loadCoachSalaryDocument(fileClub);
      const results: {
        success: boolean;
        CoachSalaryID: string;
        error?: string;
        lastUpdatedDate?: string;
      }[] = [];

      for (const patch of patches) {
        const sid = String(patch.CoachSalaryID ?? "").trim();
        if (!sid) {
          results.push({
            success: false,
            CoachSalaryID: "",
            error: "CoachSalaryID is required.",
          });
          continue;
        }
        const method = normalizeCoachSalaryPaymentMethod(
          String(patch.Payment_Method ?? ""),
        );
        const status = normalizeCoachSalaryPaymentStatus(
          String(patch.Payment_Status ?? ""),
        );
        if (!method) {
          results.push({
            success: false,
            CoachSalaryID: sid,
            error: "Invalid Payment_Method.",
          });
          continue;
        }
        if (!status) {
          results.push({
            success: false,
            CoachSalaryID: sid,
            error: "Invalid Payment_Status.",
          });
          continue;
        }
        const confirm =
          typeof patch.Payment_Confirm === "boolean"
            ? patch.Payment_Confirm
            : String(patch.Payment_Confirm ?? "").toLowerCase() === "true" ||
              patch.Payment_Confirm === 1;

        let found = false;
        for (let i = 0; i < doc.coachSalaries.length; i++) {
          const row = doc.coachSalaries[i]!;
          if (row.CoachSalaryID.trim() !== sid) {
            continue;
          }
          found = true;
          row.Payment_Method = method;
          row.Payment_Status = status;
          row.Payment_Confirm = confirm;
          row.lastUpdatedDate = now;
          doc.coachSalaries[i] = row;
          break;
        }
        if (!found) {
          results.push({
            success: false,
            CoachSalaryID: sid,
            error: "CoachSalaryID not found.",
          });
          continue;
        }
        results.push({
          success: true,
          CoachSalaryID: sid,
          lastUpdatedDate: now,
        });
      }

      const allOk = results.every((x) => x.success);
      if (!allOk) {
        res.status(400).json({
          ok: false,
          error: results.find((x) => !x.success)?.error ?? "Update failed.",
          results,
        });
        return;
      }

      saveCoachSalaryDocument(fileClub, doc);

      if (results.length === 1 && !Array.isArray(updatesRaw)) {
        const one = results[0]!;
        res.json({
          ok: true,
          CoachSalaryID: one.CoachSalaryID,
          lastUpdatedDate: one.lastUpdatedDate,
        });
        return;
      }

      res.json({ ok: true, results });
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      res.status(500).json({ ok: false, error: msg });
    }
  });

  return r;
}
