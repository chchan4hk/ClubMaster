import { Router, type Request } from "express";
import { requireAuth, requireRole } from "../middleware/requireAuth";
import {
  coachManagerClubContextAsync,
  resolveClubFolderRoleContextAsync,
  resolveClubFolderUidForCoachRequest,
  resolveStudentClubSessionFromRequest,
} from "../coachManagerSession";
import { loadCoaches } from "../coachListCsv";
import { loadStudents } from "../studentListCsv";
import {
  decrementLessonReservedNumber,
  ensureLessonListFile,
  lessonIdsEqual,
  loadLessons,
  resolveLessonFileClubId,
  type LessonCsvRow,
} from "../lessonListCsv";
import {
  ensureLessonReserveListFile,
  loadLessonReservations,
  removeLessonReservationByReserveId,
  updateLessonReservationPaymentFields,
} from "../lessonReserveList";
import {
  addPaymentsToLedger,
  clearLedgerPaymentsForLessonReserve,
  deleteLedgerEntryForLessonReserve,
  ensureLessonPaymentLedgerFile,
  loadLessonPaymentLedger,
  sumPayments,
  getLedgerEntry,
  type LedgerReservationEntry,
} from "./lessonPaymentLedger";
import { removePaymentListRecordsForLessonReserve } from "../paymentListJson";
import {
  csvCoachFieldMatchesLoggedCoach,
  findCoachRosterRow,
} from "../coachSelfFilter";
import { isMongoConfigured } from "../db/DBConnection";
import { removeStudentFromLessonSeriesForLessonMongo } from "../lessonSeriesInfoStudentSync";

async function resolvePaymentClubContextAsync(
  req: Request,
):
  Promise<
    | { ok: true; clubId: string; clubName: string }
    | { ok: false; status: number; error: string }
  > {
  const role = String(req.user?.role ?? "");
  if (role === "CoachManager") {
    return coachManagerClubContextAsync(req);
  }
  if (role === "Coach") {
    const coachId = String(req.user?.sub ?? "").trim();
    if (!coachId) {
      return { ok: false, status: 403, error: "Invalid session." };
    }
    const clubId = resolveClubFolderUidForCoachRequest(req);
    if (!clubId) {
      return {
        ok: false,
        status: 403,
        error: "No club roster found for this coach account.",
      };
    }
    const inRoster = loadCoaches(clubId).some(
      (c) => c.coachId.trim().toUpperCase() === coachId.toUpperCase(),
    );
    if (!inRoster) {
      return { ok: false, status: 403, error: "Coach not in club roster." };
    }
    const folderCtx = await resolveClubFolderRoleContextAsync(clubId, "payment");
    if (!folderCtx.ok) {
      return folderCtx;
    }
    const clubName = folderCtx.clubName;
    if (!clubName || clubName === "—") {
      return {
        ok: false,
        status: 400,
        error: "Your club has no name configured; contact an administrator.",
      };
    }
    return { ok: true, clubId, clubName };
  }
  if (role === "Student") {
    const studentId = String(req.user?.sub ?? "").trim();
    if (!studentId) {
      return { ok: false, status: 403, error: "Invalid session." };
    }
    const session = resolveStudentClubSessionFromRequest(req);
    if (!session.ok) {
      return { ok: false, status: 403, error: session.error };
    }
    const { clubId } = session;
    const folderCtx = await resolveClubFolderRoleContextAsync(clubId, "payment");
    if (!folderCtx.ok) {
      return folderCtx;
    }
    return { ok: true, clubId, clubName: folderCtx.clubName };
  }
  return { ok: false, status: 403, error: "Forbidden" };
}

function parseClassFeeToNumber(fee: string): number {
  const n = Number.parseFloat(String(fee).replace(/[^0-9.-]/g, ""));
  return Number.isFinite(n) ? Math.round(n * 100) / 100 : 0;
}

function inferLessonKind(lesson: LessonCsvRow): string {
  const t = `${lesson.classInfo} ${lesson.sportType}`.toLowerCase();
  if (t.includes("private") || t.includes("1-1") || t.includes("1:1")) {
    return "private";
  }
  if (t.includes("tournament")) {
    return "tournament_prep";
  }
  if (t.includes("group")) {
    return "group";
  }
  return "other";
}

const ISO_DATE_ONLY = /^\d{4}-\d{2}-\d{2}$/;

/** Payment due date: 7 calendar days before lesson start (UTC calendar math). */
function dueDateSevenDaysBeforeLessonStart(lessonStartDate: string): string | null {
  const s = lessonStartDate.trim().slice(0, 10);
  if (!ISO_DATE_ONLY.test(s)) {
    return null;
  }
  const y = Number(s.slice(0, 4));
  const m = Number(s.slice(5, 7));
  const d = Number(s.slice(8, 10));
  if (!y || m < 1 || m > 12 || d < 1 || d > 31) {
    return null;
  }
  const utcMs = Date.UTC(y, m - 1, d - 7);
  return new Date(utcMs).toISOString().slice(0, 10);
}

function defaultDueDate(
  entry: LedgerReservationEntry | undefined,
  lesson: LessonCsvRow | undefined,
  reservationCreated: string,
): string {
  const start = lesson?.lessonStartDate?.trim();
  if (start) {
    const fromLessonStart = dueDateSevenDaysBeforeLessonStart(start);
    if (fromLessonStart) {
      return fromLessonStart;
    }
  }
  const fromLedger = entry?.dueDate?.trim();
  if (fromLedger) {
    return fromLedger;
  }
  const end = lesson?.lessonEndDate?.trim();
  if (end) {
    return end;
  }
  if (start) {
    return start;
  }
  return reservationCreated.trim() || new Date().toISOString().slice(0, 10);
}

function deriveDisplayStatus(
  outstanding: number,
  amountPaid: number,
  due: string,
  today: string,
): "PAID" | "PARTIALLY_PAID" | "UNPAID" | "OVERDUE" {
  if (outstanding <= 0.009) {
    return "PAID";
  }
  if (due && due < today) {
    return "OVERDUE";
  }
  if (amountPaid > 0.009) {
    return "PARTIALLY_PAID";
  }
  return "UNPAID";
}

function statusForReservationFile(
  display: "PAID" | "PARTIALLY_PAID" | "UNPAID" | "OVERDUE",
): { Payment_Status: string; Payment_Confirm: boolean } {
  return {
    Payment_Status: display,
    Payment_Confirm: display === "PAID",
  };
}

function monthBounds(yyyyMm?: string): { start: string; end: string } {
  if (yyyyMm && /^\d{4}-\d{2}$/.test(yyyyMm)) {
    const [y, m] = yyyyMm.split("-").map(Number);
    const start = `${yyyyMm}-01`;
    const last = new Date(y!, m!, 0).getDate();
    const end = `${yyyyMm}-${String(last).padStart(2, "0")}`;
    return { start, end };
  }
  const d = new Date();
  const y = d.getFullYear();
  const mo = String(d.getMonth() + 1).padStart(2, "0");
  const start = `${y}-${mo}-01`;
  const last = new Date(y, d.getMonth() + 1, 0).getDate();
  const end = `${y}-${mo}-${String(last).padStart(2, "0")}`;
  return { start, end };
}

function paymentInPeriod(paidAt: string, start: string, end: string): boolean {
  const p = paidAt.trim().slice(0, 10);
  if (!p) {
    return false;
  }
  return p >= start && p <= end;
}

export type LessonPaymentStatusRow = {
  lessonReserveId: string;
  lessonId: string;
  student_id: string;
  Student_Name: string;
  studentLevel: string;
  reservationStatus: string;
  lessonPeriodLabel: string;
  lesson_start_date: string;
  lesson_end_date: string;
  class_time: string;
  sportType: string;
  lessonKind: string;
  class_info: string;
  coachName: string;
  totalFee: number;
  amountPaid: number;
  outstanding: number;
  dueDate: string;
  displayStatus: "PAID" | "PARTIALLY_PAID" | "UNPAID" | "OVERDUE";
  /** Coach manager verified in LessonReserveList (Payment_Confirm). */
  paymentConfirmedByCoach: boolean;
  paymentHistory: {
    paymentId: string;
    amount: number;
    method: string;
    reference: string;
    paidAt: string;
  }[];
  createdAt: string;
  lastUpdatedDate: string;
};

function filterLessonPaymentRowsForCoach(
  clubId: string,
  coachJwtSub: string,
  coachUsername: string,
  rows: LessonPaymentStatusRow[],
): LessonPaymentStatusRow[] {
  const crow = findCoachRosterRow(clubId, coachJwtSub);
  if (!crow) {
    return [];
  }
  return rows.filter((x) =>
    csvCoachFieldMatchesLoggedCoach(x.coachName, crow, coachUsername),
  );
}

function recomputeKpisAndPieFromRows(
  rows: LessonPaymentStatusRow[],
  totalCollectedPeriod: number,
): {
  kpis: {
    totalCollectedPeriod: number;
    outstandingBalance: number;
    overdueAmount: number;
    collectionRate: number;
  };
  chartPie: {
    PAID: number;
    PARTIALLY_PAID: number;
    UNPAID: number;
    OVERDUE: number;
  };
} {
  let outstandingBalance = 0;
  let overdueAmount = 0;
  let totalDue = 0;
  let totalPaidAll = 0;
  const chartPie: {
    PAID: number;
    PARTIALLY_PAID: number;
    UNPAID: number;
    OVERDUE: number;
  } = {
    PAID: 0,
    PARTIALLY_PAID: 0,
    UNPAID: 0,
    OVERDUE: 0,
  };
  for (const row of rows) {
    outstandingBalance += row.outstanding;
    totalDue += row.totalFee;
    totalPaidAll += row.amountPaid;
    if (row.displayStatus === "OVERDUE") {
      overdueAmount += row.outstanding;
    }
    chartPie[row.displayStatus] += 1;
  }
  outstandingBalance = Math.round(outstandingBalance * 100) / 100;
  overdueAmount = Math.round(overdueAmount * 100) / 100;
  const collectionRate =
    totalDue > 0.009 ? Math.round((totalPaidAll / totalDue) * 1000) / 10 : 0;
  return {
    kpis: {
      totalCollectedPeriod,
      outstandingBalance,
      overdueAmount,
      collectionRate,
    },
    chartPie,
  };
}

export function buildLessonPaymentSnapshot(
  fileClub: string,
  periodMonth: string | undefined,
) {
  ensureLessonListFile(fileClub);
  ensureLessonReserveListFile(fileClub);
  ensureLessonPaymentLedgerFile(fileClub);

  const lessons = loadLessons(fileClub);
  const lessonById = new Map(
    lessons.map((l) => [l.lessonId.trim().toUpperCase(), l]),
  );
  const reservations = loadLessonReservations(fileClub);
  const ledger = loadLessonPaymentLedger(fileClub);
  const students = loadStudents(fileClub);
  const studentById = new Map(
    students.map((s) => [s.studentId.trim().toUpperCase(), s]),
  );

  const today = new Date().toISOString().slice(0, 10);
  const { start: periodStart, end: periodEnd } = monthBounds(periodMonth);

  const rows: LessonPaymentStatusRow[] = [];

  for (const r of reservations) {
    if (r.status.toUpperCase() !== "ACTIVE") {
      continue;
    }
    const lesson = lessonById.get(r.lessonId.trim().toUpperCase());
    const totalFee = lesson ? parseClassFeeToNumber(lesson.classFee) : 0;
    const entry = getLedgerEntry(ledger, r.lessonReserveId);
    const amountPaid = sumPayments(entry);
    const dueDate = defaultDueDate(entry, lesson, r.createdAt);
    const outstanding = Math.max(0, Math.round((totalFee - amountPaid) * 100) / 100);
    const displayStatus = deriveDisplayStatus(
      outstanding,
      amountPaid,
      dueDate,
      today,
    );
    const stu = studentById.get(r.student_id.trim().toUpperCase());
    const studentLevel =
      (stu?.school && stu.school.trim()) ||
      (lesson?.ageGroup && lesson.ageGroup.trim()) ||
      "—";

    rows.push({
      lessonReserveId: r.lessonReserveId,
      lessonId: r.lessonId,
      student_id: r.student_id,
      Student_Name: r.Student_Name,
      studentLevel,
      reservationStatus: r.status,
      lessonPeriodLabel: lesson
        ? `${lesson.lessonStartDate} → ${lesson.lessonEndDate}`
        : "—",
      lesson_start_date: lesson?.lessonStartDate ?? "",
      lesson_end_date: lesson?.lessonEndDate ?? "",
      class_time: lesson?.classTime ?? "",
      sportType: lesson?.sportType ?? "",
      lessonKind: lesson ? inferLessonKind(lesson) : "other",
      class_info: lesson?.classInfo ?? "",
      coachName: lesson?.coachName?.trim() || "—",
      totalFee,
      amountPaid,
      outstanding,
      dueDate,
      displayStatus,
      paymentConfirmedByCoach: r.Payment_Confirm === true,
      paymentHistory: (entry?.payments ?? []).map((p) => ({
        paymentId: p.paymentId,
        amount: p.amount,
        method: p.method,
        reference: p.reference,
        paidAt: p.paidAt,
      })),
      createdAt: r.createdAt,
      lastUpdatedDate: r.lastUpdatedDate,
    });
  }

  let totalCollectedPeriod = 0;
  for (const e of Object.values(ledger.entries)) {
    for (const p of e.payments) {
      if (paymentInPeriod(p.paidAt, periodStart, periodEnd)) {
        totalCollectedPeriod += Number.isFinite(p.amount) ? p.amount : 0;
      }
    }
  }
  totalCollectedPeriod = Math.round(totalCollectedPeriod * 100) / 100;

  let outstandingBalance = 0;
  let overdueAmount = 0;
  let totalDue = 0;
  let totalPaidAll = 0;
  for (const row of rows) {
    outstandingBalance += row.outstanding;
    totalDue += row.totalFee;
    totalPaidAll += row.amountPaid;
    if (row.displayStatus === "OVERDUE") {
      overdueAmount += row.outstanding;
    }
  }
  outstandingBalance = Math.round(outstandingBalance * 100) / 100;
  overdueAmount = Math.round(overdueAmount * 100) / 100;
  const collectionRate =
    totalDue > 0.009 ? Math.round((totalPaidAll / totalDue) * 1000) / 10 : 0;

  const sportTypes = [...new Set(rows.map((x) => x.sportType).filter(Boolean))].sort();
  const lessonKinds = ["private", "group", "tournament_prep", "other"];

  const pie = {
    PAID: 0,
    PARTIALLY_PAID: 0,
    UNPAID: 0,
    OVERDUE: 0,
  };
  for (const row of rows) {
    pie[row.displayStatus] += 1;
  }

  const barMonths: { month: string; collected: number }[] = [];
  for (let i = 5; i >= 0; i--) {
    const d = new Date();
    d.setMonth(d.getMonth() - i);
    const ym = `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}`;
    const { start, end } = monthBounds(ym);
    let collected = 0;
    for (const e of Object.values(ledger.entries)) {
      for (const p of e.payments) {
        if (paymentInPeriod(p.paidAt, start, end)) {
          collected += p.amount;
        }
      }
    }
    barMonths.push({ month: ym, collected: Math.round(collected * 100) / 100 });
  }

  const trendWeeks: { week: string; outstandingSum: number }[] = [];
  for (let w = 7; w >= 0; w--) {
    const d = new Date();
    d.setDate(d.getDate() - w * 7);
    const week = d.toISOString().slice(0, 10);
    trendWeeks.push({ week, outstandingSum: outstandingBalance });
  }

  return {
    today,
    period: { start: periodStart, end: periodEnd, month: periodMonth ?? null },
    kpis: {
      totalCollectedPeriod,
      outstandingBalance,
      overdueAmount,
      collectionRate,
    },
    chartPie: pie,
    chartBarCollections: barMonths,
    chartOutstandingTrend: trendWeeks,
    sportTypes,
    lessonKinds,
    rows,
  };
}

export function syncLessonReservationPaymentStatuses(
  fileClub: string,
  lessonReserveIds: string[],
  opts?: { preservePaymentConfirm?: boolean },
): void {
  ensureLessonReserveListFile(fileClub);
  ensureLessonPaymentLedgerFile(fileClub);
  const lessons = loadLessons(fileClub);
  const lessonById = new Map(
    lessons.map((l) => [l.lessonId.trim().toUpperCase(), l]),
  );
  const reservations = loadLessonReservations(fileClub);
  const ledger = loadLessonPaymentLedger(fileClub);
  const today = new Date().toISOString().slice(0, 10);
  const seen = new Set(lessonReserveIds.map((id) => id.trim().toUpperCase()));

  for (const r of reservations) {
    if (!seen.has(r.lessonReserveId.trim().toUpperCase())) {
      continue;
    }
    const lesson = lessonById.get(r.lessonId.trim().toUpperCase());
    const totalFee = lesson ? parseClassFeeToNumber(lesson.classFee) : 0;
    const entry = getLedgerEntry(ledger, r.lessonReserveId);
    const amountPaid = sumPayments(entry);
    const dueDate = defaultDueDate(entry, lesson, r.createdAt);
    const outstanding = Math.max(0, Math.round((totalFee - amountPaid) * 100) / 100);
    const display = deriveDisplayStatus(outstanding, amountPaid, dueDate, today);
    const f = statusForReservationFile(display);
    updateLessonReservationPaymentFields(fileClub, r.lessonReserveId, f, {
      preservePaymentConfirm: opts?.preservePaymentConfirm === true,
    });
  }
}

/**
 * Express router for Lesson Payment Status (Coach Manager / Coach).
 * Mount at `/api/coach-manager/lesson-payment-status`.
 */
export function Lesson_payment_status(): Router {
  const r = Router();
  r.use(requireAuth);

  r.get(
    "/",
    requireRole("CoachManager", "Coach", "Student"),
    async (req, res) => {
      const ctx = await resolvePaymentClubContextAsync(req);
      if (!ctx.ok) {
        res.status(ctx.status).json({ ok: false, error: ctx.error });
        return;
      }
      const qClub = String(req.query?.clubId ?? "").trim();
      if (
        qClub &&
        (req.user?.role === "CoachManager" || req.user?.role === "Coach") &&
        qClub !== ctx.clubId
      ) {
        res.status(403).json({
          ok: false,
          error: "clubId does not match your club folder.",
        });
        return;
      }
      const fileClub = resolveLessonFileClubId(ctx.clubId);
      const periodMonth = String(req.query?.periodMonth ?? "").trim() || undefined;
      try {
        const snapshot = buildLessonPaymentSnapshot(fileClub, periodMonth);
        let rows = snapshot.rows;
        let kpis = snapshot.kpis;
        let chartPie = snapshot.chartPie;
        if (req.user?.role === "Student") {
          const sid = String(req.user.sub ?? "").trim().toUpperCase();
          rows = rows.filter(
            (x) => x.student_id.trim().toUpperCase() === sid,
          );
          let outstandingBalance = 0;
          let overdueAmount = 0;
          let totalDue = 0;
          let totalPaidAll = 0;
          const pie = {
            PAID: 0,
            PARTIALLY_PAID: 0,
            UNPAID: 0,
            OVERDUE: 0,
          };
          for (const row of rows) {
            outstandingBalance += row.outstanding;
            totalDue += row.totalFee;
            totalPaidAll += row.amountPaid;
            if (row.displayStatus === "OVERDUE") {
              overdueAmount += row.outstanding;
            }
            pie[row.displayStatus] += 1;
          }
          outstandingBalance = Math.round(outstandingBalance * 100) / 100;
          overdueAmount = Math.round(overdueAmount * 100) / 100;
          const collectionRate =
            totalDue > 0.009
              ? Math.round((totalPaidAll / totalDue) * 1000) / 10
              : 0;
          kpis = {
            totalCollectedPeriod: snapshot.kpis.totalCollectedPeriod,
            outstandingBalance,
            overdueAmount,
            collectionRate,
          };
          chartPie = pie;
        }
        if (req.user?.role === "Coach") {
          rows = filterLessonPaymentRowsForCoach(
            ctx.clubId,
            String(req.user?.sub ?? ""),
            String(req.user?.username ?? ""),
            rows,
          );
          const next = recomputeKpisAndPieFromRows(
            rows,
            snapshot.kpis.totalCollectedPeriod,
          );
          kpis = next.kpis;
          chartPie = next.chartPie;
        }
        const base = "/backend";
        res.json({
          ok: true,
          clubId: ctx.clubId,
          clubName: ctx.clubName,
          lessonStorageClubId: fileClub,
          ...snapshot,
          rows,
          kpis,
          chartPie,
          integration: {
            studentInformationUrl: `${base}/student_modules/student_information/student-information.html`,
            lessonReservationUrl: `${base}/lesson_modules/lesson_reservation/lesson-reservation.html`,
            revenueModuleUrl: "/coming_soon.html?m=revenue",
          },
        });
      } catch (e) {
        const msg = e instanceof Error ? e.message : String(e);
        res.status(500).json({ ok: false, error: msg });
      }
    },
  );

  r.post(
    "/record-payment",
    requireRole("CoachManager", "Coach"),
    async (req, res) => {
      const ctx = await resolvePaymentClubContextAsync(req);
      if (!ctx.ok) {
        res.status(ctx.status).json({ ok: false, error: ctx.error });
        return;
      }
      const body = req.body as Record<string, unknown> | null;
      const splitsRaw = body?.splits;
      const splits: { lessonReserveId: string; amount: number }[] = [];
      if (Array.isArray(splitsRaw)) {
        for (const x of splitsRaw) {
          if (x && typeof x === "object") {
            const o = x as Record<string, unknown>;
            splits.push({
              lessonReserveId: String(o.lessonReserveId ?? "").trim(),
              amount: Number(o.amount),
            });
          }
        }
      } else {
        const singleId = String(body?.lessonReserveId ?? "").trim();
        const amt = Number(body?.amount);
        if (singleId && Number.isFinite(amt)) {
          splits.push({ lessonReserveId: singleId, amount: amt });
        }
      }
      const method = String(body?.method ?? "").trim();
      const reference = String(body?.reference ?? "").trim();
      const paidAt = String(body?.paidAt ?? "").trim();

      if (!splits.length) {
        res.status(400).json({ ok: false, error: "Missing payment amount or splits." });
        return;
      }

      const fileClub = resolveLessonFileClubId(ctx.clubId);
      ensureLessonReserveListFile(fileClub);
      const reservations = loadLessonReservations(fileClub);
      const lessons = loadLessons(fileClub);
      const lessonById = new Map(
        lessons.map((l) => [l.lessonId.trim().toUpperCase(), l]),
      );
      const ledger = loadLessonPaymentLedger(fileClub);
      const defaultDueDates: Record<string, string> = {};

      for (const sp of splits) {
        const resv = reservations.find(
          (x) =>
            x.lessonReserveId.trim().toUpperCase() ===
            sp.lessonReserveId.trim().toUpperCase(),
        );
        if (!resv || resv.status.toUpperCase() !== "ACTIVE") {
          res.status(400).json({
            ok: false,
            error: `Invalid or inactive reservation: ${sp.lessonReserveId}`,
          });
          return;
        }
        const lesson = lessonById.get(resv.lessonId.trim().toUpperCase());
        if (req.user?.role === "Coach") {
          const crow = findCoachRosterRow(ctx.clubId, String(req.user.sub ?? ""));
          const uname = String(req.user?.username ?? "");
          if (
            !lesson ||
            !crow ||
            !csvCoachFieldMatchesLoggedCoach(lesson.coachName, crow, uname)
          ) {
            res.status(403).json({
              ok: false,
              error: "You can only record payments for your own lessons.",
            });
            return;
          }
        }
        const entry = getLedgerEntry(ledger, resv.lessonReserveId);
        const due = defaultDueDate(entry, lesson, resv.createdAt);
        defaultDueDates[sp.lessonReserveId.trim().toUpperCase()] = due;
      }

      const added = addPaymentsToLedger(fileClub, { splits, method, reference, paidAt }, defaultDueDates);
      if (!added.ok) {
        res.status(400).json({ ok: false, error: added.error });
        return;
      }

      syncLessonReservationPaymentStatuses(
        fileClub,
        splits.map((s) => s.lessonReserveId),
      );

      res.json({
        ok: true,
        paymentIds: added.paymentIds,
        message: "Payment recorded.",
      });
    },
  );

  r.post(
    "/confirm-paid-payment",
    requireRole("CoachManager", "Coach"),
    async (req, res) => {
      const ctx = await resolvePaymentClubContextAsync(req);
      if (!ctx.ok) {
        res.status(ctx.status).json({ ok: false, error: ctx.error });
        return;
      }
      const lessonReserveId = String(
        (req.body as Record<string, unknown> | null)?.lessonReserveId ?? "",
      ).trim();
      if (!lessonReserveId) {
        res.status(400).json({ ok: false, error: "Missing lessonReserveId." });
        return;
      }
      const fileClub = resolveLessonFileClubId(ctx.clubId);
      const reservations = loadLessonReservations(fileClub);
      const resv = reservations.find(
        (x) =>
          x.lessonReserveId.trim().toUpperCase() ===
          lessonReserveId.toUpperCase(),
      );
      if (!resv || resv.status.toUpperCase() !== "ACTIVE") {
        res.status(400).json({
          ok: false,
          error: "Invalid or inactive reservation.",
        });
        return;
      }
      const snapshot = buildLessonPaymentSnapshot(fileClub, undefined);
      let searchRows = snapshot.rows;
      if (req.user?.role === "Coach") {
        searchRows = filterLessonPaymentRowsForCoach(
          ctx.clubId,
          String(req.user?.sub ?? ""),
          String(req.user?.username ?? ""),
          snapshot.rows,
        );
      }
      const row = searchRows.find(
        (x) =>
          x.lessonReserveId.trim().toUpperCase() ===
          lessonReserveId.toUpperCase(),
      );
      if (!row || row.displayStatus !== "PAID") {
        res.status(400).json({
          ok: false,
          error: "Reservation is not fully paid (status must be PAID).",
        });
        return;
      }
      const upd = updateLessonReservationPaymentFields(
        fileClub,
        lessonReserveId,
        { Payment_Status: "PAID", Payment_Confirm: true },
      );
      if (!upd.ok) {
        res.status(400).json({ ok: false, error: upd.error });
        return;
      }
      res.json({ ok: true, message: "Payment confirmed." });
    },
  );

  r.post(
    "/void-paid-payment",
    requireRole("CoachManager", "Coach"),
    async (req, res) => {
      const ctx = await resolvePaymentClubContextAsync(req);
      if (!ctx.ok) {
        res.status(ctx.status).json({ ok: false, error: ctx.error });
        return;
      }
      const lessonReserveId = String(
        (req.body as Record<string, unknown> | null)?.lessonReserveId ?? "",
      ).trim();
      if (!lessonReserveId) {
        res.status(400).json({ ok: false, error: "Missing lessonReserveId." });
        return;
      }
      const fileClub = resolveLessonFileClubId(ctx.clubId);
      const reservations = loadLessonReservations(fileClub);
      const resv = reservations.find(
        (x) =>
          x.lessonReserveId.trim().toUpperCase() ===
          lessonReserveId.toUpperCase(),
      );
      if (!resv || resv.status.toUpperCase() !== "ACTIVE") {
        res.status(400).json({
          ok: false,
          error: "Invalid or inactive reservation.",
        });
        return;
      }
      const snapshot = buildLessonPaymentSnapshot(fileClub, undefined);
      let searchRowsVoid = snapshot.rows;
      if (req.user?.role === "Coach") {
        searchRowsVoid = filterLessonPaymentRowsForCoach(
          ctx.clubId,
          String(req.user?.sub ?? ""),
          String(req.user?.username ?? ""),
          snapshot.rows,
        );
      }
      const row = searchRowsVoid.find(
        (x) =>
          x.lessonReserveId.trim().toUpperCase() ===
          lessonReserveId.toUpperCase(),
      );
      if (!row || row.displayStatus !== "PAID") {
        res.status(400).json({
          ok: false,
          error: "Reservation is not fully paid (status must be PAID).",
        });
        return;
      }
      const rmList = removePaymentListRecordsForLessonReserve(
        ctx.clubId,
        lessonReserveId,
      );
      if (!rmList.ok) {
        res.status(400).json({ ok: false, error: rmList.error });
        return;
      }
      const clr = clearLedgerPaymentsForLessonReserve(
        fileClub,
        lessonReserveId,
      );
      if (!clr.ok) {
        res.status(400).json({ ok: false, error: clr.error });
        return;
      }
      const upd = updateLessonReservationPaymentFields(
        fileClub,
        lessonReserveId,
        { Payment_Status: "UNPAID", Payment_Confirm: false },
      );
      if (!upd.ok) {
        res.status(400).json({ ok: false, error: upd.error });
        return;
      }
      res.json({
        ok: true,
        message: "Payment voided; reservation set to UNPAID.",
        removedPaymentListRows: rmList.removed,
      });
    },
  );

  r.post(
    "/cancel-lesson-reservation",
    requireRole("CoachManager", "Coach"),
    async (req, res) => {
      const ctx = await resolvePaymentClubContextAsync(req);
      if (!ctx.ok) {
        res.status(ctx.status).json({ ok: false, error: ctx.error });
        return;
      }
      const lessonReserveId = String(
        (req.body as Record<string, unknown> | null)?.lessonReserveId ?? "",
      ).trim();
      if (!lessonReserveId) {
        res.status(400).json({ ok: false, error: "Missing lessonReserveId." });
        return;
      }
      const fileClub = resolveLessonFileClubId(ctx.clubId);
      const reservations = loadLessonReservations(fileClub);
      const resv = reservations.find(
        (x) =>
          x.lessonReserveId.trim().toUpperCase() ===
          lessonReserveId.toUpperCase(),
      );
      if (!resv || resv.status.toUpperCase() !== "ACTIVE") {
        res.status(400).json({
          ok: false,
          error: "Invalid or inactive reservation.",
        });
        return;
      }
      if (resv.Payment_Confirm === true) {
        res.status(400).json({
          ok: false,
          error: "Payment is completed, the Lesson can't be cancelled.",
        });
        return;
      }
      const lessons = loadLessons(fileClub);
      if (!lessons.some((l) => lessonIdsEqual(l.lessonId, resv.lessonId))) {
        res.status(400).json({
          ok: false,
          error: "Lesson not found in LessonList.",
        });
        return;
      }
      const rmRes = removeLessonReservationByReserveId(
        fileClub,
        lessonReserveId,
      );
      if (!rmRes.ok) {
        res.status(400).json({ ok: false, error: rmRes.error });
        return;
      }
      const dec = decrementLessonReservedNumber(fileClub, resv.lessonId);
      if (!dec.ok) {
        res.status(500).json({ ok: false, error: dec.error });
        return;
      }
      const delLed = deleteLedgerEntryForLessonReserve(
        fileClub,
        lessonReserveId,
      );
      if (!delLed.ok) {
        res.status(500).json({ ok: false, error: delLed.error });
        return;
      }
      const rmPay = removePaymentListRecordsForLessonReserve(
        ctx.clubId,
        lessonReserveId,
      );
      if (!rmPay.ok) {
        res.status(400).json({ ok: false, error: rmPay.error });
        return;
      }
      let lessonSeriesInfoUpdated = 0;
      let lessonSeriesMongoError: string | undefined;
      if (isMongoConfigured()) {
        try {
          lessonSeriesInfoUpdated =
            await removeStudentFromLessonSeriesForLessonMongo({
              clubId: ctx.clubId,
              lessonCanonicalId: resv.lessonId.trim(),
              studentId: resv.student_id.trim(),
            });
        } catch (e) {
          const msg = e instanceof Error ? e.message : String(e);
          lessonSeriesMongoError = msg;
          console.warn(
            "[lesson-payment/cancel-lesson-reservation] LessonSeriesInfo:",
            msg,
          );
        }
      }
      res.json({
        ok: true,
        message: "Lesson reservation cancelled.",
        newReserved: dec.newReserved,
        removedPaymentListRows: rmPay.removed,
        lessonSeriesInfoUpdated,
        ...(lessonSeriesMongoError
          ? { lessonSeriesMongoError }
          : {}),
      });
    },
  );

  r.post(
    "/bulk-remind",
    requireRole("CoachManager", "Coach"),
    async (req, res) => {
      const ctx = await resolvePaymentClubContextAsync(req);
      if (!ctx.ok) {
        res.status(ctx.status).json({ ok: false, error: ctx.error });
        return;
      }
      const body = req.body as Record<string, unknown> | null;
      const onlyOverdue = body?.onlyOverdue === true;
      const fileClub = resolveLessonFileClubId(ctx.clubId);
      const snapshot = buildLessonPaymentSnapshot(fileClub, undefined);
      let bulkRows = snapshot.rows;
      if (req.user?.role === "Coach") {
        bulkRows = filterLessonPaymentRowsForCoach(
          ctx.clubId,
          String(req.user?.sub ?? ""),
          String(req.user?.username ?? ""),
          snapshot.rows,
        );
      }
      const targets = bulkRows.filter((row) => {
        if (row.outstanding <= 0.009) {
          return false;
        }
        if (onlyOverdue) {
          return row.displayStatus === "OVERDUE";
        }
        return row.displayStatus === "UNPAID" || row.displayStatus === "OVERDUE";
      });
      res.json({
        ok: true,
        queued: targets.length,
        channel: "stub",
        message:
          "Reminder dispatch is not wired to email/SMS yet. Hook your provider here.",
        studentIds: targets.map((t) => t.student_id),
      });
    },
  );

  r.get(
    "/export.csv",
    requireRole("CoachManager", "Coach"),
    async (req, res) => {
      const ctx = await resolvePaymentClubContextAsync(req);
      if (!ctx.ok) {
        res.status(ctx.status).json({ ok: false, error: ctx.error });
        return;
      }
      const fileClub = resolveLessonFileClubId(ctx.clubId);
      const periodMonth = String(req.query?.periodMonth ?? "").trim() || undefined;
      const snapshot = buildLessonPaymentSnapshot(fileClub, periodMonth);
      let exportRows = snapshot.rows;
      if (req.user?.role === "Coach") {
        exportRows = filterLessonPaymentRowsForCoach(
          ctx.clubId,
          String(req.user?.sub ?? ""),
          String(req.user?.username ?? ""),
          snapshot.rows,
        );
      }
      const headers = [
        "lessonReserveId",
        "lessonId",
        "student_id",
        "Student_Name",
        "studentLevel",
        "totalFee",
        "amountPaid",
        "outstanding",
        "dueDate",
        "displayStatus",
        "sportType",
        "lessonKind",
        "lesson_start_date",
        "lesson_end_date",
      ];
      const esc = (v: string) => {
        const s = String(v ?? "");
        if (/[",\n]/.test(s)) {
          return `"${s.replace(/"/g, '""')}"`;
        }
        return s;
      };
      const lines = [headers.join(",")];
      for (const row of exportRows) {
        lines.push(
          [
            row.lessonReserveId,
            row.lessonId,
            row.student_id,
            row.Student_Name,
            row.studentLevel,
            row.totalFee,
            row.amountPaid,
            row.outstanding,
            row.dueDate,
            row.displayStatus,
            row.sportType,
            row.lessonKind,
            row.lesson_start_date,
            row.lesson_end_date,
          ]
            .map((x) => esc(String(x)))
            .join(","),
        );
      }
      res.setHeader("Content-Type", "text/csv; charset=utf-8");
      res.setHeader(
        "Content-Disposition",
        'attachment; filename="lesson-payment-status.csv"',
      );
      res.send("\uFEFF" + lines.join("\n"));
    },
  );

  return r;
}
