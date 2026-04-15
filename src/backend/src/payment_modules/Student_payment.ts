import { Router } from "express";
import {
  clubAssetPublicUrl,
  loadClubInfoExtended,
  CLUB_PAYMENT_QR_JSON_KEYS,
} from "../clubInfoJson";
import { requireAuth, requireRole } from "../middleware/requireAuth";
import { loadStudents, resolveStudentClubSession } from "../studentListCsv";
import { resolveLessonFileClubId } from "../lessonListCsv";
import {
  buildLessonPaymentSnapshot,
  syncLessonReservationPaymentStatuses,
} from "./Lesson_payment_status";
import { addPaymentsToLedger } from "./lessonPaymentLedger";
import {
  appendPaymentListRecords,
  ensurePaymentListFile,
  listPaymentsForStudent,
  type PaymentListRecord,
} from "../paymentListJson";
import { loadLessonReservations } from "../lessonReserveList";

function resolveStudentPaymentClub(req: {
  user?: { sub?: string; username?: string };
}):
  | { ok: true; studentId: string; clubId: string; fileClub: string }
  | { ok: false; status: number; error: string } {
  const studentId = String(req.user?.sub ?? "").trim();
  if (!studentId) {
    return { ok: false, status: 403, error: "Invalid session." };
  }
  const session = resolveStudentClubSession(studentId);
  if (!session.ok) {
    return { ok: false, status: 403, error: session.error };
  }
  const { clubId } = session;
  const fileClub = resolveLessonFileClubId(clubId);
  return { ok: true, studentId, clubId, fileClub };
}

const METHOD_ALIASES: Record<string, string> = {
  PayMe: "PayMe",
  FPS: "FPS",
  "WeChat Pay": "WeChatPay",
  WeChatPay: "WeChatPay",
  WechatPay: "WeChatPay",
  Alipay: "Alipay",
  支付寶: "支付寶",
};

function normalizePaymentMethod(raw: string): string | null {
  const t = raw.trim();
  if (METHOD_ALIASES[t]) {
    return METHOD_ALIASES[t]!;
  }
  return null;
}

function paymentQrAndRefs(clubId: string): {
  qrUrls: Record<string, string | null>;
  referenceLines: { key: string; value: string }[];
} {
  const ext = loadClubInfoExtended(clubId);
  const str = (k: string) => {
    const v = ext[k];
    return v == null ? "" : String(v).trim();
  };
  const toUrl = (rel: string) =>
    rel && !rel.includes("..") ? clubAssetPublicUrl(clubId, rel) : null;

  const qrUrls: Record<string, string | null> = {};
  for (const [channel, jsonKey] of Object.entries(CLUB_PAYMENT_QR_JSON_KEYS)) {
    qrUrls[channel] = toUrl(str(jsonKey));
  }

  const referenceLines: { key: string; value: string }[] = [];
  for (const [key, val] of Object.entries(ext)) {
    if (key === "version") {
      continue;
    }
    const ks = key.toLowerCase();
    if (
      ks.includes("ref") ||
      ks.includes("reference") ||
      ks === "fps_id" ||
      ks === "payme_id"
    ) {
      const s = val == null ? "" : String(val).trim();
      if (s && !/\.(jpe?g|png|gif|webp)$/i.test(s) && !s.includes("/")) {
        referenceLines.push({ key, value: s });
      }
    }
  }
  return { qrUrls, referenceLines };
}

function monthBoundsNow(): { start: string; end: string } {
  const d = new Date();
  const y = d.getFullYear();
  const mo = String(d.getMonth() + 1).padStart(2, "0");
  const start = `${y}-${mo}-01`;
  const last = new Date(y, d.getMonth() + 1, 0).getDate();
  const end = `${y}-${mo}-${String(last).padStart(2, "0")}`;
  return { start, end };
}

function mergeTransactionHistory(
  lessonRows: {
    lessonReserveId: string;
    paymentHistory: {
      paymentId: string;
      amount: number;
      method: string;
      reference: string;
      paidAt: string;
    }[];
  }[],
  listRows: PaymentListRecord[],
): {
  paymentId: string;
  amount: number;
  method: string;
  reference: string;
  paidAt: string;
  lessonReserveId: string;
  source: "PaymentList" | "ledger";
  status?: string;
  receiptUrl: null;
}[] {
  const out: {
    paymentId: string;
    amount: number;
    method: string;
    reference: string;
    paidAt: string;
    lessonReserveId: string;
    source: "PaymentList" | "ledger";
    status?: string;
    receiptUrl: null;
  }[] = [];
  const seenLedger = new Set<string>();
  for (const pr of listRows) {
    const d = pr.paymentDate.slice(0, 10);
    out.push({
      paymentId: pr.paymentId,
      amount: pr.amountPaid,
      method: pr.paymentMethod,
      reference: pr.transactionRef,
      paidAt: d,
      lessonReserveId: pr.lessonReserveId,
      source: "PaymentList",
      status: pr.status,
      receiptUrl: null,
    });
    const m = (pr.notes || "").match(/ledger:([A-Za-z0-9-]+)/);
    if (m) {
      seenLedger.add(m[1]!);
    }
  }
  for (const row of lessonRows) {
    for (const p of row.paymentHistory) {
      if (seenLedger.has(p.paymentId)) {
        continue;
      }
      out.push({
        paymentId: p.paymentId,
        amount: p.amount,
        method: p.method,
        reference: p.reference,
        paidAt: p.paidAt.slice(0, 10),
        lessonReserveId: row.lessonReserveId,
        source: "ledger",
        receiptUrl: null,
      });
    }
  }
  out.sort((a, b) => (a.paidAt < b.paidAt ? 1 : a.paidAt > b.paidAt ? -1 : 0));
  return out;
}

/**
 * Student-facing payment API. Mount at `/api/student/payment`.
 */
export function Student_payment(): Router {
  const r = Router();
  r.use(requireAuth, requireRole("Student"));

  r.get("/", (_req, res) => {
    const ctx = resolveStudentPaymentClub(_req);
    if (!ctx.ok) {
      res.status(ctx.status).json({ ok: false, error: ctx.error });
      return;
    }
    try {
      ensurePaymentListFile(ctx.clubId);
      const snapshot = buildLessonPaymentSnapshot(ctx.fileClub, undefined);
      const sid = ctx.studentId.trim().toUpperCase();
      const rows = snapshot.rows.filter(
        (x) => x.StudentID.trim().toUpperCase() === sid,
      );

      let outstandingBalance = 0;
      let totalPaidAll = 0;
      let hasOverdue = false;
      let nextDue = "";
      for (const row of rows) {
        outstandingBalance += row.outstanding;
        totalPaidAll += row.amountPaid;
        if (row.displayStatus === "OVERDUE") {
          hasOverdue = true;
        }
        if (row.outstanding > 0.009) {
          const d = row.dueDate?.slice(0, 10) || "";
          if (d && (!nextDue || d < nextDue)) {
            nextDue = d;
          }
        }
      }
      outstandingBalance = Math.round(outstandingBalance * 100) / 100;

      const { start: mStart, end: mEnd } = monthBoundsNow();
      let totalPaidMonth = 0;
      for (const row of rows) {
        for (const p of row.paymentHistory) {
          const pd = p.paidAt.slice(0, 10);
          if (pd >= mStart && pd <= mEnd) {
            totalPaidMonth += p.amount;
          }
        }
      }
      totalPaidMonth = Math.round(totalPaidMonth * 100) / 100;

      const listRows = listPaymentsForStudent(ctx.clubId, ctx.studentId).filter(
        (x) => x.status.toUpperCase() === "COMPLETED",
      );

      const roster = loadStudents(ctx.clubId);
      const me = roster.find(
        (s) => s.studentId.trim().toUpperCase() === sid,
      );
      const studentProfile = me
        ? {
            studentId: me.studentId,
            fullName: me.studentName,
            level: me.school?.trim() || "—",
            coach: me.studentCoach?.trim() || "—",
            email: me.email?.trim() || "",
            phone: me.phone?.trim() || "",
          }
        : {
            studentId: ctx.studentId,
            fullName: "—",
            level: "—",
            coach: "—",
            email: "",
            phone: "",
          };

      const history = mergeTransactionHistory(rows, listRows);
      const last = history[0];
      const ext = loadClubInfoExtended(ctx.clubId);
      const clubName =
        String(ext.Club_name ?? ext.club_name ?? "").trim() || "—";
      const { qrUrls, referenceLines } = paymentQrAndRefs(ctx.clubId);

      res.json({
        ok: true,
        clubId: ctx.clubId,
        lessonStorageClubId: ctx.fileClub,
        clubName,
        studentProfile,
        outstandingBalance,
        hasOverdue,
        kpis: {
          totalPaidLifetime: Math.round(totalPaidAll * 100) / 100,
          totalPaidThisMonth: totalPaidMonth,
          outstandingBalance,
          nextPaymentDueDate: nextDue || null,
          lastPaymentAmount: last ? last.amount : null,
          lastPaymentDate: last ? last.paidAt : null,
        },
        lessons: rows.map((row) => ({
          lessonReserveId: row.lessonReserveId,
          lessonId: row.lessonId,
          lessonLabel: row.lessonPeriodLabel,
          lessonKind: row.lessonKind,
          classTime: row.class_time,
          coachName: row.coachName || "—",
          sportType: row.sportType,
          totalFee: row.totalFee,
          amountPaid: row.amountPaid,
          outstanding: row.outstanding,
          dueDate: row.dueDate,
          displayStatus: row.displayStatus,
          paymentConfirmed: row.paymentConfirmedByCoach ? "yes" : "In Progress",
          canPay: row.outstanding > 0.009,
        })),
        paymentInstructions: { qrUrls, referenceLines },
        transactions: history.slice(0, 100),
        links: {
          lessonBrowse: `/backend/lesson_modules/lesson_browse/lesson-browse.html?clubId=${encodeURIComponent(ctx.clubId)}`,
          myBookings: `/backend/lesson_modules/lesson_browse/student-my-bookings.html?clubId=${encodeURIComponent(ctx.clubId)}`,
        },
        allowedMethods: ["PayMe", "FPS", "WeChatPay", "Alipay", "支付寶"],
      });
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      res.status(500).json({ ok: false, error: msg });
    }
  });

  r.post("/confirm", (req, res) => {
    const ctx = resolveStudentPaymentClub(req);
    if (!ctx.ok) {
      res.status(ctx.status).json({ ok: false, error: ctx.error });
      return;
    }
    const body = req.body as Record<string, unknown> | null;
    const allocationsRaw = body?.allocations;
    const allocations: { lessonReserveId: string; amount: number }[] = [];
    if (Array.isArray(allocationsRaw)) {
      for (const x of allocationsRaw) {
        if (x && typeof x === "object") {
          const o = x as Record<string, unknown>;
          allocations.push({
            lessonReserveId: String(o.lessonReserveId ?? "").trim(),
            amount: Number(o.amount),
          });
        }
      }
    }
    const methodNorm = normalizePaymentMethod(String(body?.method ?? ""));
    const transactionRef = String(body?.transactionRef ?? "").trim();
    const paidAt = String(body?.paidAt ?? "").trim();
    const username = String(req.user?.username ?? "").trim();

    if (!methodNorm) {
      res.status(400).json({ ok: false, error: "Invalid payment method." });
      return;
    }
    if (!allocations.length) {
      res.status(400).json({ ok: false, error: "Add at least one payment line." });
      return;
    }

    const reservations = loadLessonReservations(ctx.fileClub);
    const sid = ctx.studentId.trim().toUpperCase();
    const snap = buildLessonPaymentSnapshot(ctx.fileClub, undefined);
    const rowByRid = new Map(
      snap.rows
        .filter((x) => x.StudentID.trim().toUpperCase() === sid)
        .map((x) => [x.lessonReserveId.trim().toUpperCase(), x]),
    );

    const defaultDueDates: Record<string, string> = {};
    const splits: { lessonReserveId: string; amount: number }[] = [];

    for (const a of allocations) {
      const rid = a.lessonReserveId.trim();
      if (!rid) {
        res.status(400).json({ ok: false, error: "Missing lesson reservation id." });
        return;
      }
      const resv = reservations.find(
        (x) => x.lessonReserveId.trim().toUpperCase() === rid.toUpperCase(),
      );
      if (
        !resv ||
        resv.status.toUpperCase() !== "ACTIVE" ||
        resv.StudentID.trim().toUpperCase() !== sid
      ) {
        res.status(400).json({
          ok: false,
          error: "Invalid lesson selection for your account.",
        });
        return;
      }
      const amt = Number(a.amount);
      if (!Number.isFinite(amt) || amt <= 0) {
        res.status(400).json({ ok: false, error: "Each amount must be positive." });
        return;
      }
      const row = rowByRid.get(rid.toUpperCase());
      if (!row) {
        res.status(400).json({ ok: false, error: "Lesson payment row not found." });
        return;
      }
      if (amt > row.outstanding + 0.02) {
        res.status(400).json({
          ok: false,
          error: `Amount exceeds outstanding for ${row.lessonId}.`,
        });
        return;
      }
      defaultDueDates[rid.toUpperCase()] = row.dueDate;
      splits.push({ lessonReserveId: rid, amount: Math.round(amt * 100) / 100 });
    }

    const ledgerResult = addPaymentsToLedger(
      ctx.fileClub,
      {
        splits,
        method: methodNorm,
        reference: transactionRef || "—",
        paidAt,
      },
      defaultDueDates,
    );
    if (!ledgerResult.ok) {
      res.status(400).json({ ok: false, error: ledgerResult.error });
      return;
    }

    const paidDay =
      paidAt.slice(0, 10) || new Date().toISOString().slice(0, 10);
    const listItems: Omit<
      PaymentListRecord,
      "paymentId" | "createdAt" | "lastUpdatedAt"
    >[] = splits.map((sp, i) => ({
      lessonReserveId: sp.lessonReserveId.trim(),
      studentId: ctx.studentId.trim(),
      clubId: ctx.clubId.trim(),
      amountPaid: sp.amount,
      paymentMethod: methodNorm,
      transactionRef: transactionRef || "—",
      paymentDate: paidDay,
      status: "COMPLETED",
      notes:
        splits.length > 1
          ? `Partial/multi (${i + 1}/${splits.length}); ledger:${ledgerResult.paymentIds[i] ?? ""}`
          : `ledger:${ledgerResult.paymentIds[i] ?? ""}`,
      recordedBy: username ? `Student:${username}` : `Student:${ctx.studentId}`,
    }));

    const listApp = appendPaymentListRecords(ctx.clubId, listItems);
    if (!listApp.ok) {
      res.status(500).json({ ok: false, error: listApp.error });
      return;
    }

    syncLessonReservationPaymentStatuses(
      ctx.fileClub,
      splits.map((s) => s.lessonReserveId),
      { preservePaymentConfirm: true },
    );

    res.json({
      ok: true,
      paymentIds: listApp.paymentIds,
      ledgerPaymentIds: ledgerResult.paymentIds,
      message: "Payment recorded. Thank you.",
    });
  });

  return r;
}
