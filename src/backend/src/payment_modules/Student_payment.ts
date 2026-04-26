import { Router, type Request } from "express";
import {
  clubAssetPublicUrl,
  loadClubInfoExtended,
  CLUB_PAYMENT_QR_JSON_KEYS,
} from "../clubInfoJson";
import { requireAuth, requireRole } from "../middleware/requireAuth";
import { resolveStudentClubSessionFromRequest } from "../coachManagerSession";
import {
  lessonReservationStudentIdsEqual,
  loadStudents,
} from "../studentListCsv";
import { resolveLessonFileClubId } from "../lessonListCsv";
import {
  buildLessonPaymentSnapshot,
  syncLessonReservationPaymentStatuses,
} from "./Lesson_payment_status";
import { addPaymentsToLedgerPreferred } from "./lessonPaymentLedgerMongo";
import {
  appendPaymentListRecords,
  ensurePaymentListFile,
  listPaymentsForStudent,
  type PaymentListRecord,
} from "../paymentListJson";
import { loadLessonReservationsPreferred } from "../lessonReserveListMongo";
import { isMongoConfigured } from "../db/DBConnection";
import { findUserLoginDocumentByUid } from "../userLoginCollectionMongo";
import { loadClubInfoContactFieldsMongo } from "../clubInfoMongo";

async function resolveStudentPaymentClub(req: Request): Promise<
  | { ok: true; studentId: string; clubId: string; fileClub: string }
  | { ok: false; status: number; error: string }
> {
  const studentId = String(req.user?.sub ?? "").trim();
  if (!studentId) {
    return { ok: false, status: 403, error: "Invalid session." };
  }
  const session = await resolveStudentClubSessionFromRequest(req);
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

function pickExtString(ext: Record<string, unknown>, keys: string[]): string {
  for (const k of keys) {
    if (!Object.prototype.hasOwnProperty.call(ext, k)) {
      continue;
    }
    const s = String(ext[k] ?? "").trim();
    if (s) {
      return s;
    }
  }
  return "";
}

function digitsOnly(s: string): string {
  return String(s ?? "").replace(/\D/g, "");
}

/**
 * Mongo `clubInfo` (when configured), then `ClubInfo.json` extras, then Coach Manager `userLogin`.
 */
async function coachManagerOutreachForClub(clubFolderId: string): Promise<{
  email: string;
  whatsappDigits: string;
}> {
  let email = "";
  let phoneRaw = "";

  if (isMongoConfigured()) {
    try {
      const mongoClub = await loadClubInfoContactFieldsMongo(clubFolderId);
      if (mongoClub) {
        const em = String(mongoClub.contact_email ?? "").trim();
        const cp = String(mongoClub.contact_point ?? "").trim();
        if (em) {
          email = em;
        }
        if (cp && digitsOnly(cp).length >= 8) {
          phoneRaw = cp;
        }
      }
    } catch {
      /* ignore */
    }
  }

  const ext = loadClubInfoExtended(clubFolderId) as Record<string, unknown>;
  if (!email) {
    email = pickExtString(ext, [
      "contact_email",
      "Contact_Email",
      "contactEmail",
      "contact_mail",
    ]);
  }
  if (!phoneRaw) {
    phoneRaw = pickExtString(ext, [
      "whatsapp",
      "WhatsApp",
      "contact_whatsapp",
      "whatsapp_number",
      "contact_number",
      "Contact_number",
      "contact_phone",
      "mobile",
      "club_mobile",
      "phone",
    ]);
  }
  const contactPoint = pickExtString(ext, [
    "contact_point",
    "Contact_Point",
    "contactPoint",
  ]);
  if (!phoneRaw && digitsOnly(contactPoint).length >= 8) {
    phoneRaw = contactPoint.trim();
  }

  if (isMongoConfigured()) {
    try {
      const doc = await findUserLoginDocumentByUid(clubFolderId.trim());
      const ut = String((doc as { usertype?: string } | null)?.usertype ?? "").trim();
      if (doc && ut === "Coach Manager") {
        const raw = doc as unknown as Record<string, unknown>;
        const e = String(raw.email_address ?? raw.email ?? "").trim();
        const p = String(
          raw.contact_number ?? raw.whatsapp ?? raw.mobile ?? raw.phone ?? "",
        ).trim();
        if (!email && e) {
          email = e;
        }
        if (!phoneRaw && p) {
          phoneRaw = p;
        }
      }
    } catch {
      /* ignore */
    }
  }

  let whatsappDigits = digitsOnly(phoneRaw);
  if (whatsappDigits.startsWith("0") && whatsappDigits.length === 10) {
    whatsappDigits = `852${whatsappDigits.slice(1)}`;
  }

  return { email: email.trim(), whatsappDigits };
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

  r.get("/", async (_req, res) => {
    const ctx = await resolveStudentPaymentClub(_req);
    if (!ctx.ok) {
      res.status(ctx.status).json({ ok: false, error: ctx.error });
      return;
    }
    try {
      await ensurePaymentListFile(ctx.clubId);
      const snapshot = await buildLessonPaymentSnapshot(ctx.fileClub, undefined);
      const rows = snapshot.rows.filter((x) =>
        lessonReservationStudentIdsEqual(
          ctx.clubId,
          x.student_id,
          ctx.studentId,
        ),
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

      const listRows = (await listPaymentsForStudent(ctx.clubId, ctx.studentId)).filter(
        (x) => x.status.toUpperCase() === "COMPLETED",
      );

      const roster = await loadStudents(ctx.clubId);
      const me = roster.find((s) =>
        lessonReservationStudentIdsEqual(
          ctx.clubId,
          s.studentId,
          ctx.studentId,
        ),
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
      const coachManagerContact = await coachManagerOutreachForClub(ctx.clubId);

      res.json({
        ok: true,
        clubId: ctx.clubId,
        lessonStorageClubId: ctx.fileClub,
        clubName,
        coachManagerContact,
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

  r.post("/confirm", async (req, res) => {
    const ctx = await resolveStudentPaymentClub(req);
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

    const reservations = await loadLessonReservationsPreferred(ctx.fileClub);
    const snap = await buildLessonPaymentSnapshot(ctx.fileClub, undefined);
    const rowByRid = new Map(
      snap.rows
        .filter((x) =>
          lessonReservationStudentIdsEqual(
            ctx.clubId,
            x.student_id,
            ctx.studentId,
          ),
        )
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
        !lessonReservationStudentIdsEqual(
          ctx.clubId,
          resv.student_id,
          ctx.studentId,
        )
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

    const ledgerResult = await addPaymentsToLedgerPreferred(
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

    const listApp = await appendPaymentListRecords(ctx.clubId, listItems);
    if (!listApp.ok) {
      res.status(500).json({ ok: false, error: listApp.error });
      return;
    }

    await syncLessonReservationPaymentStatuses(
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
