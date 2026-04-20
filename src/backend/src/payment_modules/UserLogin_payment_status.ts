import fs from "fs";
import path from "path";
import { Router } from "express";
import { requireAuth, requireRole } from "../middleware/requireAuth";
import {
  loadCoachRoleLogins,
  loadStudentRoleLogins,
  setRoleLoginExpiryByUid,
  setRoleLoginExpiryForClubFolderUid,
} from "../coachStudentLoginCsv";
import { findUserByUid, setMainUserlistExpiryByUid } from "../userlistCsv";

const paymentListPath = path.join(
  __dirname,
  "..",
  "..",
  "data",
  "admin",
  "PaymentList_UserLogin.json",
);

export type UserLoginPaymentFileRow = {
  paymentId: string;
  uid: string;
  username: string;
  full_name: string;
  Expiry_date: string;
  subscription_option: string;
  subscription_amount: number;
  paymentMethod: string;
  transactionRef: string;
  paymentDate: string;
  status: string;
  createdAt: string;
  confirm_payment_date: string;
};

type PaymentListFile = {
  version: number;
  payments: UserLoginPaymentFileRow[];
};

function readPaymentFile(): PaymentListFile {
  const raw = fs.readFileSync(paymentListPath, "utf8");
  const parsed = JSON.parse(raw) as PaymentListFile;
  if (!parsed || !Array.isArray(parsed.payments)) {
    return { version: 1, payments: [] };
  }
  return {
    version: typeof parsed.version === "number" ? parsed.version : 1,
    payments: parsed.payments,
  };
}

function writePaymentFile(body: PaymentListFile): void {
  fs.writeFileSync(
    paymentListPath,
    JSON.stringify(body, null, 2) + "\n",
    "utf8",
  );
}

function normStatus(s: string): string {
  return String(s ?? "")
    .trim()
    .toUpperCase();
}

function isPlaceholderRow(p: UserLoginPaymentFileRow): boolean {
  return (
    !String(p.paymentId ?? "").trim() &&
    !String(p.uid ?? "").trim() &&
    !String(p.username ?? "").trim()
  );
}

const ISO_YMD = /^\d{4}-\d{2}-\d{2}$/;

function subscriptionOptionToMonths(opt: string): number | null {
  const s = String(opt ?? "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ");
  if (!s) {
    return null;
  }
  if (s === "1 month" || s === "1m") {
    return 1;
  }
  if (s === "3 months" || s === "3m") {
    return 3;
  }
  if (
    s === "1 year" ||
    s === "12 months" ||
    s === "1y" ||
    s === "one year"
  ) {
    return 12;
  }
  const mo = /^(\d+)\s*months?$/.exec(s);
  if (mo) {
    const n = parseInt(mo[1], 10);
    return n > 0 && n <= 120 ? n : null;
  }
  const yr = /^(\d+)\s*years?$/.exec(s);
  if (yr) {
    const n = parseInt(yr[1], 10);
    return n > 0 && n <= 20 ? n * 12 : null;
  }
  return null;
}

function ymdToParts(s: string): { y: number; m: number; d: number } | null {
  if (!ISO_YMD.test(s)) {
    return null;
  }
  const y = Number(s.slice(0, 4));
  const m = Number(s.slice(5, 7));
  const d = Number(s.slice(8, 10));
  if (!y || m < 1 || m > 12 || d < 1 || d > 31) {
    return null;
  }
  return { y, m, d };
}

function addCalendarMonthsYmd(ymd: string, addMonths: number): string {
  const p = ymdToParts(ymd);
  if (!p) {
    return new Date().toISOString().slice(0, 10);
  }
  let m0 = p.m - 1 + addMonths;
  const y = p.y + Math.floor(m0 / 12);
  m0 = ((m0 % 12) + 12) % 12;
  const lastDay = new Date(y, m0 + 1, 0).getDate();
  const d = Math.min(p.d, lastDay);
  return `${y}-${String(m0 + 1).padStart(2, "0")}-${String(d).padStart(2, "0")}`;
}

/** Extend from max(current expiry, today) by `monthsToAdd` calendar months. */
function computeNewExpiryFromSubscription(
  currentExpiryYmd: string,
  monthsToAdd: number,
): string {
  const today = new Date().toISOString().slice(0, 10);
  let base = today;
  const cur = String(currentExpiryYmd ?? "").trim();
  if (ISO_YMD.test(cur)) {
    base = cur >= today ? cur : today;
  }
  return addCalendarMonthsYmd(base, monthsToAdd);
}

function resolveCurrentExpiryForUid(uid: string): string {
  const key = String(uid).trim().toUpperCase();
  const main = findUserByUid(uid);
  if (main) {
    return String(main.expiryDate ?? "").trim();
  }
  const coach = loadCoachRoleLogins().find(
    (r) =>
      r.uid.trim().toUpperCase() === key ||
      String(r.coachId ?? "").trim().toUpperCase() === key,
  );
  if (coach) {
    return String(coach.expiryDate ?? "").trim();
  }
  const stu = loadStudentRoleLogins().find(
    (r) =>
      r.uid.trim().toUpperCase() === key ||
      String(r.studentId ?? "").trim().toUpperCase() === key,
  );
  if (stu) {
    return String(stu.expiryDate ?? "").trim();
  }
  return "";
}

function applySubscriptionExpiryOnConfirm(
  uid: string,
  subscriptionOption: string,
):
  | { ok: true; newExpiry: string; updates: string[] }
  | { ok: false; error: string } {
  const uidTrim = String(uid ?? "").trim();
  if (!uidTrim) {
    return { ok: false, error: "Payment row has no uid." };
  }
  const months = subscriptionOptionToMonths(subscriptionOption);
  if (months == null) {
    return {
      ok: false,
      error:
        'Unknown subscription_option. Use "1 month", "3 months", or "1 year" (or similar).',
    };
  }
  const current = resolveCurrentExpiryForUid(uidTrim);
  const newExpiry = computeNewExpiryFromSubscription(current, months);
  const updates: string[] = [];

  if (findUserByUid(uidTrim)) {
    const m = setMainUserlistExpiryByUid(uidTrim, newExpiry);
    if (!m.ok) {
      return { ok: false, error: m.error };
    }
    updates.push(`userLogin.csv (${uidTrim})`);
  }

  const c = setRoleLoginExpiryByUid(uidTrim, "Coach", newExpiry);
  if (c.ok) {
    updates.push(`userLogin_Coach.csv (login ${uidTrim})`);
  } else if (!/no coach login found/i.test(c.error)) {
    return { ok: false, error: c.error };
  }

  const s = setRoleLoginExpiryByUid(uidTrim, "Student", newExpiry);
  if (s.ok) {
    updates.push(`userLogin_Student.csv (login ${uidTrim})`);
  } else if (!/no student login found/i.test(s.error)) {
    return { ok: false, error: s.error };
  }

  const club = setRoleLoginExpiryForClubFolderUid(uidTrim, newExpiry);
  if (!club.ok) {
    return { ok: false, error: club.error };
  }
  if (club.coachUpdated > 0) {
    updates.push(
      `userLogin_Coach.csv (club_id=${uidTrim}, ${club.coachUpdated} row(s))`,
    );
  }
  if (club.studentUpdated > 0) {
    updates.push(
      `userLogin_Student.csv (club_id=${uidTrim}, ${club.studentUpdated} row(s))`,
    );
  }

  return { ok: true, newExpiry, updates };
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

function dateInPeriod(isoOrDate: string, start: string, end: string): boolean {
  const p = String(isoOrDate ?? "")
    .trim()
    .slice(0, 10);
  if (!p || !/^\d{4}-\d{2}-\d{2}$/.test(p)) {
    return false;
  }
  return p >= start && p <= end;
}

export function buildUserLoginPaymentSnapshot(periodMonth?: string) {
  const file = readPaymentFile();
  const rows = file.payments.filter((p) => !isPlaceholderRow(p));

  const { start: periodStart, end: periodEnd } = monthBounds(periodMonth);
  const today = new Date().toISOString().slice(0, 10);

  let pendingCount = 0;
  let completedCount = 0;
  let pendingAmount = 0;
  let completedAmountPeriod = 0;

  const statusPie = { PENDING: 0, COMPLETED: 0, OTHER: 0 };

  for (const r of rows) {
    const st = normStatus(r.status);
    const amt = Number(r.subscription_amount);
    const safeAmt = Number.isFinite(amt) && amt >= 0 ? amt : 0;

    if (st === "COMPLETED" || st === "PAID" || st === "CONFIRMED") {
      completedCount++;
      statusPie.COMPLETED++;
      const conf = String(r.confirm_payment_date ?? "")
        .trim()
        .slice(0, 10);
      const paidLike = conf || String(r.paymentDate ?? "").trim().slice(0, 10);
      if (dateInPeriod(paidLike, periodStart, periodEnd)) {
        completedAmountPeriod += safeAmt;
      }
    } else if (st === "PENDING") {
      pendingCount++;
      pendingAmount += safeAmt;
      statusPie.PENDING++;
    } else {
      statusPie.OTHER++;
    }
  }

  pendingAmount = Math.round(pendingAmount * 100) / 100;
  completedAmountPeriod = Math.round(completedAmountPeriod * 100) / 100;

  const barMonths: { month: string; amount: number }[] = [];
  for (let i = 5; i >= 0; i--) {
    const d = new Date();
    d.setMonth(d.getMonth() - i);
    const ym = `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}`;
    const { start, end } = monthBounds(ym);
    let sum = 0;
    for (const r of rows) {
      const st = normStatus(r.status);
      if (st !== "COMPLETED" && st !== "PAID" && st !== "CONFIRMED") {
        continue;
      }
      const conf = String(r.confirm_payment_date ?? "")
        .trim()
        .slice(0, 10);
      const paidLike = conf || String(r.paymentDate ?? "").trim().slice(0, 10);
      if (dateInPeriod(paidLike, start, end)) {
        const amt = Number(r.subscription_amount);
        sum += Number.isFinite(amt) ? amt : 0;
      }
    }
    barMonths.push({ month: ym, amount: Math.round(sum * 100) / 100 });
  }

  const sorted = [...rows].sort((a, b) => {
    const ca = String(a.createdAt ?? "");
    const cb = String(b.createdAt ?? "");
    return cb.localeCompare(ca);
  });

  return {
    today,
    period: { start: periodStart, end: periodEnd, month: periodMonth ?? null },
    kpis: {
      totalRows: rows.length,
      pendingCount,
      completedCount,
      pendingAmount,
      completedAmountPeriod,
    },
    chartPie: statusPie,
    chartBarConfirmed: barMonths,
    rows: sorted,
    sourceFile: "backend/data/admin/PaymentList_UserLogin.json",
  };
}

/**
 * Admin — User Account Subscription payment list (PaymentList_UserLogin.json).
 * Mount at `/api/admin/userlogin-payment-status`.
 */
export function createUserLoginPaymentStatusRouter(): Router {
  const r = Router();
  r.use(requireAuth);

  r.get("/", requireRole("Admin"), (req, res) => {
    const periodMonth =
      String(req.query?.periodMonth ?? "").trim() || undefined;
    try {
      const snapshot = buildUserLoginPaymentSnapshot(periodMonth);
      res.json({
        ok: true,
        ...snapshot,
        integration: {
          userAccountManagementUrl: "/admin-user-account-management.html",
          adminHomeUrl: "/admin.html",
        },
      });
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      res.status(500).json({ ok: false, error: msg });
    }
  });

  r.post("/confirm-payment", requireRole("Admin"), (req, res) => {
    const paymentId = String(
      (req.body as Record<string, unknown> | null)?.paymentId ?? "",
    ).trim();
    if (!paymentId) {
      res.status(400).json({ ok: false, error: "Missing paymentId." });
      return;
    }
    try {
      const file = readPaymentFile();
      const idx = file.payments.findIndex(
        (p) =>
          String(p.paymentId ?? "").trim().toUpperCase() ===
          paymentId.toUpperCase(),
      );
      if (idx < 0) {
        res.status(404).json({ ok: false, error: "Payment row not found." });
        return;
      }
      const row = file.payments[idx];
      if (isPlaceholderRow(row)) {
        res.status(400).json({ ok: false, error: "Invalid row." });
        return;
      }
      const st = normStatus(row.status);
      if (st !== "PENDING") {
        res.status(400).json({
          ok: false,
          error: "Only PENDING subscription payments can be confirmed.",
        });
        return;
      }
      const exp = applySubscriptionExpiryOnConfirm(
        row.uid,
        row.subscription_option,
      );
      if (!exp.ok) {
        res.status(400).json({ ok: false, error: exp.error });
        return;
      }
      const today = new Date().toISOString().slice(0, 10);
      file.payments[idx] = {
        ...row,
        status: "COMPLETED",
        confirm_payment_date: today,
        Expiry_date: exp.newExpiry,
      };
      writePaymentFile(file);
      res.json({
        ok: true,
        message: "Subscription payment confirmed.",
        newExpiry: exp.newExpiry,
        expiryUpdates: exp.updates,
      });
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      res.status(500).json({ ok: false, error: msg });
    }
  });

  r.get("/export.csv", requireRole("Admin"), (req, res) => {
    const periodMonth =
      String(req.query?.periodMonth ?? "").trim() || undefined;
    try {
      const snapshot = buildUserLoginPaymentSnapshot(periodMonth);
      const headers = [
        "paymentId",
        "uid",
        "username",
        "full_name",
        "subscription_option",
        "subscription_amount",
        "paymentMethod",
        "transactionRef",
        "paymentDate",
        "status",
        "createdAt",
        "confirm_payment_date",
        "Expiry_date",
      ];
      const esc = (v: string) => {
        const s = String(v ?? "");
        if (/[",\n]/.test(s)) {
          return `"${s.replace(/"/g, '""')}"`;
        }
        return s;
      };
      const lines = [headers.join(",")];
      for (const row of snapshot.rows) {
        lines.push(
          [
            row.paymentId,
            row.uid,
            row.username,
            row.full_name,
            row.subscription_option,
            row.subscription_amount,
            row.paymentMethod,
            row.transactionRef,
            row.paymentDate,
            row.status,
            row.createdAt,
            row.confirm_payment_date,
            row.Expiry_date,
          ]
            .map((x) => esc(String(x)))
            .join(","),
        );
      }
      res.setHeader("Content-Type", "text/csv; charset=utf-8");
      res.setHeader(
        "Content-Disposition",
        'attachment; filename="user-account-subscription-payments.csv"',
      );
      res.send("\uFEFF" + lines.join("\n"));
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      res.status(500).json({ ok: false, error: msg });
    }
  });

  return r;
}
