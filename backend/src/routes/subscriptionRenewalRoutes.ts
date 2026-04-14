import fs from "fs";
import path from "path";
import { Router } from "express";
import {
  findCoachRoleLoginByUsername,
  findStudentRoleLoginByUsername,
} from "../coachStudentLoginCsv";
import { findUserByUsername } from "../userlistCsv";

const dataDir = path.join(__dirname, "..", "..", "data", "admin");
const requestsFile = path.join(dataDir, "subscription_payment_requests.jsonl");
const paymentListFile = path.join(dataDir, "PaymentList_UserLogin.json");

const SUBSCRIPTION_OPTION_BY_PERIOD: Record<string, string> = {
  "1m": "1 month",
  "3m": "3 months",
  "1y": "1 year",
};

const SUBSCRIPTION_AMOUNT_BY_PERIOD: Record<string, number> = {
  "1m": 1680,
  "3m": 4580,
  "1y": 11600,
};

type UserPaymentEntry = {
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
  payments: UserPaymentEntry[];
};

function ensureDir(): void {
  if (!fs.existsSync(dataDir)) {
    fs.mkdirSync(dataDir, { recursive: true });
  }
}

function resolveFullNameAndExpiry(
  username: string,
  role: string,
): { full_name: string; Expiry_date: string } {
  const r = String(role || "").trim();
  if (r === "Coach") {
    const login = findCoachRoleLoginByUsername(username);
    if (login) {
      return {
        full_name: login.fullName.trim(),
        Expiry_date: String(login.expiryDate ?? "").trim(),
      };
    }
    return { full_name: "", Expiry_date: "" };
  }
  if (r === "Student") {
    const login = findStudentRoleLoginByUsername(username);
    if (login) {
      return {
        full_name: login.fullName.trim(),
        Expiry_date: String(login.expiryDate ?? "").trim(),
      };
    }
    return { full_name: "", Expiry_date: "" };
  }
  const row = findUserByUsername(username);
  if (row) {
    return {
      full_name: row.fullName.trim(),
      Expiry_date: String(row.expiryDate ?? "").trim(),
    };
  }
  return { full_name: "", Expiry_date: "" };
}

function nextPaymentId(payments: { paymentId?: string }[]): string {
  let max = 0;
  for (const p of payments) {
    const m = String(p.paymentId ?? "").match(/^PM(\d+)$/i);
    if (m) {
      max = Math.max(max, parseInt(m[1], 10));
    }
  }
  return `PM${String(max + 1).padStart(6, "0")}`;
}

function normalizePaymentDate(raw: string): string {
  const s = String(raw ?? "").trim();
  if (!s) {
    return "";
  }
  if (s.includes("T")) {
    return s.replace("T", " ").slice(0, 16);
  }
  return s;
}

function isPlaceholderPaymentRow(p: UserPaymentEntry): boolean {
  return (
    !String(p.paymentId ?? "").trim() &&
    !String(p.uid ?? "").trim() &&
    !String(p.username ?? "").trim()
  );
}

function appendUserLoginPayment(entry: UserPaymentEntry): void {
  ensureDir();
  let body: PaymentListFile = { version: 1, payments: [] };
  if (fs.existsSync(paymentListFile)) {
    try {
      const raw = fs.readFileSync(paymentListFile, "utf8");
      const parsed = JSON.parse(raw) as PaymentListFile;
      if (parsed && Array.isArray(parsed.payments)) {
        body = {
          version: typeof parsed.version === "number" ? parsed.version : 1,
          payments: parsed.payments.filter(
            (row) => !isPlaceholderPaymentRow(row as UserPaymentEntry),
          ) as UserPaymentEntry[],
        };
      }
    } catch {
      body = { version: 1, payments: [] };
    }
  }
  entry.paymentId = nextPaymentId(body.payments);
  body.payments.push(entry);
  fs.writeFileSync(
    paymentListFile,
    JSON.stringify(body, null, 2) + "\n",
    "utf8",
  );
}

export function createSubscriptionRenewalRouter(): Router {
  const r = Router();

  r.post("/payment-request", (req, res) => {
    const paymentMethod = String(req.body?.paymentMethod ?? "").trim();
    const subscriptionPeriod = String(req.body?.subscriptionPeriod ?? "").trim();
    const transactionDetails = String(req.body?.transactionDetails ?? "").trim();
    const transactionDate = String(req.body?.transactionDate ?? "").trim();
    const username = String(req.body?.username ?? "").trim();
    const role = String(req.body?.role ?? "").trim();
    const uid = String(req.body?.uid ?? "").trim();
    const expiryFromClient = String(
      req.body?.expiry_date ?? req.body?.Expiry_date ?? "",
    ).trim();

    const validMethods = ["PayMe", "FPS", "AliPay", "支付寶"];
    const validPeriods = ["1m", "3m", "1y"];
    if (!validMethods.includes(paymentMethod)) {
      res.status(400).json({ ok: false, error: "Select a payment method." });
      return;
    }
    if (!validPeriods.includes(subscriptionPeriod)) {
      res.status(400).json({ ok: false, error: "Select a subscription period." });
      return;
    }

    const subscriptionOptionFromBody = String(
      req.body?.subscription_option ??
        req.body?.Subscription_Option ??
        req.body?.subscriptionOption ??
        "",
    ).trim();
    const subscription_option =
      subscriptionOptionFromBody ||
      SUBSCRIPTION_OPTION_BY_PERIOD[subscriptionPeriod] ||
      subscriptionPeriod;

    const rawAmount = req.body?.subscription_amount ?? req.body?.subscriptionAmount;
    let subscription_amount = Number(rawAmount);
    if (
      !Number.isFinite(subscription_amount) ||
      subscription_amount <= 0
    ) {
      subscription_amount = SUBSCRIPTION_AMOUNT_BY_PERIOD[subscriptionPeriod] ?? 0;
    }

    const record = {
      submittedAt: new Date().toISOString(),
      username,
      role,
      uid,
      paymentMethod,
      subscriptionPeriod,
      subscription_option,
      subscription_amount,
      transactionDate,
      transactionDetails,
    };

    try {
      ensureDir();
      fs.appendFileSync(
        requestsFile,
        JSON.stringify(record) + "\n",
        "utf8",
      );
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      res.status(500).json({ ok: false, error: `Could not save request: ${msg}` });
      return;
    }

    const profile = resolveFullNameAndExpiry(username, role);
    const full_name =
      profile.full_name ||
      String(req.body?.full_name ?? req.body?.fullName ?? "").trim();
    const Expiry_date = profile.Expiry_date || expiryFromClient;

    const createdAt = new Date().toISOString();
    const paymentEntry: UserPaymentEntry = {
      paymentId: "",
      uid,
      username,
      full_name,
      Expiry_date,
      subscription_option,
      subscription_amount,
      paymentMethod,
      transactionRef: transactionDetails,
      paymentDate: normalizePaymentDate(transactionDate),
      status: "PENDING",
      createdAt,
      confirm_payment_date: "",
    };

    try {
      appendUserLoginPayment(paymentEntry);
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      res.status(500).json({
        ok: false,
        error: `Could not save payment list: ${msg}`,
      });
      return;
    }

    res.json({ ok: true, message: "Payment request recorded." });
  });

  return r;
}
