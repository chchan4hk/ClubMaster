import fs from "fs";
import path from "path";
import { clubDataDir, isValidClubFolderId, getDataClubRootPath } from "./coachListCsv";

export const PAYMENT_LIST_FILENAME = "PaymentList.json";

const PM_ID_RE = /^PM(\d+)$/i;
const PM_PAD = 6;

export type PaymentListRecord = {
  paymentId: string;
  lessonReserveId: string;
  studentId: string;
  clubId: string;
  amountPaid: number;
  paymentMethod: string;
  transactionRef: string;
  paymentDate: string;
  status: string;
  notes: string;
  recordedBy: string;
  createdAt: string;
  lastUpdatedAt: string;
};

type PaymentListFileV1 = {
  version: 1;
  payments: PaymentListRecord[];
};

function dataClubRoot(): string {
  return getDataClubRootPath();
}

export function paymentListPath(clubId: string): string {
  const dir = clubDataDir(clubId);
  return dir ? path.join(dir, PAYMENT_LIST_FILENAME) : "";
}

function emptyFile(): PaymentListFileV1 {
  return { version: 1, payments: [] };
}

function parseFile(raw: string): PaymentListFileV1 {
  const data = JSON.parse(raw) as Record<string, unknown>;
  if (Number(data.version) !== 1) {
    throw new Error("Unsupported PaymentList.json version.");
  }
  const arr = data.payments;
  const payments: PaymentListRecord[] = [];
  if (Array.isArray(arr)) {
    for (const x of arr) {
      if (!x || typeof x !== "object") {
        continue;
      }
      const o = x as Record<string, unknown>;
      const amt = Number(o.amountPaid);
      payments.push({
        paymentId: String(o.paymentId ?? "").trim(),
        lessonReserveId: String(o.lessonReserveId ?? "").trim(),
        studentId: String(o.studentId ?? "").trim(),
        clubId: String(o.clubId ?? "").trim(),
        amountPaid: Number.isFinite(amt) ? Math.round(amt * 100) / 100 : 0,
        paymentMethod: String(o.paymentMethod ?? "").trim(),
        transactionRef: String(o.transactionRef ?? "").trim(),
        paymentDate: String(o.paymentDate ?? "").trim().slice(0, 10),
        status: String(o.status ?? "COMPLETED").trim(),
        notes: String(o.notes ?? "").trim(),
        recordedBy: String(o.recordedBy ?? "").trim(),
        createdAt: String(o.createdAt ?? "").trim().slice(0, 10),
        lastUpdatedAt: String(o.lastUpdatedAt ?? "").trim().slice(0, 10),
      });
    }
  }
  return { version: 1, payments };
}

export function ensurePaymentListFile(clubId: string): void {
  if (!isValidClubFolderId(clubId)) {
    throw new Error("Invalid club ID.");
  }
  const clubDir = path.join(dataClubRoot(), clubId.trim());
  if (!fs.existsSync(clubDir)) {
    fs.mkdirSync(clubDir, { recursive: true });
  }
  const p = paymentListPath(clubId);
  if (!p) {
    throw new Error("Invalid club ID.");
  }
  if (!fs.existsSync(p)) {
    fs.writeFileSync(p, JSON.stringify(emptyFile(), null, 2) + "\n", "utf8");
  }
}

export function loadPaymentList(clubId: string): PaymentListRecord[] {
  if (!isValidClubFolderId(clubId)) {
    return [];
  }
  const p = paymentListPath(clubId);
  if (!p || !fs.existsSync(p)) {
    return [];
  }
  try {
    return parseFile(fs.readFileSync(p, "utf8")).payments;
  } catch {
    return [];
  }
}

export function savePaymentList(clubId: string, file: PaymentListFileV1): void {
  ensurePaymentListFile(clubId);
  const p = paymentListPath(clubId);
  if (!p) {
    throw new Error("Invalid club ID.");
  }
  fs.writeFileSync(p, JSON.stringify(file, null, 2) + "\n", "utf8");
}

function nextPaymentListId(existing: PaymentListRecord[]): string {
  let max = 0;
  for (const r of existing) {
    const m = r.paymentId.match(PM_ID_RE);
    if (m) {
      const n = Number.parseInt(m[1]!, 10);
      if (!Number.isNaN(n) && n > max) {
        max = n;
      }
    }
  }
  return `PM${String(max + 1).padStart(PM_PAD, "0")}`;
}

export function appendPaymentListRecords(
  clubId: string,
  items: Omit<
    PaymentListRecord,
    "paymentId" | "createdAt" | "lastUpdatedAt"
  >[],
): { ok: true; paymentIds: string[] } | { ok: false; error: string } {
  if (!items.length) {
    return { ok: false, error: "No payment rows." };
  }
  ensurePaymentListFile(clubId);
  const p = paymentListPath(clubId);
  if (!p) {
    return { ok: false, error: "Invalid club." };
  }
  let file: PaymentListFileV1;
  try {
    file = parseFile(fs.readFileSync(p, "utf8"));
  } catch {
    file = emptyFile();
  }
  const today = new Date().toISOString().slice(0, 10);
  const ids: string[] = [];
  for (const it of items) {
    const id = nextPaymentListId(file.payments);
    ids.push(id);
    file.payments.push({
      ...it,
      paymentId: id,
      createdAt: today,
      lastUpdatedAt: today,
    });
  }
  savePaymentList(clubId, file);
  return { ok: true, paymentIds: ids };
}

export function listPaymentsForStudent(
  clubId: string,
  studentId: string,
): PaymentListRecord[] {
  const sid = studentId.trim().toUpperCase();
  return loadPaymentList(clubId).filter(
    (r) => r.studentId.trim().toUpperCase() === sid,
  );
}

/** Remove all PaymentList.json rows for one lesson reservation (coach void). */
export function removePaymentListRecordsForLessonReserve(
  clubId: string,
  lessonReserveId: string,
): { ok: true; removed: number } | { ok: false; error: string } {
  if (!isValidClubFolderId(clubId)) {
    return { ok: false, error: "Invalid club." };
  }
  const rid = lessonReserveId.trim();
  if (!rid) {
    return { ok: false, error: "Missing reservation id." };
  }
  ensurePaymentListFile(clubId);
  const p = paymentListPath(clubId);
  if (!p) {
    return { ok: false, error: "Invalid club." };
  }
  let file: PaymentListFileV1;
  try {
    file = parseFile(fs.readFileSync(p, "utf8"));
  } catch {
    file = emptyFile();
  }
  const u = rid.toUpperCase();
  const before = file.payments.length;
  file.payments = file.payments.filter(
    (r) => r.lessonReserveId.trim().toUpperCase() !== u,
  );
  const removed = before - file.payments.length;
  savePaymentList(clubId, file);
  return { ok: true, removed };
}
