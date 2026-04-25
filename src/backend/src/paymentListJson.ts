import fs from "fs";
import path from "path";
import type { Document } from "mongodb";
import { clubDataDir, isValidClubFolderId, getDataClubRootPath } from "./coachListCsv";
import {
  findPaymentListClubDocument,
  paymentListUsesMongo,
  replacePaymentListForClub,
} from "./paymentListMongo";

export const PAYMENT_LIST_FILENAME = "PaymentList.json";

const PM_ID_RE = /^PM(\d+)$/i;
const PM_PAD = 6;

/** Coach-manager folder ids: new payment ids use `{club}-PY0000001`. */
const CM_FOLDER_UID_RE = /^CM\d+$/i;

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

export type PaymentListFileV1 = {
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

export function paymentRecordFromUnknown(x: unknown): PaymentListRecord | null {
  if (!x || typeof x !== "object") {
    return null;
  }
  const o = x as Record<string, unknown>;
  const amt = Number(o.amountPaid);
  return {
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
  };
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
      const r = paymentRecordFromUnknown(x);
      if (r && r.paymentId) {
        payments.push(r);
      }
    }
  }
  return { version: 1, payments };
}

function readPaymentListFromJsonFile(clubId: string): PaymentListRecord[] {
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

function paymentRecordToDocument(r: PaymentListRecord): Document {
  return { ...r } as Document;
}

async function persistPaymentList(
  clubId: string,
  payments: PaymentListRecord[],
): Promise<void> {
  const id = clubId.trim();
  if (paymentListUsesMongo()) {
    await replacePaymentListForClub(
      id,
      payments.map((row) => paymentRecordToDocument(row)),
    );
    return;
  }
  const p = paymentListPath(id);
  if (!p) {
    throw new Error("Invalid club ID.");
  }
  const file: PaymentListFileV1 = { version: 1, payments };
  fs.writeFileSync(p, JSON.stringify(file, null, 2) + "\n", "utf8");
}

function maxPaymentNumericParts(paymentId: string): { pm: number; py: number } {
  const id = paymentId.replace(/^\uFEFF/, "").trim();
  const suff = id.match(/-PY(\d+)$/i);
  if (suff) {
    const n = Number.parseInt(suff[1]!, 10);
    return { pm: 0, py: Number.isNaN(n) ? 0 : n };
  }
  const lonePy = id.match(/^PY(\d+)$/i);
  if (lonePy) {
    const n = Number.parseInt(lonePy[1]!, 10);
    return { pm: 0, py: Number.isNaN(n) ? 0 : n };
  }
  const pm = id.match(PM_ID_RE);
  if (pm) {
    const n = Number.parseInt(pm[1]!, 10);
    return { pm: Number.isNaN(n) ? 0 : n, py: 0 };
  }
  return { pm: 0, py: 0 };
}

function nextPaymentListId(
  clubFolderUid: string,
  existing: PaymentListRecord[],
): string {
  let maxPm = 0;
  let maxPy = 0;
  for (const r of existing) {
    const s = maxPaymentNumericParts(r.paymentId);
    maxPm = Math.max(maxPm, s.pm);
    maxPy = Math.max(maxPy, s.py);
  }
  const club = clubFolderUid.replace(/^\uFEFF/, "").trim();
  if (CM_FOLDER_UID_RE.test(club)) {
    const c = club.toUpperCase();
    const n = Math.max(maxPy, maxPm);
    return `${c}-PY${String(n + 1).padStart(7, "0")}`;
  }
  return `PM${String(maxPm + 1).padStart(PM_PAD, "0")}`;
}

export async function ensurePaymentListFile(clubId: string): Promise<void> {
  if (!isValidClubFolderId(clubId)) {
    throw new Error("Invalid club ID.");
  }
  const id = clubId.trim();
  const clubDir = path.join(dataClubRoot(), id);
  if (!fs.existsSync(clubDir)) {
    fs.mkdirSync(clubDir, { recursive: true });
  }
  if (paymentListUsesMongo()) {
    const existing = await findPaymentListClubDocument(id);
    if (existing) {
      return;
    }
    let payments: PaymentListRecord[] = readPaymentListFromJsonFile(id);
    await replacePaymentListForClub(
      id,
      payments.map((row) => paymentRecordToDocument(row)),
    );
    return;
  }
  const p = paymentListPath(clubId);
  if (!p) {
    throw new Error("Invalid club ID.");
  }
  if (!fs.existsSync(p)) {
    fs.writeFileSync(p, JSON.stringify(emptyFile(), null, 2) + "\n", "utf8");
  }
}

export async function loadPaymentList(clubId: string): Promise<PaymentListRecord[]> {
  if (!isValidClubFolderId(clubId)) {
    return [];
  }
  const id = clubId.trim();
  if (paymentListUsesMongo()) {
    await ensurePaymentListFile(clubId);
    const doc = await findPaymentListClubDocument(id);
    if (!doc || !Array.isArray(doc.payments)) {
      return [];
    }
    const out: PaymentListRecord[] = [];
    for (const x of doc.payments) {
      const r = paymentRecordFromUnknown(x);
      if (r && r.paymentId) {
        out.push(r);
      }
    }
    return out;
  }
  return readPaymentListFromJsonFile(id);
}

export async function savePaymentList(
  clubId: string,
  file: PaymentListFileV1,
): Promise<void> {
  await ensurePaymentListFile(clubId);
  await persistPaymentList(clubId.trim(), file.payments);
}

export async function appendPaymentListRecords(
  clubId: string,
  items: Omit<
    PaymentListRecord,
    "paymentId" | "createdAt" | "lastUpdatedAt"
  >[],
): Promise<{ ok: true; paymentIds: string[] } | { ok: false; error: string }> {
  if (!items.length) {
    return { ok: false, error: "No payment rows." };
  }
  await ensurePaymentListFile(clubId);
  const payments = await loadPaymentList(clubId);
  const today = new Date().toISOString().slice(0, 10);
  const ids: string[] = [];
  for (const it of items) {
    const id = nextPaymentListId(clubId.trim(), payments);
    ids.push(id);
    payments.push({
      ...it,
      paymentId: id,
      createdAt: today,
      lastUpdatedAt: today,
    });
  }
  await persistPaymentList(clubId.trim(), payments);
  return { ok: true, paymentIds: ids };
}

export async function listPaymentsForStudent(
  clubId: string,
  studentId: string,
): Promise<PaymentListRecord[]> {
  const sid = studentId.trim().toUpperCase();
  const rows = await loadPaymentList(clubId);
  return rows.filter((r) => r.studentId.trim().toUpperCase() === sid);
}

/** Remove all PaymentList rows for one lesson reservation (coach void). */
export async function removePaymentListRecordsForLessonReserve(
  clubId: string,
  lessonReserveId: string,
): Promise<{ ok: true; removed: number } | { ok: false; error: string }> {
  if (!isValidClubFolderId(clubId)) {
    return { ok: false, error: "Invalid club." };
  }
  const rid = lessonReserveId.trim();
  if (!rid) {
    return { ok: false, error: "Missing reservation id." };
  }
  await ensurePaymentListFile(clubId);
  const payments = await loadPaymentList(clubId);
  const u = rid.toUpperCase();
  const before = payments.length;
  const next = payments.filter(
    (r) => r.lessonReserveId.trim().toUpperCase() !== u,
  );
  const removed = before - next.length;
  await persistPaymentList(clubId.trim(), next);
  return { ok: true, removed };
}
