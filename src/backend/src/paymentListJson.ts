import type { Document } from "mongodb";
import { isValidClubFolderId } from "./coachListCsv";
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

function requirePaymentListMongo(): void {
  if (!paymentListUsesMongo()) {
    throw new Error(
      "MongoDB is required for PaymentList (ClubMaster_DB.PaymentList). Configure MONGODB_URI / MONGO_URI.",
    );
  }
}

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

/** Virtual path for diagnostics / API metadata (data lives in MongoDB only). */
export function paymentListPath(clubId: string): string {
  const id = clubId.trim();
  if (!isValidClubFolderId(id)) {
    return "";
  }
  return `mongodb:PaymentList/${encodeURIComponent(id)}`;
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

function paymentRecordToDocument(r: PaymentListRecord): Document {
  return { ...r } as Document;
}

async function persistPaymentList(
  clubId: string,
  payments: PaymentListRecord[],
): Promise<void> {
  requirePaymentListMongo();
  const id = clubId.trim();
  await replacePaymentListForClub(
    id,
    payments.map((row) => paymentRecordToDocument(row)),
  );
}

function maxPaymentNumericParts(paymentId: string): { pm: number; py: number } {
  const pid = paymentId.replace(/^\uFEFF/, "").trim();
  const suff = pid.match(/-PY(\d+)$/i);
  if (suff) {
    const n = Number.parseInt(suff[1]!, 10);
    return { pm: 0, py: Number.isNaN(n) ? 0 : n };
  }
  const lonePy = pid.match(/^PY(\d+)$/i);
  if (lonePy) {
    const n = Number.parseInt(lonePy[1]!, 10);
    return { pm: 0, py: Number.isNaN(n) ? 0 : n };
  }
  const pm = pid.match(PM_ID_RE);
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
  requirePaymentListMongo();
  if (!isValidClubFolderId(clubId)) {
    throw new Error("Invalid club ID.");
  }
  const id = clubId.trim();
  const existing = await findPaymentListClubDocument(id);
  if (existing) {
    return;
  }
  await replacePaymentListForClub(id, []);
}

export async function loadPaymentList(clubId: string): Promise<PaymentListRecord[]> {
  if (!isValidClubFolderId(clubId)) {
    return [];
  }
  requirePaymentListMongo();
  const id = clubId.trim();
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
    const pid = nextPaymentListId(clubId.trim(), payments);
    ids.push(pid);
    payments.push({
      ...it,
      paymentId: pid,
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
