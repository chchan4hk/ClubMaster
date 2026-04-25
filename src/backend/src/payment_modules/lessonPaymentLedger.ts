import fs from "fs";
import path from "path";
import { clubDataDir, isValidClubFolderId, getDataClubRootPath } from "../coachListCsv";

export const LESSON_PAYMENT_LEDGER_FILENAME = "LessonPaymentLedger.json";

export type LedgerPaymentLine = {
  paymentId: string;
  amount: number;
  method: string;
  reference: string;
  paidAt: string;
};

export type LedgerReservationEntry = {
  lessonReserveId: string;
  dueDate: string;
  payments: LedgerPaymentLine[];
};

export type LedgerFileV1 = {
  version: 1;
  entries: Record<string, LedgerReservationEntry>;
};

function dataClubRoot(): string {
  return getDataClubRootPath();
}

export function lessonPaymentLedgerPath(clubId: string): string {
  const dir = clubDataDir(clubId);
  return dir ? path.join(dir, LESSON_PAYMENT_LEDGER_FILENAME) : "";
}

function emptyLedger(): LedgerFileV1 {
  return { version: 1, entries: {} };
}

function parseLedger(raw: string): LedgerFileV1 {
  const data = JSON.parse(raw) as Record<string, unknown>;
  if (Number(data.version) !== 1) {
    throw new Error("Unsupported LessonPaymentLedger.json version.");
  }
  const entriesRaw = data.entries;
  const entries: Record<string, LedgerReservationEntry> = {};
  if (entriesRaw && typeof entriesRaw === "object" && !Array.isArray(entriesRaw)) {
    for (const [k, v] of Object.entries(entriesRaw as Record<string, unknown>)) {
      if (!v || typeof v !== "object" || Array.isArray(v)) {
        continue;
      }
      const o = v as Record<string, unknown>;
      const lessonReserveId = String(o.lessonReserveId ?? k).trim();
      if (!lessonReserveId) {
        continue;
      }
      const paymentsIn = Array.isArray(o.payments) ? o.payments : [];
      const payments: LedgerPaymentLine[] = [];
      for (const p of paymentsIn) {
        if (!p || typeof p !== "object") {
          continue;
        }
        const q = p as Record<string, unknown>;
        const amt = Number(q.amount);
        payments.push({
          paymentId: String(q.paymentId ?? "").trim() || newPaymentId(),
          amount: Number.isFinite(amt) ? Math.max(0, amt) : 0,
          method: String(q.method ?? "").trim(),
          reference: String(q.reference ?? "").trim(),
          paidAt: String(q.paidAt ?? "").trim(),
        });
      }
      entries[lessonReserveId.toUpperCase()] = {
        lessonReserveId,
        dueDate: String(o.dueDate ?? "").trim(),
        payments,
      };
    }
  }
  return { version: 1, entries };
}

export function ensureLessonPaymentLedgerFile(clubId: string): void {
  if (!isValidClubFolderId(clubId)) {
    throw new Error("Invalid club ID.");
  }
  const clubDir = path.join(dataClubRoot(), clubId.trim());
  if (!fs.existsSync(clubDir)) {
    fs.mkdirSync(clubDir, { recursive: true });
  }
  const p = lessonPaymentLedgerPath(clubId);
  if (!p) {
    throw new Error("Invalid club ID.");
  }
  if (!fs.existsSync(p)) {
    fs.writeFileSync(p, JSON.stringify(emptyLedger(), null, 2) + "\n", "utf8");
  }
}

export function loadLessonPaymentLedger(clubId: string): LedgerFileV1 {
  if (!isValidClubFolderId(clubId)) {
    return emptyLedger();
  }
  const p = lessonPaymentLedgerPath(clubId);
  if (!p || !fs.existsSync(p)) {
    return emptyLedger();
  }
  try {
    const raw = fs.readFileSync(p, "utf8");
    return parseLedger(raw);
  } catch {
    return emptyLedger();
  }
}

export function saveLessonPaymentLedger(clubId: string, ledger: LedgerFileV1): void {
  ensureLessonPaymentLedgerFile(clubId);
  const p = lessonPaymentLedgerPath(clubId);
  if (!p) {
    throw new Error("Invalid club ID.");
  }
  fs.writeFileSync(p, JSON.stringify(ledger, null, 2) + "\n", "utf8");
}

export function newPaymentId(): string {
  return `PAY-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
}

export function getLedgerEntry(
  ledger: LedgerFileV1,
  lessonReserveId: string,
): LedgerReservationEntry | undefined {
  const k = lessonReserveId.trim().toUpperCase();
  return ledger.entries[k];
}

export function ensureLedgerEntry(
  ledger: LedgerFileV1,
  lessonReserveId: string,
  defaultDueDate: string,
): LedgerReservationEntry {
  const k = lessonReserveId.trim().toUpperCase();
  let e = ledger.entries[k];
  if (!e) {
    e = {
      lessonReserveId: lessonReserveId.trim(),
      dueDate: defaultDueDate.trim(),
      payments: [],
    };
    ledger.entries[k] = e;
  } else if (!e.dueDate && defaultDueDate.trim()) {
    e.dueDate = defaultDueDate.trim();
  }
  return e;
}

export function sumPayments(entry: LedgerReservationEntry | undefined): number {
  if (!entry?.payments?.length) {
    return 0;
  }
  let s = 0;
  for (const p of entry.payments) {
    s += Number.isFinite(p.amount) ? p.amount : 0;
  }
  return Math.round(s * 100) / 100;
}

export type AddLedgerPaymentsInput = {
  splits: { lessonReserveId: string; amount: number }[];
  method: string;
  reference: string;
  paidAt: string;
};

/**
 * Applies payment splits to an in-memory ledger (used by JSON and Mongo paths).
 */
export function applyAddPaymentsToLedgerState(
  ledger: LedgerFileV1,
  input: AddLedgerPaymentsInput,
  defaultDueDates: Record<string, string>,
): { ok: true; paymentIds: string[] } | { ok: false; error: string } {
  if (!input.splits.length) {
    return { ok: false, error: "No payment splits." };
  }
  const paidAt = (input.paidAt && input.paidAt.trim()) || new Date().toISOString().slice(0, 10);
  const method = (input.method && input.method.trim()) || "Cash";
  const reference = (input.reference && input.reference.trim()) || "—";
  const paymentIds: string[] = [];
  for (const sp of input.splits) {
    const lid = sp.lessonReserveId.trim();
    if (!lid) {
      return { ok: false, error: "Missing lessonReserveId in split." };
    }
    const amt = Number(sp.amount);
    if (!Number.isFinite(amt) || amt <= 0) {
      return { ok: false, error: "Each split needs a positive amount." };
    }
    const dueDefault = defaultDueDates[lid.toUpperCase()] ?? "";
    const entry = ensureLedgerEntry(ledger, lid, dueDefault);
    const paymentId = newPaymentId();
    paymentIds.push(paymentId);
    entry.payments.push({
      paymentId,
      amount: Math.round(amt * 100) / 100,
      method,
      reference,
      paidAt,
    });
  }
  return { ok: true, paymentIds };
}

export function addPaymentsToLedger(
  clubId: string,
  input: AddLedgerPaymentsInput,
  defaultDueDates: Record<string, string>,
): { ok: true; paymentIds: string[] } | { ok: false; error: string } {
  ensureLessonPaymentLedgerFile(clubId);
  const ledger = loadLessonPaymentLedger(clubId);
  const r = applyAddPaymentsToLedgerState(ledger, input, defaultDueDates);
  if (r.ok) {
    saveLessonPaymentLedger(clubId, ledger);
  }
  return r;
}

export function applyClearLedgerPaymentsForReservation(
  ledger: LedgerFileV1,
  lessonReserveId: string,
): { ok: true } | { ok: false; error: string } {
  const rid = lessonReserveId.trim();
  if (!rid) {
    return { ok: false, error: "Missing reservation id." };
  }
  const k = rid.toUpperCase();
  const e = ledger.entries[k];
  if (e) {
    e.payments = [];
  }
  return { ok: true };
}

/** Clear all ledger payment lines for one reservation (coach void). */
export function clearLedgerPaymentsForLessonReserve(
  clubId: string,
  lessonReserveId: string,
): { ok: true } | { ok: false; error: string } {
  if (!isValidClubFolderId(clubId)) {
    return { ok: false, error: "Invalid club." };
  }
  const rid = lessonReserveId.trim();
  if (!rid) {
    return { ok: false, error: "Missing reservation id." };
  }
  ensureLessonPaymentLedgerFile(clubId);
  const ledger = loadLessonPaymentLedger(clubId);
  const r = applyClearLedgerPaymentsForReservation(ledger, rid);
  if (!r.ok) {
    return r;
  }
  saveLessonPaymentLedger(clubId, ledger);
  return { ok: true };
}

export function applyDeleteLedgerEntryForReservation(
  ledger: LedgerFileV1,
  lessonReserveId: string,
): { ok: true } | { ok: false; error: string } {
  const rid = lessonReserveId.trim();
  if (!rid) {
    return { ok: false, error: "Missing reservation id." };
  }
  const k = rid.toUpperCase();
  if (ledger.entries[k]) {
    delete ledger.entries[k];
  }
  return { ok: true };
}

/** Remove the ledger entry for one reservation (entire key). */
export function deleteLedgerEntryForLessonReserve(
  clubId: string,
  lessonReserveId: string,
): { ok: true } | { ok: false; error: string } {
  if (!isValidClubFolderId(clubId)) {
    return { ok: false, error: "Invalid club." };
  }
  const rid = lessonReserveId.trim();
  if (!rid) {
    return { ok: false, error: "Missing reservation id." };
  }
  ensureLessonPaymentLedgerFile(clubId);
  const ledger = loadLessonPaymentLedger(clubId);
  const k = rid.toUpperCase();
  if (ledger.entries[k]) {
    const r = applyDeleteLedgerEntryForReservation(ledger, rid);
    if (!r.ok) {
      return r;
    }
    saveLessonPaymentLedger(clubId, ledger);
  }
  return { ok: true };
}
