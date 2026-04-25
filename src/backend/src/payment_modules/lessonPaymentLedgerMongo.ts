import type { Filter } from "mongodb";
import { isValidClubFolderId } from "../coachListCsv";
import {
  ensureLessonPaymentLedgerCollection,
  getLessonPaymentLedgerCollection,
  isMongoConfigured,
  type LessonPaymentLedgerDocument,
  type LessonPaymentLedgerInsert,
} from "../db/DBConnection";
import {
  applyAddPaymentsToLedgerState,
  applyClearLedgerPaymentsForReservation,
  applyDeleteLedgerEntryForReservation,
  addPaymentsToLedger,
  clearLedgerPaymentsForLessonReserve,
  deleteLedgerEntryForLessonReserve,
  ensureLessonPaymentLedgerFile,
  loadLessonPaymentLedger,
  type AddLedgerPaymentsInput,
  type LedgerFileV1,
  type LedgerPaymentLine,
  type LedgerReservationEntry,
} from "./lessonPaymentLedger";

let lessonPaymentLedgerCollectionEnsured = false;

function escapeRegExp(s: string): string {
  return s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

/** Same club partition semantics as lesson reservations (folder id + `{club}-LR…` ids). */
function lessonPaymentLedgerPartitionFilter(
  clubFolderUid: string,
): Filter<LessonPaymentLedgerDocument> {
  const club = clubFolderUid.replace(/^\uFEFF/, "").trim();
  const esc = escapeRegExp(club);
  return {
    $or: [
      { ClubID: new RegExp(`^${esc}$`, "i") },
      { lessonReserveId: new RegExp(`^${esc}-LR`, "i") },
    ],
  };
}

function canonicalClubIdForLedger(clubFolderUid: string): string {
  const s = clubFolderUid.replace(/^\uFEFF/, "").trim();
  return s.replace(/^cm(?=\d)/i, "CM");
}

function paymentLineFromDoc(p: unknown): LedgerPaymentLine {
  const q = (p && typeof p === "object" ? p : {}) as Record<string, unknown>;
  const amt = Number(q.amount);
  return {
    paymentId: String(q.paymentId ?? "").trim() || "",
    amount: Number.isFinite(amt) ? Math.max(0, amt) : 0,
    method: String(q.method ?? "").trim(),
    reference: String(q.reference ?? "").trim(),
    paidAt: String(q.paidAt ?? "").trim(),
  };
}

function mongoDocsToLedger(docs: LessonPaymentLedgerDocument[]): LedgerFileV1 {
  const entries: Record<string, LedgerReservationEntry> = {};
  for (const d of docs) {
    const rid = String(d.lessonReserveId ?? "").trim();
    if (!rid) {
      continue;
    }
    const paymentsIn = Array.isArray(d.payments) ? d.payments : [];
    const payments: LedgerPaymentLine[] = [];
    for (const p of paymentsIn) {
      if (!p || typeof p !== "object") {
        continue;
      }
      payments.push(paymentLineFromDoc(p));
    }
    entries[rid.toUpperCase()] = {
      lessonReserveId: rid,
      dueDate: String(d.dueDate ?? "").trim(),
      payments,
    };
  }
  return { version: 1, entries };
}

function ledgerToInserts(
  clubFolderUid: string,
  ledger: LedgerFileV1,
): LessonPaymentLedgerInsert[] {
  const clubNorm = canonicalClubIdForLedger(clubFolderUid);
  const rows: LessonPaymentLedgerInsert[] = [];
  for (const e of Object.values(ledger.entries)) {
    if (!e.lessonReserveId?.trim()) {
      continue;
    }
    rows.push({
      ClubID: clubNorm,
      lessonReserveId: e.lessonReserveId.trim(),
      dueDate: (e.dueDate ?? "").trim(),
      payments: (e.payments ?? []).map((p) => ({
        paymentId: p.paymentId,
        amount: Number.isFinite(p.amount) ? Math.round(p.amount * 100) / 100 : 0,
        method: p.method,
        reference: p.reference,
        paidAt: p.paidAt,
      })),
    });
  }
  return rows;
}

async function ensureLessonPaymentLedgerCollectionOnce(): Promise<void> {
  if (lessonPaymentLedgerCollectionEnsured) {
    return;
  }
  lessonPaymentLedgerCollectionEnsured = true;
  try {
    await ensureLessonPaymentLedgerCollection();
  } catch (e) {
    lessonPaymentLedgerCollectionEnsured = false;
    throw e;
  }
}

export async function loadLessonPaymentLedgerMongo(
  clubFolderUid: string,
): Promise<LedgerFileV1> {
  if (!isValidClubFolderId(clubFolderUid.trim())) {
    return { version: 1, entries: {} };
  }
  await ensureLessonPaymentLedgerCollectionOnce();
  const col = await getLessonPaymentLedgerCollection();
  const docs = await col
    .find(lessonPaymentLedgerPartitionFilter(clubFolderUid))
    .sort({ lessonReserveId: 1 })
    .toArray();
  return mongoDocsToLedger(docs);
}

async function saveLessonPaymentLedgerMongo(
  clubFolderUid: string,
  ledger: LedgerFileV1,
): Promise<void> {
  if (!isValidClubFolderId(clubFolderUid.trim())) {
    throw new Error("Invalid club ID.");
  }
  await ensureLessonPaymentLedgerCollectionOnce();
  const col = await getLessonPaymentLedgerCollection();
  const filt = lessonPaymentLedgerPartitionFilter(clubFolderUid);
  await col.deleteMany(filt);
  const rows = ledgerToInserts(clubFolderUid, ledger);
  if (rows.length > 0) {
    await col.insertMany(rows);
  }
}

export async function loadLessonPaymentLedgerPreferred(
  clubId: string,
): Promise<LedgerFileV1> {
  if (!isMongoConfigured()) {
    return loadLessonPaymentLedger(clubId);
  }
  try {
    return await loadLessonPaymentLedgerMongo(clubId);
  } catch (e) {
    console.warn(
      "[LessonPaymentLedger] Mongo load failed; falling back to JSON files.",
      e instanceof Error ? e.message : e,
    );
    return loadLessonPaymentLedger(clubId);
  }
}

export async function ensureLessonPaymentLedgerPreferred(
  clubId: string,
): Promise<void> {
  if (!isValidClubFolderId(clubId.trim())) {
    throw new Error("Invalid club ID.");
  }
  if (!isMongoConfigured()) {
    ensureLessonPaymentLedgerFile(clubId);
    return;
  }
  try {
    await ensureLessonPaymentLedgerCollectionOnce();
  } catch (e) {
    console.warn(
      "[LessonPaymentLedger] Mongo ensure failed; falling back to JSON files.",
      e instanceof Error ? e.message : e,
    );
    ensureLessonPaymentLedgerFile(clubId);
  }
}

export async function addPaymentsToLedgerPreferred(
  clubId: string,
  input: AddLedgerPaymentsInput,
  defaultDueDates: Record<string, string>,
): Promise<{ ok: true; paymentIds: string[] } | { ok: false; error: string }> {
  if (!isMongoConfigured()) {
    return addPaymentsToLedger(clubId, input, defaultDueDates);
  }
  try {
    await ensureLessonPaymentLedgerCollectionOnce();
    const ledger = await loadLessonPaymentLedgerMongo(clubId);
    const r = applyAddPaymentsToLedgerState(ledger, input, defaultDueDates);
    if (!r.ok) {
      return r;
    }
    await saveLessonPaymentLedgerMongo(clubId, ledger);
    return r;
  } catch (e) {
    console.warn(
      "[LessonPaymentLedger] Mongo write failed; falling back to JSON files.",
      e instanceof Error ? e.message : e,
    );
    return addPaymentsToLedger(clubId, input, defaultDueDates);
  }
}

export async function clearLedgerPaymentsForLessonReservePreferred(
  clubId: string,
  lessonReserveId: string,
): Promise<{ ok: true } | { ok: false; error: string }> {
  if (!isMongoConfigured()) {
    return clearLedgerPaymentsForLessonReserve(clubId, lessonReserveId);
  }
  try {
    await ensureLessonPaymentLedgerCollectionOnce();
    const ledger = await loadLessonPaymentLedgerMongo(clubId);
    const r = applyClearLedgerPaymentsForReservation(ledger, lessonReserveId);
    if (!r.ok) {
      return r;
    }
    await saveLessonPaymentLedgerMongo(clubId, ledger);
    return { ok: true };
  } catch (e) {
    console.warn(
      "[LessonPaymentLedger] Mongo clear failed; falling back to JSON files.",
      e instanceof Error ? e.message : e,
    );
    return clearLedgerPaymentsForLessonReserve(clubId, lessonReserveId);
  }
}

export async function deleteLedgerEntryForLessonReservePreferred(
  clubId: string,
  lessonReserveId: string,
): Promise<{ ok: true } | { ok: false; error: string }> {
  if (!isMongoConfigured()) {
    return deleteLedgerEntryForLessonReserve(clubId, lessonReserveId);
  }
  try {
    await ensureLessonPaymentLedgerCollectionOnce();
    const ledger = await loadLessonPaymentLedgerMongo(clubId);
    const rid = lessonReserveId.trim();
    if (!rid) {
      return { ok: false, error: "Missing reservation id." };
    }
    const k = rid.toUpperCase();
    if (!ledger.entries[k]) {
      return { ok: true };
    }
    const r = applyDeleteLedgerEntryForReservation(ledger, rid);
    if (!r.ok) {
      return r;
    }
    await saveLessonPaymentLedgerMongo(clubId, ledger);
    return { ok: true };
  } catch (e) {
    console.warn(
      "[LessonPaymentLedger] Mongo delete entry failed; falling back to JSON files.",
      e instanceof Error ? e.message : e,
    );
    return deleteLedgerEntryForLessonReserve(clubId, lessonReserveId);
  }
}
