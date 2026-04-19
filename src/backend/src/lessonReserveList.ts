import fs from "fs";
import path from "path";
import { clubDataDir, isValidClubFolderId, getDataClubRootPath } from "./coachListCsv";

export const LESSON_RESERVE_LIST_FILENAME = "LessonReserveList.json";

const LR_ID_RE = /^LR(\d+)$/i;
const LR_PAD = 6;

export type LessonReserveRecord = {
  lessonReserveId: string;
  lessonId: string;
  ClubID: string;
  student_id: string;
  Student_Name: string;
  status: string;
  Payment_Status: string;
  Payment_Confirm: boolean;
  createdAt: string;
  lastUpdatedDate: string;
};

function dataClubRoot(): string {
  return getDataClubRootPath();
}

export function lessonReserveListPath(clubId: string): string {
  const dir = clubDataDir(clubId);
  return dir ? path.join(dir, LESSON_RESERVE_LIST_FILENAME) : "";
}

type ReserveFileV1 = {
  version: 1;
  reservations: Record<string, unknown>[];
};

function parseReserveFile(raw: string): ReserveFileV1 {
  const data = JSON.parse(raw) as Record<string, unknown>;
  const version = Number(data.version);
  if (version !== 1) {
    throw new Error("Unsupported LessonReserveList.json version.");
  }
  let reservations = data.reservations;
  if (!Array.isArray(reservations) && Array.isArray(data.lessons)) {
    reservations = data.lessons;
  }
  if (!Array.isArray(reservations)) {
    reservations = [];
  }
  return { version: 1, reservations: reservations as Record<string, unknown>[] };
}

function boolFromUnknown(x: unknown): boolean {
  if (typeof x === "boolean") {
    return x;
  }
  if (typeof x === "string") {
    return x.trim().toLowerCase() === "true";
  }
  return false;
}

function rowFromUnknown(o: Record<string, unknown>): LessonReserveRecord | null {
  const s = (x: unknown) => (x == null ? "" : String(x).trim());
  const id = s(o.lessonReserveId ?? o.lesson_reserve_id);
  const lid = s(o.lessonId ?? o.LessonID);
  if (!id || !lid) {
    return null;
  }
  const payStatus = s(o.Payment_Status ?? o.payment_status);
  return {
    lessonReserveId: id,
    lessonId: lid,
    ClubID: s(o.ClubID ?? o.clubID ?? o.club_id),
    student_id: s(o.student_id ?? o.StudentID ?? o.studentID),
    Student_Name: s(o.Student_Name ?? o.student_name ?? o.StudentName),
    status: s(o.status) || "ACTIVE",
    Payment_Status: payStatus || "UNPAID",
    Payment_Confirm: boolFromUnknown(o.Payment_Confirm ?? o.payment_confirm),
    createdAt: s(o.createdAt ?? o.created_at),
    lastUpdatedDate: s(o.lastUpdatedDate ?? o.last_updated_date),
  };
}

function nextLessonReserveId(existing: LessonReserveRecord[]): string {
  let max = 0;
  for (const r of existing) {
    const m = r.lessonReserveId.match(LR_ID_RE);
    if (m) {
      const n = Number.parseInt(m[1]!, 10);
      if (!Number.isNaN(n) && n > max) {
        max = n;
      }
    }
  }
  return `LR${String(max + 1).padStart(LR_PAD, "0")}`;
}

function migrateLessonReserveJsonKeysInPlace(clubId: string): void {
  const p = lessonReserveListPath(clubId);
  if (!p || !fs.existsSync(p)) {
    return;
  }
  let raw: string;
  try {
    raw = fs.readFileSync(p, "utf8");
  } catch {
    return;
  }
  let file: ReserveFileV1;
  try {
    file = parseReserveFile(raw);
  } catch {
    return;
  }
  let changed = false;
  for (const o of file.reservations) {
    if ("StudentID" in o && !("student_id" in o)) {
      o.student_id = o.StudentID;
      delete o.StudentID;
      changed = true;
    }
  }
  if (changed) {
    fs.writeFileSync(p, JSON.stringify(file, null, 2) + "\n", "utf8");
  }
}

export function ensureLessonReserveListFile(clubId: string): void {
  if (!isValidClubFolderId(clubId)) {
    throw new Error("Invalid club ID.");
  }
  const clubDir = path.join(dataClubRoot(), clubId.trim());
  if (!fs.existsSync(clubDir)) {
    fs.mkdirSync(clubDir, { recursive: true });
  }
  const p = lessonReserveListPath(clubId);
  if (!fs.existsSync(p)) {
    const body: ReserveFileV1 = { version: 1, reservations: [] };
    fs.writeFileSync(p, JSON.stringify(body, null, 2) + "\n", "utf8");
  }
  migrateLessonReserveJsonKeysInPlace(clubId);
}

export function loadLessonReservations(clubId: string): LessonReserveRecord[] {
  if (!isValidClubFolderId(clubId)) {
    return [];
  }
  const p = lessonReserveListPath(clubId);
  if (!fs.existsSync(p)) {
    return [];
  }
  migrateLessonReserveJsonKeysInPlace(clubId);
  try {
    const raw = fs.readFileSync(p, "utf8");
    const file = parseReserveFile(raw);
    const out: LessonReserveRecord[] = [];
    for (const o of file.reservations) {
      const r = rowFromUnknown(o);
      if (r) {
        out.push(r);
      }
    }
    return out;
  } catch {
    return [];
  }
}

export function hasActiveReservationForStudentLesson(
  clubId: string,
  lessonId: string,
  studentId: string,
): boolean {
  const lid = lessonId.trim().toUpperCase();
  const sid = studentId.trim().toUpperCase();
  return loadLessonReservations(clubId).some(
    (r) =>
      r.lessonId.trim().toUpperCase() === lid &&
      r.student_id.trim().toUpperCase() === sid &&
      r.status.toUpperCase() === "ACTIVE",
  );
}

export type LessonReserveAppendInput = Omit<
  LessonReserveRecord,
  | "lessonReserveId"
  | "createdAt"
  | "lastUpdatedDate"
  | "Payment_Status"
  | "Payment_Confirm"
  | "student_id"
> & {
  lessonReserveId?: string;
  Payment_Status?: string;
  Payment_Confirm?: boolean;
  student_id?: string;
  /** @deprecated use student_id */
  StudentID?: string;
};

export function appendLessonReservation(
  clubId: string,
  rec: LessonReserveAppendInput,
): { ok: true; lessonReserveId: string } | { ok: false; error: string } {
  ensureLessonReserveListFile(clubId);
  const p = lessonReserveListPath(clubId);
  const existing = loadLessonReservations(clubId);
  const today = new Date().toISOString().slice(0, 10);
  const lessonReserveId =
    rec.lessonReserveId?.trim() || nextLessonReserveId(existing);
  const sid = String(rec.student_id ?? rec.StudentID ?? "").trim();
  if (!sid) {
    return { ok: false, error: "student_id is required." };
  }
  const row: LessonReserveRecord = {
    lessonReserveId,
    lessonId: rec.lessonId.trim(),
    ClubID: rec.ClubID.trim(),
    student_id: sid,
    Student_Name: rec.Student_Name.trim(),
    status: (rec.status && rec.status.trim()) || "ACTIVE",
    Payment_Status:
      (rec.Payment_Status && rec.Payment_Status.trim()) || "UNPAID",
    Payment_Confirm: rec.Payment_Confirm === true,
    createdAt: today,
    lastUpdatedDate: today,
  };
  const fileRaw = fs.readFileSync(p, "utf8");
  let file: ReserveFileV1;
  try {
    file = parseReserveFile(fileRaw);
  } catch {
    file = { version: 1, reservations: [] };
  }
  file.reservations.push({
    lessonReserveId: row.lessonReserveId,
    lessonId: row.lessonId,
    ClubID: row.ClubID,
    student_id: row.student_id,
    Student_Name: row.Student_Name,
    status: row.status,
    Payment_Status: row.Payment_Status,
    Payment_Confirm: row.Payment_Confirm,
    createdAt: row.createdAt,
    lastUpdatedDate: row.lastUpdatedDate,
  });
  fs.writeFileSync(p, JSON.stringify(file, null, 2) + "\n", "utf8");
  return { ok: true, lessonReserveId: row.lessonReserveId };
}

/** Remove one ACTIVE reservation row for this student + lesson (file storage club id). */
export function removeActiveReservationForStudentLesson(
  clubId: string,
  lessonId: string,
  studentId: string,
):
  | { ok: true; lessonReserveId: string }
  | { ok: false; error: string } {
  ensureLessonReserveListFile(clubId);
  const p = lessonReserveListPath(clubId);
  const lid = lessonId.trim().toUpperCase();
  const sid = studentId.trim().toUpperCase();
  let raw: string;
  try {
    raw = fs.readFileSync(p, "utf8");
  } catch {
    return { ok: false, error: "Reservation list not found." };
  }
  let file: ReserveFileV1;
  try {
    file = parseReserveFile(raw);
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    return { ok: false, error: msg };
  }
  let removedId = "";
  const next: Record<string, unknown>[] = [];
  for (const o of file.reservations) {
    const r = rowFromUnknown(o);
    if (
      r &&
      r.lessonId.trim().toUpperCase() === lid &&
      r.student_id.trim().toUpperCase() === sid &&
      r.status.toUpperCase() === "ACTIVE"
    ) {
      if (!removedId) {
        removedId = r.lessonReserveId;
        continue;
      }
    }
    next.push(o);
  }
  if (!removedId) {
    return { ok: false, error: "No active reservation found for this lesson." };
  }
  file.reservations = next;
  fs.writeFileSync(p, JSON.stringify(file, null, 2) + "\n", "utf8");
  return { ok: true, lessonReserveId: removedId };
}

/** Remove one reservation row by lessonReserveId (any status). */
export function removeLessonReservationByReserveId(
  clubId: string,
  lessonReserveId: string,
): { ok: true } | { ok: false; error: string } {
  ensureLessonReserveListFile(clubId);
  const p = lessonReserveListPath(clubId);
  const idUpper = lessonReserveId.trim().toUpperCase();
  if (!idUpper) {
    return { ok: false, error: "Missing reservation id." };
  }
  let raw: string;
  try {
    raw = fs.readFileSync(p, "utf8");
  } catch {
    return { ok: false, error: "Reservation list not found." };
  }
  let file: ReserveFileV1;
  try {
    file = parseReserveFile(raw);
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    return { ok: false, error: msg };
  }
  const before = file.reservations.length;
  file.reservations = file.reservations.filter(
    (o) => String(o.lessonReserveId ?? "").trim().toUpperCase() !== idUpper,
  );
  if (file.reservations.length === before) {
    return { ok: false, error: "Reservation not found." };
  }
  fs.writeFileSync(p, JSON.stringify(file, null, 2) + "\n", "utf8");
  return { ok: true };
}

/** Update payment fields on one reservation row (by lessonReserveId). */
export function updateLessonReservationPaymentFields(
  clubId: string,
  lessonReserveId: string,
  fields: {
    Payment_Status: string;
    Payment_Confirm?: boolean;
  },
  opts?: { preservePaymentConfirm?: boolean },
): { ok: true } | { ok: false; error: string } {
  ensureLessonReserveListFile(clubId);
  const p = lessonReserveListPath(clubId);
  const id = lessonReserveId.trim();
  if (!id) {
    return { ok: false, error: "Missing reservation id." };
  }
  let raw: string;
  try {
    raw = fs.readFileSync(p, "utf8");
  } catch {
    return { ok: false, error: "Reservation list not found." };
  }
  let file: ReserveFileV1;
  try {
    file = parseReserveFile(raw);
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    return { ok: false, error: msg };
  }
  const today = new Date().toISOString().slice(0, 10);
  let found = false;
  for (const o of file.reservations) {
    const rid = String(o.lessonReserveId ?? "").trim();
    if (rid.toUpperCase() !== id.toUpperCase()) {
      continue;
    }
    found = true;
    o.Payment_Status = fields.Payment_Status.trim();
    if (!opts?.preservePaymentConfirm && fields.Payment_Confirm !== undefined) {
      o.Payment_Confirm = fields.Payment_Confirm;
    }
    o.lastUpdatedDate = today;
  }
  if (!found) {
    return { ok: false, error: "Reservation not found." };
  }
  fs.writeFileSync(p, JSON.stringify(file, null, 2) + "\n", "utf8");
  return { ok: true };
}
