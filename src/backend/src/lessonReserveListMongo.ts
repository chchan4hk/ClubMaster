import type { Filter } from "mongodb";
import { MongoServerError } from "mongodb";
import { isValidClubFolderId } from "./coachListCsv";
import {
  ensureLessonReserveListCollection,
  getLessonReserveListCollection,
  isMongoConfigured,
  type LessonReserveListDocument,
} from "./db/DBConnection";
import {
  appendLessonReservation,
  computeNextLessonReserveId,
  ensureLessonReserveListFile,
  loadLessonReservations,
  parseLessonReserveObject,
  removeActiveReservationForStudentLesson,
  removeLessonReservationByReserveId,
  updateLessonReservationPaymentFields,
  type LessonReserveAppendInput,
  type LessonReserveRecord,
} from "./lessonReserveList";

let lessonReserveCollectionEnsured = false;

async function ensureLessonReserveListCollectionOnce(): Promise<void> {
  if (lessonReserveCollectionEnsured) {
    return;
  }
  lessonReserveCollectionEnsured = true;
  try {
    await ensureLessonReserveListCollection();
  } catch (e) {
    lessonReserveCollectionEnsured = false;
    throw e;
  }
}

function escapeRegExp(s: string): string {
  return s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

/**
 * Rows for this club data partition: `ClubID` matches the folder id, or legacy / migrated
 * `{folder}-LR…` ids (covers edge cases where `ClubID` differed from storage folder).
 */
function lessonReservePartitionFilter(
  clubFolderUid: string,
): Filter<LessonReserveListDocument> {
  const club = clubFolderUid.replace(/^\uFEFF/, "").trim();
  const esc = escapeRegExp(club);
  return {
    $or: [
      { ClubID: new RegExp(`^${esc}$`, "i") },
      { lessonReserveId: new RegExp(`^${esc}-LR`, "i") },
    ],
  };
}

function mongoDocToRecord(doc: LessonReserveListDocument): LessonReserveRecord {
  const o: Record<string, unknown> = { ...doc };
  delete o._id;
  const r = parseLessonReserveObject(o as Record<string, unknown>);
  if (r) {
    return r;
  }
  return {
    lessonReserveId: String(doc.lessonReserveId ?? ""),
    lessonId: String(doc.lessonId ?? ""),
    ClubID: String(doc.ClubID ?? ""),
    student_id: String(doc.student_id ?? ""),
    Student_Name: String(doc.Student_Name ?? ""),
    status: String(doc.status ?? "ACTIVE"),
    Payment_Status: String(doc.Payment_Status ?? "UNPAID"),
    Payment_Confirm: doc.Payment_Confirm === true,
    createdAt: String(doc.createdAt ?? ""),
    lastUpdatedDate: String(doc.lastUpdatedDate ?? ""),
  };
}

function recordToInsert(row: LessonReserveRecord): Omit<LessonReserveListDocument, "_id"> {
  return {
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
  };
}

export async function loadLessonReservationsMongo(
  clubFolderUid: string,
): Promise<LessonReserveRecord[]> {
  if (!isValidClubFolderId(clubFolderUid.trim())) {
    return [];
  }
  await ensureLessonReserveListCollectionOnce();
  const col = await getLessonReserveListCollection();
  const docs = await col
    .find(lessonReservePartitionFilter(clubFolderUid))
    .sort({ lessonReserveId: 1 })
    .toArray();
  return docs.map((d) => mongoDocToRecord(d));
}

/**
 * When MongoDB is configured, reads only from `ClubMaster_DB.LessonReserveList` (see
 * `resolveLessonReserveListDatabaseName`). Otherwise uses `LessonReserveList.json`.
 */
export async function loadLessonReservationsPreferred(
  clubId: string,
): Promise<LessonReserveRecord[]> {
  if (!isMongoConfigured()) {
    return loadLessonReservations(clubId);
  }
  return loadLessonReservationsMongo(clubId);
}

export async function ensureLessonReserveListPreferred(clubId: string): Promise<void> {
  if (!isValidClubFolderId(clubId.trim())) {
    throw new Error("Invalid club ID.");
  }
  if (!isMongoConfigured()) {
    ensureLessonReserveListFile(clubId);
    return;
  }
  await ensureLessonReserveListCollectionOnce();
}

export async function appendLessonReservationPreferred(
  clubId: string,
  rec: LessonReserveAppendInput,
): Promise<{ ok: true; lessonReserveId: string } | { ok: false; error: string }> {
  if (!isMongoConfigured()) {
    return appendLessonReservation(clubId, rec);
  }
  await ensureLessonReserveListCollectionOnce();
  const col = await getLessonReserveListCollection();
  const existing = await loadLessonReservationsMongo(clubId);
  const today = new Date().toISOString().slice(0, 10);
  const sid = String(rec.student_id ?? rec.StudentID ?? "").trim();
  if (!sid) {
    return { ok: false, error: "student_id is required." };
  }
  const folderUid =
    (isValidClubFolderId(clubId.trim()) ? clubId.trim() : "") ||
    (isValidClubFolderId(rec.ClubID.trim()) ? rec.ClubID.trim() : "");
  if (!folderUid) {
    return { ok: false, error: "Invalid club folder id for reservation." };
  }
  const lessonReserveId =
    rec.lessonReserveId?.trim() ||
    computeNextLessonReserveId(folderUid, existing);
  const row: LessonReserveRecord = {
    lessonReserveId,
    lessonId: rec.lessonId.trim(),
    ClubID: folderUid,
    student_id: sid,
    Student_Name: rec.Student_Name.trim(),
    status: (rec.status && rec.status.trim()) || "ACTIVE",
    Payment_Status:
      (rec.Payment_Status && rec.Payment_Status.trim()) || "UNPAID",
    Payment_Confirm: rec.Payment_Confirm === true,
    createdAt: today,
    lastUpdatedDate: today,
  };
  try {
    await col.insertOne(recordToInsert(row));
  } catch (insertErr) {
    if (insertErr instanceof MongoServerError && insertErr.code === 11000) {
      return { ok: false, error: "Reservation id already exists." };
    }
    throw insertErr;
  }
  return { ok: true, lessonReserveId: row.lessonReserveId };
}

export async function removeActiveReservationForStudentLessonPreferred(
  clubId: string,
  lessonId: string,
  studentId: string,
): Promise<
  | { ok: true; lessonReserveId: string }
  | { ok: false; error: string }
> {
  if (!isMongoConfigured()) {
    return removeActiveReservationForStudentLesson(clubId, lessonId, studentId);
  }
  try {
    await ensureLessonReserveListCollectionOnce();
    const col = await getLessonReserveListCollection();
    const lid = lessonId.trim().toUpperCase();
    const sid = studentId.trim().toUpperCase();
    const all = await loadLessonReservationsMongo(clubId);
    const match = all.find(
      (r) =>
        r.lessonId.trim().toUpperCase() === lid &&
        r.student_id.trim().toUpperCase() === sid &&
        r.status.toUpperCase() === "ACTIVE",
    );
    if (!match) {
      return { ok: false, error: "No active reservation found for this lesson." };
    }
    const idRe = new RegExp(`^${escapeRegExp(match.lessonReserveId)}$`, "i");
    const del = await col.deleteOne({
      $and: [lessonReservePartitionFilter(clubId), { lessonReserveId: idRe }],
    });
    if (del.deletedCount === 0) {
      return { ok: false, error: "No active reservation found for this lesson." };
    }
    return { ok: true, lessonReserveId: match.lessonReserveId };
  } catch (e) {
    console.warn(
      "[LessonReserveList] Mongo remove active failed.",
      e instanceof Error ? e.message : e,
    );
    throw e;
  }
}

export async function removeLessonReservationByReserveIdPreferred(
  clubId: string,
  lessonReserveId: string,
): Promise<{ ok: true } | { ok: false; error: string }> {
  if (!isMongoConfigured()) {
    return removeLessonReservationByReserveId(clubId, lessonReserveId);
  }
  try {
    await ensureLessonReserveListCollectionOnce();
    const col = await getLessonReserveListCollection();
    const idUpper = lessonReserveId.trim().toUpperCase();
    if (!idUpper) {
      return { ok: false, error: "Missing reservation id." };
    }
    const idRe = new RegExp(`^${escapeRegExp(lessonReserveId.trim())}$`, "i");
    const del = await col.deleteOne({
      $and: [lessonReservePartitionFilter(clubId), { lessonReserveId: idRe }],
    });
    if (del.deletedCount === 0) {
      return { ok: false, error: "Reservation not found." };
    }
    return { ok: true };
  } catch (e) {
    console.warn(
      "[LessonReserveList] Mongo remove by id failed.",
      e instanceof Error ? e.message : e,
    );
    throw e;
  }
}

export async function updateLessonReservationPaymentFieldsPreferred(
  clubId: string,
  lessonReserveId: string,
  fields: {
    Payment_Status: string;
    Payment_Confirm?: boolean;
  },
  opts?: { preservePaymentConfirm?: boolean },
): Promise<{ ok: true } | { ok: false; error: string }> {
  if (!isMongoConfigured()) {
    return updateLessonReservationPaymentFields(
      clubId,
      lessonReserveId,
      fields,
      opts,
    );
  }
  try {
    await ensureLessonReserveListCollectionOnce();
    const col = await getLessonReserveListCollection();
    const id = lessonReserveId.trim();
    if (!id) {
      return { ok: false, error: "Missing reservation id." };
    }
    const today = new Date().toISOString().slice(0, 10);
    const idRe = new RegExp(`^${escapeRegExp(id)}$`, "i");
    const $set: Record<string, unknown> = {
      Payment_Status: fields.Payment_Status.trim(),
      lastUpdatedDate: today,
    };
    if (!opts?.preservePaymentConfirm && fields.Payment_Confirm !== undefined) {
      $set.Payment_Confirm = fields.Payment_Confirm;
    }
    const res = await col.updateMany(
      { $and: [lessonReservePartitionFilter(clubId), { lessonReserveId: idRe }] },
      { $set },
    );
    if (res.matchedCount === 0) {
      return { ok: false, error: "Reservation not found." };
    }
    return { ok: true };
  } catch (e) {
    console.warn(
      "[LessonReserveList] Mongo update payment fields failed.",
      e instanceof Error ? e.message : e,
    );
    throw e;
  }
}

export async function hasActiveReservationForStudentLessonPreferred(
  clubId: string,
  lessonId: string,
  studentId: string,
): Promise<boolean> {
  const list = await loadLessonReservationsPreferred(clubId);
  const lid = lessonId.trim().toUpperCase();
  const sid = studentId.trim().toUpperCase();
  return list.some(
    (r) =>
      r.lessonId.trim().toUpperCase() === lid &&
      r.student_id.trim().toUpperCase() === sid &&
      r.status.toUpperCase() === "ACTIVE",
  );
}
