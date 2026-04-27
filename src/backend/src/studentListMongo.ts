/**
 * MongoDB `UserList_Student` roster (default DB {@link DEFAULT_MONGO_APP_DATABASE} / `ClubMaster_DB`).
 * Used when {@link isMongoConfigured} is true instead of `data_club/{clubId}/UserList_Student.json`.
 */
import type { StudentCsvRow } from "./studentListCsv";
import {
  getUserListStudentCollection,
  isMongoConfigured,
  type UserListStudentDocument,
} from "./db/DBConnection";
import { isValidClubFolderId } from "./coachListCsv";

function escapeRegex(s: string): string {
  return s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function clubFolderRegex(clubFolderUid: string): RegExp {
  return new RegExp(`^${escapeRegex(clubFolderUid.trim())}$`, "i");
}

export function mongoStudentDocToCsvRow(doc: UserListStudentDocument): StudentCsvRow {
  const d = doc as unknown as Record<string, unknown>;
  const studentCoachRaw =
    doc.student_coach ??
    d["Student_coach"] ??
    d["student_Coach"] ??
    d["UserCoach"] ??
    d["user_coach"] ??
    "";
  return {
    studentId: String(doc.student_id ?? "").trim(),
    clubId: String(
      doc.club_id ?? doc.club_folder_uid ?? doc.ClubID ?? "",
    ).trim(),
    studentName: String(doc.full_name ?? "").trim(),
    sex: String(doc.sex ?? "").trim(),
    email: String(doc.email ?? "").trim(),
    phone: String(doc.contact_number ?? "").trim(),
    guardian: String(doc.guardian ?? "").trim(),
    guardianContact: String(doc.guardian_contact ?? "").trim(),
    school: String(doc.school ?? "").trim(),
    studentCoach: String(studentCoachRaw).trim(),
    status: String(doc.status ?? "ACTIVE").trim(),
    createdDate: String(doc.creation_date ?? "").trim(),
    remark: String(doc.remark ?? "").trim(),
    lastUpdateDate: String(doc.lastUpdate_date ?? "").trim(),
    dateOfBirth: String(doc.date_of_birth ?? "").trim(),
    joinedDate: String(doc.joined_date ?? "").trim(),
    homeAddress: String(doc.home_address ?? "").trim(),
    country: String(doc.country ?? "").trim(),
  };
}

export function csvRowToUserListStudentInsert(
  row: StudentCsvRow,
  clubFolderUid: string,
): Omit<UserListStudentDocument, "_id"> {
  const folder = clubFolderUid.trim();
  const cid = (row.clubId && row.clubId.trim()) || folder;
  return {
    club_folder_uid: folder,
    student_id: row.studentId.trim(),
    club_id: cid,
    full_name: row.studentName,
    sex: row.sex,
    email: row.email,
    contact_number: row.phone,
    guardian: row.guardian,
    guardian_contact: row.guardianContact,
    school: row.school,
    student_coach: row.studentCoach,
    status: row.status,
    creation_date: row.createdDate,
    remark: row.remark,
    lastUpdate_date: row.lastUpdateDate,
    date_of_birth: row.dateOfBirth,
    joined_date: row.joinedDate,
    home_address: row.homeAddress,
    country: row.country,
  };
}

/** Roster rows for this club folder (match `club_folder_uid` or `club_id`). */
export async function loadStudentsFromMongo(
  clubFolderUid: string,
): Promise<StudentCsvRow[]> {
  if (!isMongoConfigured() || !isValidClubFolderId(clubFolderUid)) {
    return [];
  }
  const re = clubFolderRegex(clubFolderUid);
  const coll = await getUserListStudentCollection();
  /** Scope roster to this club: match `club_id` (canonical), `club_folder_uid`, or legacy `ClubID`. */
  const docs = await coll
    .find({
      $or: [{ club_id: re }, { club_folder_uid: re }, { ClubID: re }],
    })
    .sort({ student_id: 1 })
    .toArray();
  return docs.map(mongoStudentDocToCsvRow);
}

export async function insertStudentMongo(
  row: StudentCsvRow,
  clubFolderUid: string,
): Promise<void> {
  const coll = await getUserListStudentCollection();
  await coll.insertOne(csvRowToUserListStudentInsert(row, clubFolderUid));
}

/** Partial roster update for self-service profile (email + phone only). */
export async function patchStudentSelfContactMongo(
  clubFolderUid: string,
  studentId: string,
  email: string,
  contactNumber: string,
): Promise<{ matched: number }> {
  const coll = await getUserListStudentCollection();
  const today = new Date().toISOString().slice(0, 10);
  const reClub = clubFolderRegex(clubFolderUid);
  const res = await coll.updateOne(
    {
      $or: [{ club_id: reClub }, { club_folder_uid: reClub }, { ClubID: reClub }],
      student_id: new RegExp(`^${escapeRegex(studentId.trim())}$`, "i"),
    },
    {
      $set: {
        email,
        contact_number: contactNumber,
        lastUpdate_date: today,
      },
    },
  );
  return { matched: res.matchedCount };
}

/** Partial roster update for student self-service profile fields. */
export async function patchStudentSelfProfileMongo(
  clubFolderUid: string,
  studentId: string,
  patch: {
    email: string;
    contact_number: string;
    school: string;
    home_address: string;
  },
): Promise<{ matched: number }> {
  const coll = await getUserListStudentCollection();
  const today = new Date().toISOString().slice(0, 10);
  const reClub = clubFolderRegex(clubFolderUid);
  const res = await coll.updateOne(
    {
      $or: [{ club_id: reClub }, { club_folder_uid: reClub }, { ClubID: reClub }],
      student_id: new RegExp(`^${escapeRegex(studentId.trim())}$`, "i"),
    },
    {
      $set: {
        email: patch.email,
        contact_number: patch.contact_number,
        school: patch.school,
        home_address: patch.home_address,
        lastUpdate_date: today,
      },
    },
  );
  return { matched: res.matchedCount };
}

export async function updateStudentMongo(
  clubFolderUid: string,
  studentId: string,
  row: StudentCsvRow,
): Promise<{ matched: number }> {
  const coll = await getUserListStudentCollection();
  const doc = csvRowToUserListStudentInsert(
    { ...row, studentId: row.studentId.trim() },
    clubFolderUid,
  );
  const reClub = clubFolderRegex(clubFolderUid);
  const res = await coll.updateOne(
    {
      $or: [{ club_id: reClub }, { club_folder_uid: reClub }, { ClubID: reClub }],
      student_id: new RegExp(`^${escapeRegex(studentId.trim())}$`, "i"),
    },
    { $set: doc },
  );
  return { matched: res.matchedCount };
}

export async function deleteStudentMongo(
  clubFolderUid: string,
  studentId: string,
): Promise<boolean> {
  const coll = await getUserListStudentCollection();
  const reClub = clubFolderRegex(clubFolderUid);
  const res = await coll.deleteOne({
    $or: [{ club_id: reClub }, { club_folder_uid: reClub }, { ClubID: reClub }],
    student_id: new RegExp(`^${escapeRegex(studentId.trim())}$`, "i"),
  });
  return res.deletedCount > 0;
}

/** Delete every roster row with this `student_id` (any club). */
export async function deleteStudentMongoAllClubs(studentId: string): Promise<string[]> {
  const coll = await getUserListStudentCollection();
  const re = new RegExp(`^${escapeRegex(studentId.trim())}$`, "i");
  const found = await coll.find({ student_id: re }).toArray();
  const clubs = found
    .map((d) => String(d.club_folder_uid ?? "").trim())
    .filter(Boolean);
  await coll.deleteMany({ student_id: re });
  return Array.from(new Set(clubs));
}

export async function findClubUidForStudentIdMongo(
  studentId: string,
): Promise<string | null> {
  if (!isMongoConfigured()) {
    return null;
  }
  const coll = await getUserListStudentCollection();
  const d = await coll.findOne({
    student_id: new RegExp(`^${escapeRegex(studentId.trim())}$`, "i"),
  });
  if (!d) {
    return null;
  }
  const c = String(
    d.club_folder_uid ??
      d.club_id ??
      (d as { ClubID?: string }).ClubID ??
      "",
  ).trim();
  return c && isValidClubFolderId(c) ? c : null;
}

export async function listAllStudentIdClubPairsFromMongo(): Promise<
  Map<string, string>
> {
  const out = new Map<string, string>();
  if (!isMongoConfigured()) {
    return out;
  }
  const coll = await getUserListStudentCollection();
  const cur = coll.find(
    {},
    { projection: { student_id: 1, club_folder_uid: 1 } },
  );
  for await (const d of cur) {
    const u = String(d.student_id ?? "")
      .replace(/^\uFEFF/, "")
      .trim()
      .toUpperCase();
    const c = String(
      d.club_folder_uid ??
        d.club_id ??
        (d as { ClubID?: string }).ClubID ??
        "",
    ).trim();
    if (u && c && isValidClubFolderId(c) && !out.has(u)) {
      out.set(u, c);
    }
  }
  return out;
}

const USERLIST_STUDENT_CLUB_STUDENT_INDEX = "userlist_student_club_student";

function rosterCompoundKeyMatches(
  key: Record<string, unknown> | undefined,
  expected: Record<string, 1>,
): boolean {
  if (!key || typeof key !== "object") {
    return false;
  }
  const expKeys = Object.keys(expected).sort().join("\0");
  const gotKeys = Object.keys(key).sort().join("\0");
  if (expKeys !== gotKeys) {
    return false;
  }
  for (const k of Object.keys(expected)) {
    if (key[k] !== 1) {
      return false;
    }
  }
  return true;
}

/**
 * Ensures the canonical compound index on `UserList_Student`.
 * If an older index exists on the same keys with Mongo’s default name (e.g.
 * `club_folder_uid_1_student_id_1`), it is dropped first — otherwise
 * `createIndex` fails with “Index already exists with a different name”.
 */
export async function ensureUserListStudentIndexes(): Promise<void> {
  if (!isMongoConfigured()) {
    return;
  }
  const coll = await getUserListStudentCollection();
  const compound = { club_folder_uid: 1 as const, student_id: 1 as const };
  const existing = await coll.indexes();
  for (const ix of existing) {
    const name = String(ix.name ?? "");
    if (!name || name === "_id_") {
      continue;
    }
    const key = ix.key as Record<string, unknown> | undefined;
    if (
      rosterCompoundKeyMatches(key, compound) &&
      name !== USERLIST_STUDENT_CLUB_STUDENT_INDEX
    ) {
      await coll.dropIndex(name);
    }
  }
  await coll.createIndex(compound, {
    unique: true,
    name: USERLIST_STUDENT_CLUB_STUDENT_INDEX,
  });
}
