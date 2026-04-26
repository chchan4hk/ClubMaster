import { findStudentRoleLoginByUid } from "./coachStudentLoginCsv";
import { findStudentRoleLoginByUidMongo } from "./userListMongo";
import { isMongoConfigured } from "./db/DBConnection";
import {
  deleteStudentMongoAllClubs,
  findClubUidForStudentIdMongo,
  insertStudentMongo,
  listAllStudentIdClubPairsFromMongo,
  loadStudentsFromMongo,
  patchStudentSelfContactMongo,
  patchStudentSelfProfileMongo,
  updateStudentMongo,
} from "./studentListMongo";

const STUDENT_ID_RE = /^S(\d+)$/i;
/** Club-scoped IDs in Mongo: `{ClubID}-S000001` (S + 6-digit sequence for new allocations). */
const CLUB_SCOPED_STUDENT_ID_RE = /^([A-Za-z0-9]+)-S(\d+)$/i;
const STUDENT_NEW_ID_PAD = 8;
const CLUB_SCOPED_SUFFIX_PAD = 6;

export type StudentCsvRow = {
  studentId: string;
  /** Club folder UID (e.g. `CM00000003`). */
  clubId: string;
  studentName: string;
  sex: string;
  email: string;
  phone: string;
  guardian: string;
  guardianContact: string;
  school: string;
  studentCoach: string;
  status: string;
  createdDate: string;
  remark: string;
  lastUpdateDate: string;
  dateOfBirth: string;
  joinedDate: string;
  homeAddress: string;
  country: string;
};

export const STUDENT_LIST_COLUMNS: string[] = [
  "student_id",
  "club_id",
  "full_name",
  "sex",
  "email",
  "contact_number",
  "guardian",
  "guardian_contact",
  "school",
  "student_coach",
  "status",
  "creation_date",
  "remark",
  "lastUpdate_date",
  "date_of_birth",
  "joined_date",
  "home_address",
  "country",
];

export function studentCsvRowToApiFields(c: StudentCsvRow): Record<string, string> {
  return {
    student_id: c.studentId,
    club_id: c.clubId,
    full_name: c.studentName,
    sex: c.sex,
    email: c.email,
    contact_number: c.phone,
    guardian: c.guardian,
    guardian_contact: c.guardianContact,
    school: c.school,
    student_coach: c.studentCoach,
    status: c.status,
    creation_date: c.createdDate,
    remark: c.remark,
    lastUpdate_date: c.lastUpdateDate,
    date_of_birth: c.dateOfBirth,
    joined_date: c.joinedDate,
    home_address: c.homeAddress,
    country: c.country,
  };
}

function sanitizeCell(s: string): string {
  return String(s ?? "").replace(/,/g, " ").trim();
}

/** Normalize `S…` / `CM…-S…` input for roster and prize lookups. */
export function normalizeStudentIdInput(raw: string): string | null {
  const s = String(raw ?? "").replace(/^\uFEFF/, "").trim();
  const scoped = s.match(CLUB_SCOPED_STUDENT_ID_RE);
  if (scoped) {
    const club = scoped[1]!.toUpperCase();
    const n = Number.parseInt(scoped[2]!, 10);
    if (Number.isNaN(n) || n < 0) {
      return null;
    }
    return `${club}-S${String(n).padStart(CLUB_SCOPED_SUFFIX_PAD, "0")}`;
  }
  const m = s.match(STUDENT_ID_RE);
  if (!m) {
    return null;
  }
  const n = Number.parseInt(m[1]!, 10);
  if (Number.isNaN(n) || n < 0) {
    return null;
  }
  return `S${String(n).padStart(STUDENT_NEW_ID_PAD, "0")}`;
}

export function studentIdsEqual(a: string, b: string): boolean {
  const x = String(a ?? "").replace(/^\uFEFF/, "").trim();
  const y = String(b ?? "").replace(/^\uFEFF/, "").trim();
  return x.length > 0 && x.toUpperCase() === y.toUpperCase();
}

/** Numeric part after `S` for `S123` or `CLUB-S123` (leading zeros ignored). */
function studentIdNumericSerial(id: string): number | null {
  const u = String(id ?? "").replace(/^\uFEFF/, "").trim().toUpperCase();
  if (!u) {
    return null;
  }
  let m = u.match(/^S(\d+)$/);
  if (m) {
    const n = Number.parseInt(m[1]!, 10);
    return Number.isNaN(n) ? null : n;
  }
  m = u.match(/^([A-Z0-9]+)-S(\d+)$/);
  if (m) {
    const n = Number.parseInt(m[2]!, 10);
    return Number.isNaN(n) ? null : n;
  }
  return null;
}

function scopedClubPrefixOfStudentId(id: string): string | null {
  const u = String(id ?? "").replace(/^\uFEFF/, "").trim().toUpperCase();
  const m = u.match(/^([A-Z0-9]+)-S\d+$/);
  return m ? m[1]! : null;
}

/**
 * True when a `LessonReserveList.student_id` refers to the same person as the
 * logged-in student (`sessionStudentId`, usually JWT `sub`) for this club folder.
 */
export function lessonReservationStudentIdsEqual(
  clubFolderUid: string,
  reservationStudentId: string,
  sessionStudentId: string,
): boolean {
  const r0 = String(reservationStudentId ?? "").replace(/^\uFEFF/, "").trim();
  const s0 = String(sessionStudentId ?? "").replace(/^\uFEFF/, "").trim();
  if (!r0 || !s0) {
    return false;
  }
  if (studentIdsEqual(r0, s0)) {
    return true;
  }
  const cu = String(clubFolderUid ?? "").replace(/^\uFEFF/, "").trim().toUpperCase();
  const nr = studentIdNumericSerial(r0);
  const ns = studentIdNumericSerial(s0);
  if (nr == null || ns == null || nr !== ns) {
    return false;
  }
  const rShort = /^S\d+$/i.test(r0);
  const sShort = /^S\d+$/i.test(s0);
  const pr = scopedClubPrefixOfStudentId(r0);
  const ps = scopedClubPrefixOfStudentId(s0);
  if (rShort && sShort) {
    return true;
  }
  if (rShort && ps === cu) {
    return true;
  }
  if (sShort && pr === cu) {
    return true;
  }
  if (pr && ps && pr === ps && pr === cu) {
    return true;
  }
  return false;
}

/** Mongo-only roster load. */
export async function loadStudents(clubId: string): Promise<StudentCsvRow[]> {
  if (!isMongoConfigured()) {
    throw new Error("MongoDB is required for student roster.");
  }
  return await loadStudentsFromMongo(clubId.trim());
}

/**
 * StudentID → club folder id. Mongo-only.
 */
const studentIdToClubId = new Map<string, string>();
let studentIdClubIndexReady = false;

export async function rebuildStudentIdClubIndex(): Promise<void> {
  studentIdToClubId.clear();
  if (!isMongoConfigured()) {
    studentIdClubIndexReady = true;
    return;
  }
  const pairs = await listAllStudentIdClubPairsFromMongo();
  for (const [k, v] of pairs) {
    studentIdToClubId.set(String(k).trim().toUpperCase(), String(v).trim());
  }
  studentIdClubIndexReady = true;
}

async function ensureStudentIdClubIndexAsync(): Promise<void> {
  if (!studentIdClubIndexReady) {
    await rebuildStudentIdClubIndex();
  }
}

/** First club folder id whose roster lists this StudentID, or null. */
export async function findClubUidForStudentId(studentId: string): Promise<string | null> {
  const uid = String(studentId ?? "").trim();
  if (!uid) {
    return null;
  }
  if (isMongoConfigured()) {
    const direct = await findClubUidForStudentIdMongo(uid);
    if (direct) {
      return direct;
    }
  }
  await ensureStudentIdClubIndexAsync();
  return studentIdToClubId.get(uid.toUpperCase()) ?? null;
}

export type StudentClubSessionOk = {
  ok: true;
  clubId: string;
  /** Set when this StudentID exists in that club’s student roster. */
  rosterRow: StudentCsvRow | null;
};
export type StudentClubSessionResult = StudentClubSessionOk | { ok: false; error: string };

/**
 * Resolves club folder for a student JWT (`sub` = StudentID).
 * Prefers `club_folder_uid` on student login; else roster in Mongo `UserList_Student`.
 */
export async function resolveStudentClubSession(
  studentId: string,
): Promise<StudentClubSessionResult> {
  const sid = String(studentId ?? "").trim();
  if (!sid) {
    return { ok: false, error: "Invalid session." };
  }
  let login: ReturnType<typeof findStudentRoleLoginByUid> | undefined;
  try {
    login = findStudentRoleLoginByUid(sid);
  } catch {
    login = undefined;
  }
  if (!login && isMongoConfigured()) {
    try {
      const mongoLogin = await findStudentRoleLoginByUidMongo(sid);
      if (mongoLogin) {
        login = mongoLogin;
      }
    } catch {
      /* ignore */
    }
  }
  const fromLogin = (login?.clubFolderUid ?? "").trim();
  const clubId = fromLogin || (await findClubUidForStudentId(sid));
  if (!clubId) {
    return { ok: false, error: "No club folder found for this student account." };
  }
  const roster = await loadStudents(clubId);
  let stu = roster.find((s) => studentIdsEqual(s.studentId, sid));
  if (!stu) {
    const ns = studentIdNumericSerial(sid);
    if (ns != null) {
      stu = roster.find((s) => studentIdNumericSerial(s.studentId) === ns);
    }
  }
  if (!stu) {
    return { ok: false, error: "Student not found in club roster." };
  }
  if (String(stu.status ?? "").trim().toUpperCase() !== "ACTIVE") {
    return { ok: false, error: "Student is not ACTIVE in club roster." };
  }
  return { ok: true, clubId, rosterRow: stu };
}

function nextClubScopedStudentId(clubId: string, rows: StudentCsvRow[]): string {
  const club = clubId.trim().toUpperCase();
  const esc = club.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  let max = 0;
  for (const r of rows) {
    const id = String(r.studentId ?? "").replace(/^\uFEFF/, "").trim();
    const m = id.match(new RegExp(`^${esc}-S(\\d+)$`, "i"));
    if (m) {
      const n = Number.parseInt(m[1]!, 10);
      if (!Number.isNaN(n) && n > max) {
        max = n;
      }
    }
  }
  return `${club}-S${String(max + 1).padStart(CLUB_SCOPED_SUFFIX_PAD, "0")}`;
}

export function bumpClubScopedStudentId(current: string): string {
  const s = String(current ?? "").replace(/^\uFEFF/, "").trim();
  const m = s.match(CLUB_SCOPED_STUDENT_ID_RE);
  if (!m) {
    return s;
  }
  const club = m[1]!.toUpperCase();
  const n = Number.parseInt(m[2]!, 10);
  if (Number.isNaN(n) || n < 0) {
    return s;
  }
  return `${club}-S${String(n + 1).padStart(CLUB_SCOPED_SUFFIX_PAD, "0")}`;
}

export async function allocateNextStudentId(clubId: string): Promise<string> {
  if (!isMongoConfigured()) {
    throw new Error("MongoDB is required for student roster.");
  }
  const rows = await loadStudentsFromMongo(clubId.trim());
  return nextClubScopedStudentId(clubId, rows);
}

export async function appendStudentRow(
  clubId: string,
  _clubName: string,
  input: {
    studentName: string;
    email: string;
    phone: string;
    sex?: string;
    dateOfBirth?: string;
    joinedDate?: string;
    homeAddress?: string;
    country?: string;
    guardian?: string;
    guardianContact?: string;
    school?: string;
    studentCoach?: string;
    remark?: string;
    status?: string;
    studentId?: string;
  },
): Promise<{ ok: true; studentId: string } | { ok: false; error: string }> {
  if (!isMongoConfigured()) {
    return { ok: false, error: "MongoDB is required for student roster." };
  }
  const name = sanitizeCell(input.studentName);
  if (!name) {
    return { ok: false, error: "full_name is required." };
  }
  const rows = await loadStudentsFromMongo(clubId.trim());
  const requested = String(input.studentId ?? "").trim();
  let studentId: string;
  if (requested) {
    const normalized = normalizeStudentIdInput(requested);
    if (!normalized) {
      return {
        ok: false,
        error: "Invalid StudentID format (expected S######### or {Club_ID}-S#######).",
      };
    }
    if (rows.some((r) => studentIdsEqual(r.studentId, normalized))) {
      return { ok: false, error: "StudentID already exists in student list." };
    }
    studentId = normalized;
  } else {
    studentId = nextClubScopedStudentId(clubId, rows);
  }
  const today = new Date().toISOString().slice(0, 10);
  const status = sanitizeCell(input.status || "ACTIVE") || "ACTIVE";
  const newRow: StudentCsvRow = {
    studentId,
    clubId: sanitizeCell(clubId),
    studentName: name,
    sex: sanitizeCell(input.sex ?? ""),
    email: sanitizeCell(input.email),
    phone: sanitizeCell(input.phone),
    guardian: sanitizeCell(input.guardian ?? ""),
    guardianContact: sanitizeCell(input.guardianContact ?? ""),
    school: sanitizeCell(input.school ?? ""),
    studentCoach: sanitizeCell(input.studentCoach ?? ""),
    status,
    createdDate: today,
    remark: sanitizeCell(input.remark ?? ""),
    lastUpdateDate: today,
    dateOfBirth: sanitizeCell(input.dateOfBirth ?? ""),
    joinedDate: sanitizeCell(input.joinedDate ?? ""),
    homeAddress: sanitizeCell(input.homeAddress ?? ""),
    country: sanitizeCell(input.country ?? ""),
  };
  await insertStudentMongo(newRow, clubId.trim());
  return { ok: true, studentId };
}

export async function updateStudentRow(
  clubId: string,
  _clubName: string,
  studentId: string,
  input: {
    studentName: string;
    email: string;
    phone: string;
    sex?: string;
    dateOfBirth?: string;
    joinedDate?: string;
    homeAddress?: string;
    country?: string;
    guardian?: string;
    guardianContact?: string;
    school?: string;
    studentCoach?: string;
    remark?: string;
    status?: string;
  },
): Promise<{ ok: true } | { ok: false; error: string }> {
  if (!isMongoConfigured()) {
    return { ok: false, error: "MongoDB is required for student roster." };
  }
  const id = String(studentId ?? "").trim();
  if (!id) {
    return { ok: false, error: "StudentID is required." };
  }
  const name = sanitizeCell(input.studentName);
  if (!name) {
    return { ok: false, error: "full_name is required." };
  }
  const today = new Date().toISOString().slice(0, 10);
  const status = sanitizeCell(input.status || "ACTIVE") || "ACTIVE";
  const updated: StudentCsvRow = {
    studentId: id,
    clubId: sanitizeCell(clubId),
    studentName: name,
    sex: sanitizeCell(input.sex ?? ""),
    email: sanitizeCell(input.email),
    phone: sanitizeCell(input.phone),
    guardian: sanitizeCell(input.guardian ?? ""),
    guardianContact: sanitizeCell(input.guardianContact ?? ""),
    school: sanitizeCell(input.school ?? ""),
    studentCoach: sanitizeCell(input.studentCoach ?? ""),
    status,
    createdDate: "",
    remark: sanitizeCell(input.remark ?? ""),
    lastUpdateDate: today,
    dateOfBirth: sanitizeCell(input.dateOfBirth ?? ""),
    joinedDate: sanitizeCell(input.joinedDate ?? ""),
    homeAddress: sanitizeCell(input.homeAddress ?? ""),
    country: sanitizeCell(input.country ?? ""),
  };
  const { matched } = await updateStudentMongo(clubId.trim(), id, updated);
  if (matched < 1) {
    return { ok: false, error: "Student not found." };
  }
  return { ok: true };
}

export async function purgeStudentRowFromAllClubFolders(
  studentId: string,
): Promise<{ ok: true; updatedClubIds: string[] } | { ok: false; error: string }> {
  const id = String(studentId ?? "").trim();
  if (!id) {
    return { ok: false, error: "StudentID is required." };
  }
  if (!isMongoConfigured()) {
    return { ok: false, error: "MongoDB is required for student roster." };
  }
  const updatedClubIds = await deleteStudentMongoAllClubs(id);
  if (updatedClubIds.length > 0) {
    await rebuildStudentIdClubIndex();
  }
  return { ok: true, updatedClubIds };
}

/**
 * Student self-service: update email + contact on `UserList_Student` (Mongo).
 */
export async function patchStudentSelfContact(
  studentId: string,
  emailRaw: string,
  phoneRaw: string,
): Promise<{ ok: true } | { ok: false; error: string }> {
  const sid = String(studentId ?? "").trim();
  if (!sid) {
    return { ok: false, error: "Invalid session." };
  }
  const sess = await resolveStudentClubSession(sid);
  if (!sess.ok) {
    return { ok: false, error: sess.error };
  }
  if (!sess.rosterRow) {
    return { ok: false, error: "Student not found in club roster." };
  }
  const rosterStudentId = String(sess.rosterRow.studentId ?? "").trim() || sid;
  const email = sanitizeCell(emailRaw);
  const phone = sanitizeCell(phoneRaw);
  const clubId = sess.clubId.trim();
  const { matched } = await patchStudentSelfContactMongo(
    clubId,
    rosterStudentId,
    email,
    phone,
  );
  if (matched < 1) {
    return { ok: false, error: "Student not found." };
  }
  return { ok: true };
}

export async function patchStudentSelfProfile(
  studentId: string,
  patch: {
    email_address: string;
    contact_number: string;
    school: string;
    home_address: string;
  },
): Promise<{ ok: true } | { ok: false; error: string }> {
  const sid = String(studentId ?? "").trim();
  if (!sid) {
    return { ok: false, error: "Invalid session." };
  }
  const sess = await resolveStudentClubSession(sid);
  if (!sess.ok) {
    return { ok: false, error: sess.error };
  }
  if (!sess.rosterRow) {
    return { ok: false, error: "Student not found in club roster." };
  }
  const rosterStudentId = String(sess.rosterRow.studentId ?? "").trim() || sid;
  const clubId = sess.clubId.trim();
  const email = sanitizeCell(patch.email_address);
  const phone = sanitizeCell(patch.contact_number);
  const school = sanitizeCell(patch.school);
  const home = sanitizeCell(patch.home_address);
  const { matched } = await patchStudentSelfProfileMongo(clubId, rosterStudentId, {
    email,
    contact_number: phone,
    school,
    home_address: home,
  });
  if (matched < 1) {
    return { ok: false, error: "Student not found." };
  }
  return { ok: true };
}

