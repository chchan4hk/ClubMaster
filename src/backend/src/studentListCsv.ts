import fs from "fs";
import path from "path";
import {
  parseCsvLine,
  clubDataDir,
  isValidClubFolderId,
  getDataClubRootPath,
} from "./coachListCsv";
import { findStudentRoleLoginByUid } from "./coachStudentLoginCsv";
import { findStudentRoleLoginByUidMongo } from "./userListMongo";
import { readFileCached } from "./dataFileCache";
import { isMongoConfigured, USER_LIST_STUDENT_COLLECTION } from "./db/DBConnection";
import {
  deleteStudentMongo,
  deleteStudentMongoAllClubs,
  findClubUidForStudentIdMongo,
  insertStudentMongo,
  listAllStudentIdClubPairsFromMongo,
  loadStudentsFromMongo,
  updateStudentMongo,
} from "./studentListMongo";

const STUDENT_ID_RE = /^S(\d+)$/i;
/** New StudentID allocations: S + 9 digits (e.g. S000000001), aligned with student login UID. */
const STUDENT_NEW_ID_PAD = 9;
/** Club-scoped IDs in Mongo: `{ClubID}-S0000001` (S + 7-digit sequence). */
const CLUB_SCOPED_STUDENT_ID_RE = /^([A-Za-z0-9]+)-S(\d+)$/i;
const CLUB_SCOPED_SUFFIX_PAD = 7;

/** Legacy filename; migrated to JSON on first access. */
const LEGACY_STUDENT_LIST_CSV = "UserList_Student.csv";

function studentIdSequenceNumber(studentId: string): number | null {
  const s = studentId.replace(/^\uFEFF/, "").trim();
  const m = s.match(STUDENT_ID_RE);
  if (!m) {
    return null;
  }
  const n = Number.parseInt(m[1]!, 10);
  return Number.isNaN(n) ? null : n;
}

/** Normalize `S…` / `CM…-S…` input for roster and prize lookups. */
export function normalizeStudentIdInput(raw: string): string | null {
  const s = raw.replace(/^\uFEFF/, "").trim();
  const scoped = s.match(CLUB_SCOPED_STUDENT_ID_RE);
  if (scoped) {
    const club = scoped[1]!.toUpperCase();
    const n = Number.parseInt(scoped[2]!, 10);
    if (Number.isNaN(n) || n < 0 || !isValidClubFolderId(club)) {
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

export type StudentCsvRow = {
  studentId: string;
  /** Club folder UID (e.g. `CM00000003`), same as `club_id` in `UserList_Student.json`. */
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

export const STUDENT_LIST_FILENAME = "UserList_Student.json";

type StudentListFileV1 = {
  version: 1;
  students: Record<string, unknown>[];
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

export const STUDENT_LIST_HEADER = STUDENT_LIST_COLUMNS.join(",");

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

export function studentRowToRecord(r: StudentCsvRow): Record<string, string> {
  return studentCsvRowToApiFields(r);
}

function dataClubRoot(): string {
  return getDataClubRootPath();
}

export function studentListPath(clubId: string): string {
  const dir = clubDataDir(clubId);
  return dir ? path.join(dir, STUDENT_LIST_FILENAME) : "";
}

export function studentListResolvedPath(clubId: string): string {
  const id = clubId.trim();
  if (!isValidClubFolderId(id)) {
    return "";
  }
  return path.normalize(studentListPath(id));
}

function normHeader(h: string): string {
  return h
    .replace(/^\uFEFF/, "")
    .replace(/^"|"$/g, "")
    .replace(/\t/g, " ")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ");
}

function normCompact(h: string): string {
  return normHeader(h).replace(/\s/g, "");
}

function colIndex(headerCells: string[], candidates: string[]): number {
  const norms = headerCells.map(normHeader);
  const compacts = headerCells.map(normCompact);
  for (const cand of candidates) {
    const n = normHeader(cand);
    let i = norms.indexOf(n);
    if (i >= 0) {
      return i;
    }
    const c = normCompact(cand);
    i = compacts.indexOf(c);
    if (i >= 0) {
      return i;
    }
  }
  return -1;
}

type StudentColIdx = {
  studentId: number;
  clubId: number;
  studentName: number;
  sex: number;
  email: number;
  phone: number;
  guardian: number;
  guardianContact: number;
  school: number;
  studentCoach: number;
  status: number;
  createdDate: number;
  remark: number;
  lastUpdateDate: number;
  dateOfBirth: number;
  joinedDate: number;
  homeAddress: number;
  country: number;
};

function resolveStudentColumnIndices(headerCells: string[]): StudentColIdx {
  return {
    studentId: colIndex(headerCells, [
      "student_id",
      "StudentID",
      "studentid",
      "Student ID",
      "student id",
    ]),
    clubId: colIndex(headerCells, [
      "club_id",
      "ClubID",
      "Club Id",
      "club id",
      "club_name",
      "Club Name",
      "ClubName",
      "club name",
      "club",
    ]),
    studentName: colIndex(headerCells, [
      "full_name",
      "Full Name",
      "Full name",
      "full name",
    ]),
    sex: colIndex(headerCells, ["sex", "Sex"]),
    email: colIndex(headerCells, [
      "email",
      "Email",
      "Email Address",
      "email address",
    ]),
    phone: colIndex(headerCells, [
      "contact_number",
      "Contact Number",
      "contact number",
      "Phone",
    ]),
    guardian: colIndex(headerCells, ["guardian", "Guardian"]),
    guardianContact: colIndex(headerCells, [
      "guardian_contact",
      "Guardian Contact",
      "guardian contact",
    ]),
    school: colIndex(headerCells, ["school", "School"]),
    studentCoach: colIndex(headerCells, [
      "student_coach",
      "Student Coach",
      "student coach",
    ]),
    status: colIndex(headerCells, ["status", "Status"]),
    createdDate: colIndex(headerCells, [
      "creation_date",
      "created_at",
      "Created At",
      "created_date",
      "Created at",
    ]),
    remark: colIndex(headerCells, ["remark", "Remark"]),
    lastUpdateDate: colIndex(headerCells, [
      "lastUpdate_date",
      "Last Update Date",
      "last update date",
      "last_update_date",
    ]),
    dateOfBirth: colIndex(headerCells, [
      "date_of_birth",
      "Date of Birth",
      "date of birth",
      "DOB",
    ]),
    joinedDate: colIndex(headerCells, [
      "joined_date",
      "Joined Date",
      "joined date",
    ]),
    homeAddress: colIndex(headerCells, [
      "home_address",
      "Home Address",
      "home address",
    ]),
    country: colIndex(headerCells, ["country", "Country"]),
  };
}

function ensureStudentCsvIndices(idx: StudentColIdx): void {
  if (idx.studentId < 0 || idx.studentName < 0) {
    throw new Error(
      "UserList_Student.csv: need student_id (or StudentID) and a name column (full_name).",
    );
  }
}

/** Parse legacy CSV (migration only). */
function parseAllStudentsFromCsvPath(csvPath: string): StudentCsvRow[] {
  if (!fs.existsSync(csvPath)) {
    return [];
  }
  const raw = fs.readFileSync(csvPath, "utf8");
  const lines = raw.split(/\r?\n/).filter((l) => l.trim());
  if (lines.length < 2) {
    return [];
  }
  const headerLine = lines[0]!.replace(/^\uFEFF/, "");
  const headerCells = parseCsvLine(headerLine);
  const idx = resolveStudentColumnIndices(headerCells);
  ensureStudentCsvIndices(idx);
  const out: StudentCsvRow[] = [];
  const get = (cells: string[], ix: number) =>
    ix >= 0 && ix < cells.length ? (cells[ix] ?? "").trim() : "";

  for (let i = 1; i < lines.length; i++) {
    const cells = parseCsvLine(lines[i]!);
    if (cells.length < 1) {
      continue;
    }
    while (cells.length < headerCells.length) {
      cells.push("");
    }
    const studentId = get(cells, idx.studentId);
    if (!studentId) {
      continue;
    }
    out.push({
      studentId,
      clubId: get(cells, idx.clubId),
      studentName: get(cells, idx.studentName),
      sex: get(cells, idx.sex),
      email: get(cells, idx.email),
      phone: get(cells, idx.phone),
      guardian: get(cells, idx.guardian),
      guardianContact: get(cells, idx.guardianContact),
      school: get(cells, idx.school),
      studentCoach: get(cells, idx.studentCoach),
      status: get(cells, idx.status) || "ACTIVE",
      createdDate: get(cells, idx.createdDate),
      remark: get(cells, idx.remark),
      lastUpdateDate: get(cells, idx.lastUpdateDate),
      dateOfBirth: get(cells, idx.dateOfBirth),
      joinedDate: get(cells, idx.joinedDate),
      homeAddress: get(cells, idx.homeAddress),
      country: get(cells, idx.country),
    });
  }
  return out;
}

function strVal(x: unknown): string {
  if (x == null) {
    return "";
  }
  return String(x).trim();
}

function recordToStudentRow(o: Record<string, unknown>): StudentCsvRow | null {
  const studentId = strVal(o.student_id ?? o.StudentID ?? o.studentID);
  if (!studentId) {
    return null;
  }
  return {
    studentId,
    clubId: strVal(o.club_id ?? o.club_name ?? o.Club_name),
    studentName: strVal(o.full_name),
    sex: strVal(o.sex),
    email: strVal(o.email),
    phone: strVal(o.contact_number),
    guardian: strVal(o.guardian),
    guardianContact: strVal(o.guardian_contact),
    school: strVal(o.school),
    studentCoach: strVal(o.student_coach),
    status: strVal(o.status) || "ACTIVE",
    createdDate: strVal(
      o.creation_date ?? o.created_at ?? o.Created_at ?? o.created_date,
    ),
    remark: strVal(o.remark),
    lastUpdateDate: strVal(o.lastUpdate_date),
    dateOfBirth: strVal(o.date_of_birth),
    joinedDate: strVal(o.joined_date),
    homeAddress: strVal(o.home_address),
    country: strVal(o.country),
  };
}

function parseStudentListJson(raw: string): StudentListFileV1 {
  const data = JSON.parse(raw) as unknown;
  if (!data || typeof data !== "object") {
    throw new Error("Invalid UserList_Student.json root.");
  }
  const rec = data as Record<string, unknown>;
  const version = Number(rec.version);
  if (version !== 1) {
    throw new Error("Unsupported UserList_Student.json version.");
  }
  const students = rec.students;
  if (!Array.isArray(students)) {
    throw new Error("UserList_Student.json must contain a students array.");
  }
  return { version: 1, students: students as Record<string, unknown>[] };
}

function stripCredentialKeysFromStudentObjects(
  students: Record<string, unknown>[],
): boolean {
  const credKeys = new Set([
    "username",
    "password",
    "Username",
    "Password",
    "USER_NAME",
  ]);
  let changed = false;
  for (const o of students) {
    for (const k of credKeys) {
      if (k in o) {
        delete o[k];
        changed = true;
      }
    }
  }
  return changed;
}

function migrateLegacyStudentFieldNamesInObjects(
  students: Record<string, unknown>[],
): boolean {
  let changed = false;
  for (const o of students) {
    if ("StudentID" in o && !("student_id" in o)) {
      o.student_id = strVal(o.StudentID);
      delete o.StudentID;
      changed = true;
    }
    if ("created_at" in o && !("creation_date" in o)) {
      o.creation_date = strVal(o.created_at);
      delete o.created_at;
      changed = true;
    }
  }
  return changed;
}

/** `club_name` / `Club_name` → `club_id` (folder UID); drops legacy name keys. */
function migrateStudentClubIdInObjects(
  clubFolderId: string,
  students: Record<string, unknown>[],
): boolean {
  let changed = false;
  for (const o of students) {
    const cid = strVal(o.club_id);
    if (cid) {
      if ("club_name" in o || "Club_name" in o) {
        delete o.club_name;
        delete o.Club_name;
        changed = true;
      }
      continue;
    }
    const legacy = strVal(o.club_name ?? o.Club_name);
    const resolved =
      legacy && isValidClubFolderId(legacy) ? legacy : clubFolderId;
    o.club_id = resolved;
    if ("club_name" in o) {
      delete o.club_name;
      changed = true;
    }
    if ("Club_name" in o) {
      delete o.Club_name;
      changed = true;
    }
    changed = true;
  }
  return changed;
}

function ensureCanonicalKeysOnStudentObjects(
  students: Record<string, unknown>[],
): boolean {
  let changed = false;
  for (const o of students) {
    for (const col of STUDENT_LIST_COLUMNS) {
      if (!(col in o)) {
        o[col] = "";
        changed = true;
      } else if (typeof o[col] !== "string") {
        o[col] = strVal(o[col]);
        changed = true;
      }
    }
  }
  return changed;
}

function writeStudentListFile(clubId: string, data: StudentListFileV1): void {
  const p = studentListPath(clubId);
  fs.writeFileSync(p, JSON.stringify(data, null, 2) + "\n", "utf8");
}

function writeStudentsArray(clubId: string, rows: StudentCsvRow[]): void {
  writeStudentListFile(clubId, {
    version: 1,
    students: rows.map((r) => studentRowToRecord(r) as Record<string, unknown>),
  });
}

function normalizeStudentListJsonInPlace(clubId: string): void {
  const p = studentListPath(clubId);
  if (!fs.existsSync(p)) {
    return;
  }
  const raw = fs.readFileSync(p, "utf8");
  let data: StudentListFileV1;
  try {
    data = parseStudentListJson(raw);
  } catch {
    return;
  }
  let changed = stripCredentialKeysFromStudentObjects(data.students);
  changed = migrateLegacyStudentFieldNamesInObjects(data.students) || changed;
  changed =
    migrateStudentClubIdInObjects(clubId, data.students) || changed;
  changed = ensureCanonicalKeysOnStudentObjects(data.students) || changed;
  if (changed) {
    writeStudentListFile(clubId, data);
  }
}

function migrateLegacyCsvIfNeeded(clubId: string): void {
  const dir = clubDataDir(clubId);
  if (!dir) {
    return;
  }
  const jsonP = path.join(dir, STUDENT_LIST_FILENAME);
  const csvP = path.join(dir, LEGACY_STUDENT_LIST_CSV);
  if (fs.existsSync(jsonP) || !fs.existsSync(csvP)) {
    return;
  }
  const legacyRows = parseAllStudentsFromCsvPath(csvP);
  writeStudentsArray(clubId, legacyRows);
  fs.unlinkSync(csvP);
}

function parseStudentListJsonToRows(raw: string): StudentCsvRow[] {
  let data: StudentListFileV1;
  try {
    data = parseStudentListJson(raw);
  } catch {
    return [];
  }
  const out: StudentCsvRow[] = [];
  for (const o of data.students) {
    const row = recordToStudentRow(o);
    if (row) {
      out.push(row);
    }
  }
  return out;
}

function readStudentRowsFromDisk(clubId: string): StudentCsvRow[] {
  if (!isValidClubFolderId(clubId)) {
    return [];
  }
  migrateLegacyCsvIfNeeded(clubId);
  const p = studentListPath(clubId);
  if (!fs.existsSync(p)) {
    return [];
  }
  normalizeStudentListJsonInPlace(clubId);
  return readFileCached(p, (raw) => parseStudentListJsonToRows(raw), []);
}

export function ensureStudentListFile(clubId: string): void {
  if (!isValidClubFolderId(clubId)) {
    throw new Error("Invalid club ID.");
  }
  if (isMongoConfigured()) {
    return;
  }
  const clubDir = path.join(dataClubRoot(), clubId.trim());
  if (!fs.existsSync(clubDir)) {
    fs.mkdirSync(clubDir, { recursive: true });
  }
  migrateLegacyCsvIfNeeded(clubId);
  const p = studentListPath(clubId);
  if (!fs.existsSync(p)) {
    writeStudentListFile(clubId, { version: 1, students: [] });
  } else {
    normalizeStudentListJsonInPlace(clubId);
  }
}

export async function loadStudents(clubId: string): Promise<StudentCsvRow[]> {
  if (!isValidClubFolderId(clubId)) {
    return [];
  }
  if (isMongoConfigured()) {
    try {
      return await loadStudentsFromMongo(clubId.trim());
    } catch (e) {
      console.warn("[studentList] Mongo UserList_Student load failed; disk fallback", e);
      return readStudentRowsFromDisk(clubId);
    }
  }
  return readStudentRowsFromDisk(clubId);
}

/**
 * Uppercase trimmed StudentID → club folder id. With Mongo: from `UserList_Student`;
 * otherwise first `data_club` folder (readdir order) whose JSON lists this StudentID.
 */
const studentIdToClubId = new Map<string, string>();
let studentIdClubIndexReady = false;

/**
 * Rebuilds the StudentID → club folder index. With Mongo: from `UserList_Student`;
 * otherwise scans all `UserList_Student.json` files on disk.
 */
export async function rebuildStudentIdClubIndex(): Promise<void> {
  const next = new Map<string, string>();
  if (isMongoConfigured()) {
    const fromMongo = await listAllStudentIdClubPairsFromMongo();
    for (const [k, v] of fromMongo) {
      next.set(k, v);
    }
  } else {
    const root = dataClubRoot();
    if (fs.existsSync(root)) {
      for (const name of fs.readdirSync(root)) {
        if (!isValidClubFolderId(name)) {
          continue;
        }
        const students = readStudentRowsFromDisk(name);
        for (const s of students) {
          const u = s.studentId.replace(/^\uFEFF/, "").trim().toUpperCase();
          if (!u || next.has(u)) {
            continue;
          }
          next.set(u, name);
        }
      }
    }
  }
  studentIdToClubId.clear();
  for (const [k, v] of next) {
    studentIdToClubId.set(k, v);
  }
  studentIdClubIndexReady = true;
}

async function ensureStudentIdClubIndexAsync(): Promise<void> {
  if (!studentIdClubIndexReady) {
    await rebuildStudentIdClubIndex();
  }
}

/** Register a newly added student; preserves first-wins if the ID already exists in the index. */
function registerStudentIdInIndex(studentId: string, clubId: string): void {
  const u = studentId.replace(/^\uFEFF/, "").trim().toUpperCase();
  if (!u) {
    return;
  }
  if (!studentIdToClubId.has(u)) {
    studentIdToClubId.set(u, clubId);
  }
}

/** First club folder id whose roster lists this StudentID, or null. */
export async function findClubUidForStudentId(
  studentId: string,
): Promise<string | null> {
  const uid = studentId.trim();
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
  /** Set when this StudentID exists in that club’s student roster (Mongo or JSON). */
  rosterRow: StudentCsvRow | null;
};

export type StudentClubSessionResult = StudentClubSessionOk | { ok: false; error: string };

/**
 * Resolves club folder for a student JWT (`sub` = StudentID).
 * Prefers `club_folder_uid` on student login; else roster in Mongo `UserList_Student` or disk JSON.
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
  let clubId: string | null = null;
  if (fromLogin && isValidClubFolderId(fromLogin)) {
    clubId = fromLogin;
  } else {
    clubId = await findClubUidForStudentId(sid);
  }
  if (!clubId) {
    return {
      ok: false,
      error: "No club folder found for this student account.",
    };
  }
  const roster = await loadStudents(clubId);
  const stu = roster.find((s) => studentIdsEqual(s.studentId, sid));
  const loginAnchorsThisFolder =
    Boolean(fromLogin) &&
    isValidClubFolderId(fromLogin) &&
    fromLogin.toUpperCase() === clubId.toUpperCase();
  if (!stu) {
    if (loginAnchorsThisFolder) {
      return { ok: true, clubId, rosterRow: null };
    }
    return { ok: false, error: "Student not found in club roster." };
  }
  if (stu.status.toUpperCase() !== "ACTIVE") {
    return { ok: false, error: "Student is not ACTIVE in club roster." };
  }
  return { ok: true, clubId, rosterRow: stu };
}

export type StudentListRaw = {
  relativePath: string;
  headers: string[];
  rows: string[][];
};

export async function loadStudentListRaw(clubId: string): Promise<StudentListRaw> {
  const id = clubId.trim();
  const relativePath = isMongoConfigured()
    ? `MongoDB/${USER_LIST_STUDENT_COLLECTION}/${id}`
    : `data_club/${id}/${STUDENT_LIST_FILENAME}`;
  if (!isValidClubFolderId(id)) {
    return { relativePath, headers: [], rows: [] };
  }
  ensureStudentListFile(id);
  const headers = [...STUDENT_LIST_COLUMNS];
  const students = await loadStudents(id);
  const rows = students.map((r) => {
    const rec = studentRowToRecord(r);
    return STUDENT_LIST_COLUMNS.map((h) => rec[h] ?? "");
  });
  return { relativePath, headers, rows };
}

function nextLegacyNumericStudentId(rows: StudentCsvRow[]): string {
  let max = 0;
  for (const r of rows) {
    const n = studentIdSequenceNumber(r.studentId);
    if (n != null && n > max) {
      max = n;
    }
  }
  return `S${String(max + 1).padStart(STUDENT_NEW_ID_PAD, "0")}`;
}

function nextClubScopedStudentId(clubId: string, rows: StudentCsvRow[]): string {
  const club = clubId.trim();
  const esc = club.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  let max = 0;
  for (const r of rows) {
    const id = r.studentId.replace(/^\uFEFF/, "").trim();
    const m = id.match(new RegExp(`^${esc}-S(\\d+)$`, "i"));
    if (m) {
      const n = Number.parseInt(m[1]!, 10);
      if (!Number.isNaN(n) && n > max) {
        max = n;
      }
    }
  }
  return `${club.toUpperCase()}-S${String(max + 1).padStart(CLUB_SCOPED_SUFFIX_PAD, "0")}`;
}

/** Bump `S#########` by one (Mongo `userLogin.uid` is global across clubs). */
export function bumpNumericStudentLoginStyleId(current: string): string {
  const s = String(current ?? "").replace(/^\uFEFF/, "").trim();
  const m = s.match(STUDENT_ID_RE);
  if (!m) {
    return s;
  }
  const n = Number.parseInt(m[1]!, 10);
  if (Number.isNaN(n) || n < 0) {
    return s;
  }
  return `S${String(n + 1).padStart(STUDENT_NEW_ID_PAD, "0")}`;
}

/** Next ID after allocation collisions (`CM…-S0000001` style, or legacy `S#########`). */
export function bumpClubScopedStudentId(current: string): string {
  const s = String(current ?? "").replace(/^\uFEFF/, "").trim();
  const m = s.match(CLUB_SCOPED_STUDENT_ID_RE);
  if (!m) {
    return bumpNumericStudentLoginStyleId(s);
  }
  const club = m[1]!.toUpperCase();
  const n = Number.parseInt(m[2]!, 10);
  if (Number.isNaN(n) || n < 0) {
    return s;
  }
  return `${club}-S${String(n + 1).padStart(CLUB_SCOPED_SUFFIX_PAD, "0")}`;
}

export async function allocateNextStudentId(clubId: string): Promise<string> {
  if (!isValidClubFolderId(clubId)) {
    throw new Error("Invalid club ID.");
  }
  if (isMongoConfigured()) {
    const rows = await loadStudentsFromMongo(clubId.trim());
    return nextClubScopedStudentId(clubId, rows);
  }
  ensureStudentListFile(clubId);
  return nextLegacyNumericStudentId(readStudentRowsFromDisk(clubId));
}

function sanitizeCell(s: string): string {
  return String(s ?? "").replace(/,/g, " ").trim();
}

export function studentIdsEqual(a: string, b: string): boolean {
  const x = String(a ?? "")
    .replace(/^\uFEFF/, "")
    .trim();
  const y = String(b ?? "")
    .replace(/^\uFEFF/, "")
    .trim();
  return x.length > 0 && x.toUpperCase() === y.toUpperCase();
}

export async function appendStudentRow(
  clubId: string,
  clubName: string,
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
  const name = sanitizeCell(input.studentName);
  if (!name) {
    return { ok: false, error: "full_name is required." };
  }
  const email = sanitizeCell(input.email);
  if (!email) {
    return { ok: false, error: "email is required." };
  }
  ensureStudentListFile(clubId);
  const rows = isMongoConfigured()
    ? await loadStudentsFromMongo(clubId.trim())
    : readStudentRowsFromDisk(clubId);
  const requested = input.studentId?.trim();
  let studentId: string;
  if (requested) {
    const normalized = normalizeStudentIdInput(requested);
    if (!normalized) {
      return {
        ok: false,
        error:
          "Invalid StudentID format (expected S######### or {Club_ID}-S#######).",
      };
    }
    if (rows.some((r) => studentIdsEqual(r.studentId, normalized))) {
      return { ok: false, error: "StudentID already exists in student list." };
    }
    studentId = normalized;
  } else {
    studentId = isMongoConfigured()
      ? nextClubScopedStudentId(clubId, rows)
      : nextLegacyNumericStudentId(rows);
  }
  const today = new Date().toISOString().slice(0, 10);
  const status = sanitizeCell(input.status || "ACTIVE") || "ACTIVE";

  const newRow: StudentCsvRow = {
    studentId,
    clubId: sanitizeCell(clubId),
    studentName: name,
    sex: sanitizeCell(input.sex ?? ""),
    email,
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
  if (isMongoConfigured()) {
    await insertStudentMongo(newRow, clubId.trim());
  } else {
    rows.push(newRow);
    writeStudentsArray(clubId, rows);
  }
  registerStudentIdInIndex(studentId, clubId);
  return { ok: true, studentId };
}

export async function updateStudentRow(
  clubId: string,
  clubName: string,
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
  const id = studentId.trim();
  if (!id) {
    return { ok: false, error: "StudentID is required." };
  }
  const name = sanitizeCell(input.studentName);
  if (!name) {
    return { ok: false, error: "full_name is required." };
  }
  const email = sanitizeCell(input.email);
  if (!email) {
    return { ok: false, error: "email is required." };
  }
  ensureStudentListFile(clubId);
  const rows = isMongoConfigured()
    ? await loadStudentsFromMongo(clubId.trim())
    : readStudentRowsFromDisk(clubId);
  const idx = rows.findIndex((r) => studentIdsEqual(r.studentId, id));
  if (idx < 0) {
    return { ok: false, error: "Student not found." };
  }
  const today = new Date().toISOString().slice(0, 10);
  const status = sanitizeCell(input.status || "ACTIVE") || "ACTIVE";
  const cur = rows[idx]!;
  const updated: StudentCsvRow = {
    ...cur,
    clubId: sanitizeCell(clubId),
    studentName: name,
    sex: sanitizeCell(input.sex ?? ""),
    email,
    phone: sanitizeCell(input.phone),
    guardian: sanitizeCell(input.guardian ?? ""),
    guardianContact: sanitizeCell(input.guardianContact ?? ""),
    school: sanitizeCell(input.school ?? ""),
    studentCoach: sanitizeCell(input.studentCoach ?? ""),
    remark: sanitizeCell(input.remark ?? ""),
    status,
    lastUpdateDate: today,
    dateOfBirth: sanitizeCell(input.dateOfBirth ?? ""),
    joinedDate: sanitizeCell(input.joinedDate ?? ""),
    homeAddress: sanitizeCell(input.homeAddress ?? ""),
    country: sanitizeCell(input.country ?? ""),
  };
  if (isMongoConfigured()) {
    const { matched } = await updateStudentMongo(clubId.trim(), id, updated);
    if (matched < 1) {
      return { ok: false, error: "Student not found." };
    }
  } else {
    rows[idx] = updated;
    writeStudentsArray(clubId, rows);
  }
  return { ok: true };
}

/** Deletes the student record from UserList_Student.json (disk only; no index rebuild). */
function purgeStudentRowOnDisk(
  clubId: string,
  studentId: string,
): { ok: true } | { ok: false; error: string } {
  const id = studentId.trim();
  if (!id) {
    return { ok: false, error: "StudentID is required." };
  }
  const p = studentListPath(clubId);
  if (!fs.existsSync(p)) {
    return { ok: false, error: "Student list not found." };
  }
  ensureStudentListFile(clubId);
  const rows = readStudentRowsFromDisk(clubId);
  const next = rows.filter((r) => !studentIdsEqual(r.studentId, id));
  if (next.length === rows.length) {
    return { ok: false, error: "Student not found." };
  }
  writeStudentsArray(clubId, next);
  return { ok: true };
}

/** Deletes the student roster row (Mongo `UserList_Student` or disk JSON). */
export async function purgeStudentRow(
  clubId: string,
  studentId: string,
): Promise<{ ok: true } | { ok: false; error: string }> {
  if (isMongoConfigured()) {
    const ok = await deleteStudentMongo(clubId.trim(), studentId);
    if (!ok) {
      return { ok: false, error: "Student not found." };
    }
    await rebuildStudentIdClubIndex();
    return { ok: true };
  }
  const r = purgeStudentRowOnDisk(clubId, studentId);
  if (r.ok) {
    await rebuildStudentIdClubIndex();
  }
  return r;
}

const PURGE_STUDENT_SKIP_ERRORS = new Set([
  "Student not found.",
  "Student list not found.",
]);

function purgeStudentRowSafe(
  clubId: string,
  studentId: string,
): { ok: true } | { ok: false; error: string } {
  try {
    return purgeStudentRowOnDisk(clubId, studentId);
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    if (
      msg.includes("UserList_Student.csv:") ||
      msg.includes("UserList_Student.json")
    ) {
      return { ok: false, error: "Student list not found." };
    }
    return { ok: false, error: msg };
  }
}

/**
 * Removes the student from every roster (Mongo `UserList_Student` or each club JSON) that lists this StudentID.
 * Returns `{ ok: true, updatedClubIds: [] }` when none matched (not an error for login-only cleanup).
 */
export async function purgeStudentRowFromAllClubFolders(
  studentId: string,
): Promise<
  { ok: true; updatedClubIds: string[] } | { ok: false; error: string }
> {
  const id = studentId.trim();
  if (!id) {
    return { ok: false, error: "StudentID is required." };
  }
  if (isMongoConfigured()) {
    const updatedClubIds = await deleteStudentMongoAllClubs(id);
    if (updatedClubIds.length > 0) {
      await rebuildStudentIdClubIndex();
    }
    return { ok: true, updatedClubIds };
  }
  const root = dataClubRoot();
  if (!fs.existsSync(root)) {
    return { ok: true, updatedClubIds: [] };
  }
  const updatedClubIds: string[] = [];
  const entries = fs.readdirSync(root, { withFileTypes: true });
  for (const ent of entries) {
    if (!ent.isDirectory()) {
      continue;
    }
    const folder = ent.name;
    if (!isValidClubFolderId(folder)) {
      continue;
    }
    const r = purgeStudentRowSafe(folder, id);
    if (r.ok) {
      updatedClubIds.push(folder);
      continue;
    }
    if (PURGE_STUDENT_SKIP_ERRORS.has(r.error)) {
      continue;
    }
    if (
      r.error.includes("UserList_Student.csv:") ||
      r.error.includes("UserList_Student.json")
    ) {
      continue;
    }
    return r;
  }
  if (updatedClubIds.length > 0) {
    await rebuildStudentIdClubIndex();
  }
  return { ok: true, updatedClubIds };
}
