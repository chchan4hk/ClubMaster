import fs from "fs";
import path from "path";
import {
  hashPassword,
  looksLikeBcrypt,
  verifyPassword,
} from "./userLoginPassword";
import {
  findCoachManagerClubUidByClubName,
  findUserByUid,
  findUserByUsername,
  loadUsersFromCsv,
  mapUserTypeToRole,
  parseLine,
} from "./userlistCsv";
import { readFileCached, readFileCachedStrict } from "./dataFileCache";
import { isMongoConfigured } from "./db/DBConnection";
import {
  allocateNextCoachLoginUidMongo,
  allocateNextStudentLoginUidMongo,
  coachRoleLoginExistsForCoachIdAndClubMongo,
  findUserByUsernameAnyStoreMongo,
  insertCoachRoleMongo,
  insertStudentRoleMongo,
  studentRoleLoginExistsForStudentIdAndClubMongo,
  userLoginUidExistsMongo,
} from "./userListMongo";

/** Coach login / roster-style ids: CH… or C… (numeric tail for sequencing). */
const COACH_LOGIN_UID_NUM_RE = /^(?:CH|C)(\d+)$/i;
const STUDENT_ID_NUM_RE = /^S(\d+)$/i;
/** New coach logins use C + 6 digits (e.g. C000001). */
const COACH_LOGIN_UID_PAD = 5;
/** New student logins use S + 8 digits (e.g. S00000001). */
const STUDENT_LOGIN_UID_PAD = 8;

export type CoachStudentLoginRow = {
  uid: string;
  username: string;
  password: string;
  /** Bcrypt hash when using JSON store; empty password in that case. */
  passwordHash?: string | null;
  fullName: string;
  isActivated: boolean;
  clubName: string;
  /** data_club folder id = Coach Manager UID (e.g. CM00000001); JSON: `club_id` / `club_folder_uid`. */
  clubFolderUid?: string;
  /** Roster-aligned id in JSON as `StudentID` (defaults to `uid`). */
  studentId?: string;
  /** Roster-aligned id in JSON as `CoachID` (defaults to `uid`). */
  coachId?: string;
  status: string;
  creationDate: string;
  lastUpdateDate: string;
  /** YYYY-MM-DD or empty */
  expiryDate: string;
};

const dataDir = path.join(__dirname, "..", "data");
const COACH_LOGIN_JSON_FILE = "userLogin_Coach.json";
const STUDENT_LOGIN_JSON_FILE = "userLogin_Student.json";
const COACH_LOGIN_CSV_FILE = "userLogin_Coach.csv";
const STUDENT_LOGIN_CSV_FILE = "userLogin_Student.csv";

/** Header line for coach/student login CSVs (no club_photo column). */
export const COACH_STUDENT_LOGIN_HEADER =
  "UID,usertype,Username,password,full_name,is_activated,creation_date,club_name,status,lastUpdate_date";

type RoleLoginFileV1Json = {
  version: 1;
  users: Array<Record<string, string>>;
};

function resolvePathEnv(absOrRel: string): string {
  return path.isAbsolute(absOrRel) ? absOrRel : path.resolve(process.cwd(), absOrRel);
}

function coachLoginPath(): string {
  const j = process.env.USERLOGIN_COACH_JSON_PATH?.trim();
  if (j) {
    return resolvePathEnv(j);
  }
  const c = process.env.USERLOGIN_COACH_CSV_PATH?.trim();
  if (c) {
    return resolvePathEnv(c);
  }
  return path.join(dataDir, COACH_LOGIN_JSON_FILE);
}

function studentLoginPath(): string {
  const j = process.env.USERLOGIN_STUDENT_JSON_PATH?.trim();
  if (j) {
    return resolvePathEnv(j);
  }
  const c = process.env.USERLOGIN_STUDENT_CSV_PATH?.trim();
  if (c) {
    return resolvePathEnv(c);
  }
  return path.join(dataDir, STUDENT_LOGIN_JSON_FILE);
}

function coachLoginUsesJson(): boolean {
  return coachLoginPath().toLowerCase().endsWith(".json");
}

function studentLoginUsesJson(): boolean {
  return studentLoginPath().toLowerCase().endsWith(".json");
}

export function verifyRoleLoginPassword(
  row: CoachStudentLoginRow,
  plain: string,
): boolean {
  const h = row.passwordHash != null ? String(row.passwordHash).trim() : "";
  const stored = h !== "" ? h : row.password;
  return verifyPassword(plain, stored);
}

function normalizeRoleRowCredential(r: CoachStudentLoginRow): CoachStudentLoginRow {
  let hash = (r.passwordHash ?? "").trim();
  if (hash && looksLikeBcrypt(hash)) {
    return { ...r, password: "", passwordHash: hash };
  }
  const p = (r.password ?? "").trim();
  if (p && looksLikeBcrypt(p)) {
    return { ...r, password: "", passwordHash: p };
  }
  if (p) {
    return { ...r, password: "", passwordHash: hashPassword(p) };
  }
  throw new Error(`Missing password credential for uid ${r.uid}`);
}

function saveRoleLoginJson(
  absPath: string,
  rows: CoachStudentLoginRow[],
  fileRole: "Coach" | "Student",
): void {
  if (!absPath.toLowerCase().endsWith(".json")) {
    throw new Error("saveRoleLoginJson: path must be .json");
  }
  const normalized = rows.map((r) => normalizeRoleRowCredential(r));
  const body: RoleLoginFileV1Json = {
    version: 1,
    users: normalized.map((r) => {
      const u: Record<string, string> = {
        uid: r.uid,
        usertype: fileRole,
        username: r.username,
        password_hash: String(r.passwordHash ?? "").trim(),
        full_name: r.fullName,
        is_activated: r.isActivated ? "YES" : "NO",
        creation_date: r.creationDate,
        club_name: r.clubName,
        status: r.status,
        lastUpdate_date: r.lastUpdateDate,
        Expiry_date: (r.expiryDate ?? "").trim(),
      };
      const cfu = (r.clubFolderUid ?? "").trim();
      if (cfu) {
        u.club_id = cfu;
        u.club_folder_uid = cfu;
      }
      if (fileRole === "Student") {
        u.StudentID = String(r.studentId ?? r.uid).trim() || r.uid;
      }
      if (fileRole === "Coach") {
        u.CoachID = String(r.coachId ?? r.uid).trim() || r.uid;
      }
      return u;
    }),
  };
  fs.writeFileSync(absPath, JSON.stringify(body, null, 2) + "\n", "utf8");
}

function parseRoleLoginJsonRaw(
  raw: string,
  absPathForErrors: string,
  fileRole: "Coach" | "Student",
): CoachStudentLoginRow[] {
  let data: RoleLoginFileV1Json;
  try {
    data = JSON.parse(raw) as RoleLoginFileV1Json;
  } catch {
    throw new Error(`${absPathForErrors} is not valid JSON.`);
  }
  if (!data.users || !Array.isArray(data.users)) {
    throw new Error(`${absPathForErrors} must contain a users array.`);
  }
  const out: CoachStudentLoginRow[] = [];
  for (let i = 0; i < data.users.length; i++) {
    const row = data.users[i]!;
    let uid = String(row.uid ?? "").trim();
    if (!uid && fileRole === "Student") {
      uid = String(
        row.StudentID ?? row.Student_ID ?? row.studentId ?? "",
      ).trim();
    }
    if (!uid && fileRole === "Coach") {
      uid = String(row.CoachID ?? row.Coach_ID ?? row.coachId ?? "").trim();
    }
    if (!uid) {
      continue;
    }
    const ph =
      String(row.password_hash ?? row.passwordHash ?? "").trim() ||
      String(row.password ?? "").trim();
    const passwordHash = ph && looksLikeBcrypt(ph) ? ph : null;
    const password = ph && !looksLikeBcrypt(ph) ? ph : "";
    if (!passwordHash && !password) {
      continue;
    }
    const isActivated = parseBoolCell(String(row.is_activated ?? row.isActivated ?? ""));
    let status = String(row.status ?? "").trim() || "ACTIVE";
    let isAct = isActivated;
    if (status.toUpperCase() === "INACTIVE") {
      isAct = false;
    }
    let clubFolderUidRaw = String(
      row.club_id ?? row.club_folder_uid ?? row.clubFolderUid ?? "",
    ).trim();
    const clubNameForLookup = String(
      row.club_name ?? row.clubName ?? "",
    ).trim();
    if (!clubFolderUidRaw && clubNameForLookup) {
      const resolved = findCoachManagerClubUidByClubName(clubNameForLookup);
      if (resolved) {
        clubFolderUidRaw = resolved.trim();
      }
    }
    const studentIdRaw = String(
      row.StudentID ?? row.Student_ID ?? row.studentId ?? "",
    ).trim();
    const coachIdRaw = String(
      row.CoachID ?? row.Coach_ID ?? row.coachId ?? "",
    ).trim();
    out.push({
      uid,
      username: String(row.username ?? "").trim(),
      password,
      passwordHash,
      fullName: String(row.full_name ?? row.fullName ?? "").trim(),
      isActivated: isAct,
      clubName: String(row.club_name ?? row.clubName ?? "").trim(),
      ...(clubFolderUidRaw ? { clubFolderUid: clubFolderUidRaw } : {}),
      ...(fileRole === "Student"
        ? { studentId: studentIdRaw || uid }
        : { coachId: coachIdRaw || uid }),
      status,
      creationDate: String(row.creation_date ?? row.creationDate ?? "").trim(),
      lastUpdateDate: String(
        row.lastUpdate_date ?? row.last_update_date ?? row.lastUpdateDate ?? "",
      ).trim(),
      expiryDate: String(
        row.Expiry_date ?? row.expiry_date ?? row.ExpiryDate ?? row.expiryDate ?? "",
      ).trim(),
    });
  }
  return out;
}

function loadRoleLoginFromJson(
  absPath: string,
  fileRole: "Coach" | "Student",
): CoachStudentLoginRow[] {
  return readFileCachedStrict(absPath, (raw) =>
    parseRoleLoginJsonRaw(raw, absPath, fileRole),
  );
}

function parseRoleCsvRawToRows(raw: string): CoachStudentLoginRow[] {
  const lines = raw.split(/\r?\n/).filter((l) => l.trim());
  if (lines.length < 2) {
    return [];
  }
  const headerCells = parseLine(lines[0]!.replace(/^\uFEFF/, "").trim());
  const h = headerCells.map((c) => c.trim().toLowerCase());
  const col = (name: string) => h.indexOf(name.toLowerCase());
  const iUid = col("uid");
  const iUser = col("username");
  const iPass = col("password");
  const iFull = col("full_name");
  const iAct = col("is_activated");
  const iClub = col("club_name");
  const iStat = col("status");
  const iCreate = col("creation_date");
  const iLast = col("lastupdate_date");
  const iExpiry = col("expiry_date");
  if (iUid < 0 || iUser < 0 || iPass < 0) {
    return [];
  }
  const out: CoachStudentLoginRow[] = [];
  for (let li = 1; li < lines.length; li++) {
    const cells = parseLine(lines[li]!);
    const get = (i: number) => (i >= 0 && i < cells.length ? cells[i]! : "").trim();
    const rawCred = get(iPass);
    const passwordHash = looksLikeBcrypt(rawCred) ? rawCred : null;
    const password = looksLikeBcrypt(rawCred) ? "" : rawCred;
    out.push({
      uid: get(iUid),
      username: get(iUser),
      password,
      passwordHash,
      fullName: iFull >= 0 ? get(iFull) : "",
      isActivated: iAct >= 0 ? parseBoolCell(get(iAct)) : true,
      clubName: iClub >= 0 ? get(iClub) : "",
      status: iStat >= 0 ? get(iStat) : "ACTIVE",
      creationDate: iCreate >= 0 ? get(iCreate) : "",
      lastUpdateDate: iLast >= 0 ? get(iLast) : "",
      expiryDate: iExpiry >= 0 ? get(iExpiry) : "",
    });
  }
  return out.filter((r) => r.uid || r.username);
}

function migrateRoleCsvToJson(
  csvPath: string,
  jsonPath: string,
  fileRole: "Coach" | "Student",
): void {
  const raw = fs.readFileSync(csvPath, "utf8");
  const rows = parseRoleCsvRawToRows(raw).map((r) => normalizeRoleRowCredential(r));
  saveRoleLoginJson(jsonPath, rows, fileRole);
}

function writeEmptyRoleJson(absPath: string, fileRole: "Coach" | "Student"): void {
  saveRoleLoginJson(absPath, [], fileRole);
}

function parseBoolCell(v: string): boolean {
  const s = v.trim().toUpperCase();
  return s === "YES" || s === "Y" || s === "TRUE" || s === "1";
}

function loadRoleLoginFile(
  absPath: string,
  fileRole: "Coach" | "Student",
): CoachStudentLoginRow[] {
  if (!fs.existsSync(absPath)) {
    return [];
  }
  if (absPath.toLowerCase().endsWith(".json")) {
    return loadRoleLoginFromJson(absPath, fileRole);
  }
  return readFileCached(absPath, (raw) => parseRoleCsvRawToRows(raw), []);
}

export function loadCoachRoleLogins(): CoachStudentLoginRow[] {
  return loadRoleLoginFile(coachLoginPath(), "Coach");
}

export function loadStudentRoleLogins(): CoachStudentLoginRow[] {
  return loadRoleLoginFile(studentLoginPath(), "Student");
}

function findByUsername(
  rows: CoachStudentLoginRow[],
  username: string
): CoachStudentLoginRow | undefined {
  const q = username.trim().toLowerCase();
  return rows.find((r) => r.username.trim().toLowerCase() === q);
}

export function findCoachRoleLoginByUsername(
  username: string
): CoachStudentLoginRow | undefined {
  return findByUsername(loadCoachRoleLogins(), username);
}

export function findStudentRoleLoginByUsername(
  username: string
): CoachStudentLoginRow | undefined {
  return findByUsername(loadStudentRoleLogins(), username);
}

export function findStudentRoleLoginByUid(
  uid: string | number
): CoachStudentLoginRow | undefined {
  const q = String(uid ?? "").trim().toUpperCase();
  if (!q) {
    return undefined;
  }
  const rows = loadStudentRoleLogins();
  const byUid = rows.find((r) => r.uid.trim().toUpperCase() === q);
  if (byUid) {
    return byUid;
  }
  return rows.find(
    (r) => String(r.studentId ?? r.uid).trim().toUpperCase() === q,
  );
}

/** Numeric part of CH… ids only (padding variants); C######## logins match by exact uid. */
function coachUidNumericKey(uid: string): string | null {
  const m = String(uid ?? "")
    .trim()
    .toUpperCase()
    .match(/^CH(\d+)$/);
  return m ? String(Number.parseInt(m[1]!, 10)) : null;
}

/** Row in `userLogin_Coach` for JWT sub / roster CoachID (matches `uid`, `CoachID`/`coachId`, CH padding like delete). */
export function findCoachRoleLoginByUid(
  uid: string | number,
): CoachStudentLoginRow | undefined {
  const raw = String(uid ?? "").trim();
  if (!raw) {
    return undefined;
  }
  const q = raw.toUpperCase();
  const rows = loadCoachRoleLogins();
  return rows.find((r) => {
    if (r.uid.trim().toUpperCase() === q) {
      return true;
    }
    if (String(r.coachId ?? "").trim().toUpperCase() === q) {
      return true;
    }
    const nq = coachUidNumericKey(q);
    if (nq != null) {
      if (coachUidNumericKey(r.uid) === nq) {
        return true;
      }
      const cid = String(r.coachId ?? "").trim();
      if (cid && coachUidNumericKey(cid) === nq) {
        return true;
      }
    }
    return false;
  });
}

function normEqRoleLogin(a: string, b: string): boolean {
  return a.trim().toLowerCase() === b.trim().toLowerCase();
}

export function searchCoachRoleByUsernameOrClub(
  username: string,
  clubName: string
): CoachStudentLoginRow[] {
  const uq = username.trim();
  const cq = clubName.trim();
  if (!uq && !cq) {
    return [];
  }
  return loadCoachRoleLogins().filter((r) => {
    if (uq && !normEqRoleLogin(r.username, uq)) {
      return false;
    }
    if (cq && !normEqRoleLogin(r.clubName, cq)) {
      return false;
    }
    return true;
  });
}

export function searchStudentRoleByUsernameOrClub(
  username: string,
  clubName: string
): CoachStudentLoginRow[] {
  const uq = username.trim();
  const cq = clubName.trim();
  if (!uq && !cq) {
    return [];
  }
  return loadStudentRoleLogins().filter((r) => {
    if (uq && !normEqRoleLogin(r.username, uq)) {
      return false;
    }
    if (cq && !normEqRoleLogin(r.clubName, cq)) {
      return false;
    }
    return true;
  });
}

type RoleCsvIdx = {
  uid: number;
  isActivated: number;
  status: number;
  lastUpdateDate: number;
};

function indicesForRoleLoginCsv(headerCells: string[]): RoleCsvIdx | null {
  const h = headerCells.map((c) => c.trim().toLowerCase());
  const uid = h.indexOf("uid");
  const isActivated = h.indexOf("is_activated");
  const status = h.indexOf("status");
  const lastUpdateDate = h.indexOf("lastupdate_date");
  if (uid < 0 || isActivated < 0 || status < 0) {
    return null;
  }
  return { uid, isActivated, status, lastUpdateDate };
}

function writeRoleLoginActivation(
  absPath: string,
  fileLabel: string,
  uidKey: string,
  activate: boolean,
  fileRole: "Coach" | "Student",
): { ok: true } | { ok: false; error: string } {
  if (!fs.existsSync(absPath)) {
    return { ok: false, error: `${fileLabel} is missing.` };
  }
  const today = new Date().toISOString().slice(0, 10);
  const key = uidKey.trim().toUpperCase();

  if (absPath.toLowerCase().endsWith(".json")) {
    const rows = loadRoleLoginFile(absPath, fileRole);
    const i = rows.findIndex((r) => r.uid.trim().toUpperCase() === key);
    if (i < 0) {
      return { ok: false, error: "Could not update row." };
    }
    rows[i] = {
      ...rows[i]!,
      isActivated: activate,
      status: activate ? "ACTIVE" : "INACTIVE",
      lastUpdateDate: today,
    };
    saveRoleLoginJson(absPath, rows, fileRole);
    return { ok: true };
  }

  const raw = fs.readFileSync(absPath, "utf8");
  const lines = raw.split(/\r?\n/);
  const headerCells = parseLine(lines[0]!.replace(/^\uFEFF/, "").trim());
  const idx = indicesForRoleLoginCsv(headerCells);
  if (!idx) {
    return { ok: false, error: `${fileLabel} has an invalid header.` };
  }
  let found = false;
  const out = [...lines];
  for (let i = 1; i < out.length; i++) {
    const line = out[i];
    if (!line?.trim()) {
      continue;
    }
    const cells = parseLine(line);
    while (cells.length < headerCells.length) {
      cells.push("");
    }
    if ((cells[idx.uid] ?? "").trim().toUpperCase() !== key) {
      continue;
    }
    cells[idx.isActivated] = activate ? "YES" : "NO";
    cells[idx.status] = activate ? "ACTIVE" : "INACTIVE";
    if (idx.lastUpdateDate >= 0) {
      cells[idx.lastUpdateDate] = today;
    }
    out[i] = cells.join(",");
    found = true;
    break;
  }
  if (!found) {
    return { ok: false, error: "Could not update CSV row." };
  }
  fs.writeFileSync(absPath, out.join("\n").replace(/\n*$/, "") + "\n", "utf8");
  return { ok: true };
}

/**
 * Update username, full name, and club for a coach or student role login row (userLogin_Coach / userLogin_Student).
 * Caller should ensure the username is not already used on another account (any store).
 */
export function updateRoleLoginProfileByUid(
  uid: string,
  fileRole: "Coach" | "Student",
  input: {
    username: string;
    fullName: string;
    clubName: string;
    /** YYYY-MM-DD or empty (caller-validated). */
    expiryDate?: string;
  },
): { ok: true } | { ok: false; error: string } {
  const uidKey = String(uid).trim().toUpperCase();
  const safeUser = String(input.username ?? "").replace(/,/g, " ").trim();
  const safeFull = String(input.fullName ?? "").replace(/,/g, " ").trim();
  const safeClub = String(input.clubName ?? "").replace(/,/g, " ").trim();
  const safeExpiry = String(input.expiryDate ?? "")
    .trim()
    .replace(/,/g, "");
  if (!uidKey) {
    return { ok: false, error: "UID is required." };
  }
  if (!safeUser) {
    return { ok: false, error: "Username is required." };
  }

  const absPath =
    fileRole === "Coach" ? coachLoginPath() : studentLoginPath();
  ensureCoachStudentLoginFilesExist();
  const today = new Date().toISOString().slice(0, 10);

  if (absPath.toLowerCase().endsWith(".json")) {
    const rows = loadRoleLoginFile(absPath, fileRole);
    const i = rows.findIndex((r) => r.uid.trim().toUpperCase() === uidKey);
    if (i < 0) {
      return {
        ok: false,
        error: `No ${fileRole} login found with that UID.`,
      };
    }
    rows[i] = {
      ...rows[i]!,
      username: safeUser,
      fullName: safeFull,
      clubName: safeClub,
      lastUpdateDate: today,
      expiryDate: safeExpiry,
    };
    saveRoleLoginJson(absPath, rows, fileRole);
    return { ok: true };
  }

  const raw = fs.readFileSync(absPath, "utf8");
  const lines = raw.split(/\r?\n/);
  const headerCells = parseLine(lines[0]!.replace(/^\uFEFF/, "").trim());
  const h = headerCells.map((c) => c.trim().toLowerCase());
  const col = (name: string) => h.indexOf(name.toLowerCase());
  const iUid = col("uid");
  const iUser = col("username");
  const iFull = col("full_name");
  const iClub = col("club_name");
  const iLast = col("lastupdate_date");
  const iExpiry = col("expiry_date");
  if (iUid < 0 || iUser < 0) {
    return { ok: false, error: "Role login CSV has an invalid header." };
  }

  let found = false;
  const out = [...lines];
  for (let li = 1; li < out.length; li++) {
    const line = out[li];
    if (!line?.trim()) {
      continue;
    }
    const cells = parseLine(line);
    while (cells.length < headerCells.length) {
      cells.push("");
    }
    if ((cells[iUid] ?? "").trim().toUpperCase() !== uidKey) {
      continue;
    }
    cells[iUser] = safeUser;
    if (iFull >= 0) {
      cells[iFull] = safeFull;
    }
    if (iClub >= 0) {
      cells[iClub] = safeClub;
    }
    if (iLast >= 0) {
      cells[iLast] = today;
    }
    if (iExpiry >= 0) {
      cells[iExpiry] = safeExpiry;
    }
    out[li] = cells.join(",");
    found = true;
    break;
  }

  if (!found) {
    return {
      ok: false,
      error: `No ${fileRole} login found with that UID.`,
    };
  }
  fs.writeFileSync(absPath, out.join("\n").replace(/\n*$/, "") + "\n", "utf8");
  return { ok: true };
}

/**
 * Update only `Expiry_date` on a coach/student role login row (JSON or CSV with expiry_date column).
 */
export function setRoleLoginExpiryByUid(
  uid: string,
  fileRole: "Coach" | "Student",
  expiryDate: string,
): { ok: true } | { ok: false; error: string } {
  const uidKey = String(uid).trim().toUpperCase();
  const safeExpiry = String(expiryDate ?? "").trim().replace(/,/g, "");
  if (!uidKey) {
    return { ok: false, error: "UID is required." };
  }

  const absPath =
    fileRole === "Coach" ? coachLoginPath() : studentLoginPath();
  ensureCoachStudentLoginFilesExist();
  const today = new Date().toISOString().slice(0, 10);

  if (absPath.toLowerCase().endsWith(".json")) {
    const rows = loadRoleLoginFile(absPath, fileRole);
    const i = rows.findIndex((r) => r.uid.trim().toUpperCase() === uidKey);
    if (i < 0) {
      return {
        ok: false,
        error: `No ${fileRole} login found with that UID.`,
      };
    }
    rows[i] = {
      ...rows[i]!,
      expiryDate: safeExpiry,
      lastUpdateDate: today,
    };
    saveRoleLoginJson(absPath, rows, fileRole);
    return { ok: true };
  }

  const raw = fs.readFileSync(absPath, "utf8");
  const lines = raw.split(/\r?\n/);
  const headerCells = parseLine(lines[0]!.replace(/^\uFEFF/, "").trim());
  const h = headerCells.map((c) => c.trim().toLowerCase());
  const col = (name: string) => h.indexOf(name.toLowerCase());
  const iUid = col("uid");
  const iExpiry = col("expiry_date");
  const iLast = col("lastupdate_date");
  if (iUid < 0 || iExpiry < 0) {
    return {
      ok: false,
      error: `Role login CSV has no expiry_date column; use JSON store.`,
    };
  }

  let found = false;
  const out = [...lines];
  for (let li = 1; li < out.length; li++) {
    const line = out[li];
    if (!line?.trim()) {
      continue;
    }
    const cells = parseLine(line);
    while (cells.length < headerCells.length) {
      cells.push("");
    }
    if ((cells[iUid] ?? "").trim().toUpperCase() !== uidKey) {
      continue;
    }
    cells[iExpiry] = safeExpiry;
    if (iLast >= 0) {
      cells[iLast] = today;
    }
    out[li] = cells.join(",");
    found = true;
    break;
  }

  if (!found) {
    return {
      ok: false,
      error: `No ${fileRole} login found with that UID.`,
    };
  }
  fs.writeFileSync(absPath, out.join("\n").replace(/\n*$/, "") + "\n", "utf8");
  return { ok: true };
}

/**
 * Set `Expiry_date` on every Coach / Student role login whose `club_id` / `club_folder_uid`
 * matches `clubFolderUid` (e.g. Coach Manager folder id `CM00000003`).
 */
export function setRoleLoginExpiryForClubFolderUid(
  clubFolderUid: string,
  expiryDate: string,
):
  | { ok: true; coachUpdated: number; studentUpdated: number }
  | { ok: false; error: string } {
  const clubKey = String(clubFolderUid ?? "").trim().toUpperCase();
  const safeExpiry = String(expiryDate ?? "").trim().replace(/,/g, "");
  if (!clubKey) {
    return { ok: false, error: "Club folder UID is required." };
  }
  ensureCoachStudentLoginFilesExist();
  const coachPath = coachLoginPath();
  const studentPath = studentLoginPath();
  if (
    !coachPath.toLowerCase().endsWith(".json") ||
    !studentPath.toLowerCase().endsWith(".json")
  ) {
    return {
      ok: false,
      error: "Bulk club expiry update requires JSON coach/student login stores.",
    };
  }
  const today = new Date().toISOString().slice(0, 10);
  let coachUpdated = 0;
  let studentUpdated = 0;

  const coachRows = loadCoachRoleLogins();
  for (let i = 0; i < coachRows.length; i++) {
    const cfu = (coachRows[i]!.clubFolderUid ?? "").trim().toUpperCase();
    if (cfu === clubKey) {
      coachRows[i] = {
        ...coachRows[i]!,
        expiryDate: safeExpiry,
        lastUpdateDate: today,
      };
      coachUpdated++;
    }
  }
  if (coachUpdated > 0) {
    saveRoleLoginJson(coachPath, coachRows, "Coach");
  }

  const studentRows = loadStudentRoleLogins();
  for (let i = 0; i < studentRows.length; i++) {
    const cfu = (studentRows[i]!.clubFolderUid ?? "").trim().toUpperCase();
    if (cfu === clubKey) {
      studentRows[i] = {
        ...studentRows[i]!,
        expiryDate: safeExpiry,
        lastUpdateDate: today,
      };
      studentUpdated++;
    }
  }
  if (studentUpdated > 0) {
    saveRoleLoginJson(studentPath, studentRows, "Student");
  }

  return { ok: true, coachUpdated, studentUpdated };
}

export function setCoachRoleLoginActiveByUid(
  uid: string,
  activate: boolean,
): { ok: true } | { ok: false; error: string } {
  const rows = loadCoachRoleLogins();
  const t = rows.find(
    (r) => r.uid.trim().toUpperCase() === String(uid).trim().toUpperCase(),
  );
  if (!t) {
    return { ok: false, error: "No Coach login found with that UID." };
  }
  const st = t.status.trim().toUpperCase();
  if (activate) {
    if (st === "ACTIVE" && t.isActivated) {
      return { ok: true };
    }
  } else if (st === "INACTIVE") {
    return { ok: true };
  }
  ensureCoachStudentLoginFilesExist();
  return writeRoleLoginActivation(
    coachLoginPath(),
    "userLogin_Coach",
    t.uid,
    activate,
    "Coach",
  );
}

export function setStudentRoleLoginActiveByUid(
  uid: string,
  activate: boolean,
): { ok: true } | { ok: false; error: string } {
  const rows = loadStudentRoleLogins();
  const t = rows.find(
    (r) => r.uid.trim().toUpperCase() === String(uid).trim().toUpperCase(),
  );
  if (!t) {
    return { ok: false, error: "No Student login found with that UID." };
  }
  const st = t.status.trim().toUpperCase();
  if (activate) {
    if (st === "ACTIVE" && t.isActivated) {
      return { ok: true };
    }
  } else if (st === "INACTIVE") {
    return { ok: true };
  }
  ensureCoachStudentLoginFilesExist();
  return writeRoleLoginActivation(
    studentLoginPath(),
    "userLogin_Student",
    t.uid,
    activate,
    "Student",
  );
}

export function deactivateCoachRoleLogin(
  username: string,
  clubName: string
): { ok: true } | { ok: false; error: string } {
  const matches = searchCoachRoleByUsernameOrClub(username, clubName);
  if (matches.length === 0) {
    return {
      ok: false,
      error:
        "No Coach found in userLogin_Coach for that username and/or club name (club_name).",
    };
  }
  if (matches.length > 1) {
    return { ok: false, error: "Multiple rows matched; narrow with username and club name." };
  }
  const t = matches[0]!;
  if (t.status.trim().toUpperCase() === "INACTIVE") {
    return { ok: false, error: "This Coach login is already inactive." };
  }
  ensureCoachStudentLoginFilesExist();
  return writeRoleLoginActivation(
    coachLoginPath(),
    "userLogin_Coach",
    t.uid,
    false,
    "Coach",
  );
}

export function activateCoachRoleLogin(
  username: string,
  clubName: string
): { ok: true } | { ok: false; error: string } {
  const matches = searchCoachRoleByUsernameOrClub(username, clubName);
  if (matches.length === 0) {
    return {
      ok: false,
      error:
        "No Coach found in userLogin_Coach for that username and/or club name (club_name).",
    };
  }
  if (matches.length > 1) {
    return { ok: false, error: "Multiple rows matched; narrow with username and club name." };
  }
  const t = matches[0]!;
  const st = t.status.trim().toUpperCase();
  if (st === "ACTIVE" && t.isActivated) {
    return { ok: false, error: "This Coach login is already active." };
  }
  ensureCoachStudentLoginFilesExist();
  return writeRoleLoginActivation(
    coachLoginPath(),
    "userLogin_Coach",
    t.uid,
    true,
    "Coach",
  );
}

export function deactivateStudentRoleLogin(
  username: string,
  clubName: string
): { ok: true } | { ok: false; error: string } {
  const matches = searchStudentRoleByUsernameOrClub(username, clubName);
  if (matches.length === 0) {
    return {
      ok: false,
      error:
        "No Student found in userLogin_Student for that username and/or club name (club_name).",
    };
  }
  if (matches.length > 1) {
    return { ok: false, error: "Multiple rows matched; narrow with username and club name." };
  }
  const t = matches[0]!;
  if (t.status.trim().toUpperCase() === "INACTIVE") {
    return { ok: false, error: "This Student login is already inactive." };
  }
  ensureCoachStudentLoginFilesExist();
  return writeRoleLoginActivation(
    studentLoginPath(),
    "userLogin_Student",
    t.uid,
    false,
    "Student",
  );
}

export function activateStudentRoleLogin(
  username: string,
  clubName: string
): { ok: true } | { ok: false; error: string } {
  const matches = searchStudentRoleByUsernameOrClub(username, clubName);
  if (matches.length === 0) {
    return {
      ok: false,
      error:
        "No Student found in userLogin_Student for that username and/or club name (club_name).",
    };
  }
  if (matches.length > 1) {
    return { ok: false, error: "Multiple rows matched; narrow with username and club name." };
  }
  const t = matches[0]!;
  const st = t.status.trim().toUpperCase();
  if (st === "ACTIVE" && t.isActivated) {
    return { ok: false, error: "This Student login is already active." };
  }
  ensureCoachStudentLoginFilesExist();
  return writeRoleLoginActivation(
    studentLoginPath(),
    "userLogin_Student",
    t.uid,
    true,
    "Student",
  );
}

function migrateRoleLoginFullNameColumn(absPath: string): void {
  if (!fs.existsSync(absPath)) {
    return;
  }
  const raw = fs.readFileSync(absPath, "utf8");
  const lines = raw.split(/\r?\n/).filter((l) => l.trim());
  if (lines.length < 1) {
    return;
  }
  let headerCells = parseLine(lines[0]!.replace(/^\uFEFF/, "").trim());
  let lower = headerCells.map((c) => c.trim().toLowerCase());
  if (lower.includes("full_name")) {
    return;
  }
  const iPass = lower.indexOf("password");
  if (iPass < 0) {
    return;
  }
  headerCells.splice(iPass + 1, 0, "full_name");
  lower = headerCells.map((c) => c.trim().toLowerCase());
  lines[0] = headerCells.join(",");
  for (let i = 1; i < lines.length; i++) {
    const c = parseLine(lines[i]!);
    c.splice(iPass + 1, 0, "");
    lines[i] = c.join(",");
  }
  fs.writeFileSync(absPath, lines.join("\n").replace(/\n*$/, "") + "\n", "utf8");
}

function ensureOneRoleLoginStore(
  storePath: string,
  legacyCsvName: string,
  fileRole: "Coach" | "Student",
): void {
  const usesJson = storePath.toLowerCase().endsWith(".json");
  if (usesJson) {
    if (!fs.existsSync(storePath)) {
      const legacyCsv = path.join(dataDir, legacyCsvName);
      if (fs.existsSync(legacyCsv)) {
        migrateRoleCsvToJson(legacyCsv, storePath, fileRole);
        try {
          fs.renameSync(legacyCsv, `${legacyCsv}.legacy-backup`);
        } catch {
          /* JSON already written */
        }
      } else {
        writeEmptyRoleJson(storePath, fileRole);
      }
    }
    return;
  }
  if (!fs.existsSync(storePath)) {
    fs.writeFileSync(storePath, `${COACH_STUDENT_LOGIN_HEADER}\n`, "utf8");
  } else {
    migrateRoleLoginFullNameColumn(storePath);
  }
}

export function ensureCoachStudentLoginFilesExist(): void {
  /** Coach/student logins are stored in MongoDB `userLogin` — do not create `userLogin_Coach.*` / `userLogin_Student.*`. */
  if (isMongoConfigured()) {
    return;
  }
  if (!fs.existsSync(dataDir)) {
    fs.mkdirSync(dataDir, { recursive: true });
  }
  ensureOneRoleLoginStore(coachLoginPath(), COACH_LOGIN_CSV_FILE, "Coach");
  ensureOneRoleLoginStore(studentLoginPath(), STUDENT_LOGIN_CSV_FILE, "Student");
}

function updatePasswordInRoleCsv(
  absPath: string,
  fileLabel: string,
  uid: string | number,
  oldPassword: string,
  newPassword: string,
  fileRole: "Coach" | "Student",
): { ok: true } | { ok: false; error: string } {
  if (!newPassword) {
    return { ok: false, error: "New password is required." };
  }
  if (!fs.existsSync(absPath)) {
    return { ok: false, error: `${fileLabel} is missing.` };
  }

  const uidKey = String(uid).trim();

  if (absPath.toLowerCase().endsWith(".json")) {
    const rows = loadRoleLoginFile(absPath, fileRole);
    const i = rows.findIndex(
      (r) => r.uid.trim().toUpperCase() === uidKey.toUpperCase(),
    );
    if (i < 0) {
      return { ok: false, error: `User not found in ${fileLabel}.` };
    }
    if (!verifyRoleLoginPassword(rows[i]!, oldPassword)) {
      return { ok: false, error: "Old password does not match our records." };
    }
    rows[i] = {
      ...rows[i]!,
      password: "",
      passwordHash: hashPassword(newPassword),
    };
    saveRoleLoginJson(absPath, rows, fileRole);
    return { ok: true };
  }

  const raw = fs.readFileSync(absPath, "utf8");
  const lines = raw.split(/\r?\n/);
  if (lines.length < 2) {
    return { ok: false, error: `${fileLabel} is empty.` };
  }

  const headerCells = parseLine(lines[0]!.replace(/^\uFEFF/, "").trim());
  const header = headerCells.map((h) => h.toLowerCase());
  const idxUid = header.indexOf("uid");
  const idxPass = header.indexOf("password");
  if (idxUid < 0 || idxPass < 0) {
    return { ok: false, error: "Invalid CSV header." };
  }

  let found = false;
  const out = [...lines];
  for (let i = 1; i < out.length; i++) {
    const line = out[i];
    if (line === undefined || !line.trim()) {
      continue;
    }
    const cells = parseLine(line);
    const rowUid = (cells[idxUid] ?? "").trim();
    if (rowUid.toUpperCase() !== uidKey.toUpperCase()) {
      continue;
    }
    const currentPass = cells[idxPass] ?? "";
    if (!verifyPassword(oldPassword, currentPass)) {
      return { ok: false, error: "Old password does not match our records." };
    }
    while (cells.length < headerCells.length) {
      cells.push("");
    }
    cells[idxPass] = newPassword;
    out[i] = cells.join(",");
    found = true;
    break;
  }

  if (!found) {
    return { ok: false, error: `User not found in ${fileLabel}.` };
  }

  const text = out.join("\n").replace(/\n*$/, "") + "\n";
  fs.writeFileSync(absPath, text, "utf8");
  return { ok: true };
}

export function updateCoachLoginPasswordInCsv(
  uid: string | number,
  oldPassword: string,
  newPassword: string
): { ok: true } | { ok: false; error: string } {
  return updatePasswordInRoleCsv(
    coachLoginPath(),
    "userLogin_Coach",
    uid,
    oldPassword,
    newPassword,
    "Coach",
  );
}

export function updateStudentLoginPasswordInCsv(
  uid: string | number,
  oldPassword: string,
  newPassword: string
): { ok: true } | { ok: false; error: string } {
  return updatePasswordInRoleCsv(
    studentLoginPath(),
    "userLogin_Student",
    uid,
    oldPassword,
    newPassword,
    "Student",
  );
}

/** Admin: set password for a coach/student role login row (no old password check). */
export function setRoleLoginPasswordByUid(
  uid: string,
  fileRole: "Coach" | "Student",
  newPassword: string,
): { ok: true } | { ok: false; error: string } {
  const safePass = String(newPassword ?? "").replace(/,/g, " ").trim();
  if (!safePass) {
    return { ok: false, error: "Password is required." };
  }
  const absPath =
    fileRole === "Coach" ? coachLoginPath() : studentLoginPath();
  ensureCoachStudentLoginFilesExist();
  const uidKey = String(uid).trim().toUpperCase();

  if (absPath.toLowerCase().endsWith(".json")) {
    const rows = loadRoleLoginFile(absPath, fileRole);
    const i = rows.findIndex((r) => r.uid.trim().toUpperCase() === uidKey);
    if (i < 0) {
      return {
        ok: false,
        error: `No ${fileRole} login found with that UID.`,
      };
    }
    rows[i] = {
      ...rows[i]!,
      password: "",
      passwordHash: hashPassword(safePass),
    };
    saveRoleLoginJson(absPath, rows, fileRole);
    return { ok: true };
  }

  const raw = fs.readFileSync(absPath, "utf8");
  const lines = raw.split(/\r?\n/);
  if (lines.length < 2) {
    return { ok: false, error: "Role login file is empty." };
  }
  const headerCells = parseLine(lines[0]!.replace(/^\uFEFF/, "").trim());
  const h = headerCells.map((c) => c.trim().toLowerCase());
  const iUid = h.indexOf("uid");
  const iPass = h.indexOf("password");
  if (iUid < 0 || iPass < 0) {
    return { ok: false, error: "Invalid CSV header." };
  }
  let found = false;
  const out = [...lines];
  for (let li = 1; li < out.length; li++) {
    const line = out[li];
    if (!line?.trim()) {
      continue;
    }
    const cells = parseLine(line);
    while (cells.length < headerCells.length) {
      cells.push("");
    }
    if ((cells[iUid] ?? "").trim().toUpperCase() !== uidKey) {
      continue;
    }
    cells[iPass] = safePass;
    out[li] = cells.join(",");
    found = true;
    break;
  }
  if (!found) {
    return {
      ok: false,
      error: `No ${fileRole} login found with that UID.`,
    };
  }
  fs.writeFileSync(absPath, out.join("\n").replace(/\n*$/, "") + "\n", "utf8");
  return { ok: true };
}

/**
 * True if this role-login row is the account identified by roster / login id `uidKey`
 * (matches `uid`, roster StudentID / CoachID, or CH-padded coach uid where applicable).
 */
function roleLoginRowMatchesUidForDelete(
  r: CoachStudentLoginRow,
  uidKey: string,
  fileRole: "Coach" | "Student",
): boolean {
  const q = uidKey.trim().toUpperCase();
  if (!q) {
    return false;
  }
  if (fileRole === "Student") {
    if (r.uid.trim().toUpperCase() === q) {
      return true;
    }
    return String(r.studentId ?? "").trim().toUpperCase() === q;
  }
  if (r.uid.trim().toUpperCase() === q) {
    return true;
  }
  if (String(r.coachId ?? "").trim().toUpperCase() === q) {
    return true;
  }
  const nq = coachUidNumericKey(q);
  if (nq != null) {
    if (coachUidNumericKey(r.uid) === nq) {
      return true;
    }
    const cid = String(r.coachId ?? "").trim();
    if (cid && coachUidNumericKey(cid) === nq) {
      return true;
    }
  }
  return false;
}

/** Remove a coach or student role login row from userLogin_Coach / userLogin_Student (JSON or CSV). */
export function deleteRoleLoginByUid(
  uid: string,
  fileRole: "Coach" | "Student",
): { ok: true } | { ok: false; error: string } {
  const absPath =
    fileRole === "Coach" ? coachLoginPath() : studentLoginPath();
  if (!fs.existsSync(absPath)) {
    return { ok: false, error: "Login file is missing." };
  }
  const uidKey = String(uid).trim().toUpperCase();

  if (absPath.toLowerCase().endsWith(".json")) {
    const rows = loadRoleLoginFile(absPath, fileRole);
    const next = rows.filter(
      (r) => !roleLoginRowMatchesUidForDelete(r, uidKey, fileRole),
    );
    if (next.length === rows.length) {
      return {
        ok: false,
        error: `No ${fileRole} login found with that UID.`,
      };
    }
    saveRoleLoginJson(absPath, next, fileRole);
    return { ok: true };
  }

  const raw = fs.readFileSync(absPath, "utf8");
  const lines = raw.split(/\r?\n/);
  if (lines.length < 2) {
    return { ok: false, error: "Role login file is empty." };
  }
  const headerCells = parseLine(lines[0]!.replace(/^\uFEFF/, "").trim());
  const h = headerCells.map((c) => c.trim().toLowerCase());
  const iUid = h.indexOf("uid");
  if (iUid < 0) {
    return { ok: false, error: "Invalid CSV header." };
  }
  const iStudentId = h.indexOf("studentid");
  const iCoachId = h.indexOf("coachid");
  const out: string[] = [lines[0]!];
  let removed = false;
  for (let li = 1; li < lines.length; li++) {
    const line = lines[li];
    if (!line?.trim()) {
      continue;
    }
    const cells = parseLine(line);
    const rowUid = (cells[iUid] ?? "").trim().toUpperCase();
    let isMatch = rowUid === uidKey;
    if (!isMatch && fileRole === "Student" && iStudentId >= 0) {
      isMatch =
        (cells[iStudentId] ?? "").trim().toUpperCase() === uidKey;
    }
    if (!isMatch && fileRole === "Coach" && iCoachId >= 0) {
      isMatch = (cells[iCoachId] ?? "").trim().toUpperCase() === uidKey;
    }
    if (isMatch) {
      removed = true;
      continue;
    }
    out.push(line);
  }
  if (!removed) {
    return {
      ok: false,
      error: `No ${fileRole} login found with that UID.`,
    };
  }
  fs.writeFileSync(absPath, out.join("\n").replace(/\n*$/, "") + "\n", "utf8");
  return { ok: true };
}

/** Like {@link deleteRoleLoginByUid} but succeeds when no row exists (for cleanup after other stores). */
export function deleteRoleLoginByUidOrMissing(
  uid: string,
  fileRole: "Coach" | "Student",
): { ok: true } | { ok: false; error: string } {
  const r = deleteRoleLoginByUid(uid, fileRole);
  if (r.ok) {
    return r;
  }
  const low = r.error.toLowerCase();
  if (
    low.includes("no coach login found") ||
    low.includes("no student login found") ||
    low.includes("login file is missing")
  ) {
    return { ok: true };
  }
  return r;
}

function roleLoginRowClubFolderUpper(r: CoachStudentLoginRow): string {
  return (r.clubFolderUid ?? "").trim().toUpperCase();
}

/** Legacy CSV: drop lines whose `club_id` / `club_folder_uid` matches the coach-manager folder UID. */
function removeRoleCsvRowsMatchingClubFolder(
  absPath: string,
  clubKeyUpper: string,
): number {
  const raw = fs.readFileSync(absPath, "utf8");
  const lines = raw.split(/\r?\n/);
  if (lines.length < 2) {
    return 0;
  }
  const headerCells = parseLine(lines[0]!.replace(/^\uFEFF/, "").trim());
  const h = headerCells.map((c) => c.trim().toLowerCase());
  let iClub = h.indexOf("club_id");
  if (iClub < 0) {
    iClub = h.indexOf("club_folder_uid");
  }
  if (iClub < 0) {
    return 0;
  }
  const out: string[] = [lines[0]!];
  let removed = 0;
  for (let li = 1; li < lines.length; li++) {
    const line = lines[li];
    if (!line?.trim()) {
      continue;
    }
    const cells = parseLine(line);
    while (cells.length < headerCells.length) {
      cells.push("");
    }
    const cellClub = (cells[iClub] ?? "").trim().toUpperCase();
    if (cellClub === clubKeyUpper) {
      removed++;
      continue;
    }
    out.push(line);
  }
  if (removed > 0) {
    fs.writeFileSync(absPath, out.join("\n").replace(/\n*$/, "") + "\n", "utf8");
  }
  return removed;
}

/**
 * Removes every coach and student role-login row whose `club_folder_uid` / `club_id`
 * equals the Coach Manager’s folder UID (same as `userLogin` UID, e.g. CM00000005).
 * Used when an admin permanently deletes that Coach Manager.
 */
export function removeRoleLoginRowsForCoachManagerFolderUid(
  coachManagerFolderUid: string,
):
  | { ok: true; removedCoach: number; removedStudent: number }
  | { ok: false; error: string } {
  const key = String(coachManagerFolderUid ?? "").trim().toUpperCase();
  if (!key) {
    return { ok: false, error: "Club folder UID is required." };
  }
  let removedCoach = 0;
  let removedStudent = 0;
  for (const fileRole of ["Coach", "Student"] as const) {
    const absPath =
      fileRole === "Coach" ? coachLoginPath() : studentLoginPath();
    if (!fs.existsSync(absPath)) {
      continue;
    }
    try {
      if (absPath.toLowerCase().endsWith(".json")) {
        const rows = loadRoleLoginFile(absPath, fileRole);
        const next = rows.filter((r) => roleLoginRowClubFolderUpper(r) !== key);
        const removed = rows.length - next.length;
        if (fileRole === "Coach") {
          removedCoach += removed;
        } else {
          removedStudent += removed;
        }
        if (removed > 0) {
          saveRoleLoginJson(absPath, next, fileRole);
        }
      } else {
        const n = removeRoleCsvRowsMatchingClubFolder(absPath, key);
        if (fileRole === "Coach") {
          removedCoach += n;
        } else {
          removedStudent += n;
        }
      }
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      return { ok: false, error: msg };
    }
  }
  return { ok: true, removedCoach, removedStudent };
}

/** Next coach login UID: only follow userLogin_Coach + main userLogin coach rows (not club rosters). */
function maxCoachNumericId(): number {
  let max = 0;
  for (const r of loadCoachRoleLogins()) {
    const m = r.uid.trim().match(COACH_LOGIN_UID_NUM_RE);
    if (m) {
      const n = Number.parseInt(m[1]!, 10);
      if (!Number.isNaN(n)) {
        max = Math.max(max, n);
      }
    }
  }
  for (const u of loadUsersFromCsv()) {
    if (mapUserTypeToRole(u.usertype) === "Coach") {
      const m = u.uid.trim().match(COACH_LOGIN_UID_NUM_RE);
      if (m) {
        const n = Number.parseInt(m[1]!, 10);
        if (!Number.isNaN(n)) {
          max = Math.max(max, n);
        }
      }
    }
  }
  return max;
}

/** Next student login UID: only follow userLogin_Student + main userLogin student rows (not club rosters). */
function maxStudentNumericId(): number {
  let max = 0;
  for (const r of loadStudentRoleLogins()) {
    const m = r.uid.trim().match(STUDENT_ID_NUM_RE);
    if (m) {
      const n = Number.parseInt(m[1]!, 10);
      if (!Number.isNaN(n)) {
        max = Math.max(max, n);
      }
    }
  }
  for (const u of loadUsersFromCsv()) {
    if (mapUserTypeToRole(u.usertype) === "Student") {
      const m = u.uid.trim().match(STUDENT_ID_NUM_RE);
      if (m) {
        const n = Number.parseInt(m[1]!, 10);
        if (!Number.isNaN(n)) {
          max = Math.max(max, n);
        }
      }
    }
  }
  return max;
}

export function allocateNextCoachLoginUid(): string {
  const n = maxCoachNumericId() + 1;
  return `C${String(n).padStart(COACH_LOGIN_UID_PAD, "0")}`;
}

export function allocateNextStudentLoginUid(): string {
  const n = maxStudentNumericId() + 1;
  return `S${String(n).padStart(STUDENT_LOGIN_UID_PAD, "0")}`;
}

/** Username collision: Mongo `userLogin` when configured (all roles), else CSV/role files. */
export async function usernameTakenForNewLoginPreferred(
  username: string,
): Promise<boolean> {
  if (isMongoConfigured()) {
    try {
      return await findUserByUsernameAnyStoreMongo(username);
    } catch {
      return usernameTakenForNewLogin(username);
    }
  }
  return usernameTakenForNewLogin(username);
}

/** True if `uid` is already used in unified `userLogin` (Mongo) or main CSV (file mode). */
export async function userLoginUidCollisionPreferred(
  uid: string,
): Promise<boolean> {
  if (isMongoConfigured()) {
    try {
      return await userLoginUidExistsMongo(uid);
    } catch {
      return Boolean(findUserByUid(uid));
    }
  }
  return Boolean(findUserByUid(uid));
}

export function usernameTakenForNewLogin(username: string): boolean {
  const u = username.trim();
  if (!u) {
    return true;
  }
  if (findUserByUsername(u)) {
    return true;
  }
  if (findCoachRoleLoginByUsername(u) || findStudentRoleLoginByUsername(u)) {
    return true;
  }
  return false;
}

function normClubOrName(s: string): string {
  return String(s ?? "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ");
}

/**
 * True if `userLogin_Student` already has a row with this roster StudentID and club name
 * (matches `StudentID` / `uid` and `club_name`).
 */
export function studentRoleLoginExistsForStudentIdAndClub(
  studentId: string,
  clubName: string,
): boolean {
  const sid = String(studentId ?? "").trim().toUpperCase();
  const club = normClubOrName(clubName);
  if (!sid || !club) {
    return false;
  }
  const rows = loadStudentRoleLogins();
  return rows.some((r) => {
    const rid = String(r.studentId ?? r.uid ?? "").trim().toUpperCase();
    const rclub = normClubOrName(r.clubName);
    return rid === sid && rclub === club;
  });
}

/** File-backed or Mongo `userLogin`: duplicate student login for roster id + club. */
export async function studentRoleLoginExistsForStudentIdAndClubPreferred(
  studentId: string,
  clubFolderUid: string,
  clubName: string,
): Promise<boolean> {
  if (isMongoConfigured()) {
    try {
      return await studentRoleLoginExistsForStudentIdAndClubMongo(
        studentId,
        clubFolderUid,
        clubName,
      );
    } catch {
      return studentRoleLoginExistsForStudentIdAndClub(studentId, clubName);
    }
  }
  return studentRoleLoginExistsForStudentIdAndClub(studentId, clubName);
}

/**
 * True if `userLogin_Coach` already has a row with this roster CoachID and club name
 * (matches `CoachID` / `uid` and `club_name`).
 */
export function coachRoleLoginExistsForCoachIdAndClub(
  coachId: string,
  clubName: string,
): boolean {
  const cid = String(coachId ?? "").trim().toUpperCase();
  const club = normClubOrName(clubName);
  if (!cid || !club) {
    return false;
  }
  const rows = loadCoachRoleLogins();
  return rows.some((r) => {
    const rid = String(r.coachId ?? r.uid ?? "").trim().toUpperCase();
    const rclub = normClubOrName(r.clubName);
    return rid === cid && rclub === club;
  });
}

/** File-backed or Mongo `userLogin`: duplicate coach login for roster coach id + club. */
export async function coachRoleLoginExistsForCoachIdAndClubPreferred(
  coachId: string,
  clubFolderUid: string,
  clubName: string,
): Promise<boolean> {
  if (isMongoConfigured()) {
    try {
      return await coachRoleLoginExistsForCoachIdAndClubMongo(
        coachId,
        clubFolderUid,
        clubName,
      );
    } catch {
      return coachRoleLoginExistsForCoachIdAndClub(coachId, clubName);
    }
  }
  return coachRoleLoginExistsForCoachIdAndClub(coachId, clubName);
}

/** Appends a row to userLogin_Coach.csv (UID must later match CoachID in a club roster to sign in). */
export async function appendCoachRoleLoginRow(input: {
  username: string;
  password: string;
  fullName: string;
  clubName: string;
  /** Coach Manager folder UID (`data_club/{id}/`); JSON writes `club_id` + `club_folder_uid`; Mongo writes both on `userLogin`. */
  clubFolderUid?: string;
  /** Roster `coach_id` from `UserList_Coach` — Mongo `uid` and `coach_id` use this value when set. */
  rosterCoachId?: string;
  /** YYYY-MM-DD or empty */
  expiryDate?: string;
}): Promise<{ ok: true; uid: string } | { ok: false; error: string }> {
  const username = input.username.trim();
  if (!username) {
    return { ok: false, error: "Username is required." };
  }
  if (await usernameTakenForNewLoginPreferred(username)) {
    return { ok: false, error: "The user already existed !" };
  }
  const safePass = String(input.password ?? "").replace(/,/g, " ").trim();
  if (!safePass) {
    return { ok: false, error: "Password is required." };
  }
  const safeClub = String(input.clubName ?? "").replace(/,/g, " ").trim();
  if (!safeClub) {
    return { ok: false, error: "Club name is required." };
  }
  const safeFull = String(input.fullName ?? "").replace(/,/g, " ").trim();
  if (!safeFull) {
    return { ok: false, error: "Full name is required." };
  }
  const safeFolderUid =
    String(input.clubFolderUid ?? "").trim() ||
    findCoachManagerClubUidByClubName(safeClub) ||
    "";
  const safeExpiry = String(input.expiryDate ?? "")
    .trim()
    .replace(/,/g, "");
  const rosterCid = String(input.rosterCoachId ?? "")
    .trim()
    .replace(/,/g, " ");
  const useRosterUid = Boolean(rosterCid);
  if (isMongoConfigured()) {
    const uid = useRosterUid ? rosterCid : await allocateNextCoachLoginUidMongo();
    if (await userLoginUidCollisionPreferred(uid)) {
      return {
        ok: false,
        error: useRosterUid
          ? "A coach login already exists for that roster coach id / uid in userLogin."
          : "Could not allocate a new Coach ID; try again.",
      };
    }
    const ins = await insertCoachRoleMongo({
      uid,
      username: username.replace(/,/g, " "),
      password: safePass,
      fullName: safeFull,
      clubName: safeClub,
      clubFolderUid: safeFolderUid || undefined,
      ...(useRosterUid ? { coach_id: rosterCid } : {}),
      expiryDate: safeExpiry || undefined,
    });
    if (!ins.ok) {
      return { ok: false, error: ins.error };
    }
    return { ok: true, uid };
  }

  ensureCoachStudentLoginFilesExist();
  const uid = useRosterUid ? rosterCid : allocateNextCoachLoginUid();
  if (await userLoginUidCollisionPreferred(uid)) {
    return {
      ok: false,
      error: useRosterUid
        ? "A coach login already exists for that roster coach id / uid."
        : "Could not allocate a new Coach ID; try again.",
    };
  }
  const today = new Date().toISOString().slice(0, 10);
  const storePath = coachLoginPath();

  if (coachLoginUsesJson()) {
    const rows = loadCoachRoleLogins();
    rows.push({
      uid,
      coachId: useRosterUid ? rosterCid : uid,
      username: username.replace(/,/g, " "),
      password: "",
      passwordHash: hashPassword(safePass),
      fullName: safeFull,
      isActivated: true,
      clubName: safeClub,
      ...(safeFolderUid ? { clubFolderUid: safeFolderUid } : {}),
      status: "ACTIVE",
      creationDate: today,
      lastUpdateDate: today,
      expiryDate: safeExpiry,
    });
    saveRoleLoginJson(storePath, rows, "Coach");
    return { ok: true, uid };
  }

  const line = [
    uid,
    "Coach",
    username.replace(/,/g, " "),
    safePass,
    safeFull,
    "YES",
    today,
    safeClub,
    "ACTIVE",
    today,
  ].join(",");
  fs.appendFileSync(storePath, line + "\n", "utf8");
  return { ok: true, uid };
}

/** Appends a row to userLogin_Student.csv (UID must later match StudentID in a club roster to sign in). */
export async function appendStudentRoleLoginRow(input: {
  username: string;
  password: string;
  fullName: string;
  clubName: string;
  clubFolderUid?: string;
  /** Roster `student_id` from `UserList_Student` (e.g. `HK00004-S000004`); Mongo `uid` and `student_id` copy this when set. */
  rosterStudentId?: string;
  /** YYYY-MM-DD or empty */
  expiryDate?: string;
}): Promise<{ ok: true; uid: string } | { ok: false; error: string }> {
  const username = input.username.trim();
  if (!username) {
    return { ok: false, error: "Username is required." };
  }
  if (await usernameTakenForNewLoginPreferred(username)) {
    return { ok: false, error: "The user already existed !" };
  }
  const safePass = String(input.password ?? "").replace(/,/g, " ").trim();
  if (!safePass) {
    return { ok: false, error: "Password is required." };
  }
  const safeClub = String(input.clubName ?? "").replace(/,/g, " ").trim();
  if (!safeClub) {
    return { ok: false, error: "Club name is required." };
  }
  const safeFull = String(input.fullName ?? "").replace(/,/g, " ").trim();
  if (!safeFull) {
    return { ok: false, error: "Full name is required." };
  }
  const safeFolderUid =
    String(input.clubFolderUid ?? "").trim() ||
    findCoachManagerClubUidByClubName(safeClub) ||
    "";
  const safeExpiry = String(input.expiryDate ?? "")
    .trim()
    .replace(/,/g, "");
  const rawRoster = String(input.rosterStudentId ?? "").trim();
  let rosterSid = rawRoster.replace(/,/g, " ").trim();
  if (rosterSid && safeFolderUid) {
    const p = `${safeFolderUid}-`;
    if (rosterSid.toUpperCase().startsWith(p.toUpperCase())) {
      rosterSid = rosterSid.slice(p.length).trim();
    }
  }
  const useScopedUid = Boolean(rosterSid && safeFolderUid);
  const uidScoped = useScopedUid ? `${safeFolderUid}-${rosterSid}` : "";

  if (isMongoConfigured()) {
    const uid = useScopedUid
      ? uidScoped
      : await allocateNextStudentLoginUidMongo();
    if (await userLoginUidCollisionPreferred(uid)) {
      return {
        ok: false,
        error: useScopedUid
          ? "A student login already exists for that roster id / uid in userLogin."
          : "Could not allocate a new Student ID; try again.",
      };
    }
    const ins = await insertStudentRoleMongo({
      uid,
      username: username.replace(/,/g, " "),
      password: safePass,
      fullName: safeFull,
      clubName: safeClub,
      clubFolderUid: safeFolderUid || undefined,
      /** Same as roster / JWT `sub`: full `{club}-S…` id, not the `S…` suffix alone. */
      ...(useScopedUid ? { student_id: uid } : {}),
      expiryDate: safeExpiry || undefined,
    });
    if (!ins.ok) {
      return { ok: false, error: ins.error };
    }
    return { ok: true, uid };
  }

  ensureCoachStudentLoginFilesExist();
  const uid = useScopedUid ? uidScoped : allocateNextStudentLoginUid();
  if (await userLoginUidCollisionPreferred(uid)) {
    return {
      ok: false,
      error: useScopedUid
        ? "A student login already exists for that roster id / uid."
        : "Could not allocate a new Student ID; try again.",
    };
  }
  const today = new Date().toISOString().slice(0, 10);
  const storePath = studentLoginPath();

  if (studentLoginUsesJson()) {
    const rows = loadStudentRoleLogins();
    rows.push({
      uid,
      studentId: uid,
      username: username.replace(/,/g, " "),
      password: "",
      passwordHash: hashPassword(safePass),
      fullName: safeFull,
      isActivated: true,
      clubName: safeClub,
      ...(safeFolderUid ? { clubFolderUid: safeFolderUid } : {}),
      status: "ACTIVE",
      creationDate: today,
      lastUpdateDate: today,
      expiryDate: safeExpiry,
    });
    saveRoleLoginJson(storePath, rows, "Student");
    return { ok: true, uid };
  }

  const line = [
    uid,
    "Student",
    username.replace(/,/g, " "),
    safePass,
    safeFull,
    "YES",
    today,
    safeClub,
    "ACTIVE",
    today,
  ].join(",");
  fs.appendFileSync(storePath, line + "\n", "utf8");
  return { ok: true, uid };
}
