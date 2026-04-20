import fs from "fs";
import path from "path";
import { looksLikeBcrypt, verifyPassword } from "./userLoginPassword";
import { readFileCached } from "./dataFileCache";

export type CsvUser = {
  uid: string;
  lineIndex: number;
  usertype: string;
  username: string;
  password: string;
  /** Bcrypt hash when password is stored as bcrypt in CSV; otherwise null/undefined. */
  passwordHash?: string | null;
  fullName: string;
  isActivated: boolean;
  role: string;
  creationDate: string;
  clubName: string;
  clubPhoto: string;
  /** ACTIVE | INACTIVE */
  status: string;
  lastUpdateDate: string;
  /** Optional account expiry (YYYY-MM-DD); empty = none. */
  expiryDate: string;
};

const dataDir = path.join(__dirname, "..", "data");
/** Default login store on disk (`userLogin.csv`). */
const defaultUserLoginCsvFile = "userLogin.csv";
const legacyUserlistFile = "userlist.csv";

export const defaultUserLoginPath = path.join(dataDir, defaultUserLoginCsvFile);
/** @deprecated use defaultUserLoginPath */
export const defaultUserlistPath = defaultUserLoginPath;

/** Stored CSV header (exact). */
export const USERLIST_CANONICAL_HEADER =
  "UID,usertype,Username,password,full_name,is_activated,creation_date,club_name,club_photo,status,lastUpdate_date";

/**
 * Resolved main login CSV path.
 * - Default: `backend/data/userLogin.csv`.
 * - `USERLOGIN_CSV_PATH` / `USERLIST_CSV_PATH`: override path (absolute or relative to cwd).
 */
export function userlistPath(): string {
  const csvEnv =
    process.env.USERLOGIN_CSV_PATH?.trim() || process.env.USERLIST_CSV_PATH?.trim();
  if (csvEnv) {
    return path.isAbsolute(csvEnv) ? csvEnv : path.resolve(process.cwd(), csvEnv);
  }
  return path.join(dataDir, defaultUserLoginCsvFile);
}

function legacyCsvOnDiskPath(): string {
  return path.join(dataDir, defaultUserLoginCsvFile);
}

export function verifyMainLoginPassword(user: CsvUser, plain: string): boolean {
  const h = user.passwordHash != null ? String(user.passwordHash).trim() : "";
  const stored = h !== "" ? h : user.password;
  return verifyPassword(plain, stored);
}

function ensureDataDir(): void {
  if (!fs.existsSync(dataDir)) {
    fs.mkdirSync(dataDir, { recursive: true });
  }
}

const DEFAULT_CSV = `${USERLIST_CANONICAL_HEADER}
C0000,administrator,admin,admin123,System Admin,YES,2024-01-15,,,ACTIVE,
`;

export function ensureUserlistFileExists(): void {
  ensureDataDir();
  const csvEnv =
    Boolean(process.env.USERLOGIN_CSV_PATH?.trim()) ||
    Boolean(process.env.USERLIST_CSV_PATH?.trim());
  if (!csvEnv) {
    const csvP = legacyCsvOnDiskPath();
    const oldP = path.join(dataDir, legacyUserlistFile);
    if (!fs.existsSync(csvP) && fs.existsSync(oldP)) {
      try {
        fs.renameSync(oldP, csvP);
      } catch {
        /* ignore */
      }
    }
  }
  const p = userlistPath();
  if (!fs.existsSync(p)) {
    fs.writeFileSync(p, DEFAULT_CSV, "utf8");
  }
}

function parseBoolCell(v: string): boolean {
  const s = v.trim().toUpperCase();
  return s === "YES" || s === "Y" || s === "TRUE" || s === "1";
}

export function mapUserTypeToRole(usertype: string): string | null {
  const n = usertype.trim().toLowerCase().replace(/\s+/g, " ");
  if (n === "administrator") return "Admin";
  if (n === "coach manager") return "CoachManager";
  if (n === "coach") return "Coach";
  if (n === "student") return "Student";
  return null;
}

export function parseLine(line: string): string[] {
  return line.split(",").map((c) => c.trim());
}

function normalizeUid(raw: string): string {
  return raw.trim();
}

/**
 * Insert missing full_name (after password) and club_photo (after club_name) so rows match canonical layout.
 */
function migrateUserLoginFullNameAndClubPhoto(p: string): void {
  if (!fs.existsSync(p)) {
    return;
  }
  const raw = fs.readFileSync(p, "utf8");
  const lines = raw.split(/\r?\n/).filter((l) => l.trim());
  if (lines.length < 1) {
    return;
  }
  let headerCells = parseLine(lines[0]!);
  let lower = headerCells.map((h) => h.toLowerCase());
  let changed = false;

  const insertColumnAfter = (afterLower: string, newHeader: string): void => {
    const idx = lower.indexOf(afterLower);
    if (idx < 0 || lower.includes(newHeader.toLowerCase())) {
      return;
    }
    headerCells.splice(idx + 1, 0, newHeader);
    lower = headerCells.map((h) => h.toLowerCase());
    for (let i = 1; i < lines.length; i++) {
      const c = parseLine(lines[i]!);
      c.splice(idx + 1, 0, "");
      lines[i] = c.join(",");
    }
    lines[0] = headerCells.join(",");
    changed = true;
  };

  insertColumnAfter("password", "full_name");
  headerCells = parseLine(lines[0]!);
  lower = headerCells.map((h) => h.toLowerCase());
  const idxClub = lower.indexOf("club_name");
  const idxStat = lower.indexOf("status");
  if (
    idxClub >= 0 &&
    idxStat >= 0 &&
    !lower.includes("club_photo") &&
    idxStat === idxClub + 1
  ) {
    insertColumnAfter("club_name", "club_photo");
  }

  if (changed) {
    fs.writeFileSync(p, lines.join("\n").replace(/\n*$/, "") + "\n", "utf8");
  }
}

/**
 * Migrate older userLogin.csv / userlist.csv layouts to canonical header:
 * UID,usertype,Username,password,full_name,is_activated,creation_date,club_name,club_photo,status,lastUpdate_date
 */
export function ensureUserlistSchema(): void {
  const p = userlistPath();
  if (!fs.existsSync(p)) {
    return;
  }
  migrateUserLoginFullNameAndClubPhoto(p);

  const raw = fs.readFileSync(p, "utf8");
  const lines = raw.split(/\r?\n/).filter((l) => l.trim());
  if (lines.length < 1) {
    return;
  }

  const headerCells = parseLine(lines[0]!);
  const lower = headerCells.map((h) => h.toLowerCase());
  const has = (name: string) => lower.includes(name);

  const hasLast =
    has("lastupdate_date") || has("last_update_date");

  // Canonical: all required columns present
  if (has("is_activated") && has("status") && hasLast) {
    const idxLast = lower.indexOf("last_update_date");
    if (idxLast >= 0 && lower.indexOf("lastupdate_date") < 0) {
      headerCells[idxLast] = "lastUpdate_date";
      const out = [headerCells.join(",")];
      for (let i = 1; i < lines.length; i++) {
        out.push(lines[i]!);
      }
      fs.writeFileSync(p, out.join("\n").replace(/\n*$/, "") + "\n", "utf8");
    }
    return;
  }

  // Old: ... password, Status, creation_date, club_name, club_photo, lastUpdate_date (no is_activated)
  if (has("status") && !has("is_activated") && lower[4] === "status") {
    const out: string[] = [USERLIST_CANONICAL_HEADER];
    for (let i = 1; i < lines.length; i++) {
      const c = parseLine(lines[i]!);
      if (c.length < 5) {
        continue;
      }
      const statusVal = (c[4] ?? "").trim() || "ACTIVE";
      const isAct = statusVal.toUpperCase() === "ACTIVE" ? "YES" : "NO";
      const row = [
        c[0] ?? "",
        c[1] ?? "",
        c[2] ?? "",
        c[3] ?? "",
        "",
        isAct,
        c[5] ?? "",
        c[6] ?? "",
        c[7] ?? "",
        statusVal,
        c[8] ?? "",
      ];
      out.push(row.join(","));
    }
    fs.writeFileSync(p, out.join("\n") + "\n", "utf8");
    return;
  }

  // Has is_activated + clubs but missing status and/or lastUpdate_date
  if (has("is_activated") && has("club_name")) {
    const idxAct = lower.indexOf("is_activated");
    let newHeader = lines[0]!.replace(/\s*$/, "");
    if (!has("status")) {
      newHeader += ",status";
    }
    if (!hasLast) {
      newHeader += ",lastUpdate_date";
    }
    const out: string[] = [newHeader];
    for (let i = 1; i < lines.length; i++) {
      let row = lines[i]!.replace(/\s*$/, "");
      const c = parseLine(row);
      const st = parseBoolCell(c[idxAct] ?? "") ? "ACTIVE" : "INACTIVE";
      if (!has("status")) {
        row += "," + st;
      }
      if (!hasLast) {
        row += ",";
      }
      out.push(row);
    }
    fs.writeFileSync(p, out.join("\n").replace(/\n*$/, "") + "\n", "utf8");
  }
}

/** @deprecated use ensureUserlistSchema */
export function ensureUserlistClubColumns(): void {
  ensureUserlistSchema();
}

function headerIndices(headerLower: string[]) {
  return {
    uid: headerLower.indexOf("uid"),
    usertype: headerLower.indexOf("usertype"),
    username: headerLower.indexOf("username"),
    password: headerLower.indexOf("password"),
    fullName: headerLower.indexOf("full_name"),
    isActivated: headerLower.indexOf("is_activated"),
    creationDate: headerLower.indexOf("creation_date"),
    clubName: headerLower.indexOf("club_name"),
    clubPhoto: headerLower.indexOf("club_photo"),
    status: headerLower.indexOf("status"),
    lastUpdateDate: headerLower.indexOf("lastupdate_date"),
    expiryDate: headerLower.indexOf("expiry_date"),
  };
}

/** Parse legacy `userLogin.csv` content into users (plaintext or bcrypt in password column). */
function parseCsvRawToUsers(raw: string): CsvUser[] {
  const lines = raw.split(/\r?\n/).map((l) => l.trim()).filter(Boolean);
  if (lines.length < 2) {
    return [];
  }

  const header = parseLine(lines[0]!).map((h) => h.toLowerCase());
  const idx = headerIndices(header);

  if (
    idx.uid < 0 ||
    idx.usertype < 0 ||
    idx.username < 0 ||
    idx.password < 0 ||
    idx.isActivated < 0
  ) {
    throw new Error(
      "userLogin.csv must include UID, usertype, Username, password, is_activated"
    );
  }

  const out: CsvUser[] = [];
  for (let i = 1; i < lines.length; i++) {
    const cells = parseLine(lines[i]!);
    if (cells.length < 5) {
      continue;
    }
    const uid = normalizeUid(cells[idx.uid] ?? "");
    if (!uid) {
      continue;
    }

    const usertype = cells[idx.usertype] ?? "";
    const username = cells[idx.username] ?? "";
    const rawCred = (cells[idx.password] ?? "").trim();
    const passwordHash = looksLikeBcrypt(rawCred) ? rawCred : null;
    const password = looksLikeBcrypt(rawCred) ? "" : rawCred;
    let isActivated = parseBoolCell(cells[idx.isActivated] ?? "");
    const role = mapUserTypeToRole(usertype);
    if (!role || !username) {
      continue;
    }
    const fullName =
      idx.fullName >= 0 ? (cells[idx.fullName] ?? "").trim() : "";
    const creationDate =
      idx.creationDate >= 0 ? (cells[idx.creationDate] ?? "").trim() : "";
    const clubName =
      idx.clubName >= 0 ? (cells[idx.clubName] ?? "").trim() : "";
    const clubPhoto =
      idx.clubPhoto >= 0 ? (cells[idx.clubPhoto] ?? "").trim() : "";
    let status =
      idx.status >= 0 ? (cells[idx.status] ?? "").trim() : "";
    if (!status) {
      status = isActivated ? "ACTIVE" : "INACTIVE";
    }
    if (status.toUpperCase() === "INACTIVE") {
      isActivated = false;
    }
    if (status.toUpperCase() === "ACTIVE" && !parseBoolCell(cells[idx.isActivated] ?? "")) {
      isActivated = false;
    }
    const lastUpdateDate =
      idx.lastUpdateDate >= 0 ? (cells[idx.lastUpdateDate] ?? "").trim() : "";
    const expiryDate =
      idx.expiryDate >= 0 ? (cells[idx.expiryDate] ?? "").trim() : "";

    out.push({
      uid,
      lineIndex: i,
      usertype,
      username,
      password,
      passwordHash,
      fullName,
      isActivated,
      role,
      creationDate,
      clubName,
      clubPhoto,
      status,
      lastUpdateDate,
      expiryDate,
    });
  }
  return out;
}

export function loadUsersFromCsv(): CsvUser[] {
  ensureDataDir();
  const p = userlistPath();
  if (!fs.existsSync(p)) {
    return [];
  }
  return readFileCached(p, (raw) => parseCsvRawToUsers(raw), []);
}

export function findUserByUsername(name: string): CsvUser | undefined {
  const q = name.trim().toLowerCase();
  return loadUsersFromCsv().find((u) => u.username.toLowerCase() === q);
}

export function findUserByUid(uid: string | number): CsvUser | undefined {
  const key = String(uid).trim();
  return loadUsersFromCsv().find((u) => u.uid === key);
}

/** @deprecated use findUserByUsername */
export function findUserByUserId(id: string): CsvUser | undefined {
  return findUserByUsername(id);
}

function normEq(a: string, b: string): boolean {
  return a.trim().toLowerCase() === b.trim().toLowerCase();
}

/**
 * Find Coach Manager rows in backend/data/userLogin.csv.
 * Pass username and/or club name (club_name column); at least one must be provided.
 * Matching is case-insensitive; if both are set, both must match.
 */
export function searchCoachManagers(
  username?: string,
  clubName?: string
): CsvUser[] {
  const uq = (username ?? "").trim();
  const cq = (clubName ?? "").trim();
  if (!uq && !cq) {
    return [];
  }
  return loadUsersFromCsv().filter((row) => {
    if (mapUserTypeToRole(row.usertype) !== "CoachManager") {
      return false;
    }
    if (uq && !normEq(row.username, uq)) {
      return false;
    }
    if (cq && !normEq(row.clubName, cq)) {
      return false;
    }
    return true;
  });
}

/**
 * Coach Manager UID (club folder id, e.g. C0001) for a given club_name in userLogin.csv.
 */
export function findCoachManagerClubUidByClubName(clubName: string): string | null {
  const matches = searchCoachManagers(undefined, clubName);
  if (matches.length === 0) {
    return null;
  }
  return matches[0]!.uid;
}

/**
 * `Expiry_date` from the Coach Manager row whose UID is the club folder id (e.g. CM00000005).
 */
export function getCoachManagerExpiryDateForClubFolderUid(
  clubFolderUid: string,
): string {
  const id = String(clubFolderUid ?? "").trim();
  if (!id) {
    return "";
  }
  const row = findUserByUid(id);
  if (!row || row.role !== "CoachManager") {
    return "";
  }
  return String(row.expiryDate ?? "").trim();
}

/** Distinct non-empty club_name values from userLogin.csv (for admin dropdowns). */
export function distinctClubNamesFromUserlist(): string[] {
  const seen = new Set<string>();
  const out: string[] = [];
  for (const row of loadUsersFromCsv()) {
    const cn = row.clubName.trim();
    if (!cn) {
      continue;
    }
    const key = cn.toLowerCase();
    if (seen.has(key)) {
      continue;
    }
    seen.add(key);
    out.push(cn);
  }
  out.sort((a, b) => a.localeCompare(b, undefined, { sensitivity: "base" }));
  return out;
}

export function deactivateCoachManager(
  username: string,
  clubName: string
): { ok: true } | { ok: false; error: string } {
  const matches = searchCoachManagers(username, clubName);
  if (matches.length === 0) {
    return {
      ok: false,
      error:
        "No Coach Manager found in userLogin.csv for that username and/or club name (club_name).",
    };
  }
  if (matches.length > 1) {
    return { ok: false, error: "Multiple rows matched; narrow with username and club name." };
  }
  const target = matches[0]!;
  if (target.status.toUpperCase() === "INACTIVE") {
    return { ok: false, error: "This Coach Manager is already inactive." };
  }

  const today = new Date().toISOString().slice(0, 10);
  const uidKey = target.uid;

  const p = userlistPath();
  const raw = fs.readFileSync(p, "utf8");
  const lines = raw.split(/\r?\n/);
  const headerCells = parseLine(lines[0]!.trim());
  const header = headerCells.map((h) => h.toLowerCase());
  const idx = headerIndices(header);
  if (idx.isActivated < 0 || idx.status < 0) {
    return { ok: false, error: "userLogin.csv is missing is_activated or status column." };
  }

  let found = false;
  const out = [...lines];
  for (let i = 1; i < out.length; i++) {
    const line = out[i];
    if (line === undefined || !line.trim()) {
      continue;
    }
    const cells = parseLine(line);
    while (cells.length < headerCells.length) {
      cells.push("");
    }
    const rowUid = normalizeUid(cells[idx.uid] ?? "");
    if (rowUid !== uidKey) {
      continue;
    }
    cells[idx.isActivated] = "NO";
    cells[idx.status] = "INACTIVE";
    if (idx.lastUpdateDate >= 0) {
      cells[idx.lastUpdateDate] = today;
    }
    out[i] = cells.join(",");
    found = true;
    break;
  }

  if (!found) {
    return { ok: false, error: "Could not update user row." };
  }
  fs.writeFileSync(p, out.join("\n").replace(/\n*$/, "") + "\n", "utf8");
  return { ok: true };
}

export function activateCoachManager(
  username: string,
  clubName: string
): { ok: true } | { ok: false; error: string } {
  const matches = searchCoachManagers(username, clubName);
  if (matches.length === 0) {
    return {
      ok: false,
      error:
        "No Coach Manager found in userLogin.csv for that username and/or club name (club_name).",
    };
  }
  if (matches.length > 1) {
    return { ok: false, error: "Multiple rows matched; narrow with username and club name." };
  }
  const target = matches[0]!;
  const st = target.status.trim().toUpperCase();
  if (st === "ACTIVE" && target.isActivated) {
    return { ok: false, error: "This Coach Manager is already active." };
  }

  const today = new Date().toISOString().slice(0, 10);
  const uidKey = target.uid;

  const p = userlistPath();
  const raw = fs.readFileSync(p, "utf8");
  const lines = raw.split(/\r?\n/);
  const headerCells = parseLine(lines[0]!.trim());
  const header = headerCells.map((h) => h.toLowerCase());
  const idx = headerIndices(header);
  if (idx.isActivated < 0 || idx.status < 0) {
    return { ok: false, error: "userLogin.csv is missing is_activated or status column." };
  }

  let found = false;
  const out = [...lines];
  for (let i = 1; i < out.length; i++) {
    const line = out[i];
    if (line === undefined || !line.trim()) {
      continue;
    }
    const cells = parseLine(line);
    while (cells.length < headerCells.length) {
      cells.push("");
    }
    const rowUid = normalizeUid(cells[idx.uid] ?? "");
    if (rowUid !== uidKey) {
      continue;
    }
    cells[idx.isActivated] = "YES";
    cells[idx.status] = "ACTIVE";
    if (idx.lastUpdateDate >= 0) {
      cells[idx.lastUpdateDate] = today;
    }
    out[i] = cells.join(",");
    found = true;
    break;
  }

  if (!found) {
    return { ok: false, error: "Could not update user row." };
  }
  fs.writeFileSync(p, out.join("\n").replace(/\n*$/, "") + "\n", "utf8");
  return { ok: true };
}

/**
 * Set ACTIVE/INACTIVE for any row in the main user login store (`userLogin.csv`) by UID.
 * Used by admin account lists (Admin, Coach Manager, and legacy rows in the main file).
 */
export function setMainUserlistActivationByUid(
  uid: string,
  activate: boolean,
): { ok: true } | { ok: false; error: string } {
  const uidKey = String(uid).trim();
  if (!uidKey) {
    return { ok: false, error: "UID is required." };
  }
  const p = userlistPath();
  if (!fs.existsSync(p)) {
    return { ok: false, error: "User list file is missing." };
  }
  const today = new Date().toISOString().slice(0, 10);

  const raw = fs.readFileSync(p, "utf8");
  const lines = raw.split(/\r?\n/);
  const headerCells = parseLine(lines[0]!.trim());
  const header = headerCells.map((h) => h.toLowerCase());
  const idx = headerIndices(header);
  if (idx.uid < 0 || idx.isActivated < 0 || idx.status < 0) {
    return {
      ok: false,
      error: "userLogin.csv is missing UID, is_activated, or status column.",
    };
  }

  let found = false;
  const out = [...lines];
  for (let i = 1; i < out.length; i++) {
    const line = out[i];
    if (line === undefined || !line.trim()) {
      continue;
    }
    const cells = parseLine(line);
    while (cells.length < headerCells.length) {
      cells.push("");
    }
    const rowUid = normalizeUid(cells[idx.uid] ?? "");
    if (rowUid !== uidKey) {
      continue;
    }
    const curStatus = (cells[idx.status] ?? "").trim().toUpperCase();
    const curAct = parseBoolCell(cells[idx.isActivated] ?? "");
    if (activate) {
      if (curStatus === "ACTIVE" && curAct) {
        return { ok: true };
      }
    } else if (curStatus === "INACTIVE" && !curAct) {
      return { ok: true };
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
    return { ok: false, error: "No user found with that UID in UserLogin." };
  }
  fs.writeFileSync(p, out.join("\n").replace(/\n*$/, "") + "\n", "utf8");
  return { ok: true };
}

/**
 * Permanently delete a Coach Manager row from the main login store (`userLogin.csv`).
 * Rejects Admin, Coach, Student, and other roles.
 */
export function removeCoachManagerFromUserLoginStore(
  uid: string,
): { ok: true } | { ok: false; error: string } {
  const uidKey = String(uid).trim();
  if (!uidKey) {
    return { ok: false, error: "UID is required." };
  }
  const user = findUserByUid(uidKey);
  if (!user) {
    return { ok: false, error: "No user found with that UID in UserLogin." };
  }
  if (user.role !== "CoachManager") {
    return {
      ok: false,
      error:
        "Only Coach Manager accounts can be fully removed from UserLogin this way.",
    };
  }

  const p = userlistPath();
  if (!fs.existsSync(p)) {
    return { ok: false, error: "User list file is missing." };
  }

  const raw = fs.readFileSync(p, "utf8");
  const lines = raw.split(/\r?\n/);
  if (lines.length < 2) {
    return { ok: false, error: "User list is empty." };
  }
  const headerCells = parseLine(lines[0]!.trim());
  const header = headerCells.map((h) => h.toLowerCase());
  const idx = headerIndices(header);
  if (idx.uid < 0) {
    return { ok: false, error: "userLogin.csv is missing UID column." };
  }

  const out: string[] = [lines[0]!];
  let removed = false;
  for (let i = 1; i < lines.length; i++) {
    const line = lines[i];
    if (line === undefined || !line.trim()) {
      continue;
    }
    const cells = parseLine(line);
    while (cells.length < headerCells.length) {
      cells.push("");
    }
    if (normalizeUid(cells[idx.uid] ?? "") === normalizeUid(uidKey)) {
      removed = true;
      continue;
    }
    out.push(line);
  }

  if (!removed) {
    return { ok: false, error: "No user found with that UID in UserLogin." };
  }
  fs.writeFileSync(p, out.join("\n").replace(/\n*$/, "") + "\n", "utf8");
  return { ok: true };
}

/**
 * Removes a Coach or Student row from the main userLogin store when UID and role match.
 * No-op if no row exists for that UID.
 */
export function removeMainUserlistCoachOrStudentByUid(
  uid: string,
  role: "Coach" | "Student",
): { ok: true } | { ok: false; error: string } {
  const uidKey = String(uid ?? "").trim();
  if (!uidKey) {
    return { ok: false, error: "UID is required." };
  }
  const user = findUserByUid(uidKey);
  if (!user) {
    return { ok: true };
  }
  if (user.role !== role) {
    return {
      ok: false,
      error: `UserLogin row exists for this UID but is not a ${role} account.`,
    };
  }

  const p = userlistPath();
  if (!fs.existsSync(p)) {
    return { ok: true };
  }

  const raw = fs.readFileSync(p, "utf8");
  const lines = raw.split(/\r?\n/);
  if (lines.length < 2) {
    return { ok: true };
  }
  const headerCells = parseLine(lines[0]!.trim());
  const header = headerCells.map((h) => h.toLowerCase());
  const idx = headerIndices(header);
  if (idx.uid < 0) {
    return { ok: false, error: "userLogin.csv is missing UID column." };
  }

  const out: string[] = [lines[0]!];
  let removed = false;
  for (let i = 1; i < lines.length; i++) {
    const line = lines[i];
    if (line === undefined || !line.trim()) {
      continue;
    }
    const cells = parseLine(line);
    while (cells.length < headerCells.length) {
      cells.push("");
    }
    if (normalizeUid(cells[idx.uid] ?? "") === normalizeUid(uidKey)) {
      removed = true;
      continue;
    }
    out.push(line);
  }

  if (removed) {
    fs.writeFileSync(p, out.join("\n").replace(/\n*$/, "") + "\n", "utf8");
  }
  return { ok: true };
}

/**
 * Update Username, full name, and club name for a row in the main user login store (by UID).
 * Caller should ensure the username is not already used in coach/student login files.
 */
export function updateMainUserlistProfileByUid(
  uid: string,
  input: {
    username: string;
    fullName: string;
    clubName: string;
    /** YYYY-MM-DD or empty (caller-validated). */
    expiryDate?: string;
  },
): { ok: true } | { ok: false; error: string } {
  const uidKey = String(uid).trim();
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
  const conflict = findUserByUsername(safeUser);
  if (conflict && normalizeUid(conflict.uid) !== uidKey) {
    return { ok: false, error: "That username is already in use." };
  }

  const p = userlistPath();
  if (!fs.existsSync(p)) {
    return { ok: false, error: "User list file is missing." };
  }
  const today = new Date().toISOString().slice(0, 10);

  const raw = fs.readFileSync(p, "utf8");
  const lines = raw.split(/\r?\n/);
  const headerCells = parseLine(lines[0]!.trim());
  const header = headerCells.map((h) => h.toLowerCase());
  const idx = headerIndices(header);
  if (idx.uid < 0 || idx.username < 0) {
    return { ok: false, error: "userLogin.csv is missing UID or Username column." };
  }

  let found = false;
  const out = [...lines];
  for (let i = 1; i < out.length; i++) {
    const line = out[i];
    if (line === undefined || !line.trim()) {
      continue;
    }
    const cells = parseLine(line);
    while (cells.length < headerCells.length) {
      cells.push("");
    }
    const rowUid = normalizeUid(cells[idx.uid] ?? "");
    if (rowUid !== uidKey) {
      continue;
    }
    cells[idx.username] = safeUser;
    if (idx.fullName >= 0) {
      cells[idx.fullName] = safeFull;
    }
    if (idx.clubName >= 0) {
      cells[idx.clubName] = safeClub;
    }
    if (idx.lastUpdateDate >= 0) {
      cells[idx.lastUpdateDate] = today;
    }
    if (idx.expiryDate >= 0) {
      cells[idx.expiryDate] = safeExpiry;
    }
    out[i] = cells.join(",");
    found = true;
    break;
  }

  if (!found) {
    return { ok: false, error: "No user found with that UID in UserLogin." };
  }
  fs.writeFileSync(p, out.join("\n").replace(/\n*$/, "") + "\n", "utf8");
  return { ok: true };
}

/**
 * Update only `expiry_date` for a main userLogin row (`userLogin.csv`).
 */
export function setMainUserlistExpiryByUid(
  uid: string,
  expiryDate: string,
): { ok: true } | { ok: false; error: string } {
  const uidKey = String(uid).trim();
  const safeExpiry = String(expiryDate ?? "").trim().replace(/,/g, "");
  if (!uidKey) {
    return { ok: false, error: "UID is required." };
  }
  const p = userlistPath();
  if (!fs.existsSync(p)) {
    return { ok: false, error: "User list file is missing." };
  }
  const today = new Date().toISOString().slice(0, 10);

  const raw = fs.readFileSync(p, "utf8");
  const lines = raw.split(/\r?\n/);
  const headerCells = parseLine(lines[0]!.trim());
  const header = headerCells.map((h) => h.toLowerCase());
  const idx = headerIndices(header);
  if (idx.uid < 0 || idx.expiryDate < 0) {
    return {
      ok: false,
      error:
        "userLogin.csv has no expiry_date column; add an expiry_date column to the file.",
    };
  }

  let found = false;
  const out = [...lines];
  for (let i = 1; i < out.length; i++) {
    const line = out[i];
    if (line === undefined || !line.trim()) {
      continue;
    }
    const cells = parseLine(line);
    while (cells.length < headerCells.length) {
      cells.push("");
    }
    const rowUid = normalizeUid(cells[idx.uid] ?? "");
    if (rowUid !== uidKey) {
      continue;
    }
    cells[idx.expiryDate] = safeExpiry;
    if (idx.lastUpdateDate >= 0) {
      cells[idx.lastUpdateDate] = today;
    }
    out[i] = cells.join(",");
    found = true;
    break;
  }

  if (!found) {
    return { ok: false, error: "No user found with that UID in UserLogin." };
  }
  fs.writeFileSync(p, out.join("\n").replace(/\n*$/, "") + "\n", "utf8");
  return { ok: true };
}

export function updateUserPasswordInCsv(
  uid: string | number,
  oldPassword: string,
  newPassword: string
): { ok: true } | { ok: false; error: string } {
  if (!newPassword) {
    return { ok: false, error: "New password is required." };
  }
  const p = userlistPath();
  if (!fs.existsSync(p)) {
    return { ok: false, error: "User list file is missing." };
  }

  const uidKey = String(uid).trim();

  const raw = fs.readFileSync(p, "utf8");
  const lines = raw.split(/\r?\n/);
  if (lines.length < 2) {
    return { ok: false, error: "User list is empty." };
  }

  const headerCells = parseLine(lines[0]!.trim());
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
    const rowUid = normalizeUid(cells[idxUid] ?? "");
    if (rowUid !== uidKey) {
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
    return { ok: false, error: "User not found in user list." };
  }

  const text = out.join("\n").replace(/\n*$/, "") + "\n";
  fs.writeFileSync(p, text, "utf8");
  return { ok: true };
}

/**
 * Admin: set login password for any row in the main user login store by UID (no old password check).
 */
export function setMainLoginPasswordByUid(
  uid: string,
  newPassword: string,
): { ok: true } | { ok: false; error: string } {
  const uidKey = normalizeUid(uid);
  const safePass = String(newPassword ?? "").replace(/,/g, " ").trim();
  if (!uidKey) {
    return { ok: false, error: "UID is required." };
  }
  if (!safePass) {
    return { ok: false, error: "Password is required." };
  }
  const user = findUserByUid(uidKey);
  if (!user) {
    return { ok: false, error: "User not found in user list." };
  }
  const p = userlistPath();
  if (!fs.existsSync(p)) {
    return { ok: false, error: "User list file is missing." };
  }

  const raw = fs.readFileSync(p, "utf8");
  const lines = raw.split(/\r?\n/);
  if (lines.length < 2) {
    return { ok: false, error: "User list is empty." };
  }
  const headerCells = parseLine(lines[0]!.trim());
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
    const rowUid = normalizeUid(cells[idxUid] ?? "");
    if (rowUid !== uidKey) {
      continue;
    }
    while (cells.length < headerCells.length) {
      cells.push("");
    }
    cells[idxPass] = safePass;
    out[i] = cells.join(",");
    found = true;
    break;
  }
  if (!found) {
    return { ok: false, error: "User not found in user list." };
  }
  fs.writeFileSync(p, out.join("\n").replace(/\n*$/, "") + "\n", "utf8");
  return { ok: true };
}

/** Coach Manager: set login password for a Coach row (UID = CoachID) without old password check. */
export function setCoachPasswordByUid(
  coachUid: string,
  newPassword: string
): { ok: true } | { ok: false; error: string } {
  const uidKey = normalizeUid(coachUid);
  const safePass = String(newPassword ?? "").replace(/,/g, " ").trim();
  if (!uidKey) {
    return { ok: false, error: "Coach ID is required." };
  }
  if (!safePass) {
    return { ok: false, error: "Password is required." };
  }
  const user = findUserByUid(uidKey);
  if (!user || user.role !== "Coach") {
    return {
      ok: false,
      error:
        "No Coach login in userLogin.csv matches this Coach ID (add the coach via Create New Coach first).",
    };
  }
  return setMainLoginPasswordByUid(uidKey, safePass);
}

/** Update login username in userLogin.csv for a Coach or Student (UID = CoachID / StudentID). */
export function setLoginUsernameByUid(
  uid: string,
  newUsername: string,
  expectedRole: "Coach" | "Student",
): { ok: true } | { ok: false; error: string } {
  const uidKey = normalizeUid(uid);
  const safeUser = String(newUsername ?? "").replace(/,/g, " ").trim();
  if (!uidKey) {
    return { ok: false, error: "UID is required." };
  }
  if (!safeUser) {
    return { ok: false, error: "Username is required." };
  }
  const user = findUserByUid(uidKey);
  if (!user || user.role !== expectedRole) {
    return {
      ok: false,
      error: `No ${expectedRole} login in userLogin.csv matches this ID.`,
    };
  }
  if (user.username.toLowerCase() === safeUser.toLowerCase()) {
    return { ok: true };
  }
  const conflict = findUserByUsername(safeUser);
  if (conflict && normalizeUid(conflict.uid) !== uidKey) {
    return { ok: false, error: "That username is already in use." };
  }
  const p = userlistPath();
  if (!fs.existsSync(p)) {
    return { ok: false, error: "User list file is missing." };
  }

  const raw = fs.readFileSync(p, "utf8");
  const lines = raw.split(/\r?\n/);
  if (lines.length < 2) {
    return { ok: false, error: "User list is empty." };
  }
  const headerCells = parseLine(lines[0]!);
  const headerLower = headerCells.map((h) => h.toLowerCase());
  const idxUid = headerLower.indexOf("uid");
  const idxUsername = headerLower.indexOf("username");
  if (idxUid < 0 || idxUsername < 0) {
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
    if (normalizeUid(cells[idxUid] ?? "") !== uidKey) {
      continue;
    }
    while (cells.length < headerCells.length) {
      cells.push("");
    }
    cells[idxUsername] = safeUser;
    out[i] = cells.join(",");
    found = true;
    break;
  }
  if (!found) {
    return { ok: false, error: "User not found in user list." };
  }
  fs.writeFileSync(p, out.join("\n").replace(/\n*$/, "") + "\n", "utf8");
  return { ok: true };
}

export function appendCoachManagerRow(input: {
  uid: string;
  username: string;
  password: string;
  fullName: string;
  clubName: string;
  clubPhoto: string;
  /** YYYY-MM-DD or empty */
  expiryDate?: string;
}): { ok: true } | { ok: false; error: string } {
  const username = input.username.trim();
  if (!username) {
    return { ok: false, error: "Username is required." };
  }
  if (findUserByUsername(username)) {
    return { ok: false, error: "The user already existed !" };
  }
  ensureDataDir();
  ensureUserlistSchema();
  const p = userlistPath();
  const today = new Date().toISOString().slice(0, 10);
  const safePass = input.password.replace(/,/g, " ").trim();
  if (!safePass) {
    return { ok: false, error: "Password is required." };
  }
  const safeClub = input.clubName.replace(/,/g, " ").trim();
  const safePhoto = input.clubPhoto.replace(/,/g, "").trim();
  const safeFull = String(input.fullName ?? "").replace(/,/g, " ").trim();
  if (!safeFull) {
    return { ok: false, error: "Full name is required." };
  }
  const line = [
    input.uid.trim(),
    "Coach Manager",
    username.replace(/,/g, " "),
    safePass,
    safeFull,
    "YES",
    today,
    safeClub,
    safePhoto,
    "ACTIVE",
    "",
  ].join(",");
  fs.appendFileSync(p, line + "\n", "utf8");
  return { ok: true };
}

/** Appends a Coach row to userLogin.csv (UID should match CoachID in the club roster, e.g. CH0001). */
export function appendCoachUserRow(input: {
  uid: string;
  username: string;
  password: string;
  clubName: string;
}): { ok: true } | { ok: false; error: string } {
  const username = input.username.trim();
  const uid = input.uid.trim();
  if (!username) {
    return { ok: false, error: "Username is required." };
  }
  if (!uid) {
    return { ok: false, error: "UID is required." };
  }
  if (findUserByUsername(username)) {
    return { ok: false, error: "The user already existed !" };
  }
  if (findUserByUid(uid)) {
    return { ok: false, error: "A user with this UID already exists." };
  }
  ensureDataDir();
  ensureUserlistSchema();
  const p = userlistPath();
  const today = new Date().toISOString().slice(0, 10);
  const safePass = input.password.replace(/,/g, " ").trim();
  if (!safePass) {
    return { ok: false, error: "Password is required." };
  }
  const safeClub = input.clubName.replace(/,/g, " ").trim();

  const line = [
    uid,
    "Coach",
    username.replace(/,/g, " "),
    safePass,
    "",
    "YES",
    today,
    safeClub,
    "",
    "ACTIVE",
    today,
  ].join(",");
  fs.appendFileSync(p, line + "\n", "utf8");
  return { ok: true };
}

/** Appends a Student row to userLogin.csv (UID should match StudentID in the club roster, e.g. S000000001). */
export function appendStudentUserRow(input: {
  uid: string;
  username: string;
  password: string;
  clubName: string;
}): { ok: true } | { ok: false; error: string } {
  const username = input.username.trim();
  const uid = input.uid.trim();
  if (!username) {
    return { ok: false, error: "Username is required." };
  }
  if (!uid) {
    return { ok: false, error: "UID is required." };
  }
  if (findUserByUsername(username)) {
    return { ok: false, error: "The user already existed !" };
  }
  if (findUserByUid(uid)) {
    return { ok: false, error: "A user with this UID already exists." };
  }
  ensureDataDir();
  ensureUserlistSchema();
  const p = userlistPath();
  const today = new Date().toISOString().slice(0, 10);
  const safePass = input.password.replace(/,/g, " ").trim();
  if (!safePass) {
    return { ok: false, error: "Password is required." };
  }
  const safeClub = input.clubName.replace(/,/g, " ").trim();

  const line = [
    uid,
    "Student",
    username.replace(/,/g, " "),
    safePass,
    "",
    "YES",
    today,
    safeClub,
    "",
    "ACTIVE",
    today,
  ].join(",");
  fs.appendFileSync(p, line + "\n", "utf8");
  return { ok: true };
}
