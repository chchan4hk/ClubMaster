import fs from "fs";
import path from "path";
import { readFileCachedStrict } from "./dataFileCache";
import { loadUsersFromCsv, mapUserTypeToRole } from "./userlistCsv";

/** New coach-manager club folders: CM + 8 digits (e.g. CM00000001). */
const COACH_MANAGER_FOLDER_RE = /^CM(\d+)$/i;
/** New country-scoped club folders: 2–3 letters + 7 digits (e.g. HK0000001, USA0000001). */
const COUNTRY_CLUB_FOLDER_RE = /^[A-Z]{2,3}(\d{7})$/i;
/**
 * Legacy club folder ids: C + 1–5 digits (e.g. C0001, C99999).
 * Coach login UIDs use C + 6 digits (C000001); those must not match as folder ids.
 */
const LEGACY_CLUB_FOLDER_C_RE = /^C(\d{1,5})$/i;

/**
 * Legacy roster CoachID: CH + digits (e.g. CH0001).
 * New allocations: C + 6 digits (e.g. C000001), aligned with coach login UID in userLogin_Coach.
 */
function coachIdSequenceNumber(coachId: string): number | null {
  const s = coachId.replace(/^\uFEFF/, "").trim();
  const ch = s.match(/^CH(\d+)$/i);
  if (ch) {
    const n = Number.parseInt(ch[1]!, 10);
    return Number.isNaN(n) ? null : n;
  }
  const cn = s.match(/^C(\d+)$/i);
  if (cn) {
    const n = Number.parseInt(cn[1]!, 10);
    return Number.isNaN(n) ? null : n;
  }
  return null;
}

const COACH_NEW_ID_PAD = 6;

/** Digits after `C` for `{clubFolderUid}-C00001` style IDs (Mongo coach-manager clubs). */
export const COACH_CLUB_PREFIX_ID_PAD = 5;

function escapeRegExpLiteral(s: string): string {
  return s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

/**
 * Normalizes `CM00000008-C1` → `CM00000008-C00001` when the prefix matches `clubFolderUid`.
 * Returns null if the string is not a valid prefixed coach id for that club.
 */
export function normalizePrefixedCoachIdForClub(
  raw: string,
  clubFolderUid: string,
): string | null {
  const club = clubFolderUid.replace(/^\uFEFF/, "").trim();
  const s = raw.replace(/^\uFEFF/, "").trim();
  if (!club || !s) {
    return null;
  }
  const m = s.match(
    new RegExp(`^${escapeRegExpLiteral(club)}-C(\\d+)$`, "i"),
  );
  if (!m) {
    return null;
  }
  const n = Number.parseInt(m[1]!, 10);
  if (Number.isNaN(n) || n < 0) {
    return null;
  }
  return `${club}-C${String(n).padStart(COACH_CLUB_PREFIX_ID_PAD, "0")}`;
}

/** Increments `{club}-C00001` → `{club}-C00002`; falls back to {@link bumpNumericCoachLoginStyleId}. */
export function bumpPrefixedCoachIdForClub(
  clubFolderUid: string,
  current: string,
): string {
  const norm = normalizePrefixedCoachIdForClub(current, clubFolderUid);
  if (!norm) {
    return bumpNumericCoachLoginStyleId(current);
  }
  const club = clubFolderUid.replace(/^\uFEFF/, "").trim();
  const m = norm.match(new RegExp(`^${escapeRegExpLiteral(club)}-C(\\d+)$`, "i"));
  if (!m) {
    return bumpNumericCoachLoginStyleId(current);
  }
  const n = Number.parseInt(m[1]!, 10);
  if (Number.isNaN(n) || n < 0) {
    return bumpNumericCoachLoginStyleId(current);
  }
  return `${club}-C${String(n + 1).padStart(COACH_CLUB_PREFIX_ID_PAD, "0")}`;
}

export function normalizeCoachIdInput(raw: string): string | null {
  const s = raw.replace(/^\uFEFF/, "").trim();
  const ch = s.match(/^CH(\d+)$/i);
  if (ch) {
    return `CH${ch[1]!.padStart(4, "0")}`;
  }
  const cn = s.match(/^C(\d+)$/i);
  if (cn) {
    const n = Number.parseInt(cn[1]!, 10);
    if (Number.isNaN(n) || n < 0) {
      return null;
    }
    return `C${String(n).padStart(COACH_NEW_ID_PAD, "0")}`;
  }
  return null;
}

export type CoachCsvRow = {
  coachId: string;
  clubName: string;
  coachName: string;
  sex: string;
  dateOfBirth: string;
  joinedDate: string;
  homeAddress: string;
  country: string;
  email: string;
  phone: string;
  remark: string;
  hourlyRate: string;
  status: string;
  createdDate: string;
  lastUpdateDate: string;
};

export const COACH_LIST_FILENAME = "UserList_Coach.json";

/** Legacy roster filename; migrated to JSON on first access. */
export const COACH_LIST_CSV_LEGACY = "UserList_Coach.csv";

const COACH_JSON_VERSION = 1;

type CoachListFileJson = {
  version: number;
  coaches: Record<string, string>[];
};

/** Canonical column order for new files and for JSON / raw table rows. */
export const COACH_LIST_COLUMNS: string[] = [
  "coach_id",
  "club_name",
  "full_name",
  "sex",
  "date_of_birth",
  "joined_date",
  "home_address",
  "country",
  "email",
  "contact_number",
  "status",
  "creation_date",
  "remark",
  "lastUpdate_date",
  "hourly_rate (HKD)",
];

export const COACH_LIST_HEADER = COACH_LIST_COLUMNS.join(",");

/** API / UI shape matching UserList_Coach.json field names. */
export function coachCsvRowToApiFields(c: CoachCsvRow): Record<string, string> {
  return {
    coach_id: c.coachId,
    club_name: c.clubName,
    full_name: c.coachName,
    sex: c.sex,
    date_of_birth: c.dateOfBirth,
    joined_date: c.joinedDate,
    home_address: c.homeAddress,
    country: c.country,
    email: c.email,
    contact_number: c.phone,
    status: c.status,
    creation_date: c.createdDate,
    remark: c.remark,
    lastUpdate_date: c.lastUpdateDate,
    "hourly_rate (HKD)": c.hourlyRate,
  };
}

function dataClubRoot(): string {
  const raw = process.env.DATA_CLUB_ROOT?.trim();
  if (raw) {
    return path.isAbsolute(raw)
      ? path.normalize(raw)
      : path.resolve(process.cwd(), raw);
  }
  return path.join(__dirname, "..", "data_club");
}

/** Resolved `data_club` root directory (for diagnostics). */
export function getDataClubRootPath(): string {
  return path.normalize(dataClubRoot());
}

export function coachListPath(clubId: string): string {
  return path.join(dataClubRoot(), clubId.trim(), COACH_LIST_FILENAME);
}

/** Absolute, normalized path to UserList_Coach.json (empty if club id invalid). */
export function coachListResolvedPath(clubId: string): string {
  const id = clubId.trim();
  if (!isValidClubFolderId(id)) {
    return "";
  }
  return path.normalize(coachListPath(id));
}

/** Absolute path to backend/data_club/{clubId}/ (empty string if invalid id). */
export function clubDataDir(clubId: string): string {
  const id = clubId.trim();
  if (!isValidClubFolderId(id)) {
    return "";
  }
  return path.join(dataClubRoot(), id);
}

export function isValidClubFolderId(clubId: string): boolean {
  const s = clubId.trim();
  if (COACH_MANAGER_FOLDER_RE.test(s)) {
    return true;
  }
  if (COUNTRY_CLUB_FOLDER_RE.test(s)) {
    return true;
  }
  return LEGACY_CLUB_FOLDER_C_RE.test(s);
}

const COACH_MANAGER_UID_PAD = 8;

/**
 * Next coach-manager UID / data_club folder id (CM00000001, …).
 * Uses only CM######## rows and folders so the first new club is CM00000001 even if legacy C#### exists.
 */
export function allocateNextClubUid(): string {
  let max = 0;
  for (const u of loadUsersFromCsv()) {
    if (mapUserTypeToRole(u.usertype) !== "CoachManager") {
      continue;
    }
    const uid = u.uid.trim();
    const m = uid.match(COACH_MANAGER_FOLDER_RE);
    if (m) {
      const n = Number.parseInt(m[1]!, 10);
      if (!Number.isNaN(n)) {
        max = Math.max(max, n);
      }
    }
  }
  const root = dataClubRoot();
  if (fs.existsSync(root)) {
    for (const name of fs.readdirSync(root)) {
      const m = name.match(COACH_MANAGER_FOLDER_RE);
      if (m) {
        const n = Number.parseInt(m[1]!, 10);
        if (!Number.isNaN(n)) {
          max = Math.max(max, n);
        }
      }
    }
  }
  const next = max + 1;
  return `CM${String(next).padStart(COACH_MANAGER_UID_PAD, "0")}`;
}

/** RFC-style CSV line parse (handles commas inside "quoted" fields). */
export function parseCsvLine(line: string): string[] {
  const result: string[] = [];
  let current = "";
  let inQuotes = false;
  for (let i = 0; i < line.length; i++) {
    const c = line[i]!;
    if (inQuotes) {
      if (c === '"') {
        if (line[i + 1] === '"') {
          current += '"';
          i++;
        } else {
          inQuotes = false;
        }
      } else {
        current += c;
      }
    } else if (c === '"') {
      inQuotes = true;
    } else if (c === ",") {
      result.push(current.trim());
      current = "";
    } else {
      current += c;
    }
  }
  result.push(current.trim());
  return result;
}

function escapeCsvField(s: string): string {
  const v = String(s ?? "");
  if (/[",\n\r]/.test(v)) {
    return `"${v.replace(/"/g, '""')}"`;
  }
  return v;
}

function joinCsvRow(cells: string[]): string {
  return cells.map(escapeCsvField).join(",");
}

export function coachRowToRecord(c: CoachCsvRow): Record<string, string> {
  const rec: Record<string, string> = {};
  for (const col of COACH_LIST_COLUMNS) {
    rec[col] = "";
  }
  rec.coach_id = c.coachId;
  rec.club_name = c.clubName;
  rec.full_name = c.coachName;
  rec.sex = c.sex;
  rec.date_of_birth = c.dateOfBirth;
  rec.joined_date = c.joinedDate;
  rec.home_address = c.homeAddress;
  rec.country = c.country;
  rec.email = c.email;
  rec.contact_number = c.phone;
  rec.status = c.status;
  rec.creation_date = c.createdDate;
  rec.remark = c.remark;
  rec.lastUpdate_date = c.lastUpdateDate;
  rec["hourly_rate (HKD)"] = c.hourlyRate;
  return rec;
}

function getRecStr(rec: Record<string, unknown>, ...keys: string[]): string {
  for (const k of keys) {
    const v = rec[k];
    if (v != null && String(v).trim() !== "") {
      return String(v).trim();
    }
  }
  return "";
}

function recordToCoachRow(rec: Record<string, unknown>): CoachCsvRow | null {
  const coachId = getRecStr(rec, "coach_id", "CoachID", "coachID");
  if (!coachId) {
    return null;
  }
  return {
    coachId,
    clubName: getRecStr(rec, "club_name", "Club_name"),
    coachName: getRecStr(rec, "full_name", "CoachName"),
    sex: getRecStr(rec, "sex", "Sex"),
    dateOfBirth: getRecStr(rec, "date_of_birth", "Date_of_birth"),
    joinedDate: getRecStr(rec, "joined_date", "Joined_date"),
    homeAddress: getRecStr(rec, "home_address", "Home_address"),
    country: getRecStr(rec, "country", "Country"),
    email: getRecStr(rec, "email", "Email"),
    phone: getRecStr(rec, "contact_number", "Contact_number", "Phone"),
    remark: getRecStr(rec, "remark", "Remark"),
    hourlyRate: getRecStr(
      rec,
      "hourly_rate (HKD)",
      "hourly_rate",
      "Hourly_rate (HKD)",
    ),
    status: getRecStr(rec, "status", "Status") || "ACTIVE",
    createdDate: getRecStr(
      rec,
      "creation_date",
      "created_at",
      "Created_at",
      "created_date",
    ),
    lastUpdateDate: getRecStr(
      rec,
      "lastUpdate_date",
      "LastUpdate_date",
      "last_update_date",
    ),
  };
}

function writeCoachListJsonRaw(absPath: string, data: CoachListFileJson): void {
  fs.writeFileSync(absPath, `${JSON.stringify(data, null, 2)}\n`, "utf8");
}

export function writeCoachListJsonAtPath(absPath: string, coaches: CoachCsvRow[]): void {
  const data: CoachListFileJson = {
    version: COACH_JSON_VERSION,
    coaches: coaches.map(coachRowToRecord),
  };
  writeCoachListJsonRaw(absPath, data);
}

function readCoachListJsonFromPath(absPath: string): CoachCsvRow[] {
  return readFileCachedStrict(absPath, (raw) => {
    let data: CoachListFileJson;
    try {
      data = JSON.parse(raw) as CoachListFileJson;
    } catch (e) {
      throw new Error(
        `UserList_Coach.json: invalid JSON (${e instanceof Error ? e.message : String(e)})`,
      );
    }
    if (!data || !Array.isArray(data.coaches)) {
      return [];
    }
    const out: CoachCsvRow[] = [];
    for (const rec of data.coaches) {
      const row = recordToCoachRow(rec as Record<string, unknown>);
      if (row) {
        out.push(row);
      }
    }
    return out;
  });
}

function stripCredentialKeysFromCoachJsonFile(filePath: string): void {
  if (!filePath || !fs.existsSync(filePath)) {
    return;
  }
  let data: CoachListFileJson;
  try {
    data = JSON.parse(fs.readFileSync(filePath, "utf8")) as CoachListFileJson;
  } catch {
    return;
  }
  if (!data || !Array.isArray(data.coaches)) {
    return;
  }
  let changed = false;
  for (const rec of data.coaches) {
    for (const k of Object.keys(rec)) {
      const nk = legacyNormHeaderCell(k);
      if (nk === "username" || nk === "password") {
        delete rec[k];
        changed = true;
      }
    }
  }
  if (changed) {
    writeCoachListJsonRaw(filePath, data);
  }
}

function upgradeCoachListJsonColumns(clubId: string): void {
  const p = coachListPath(clubId);
  if (!fs.existsSync(p)) {
    return;
  }
  let data: CoachListFileJson;
  try {
    data = JSON.parse(fs.readFileSync(p, "utf8")) as CoachListFileJson;
  } catch {
    return;
  }
  if (!data || !Array.isArray(data.coaches)) {
    return;
  }
  let changed = false;
  for (const rec of data.coaches) {
    const r = rec as Record<string, unknown>;
    if ("CoachID" in r && !("coach_id" in r)) {
      r.coach_id = String(r.CoachID ?? "");
      delete r.CoachID;
      changed = true;
    }
    if ("created_at" in r && !("creation_date" in r)) {
      r.creation_date = String(r.created_at ?? "");
      delete r.created_at;
      changed = true;
    }
  }
  for (const rec of data.coaches) {
    for (const col of COACH_LIST_COLUMNS) {
      if (rec[col] === undefined || rec[col] === null) {
        rec[col] = "";
        changed = true;
      }
    }
  }
  if (changed) {
    data.version = COACH_JSON_VERSION;
    writeCoachListJsonRaw(p, data);
  }
}

function migrateLegacyCoachCsvToJson(clubId: string): void {
  const id = clubId.trim();
  if (!isValidClubFolderId(id)) {
    return;
  }
  const pJson = coachListPath(id);
  const pCsv = path.join(dataClubRoot(), id, COACH_LIST_CSV_LEGACY);
  if (fs.existsSync(pJson) || !fs.existsSync(pCsv)) {
    return;
  }
  const coaches = parseAllCoachesFromCsvFilePath(pCsv);
  writeCoachListJsonAtPath(pJson, coaches);
  fs.unlinkSync(pCsv);
}

function writeCoachListFile(clubId: string, coaches: CoachCsvRow[]): void {
  writeCoachListJsonAtPath(coachListPath(clubId), coaches);
}

/** Normalize header cell for legacy column removal (username / password). */
function legacyNormHeaderCell(h: string): string {
  return h
    .replace(/^\uFEFF/, "")
    .replace(/^"|"$/g, "")
    .replace(/\t/g, " ")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ");
}

/**
 * Remove named columns from a CSV file (e.g. username, password) in place.
 * Used to migrate UserList_Coach.csv / legacy UserList_Student.csv off credential columns.
 */
export function stripCredentialColumnsFromCsvFile(
  filePath: string,
  dropNames: string[],
): void {
  if (!filePath || !fs.existsSync(filePath)) {
    return;
  }
  const drop = new Set(dropNames.map((n) => legacyNormHeaderCell(n)));
  const raw = fs.readFileSync(filePath, "utf8");
  const allLines = raw.split(/\r?\n/);
  let hi = 0;
  while (hi < allLines.length && !allLines[hi]!.trim()) {
    hi++;
  }
  if (hi >= allLines.length) {
    return;
  }
  const headerCells = parseCsvLine(
    allLines[hi]!.trim().replace(/^\uFEFF/, ""),
  );
  const dropIdx: number[] = [];
  headerCells.forEach((cell, i) => {
    if (drop.has(legacyNormHeaderCell(cell))) {
      dropIdx.push(i);
    }
  });
  if (dropIdx.length === 0) {
    return;
  }
  const dropSet = new Set(dropIdx);
  const newHeader = headerCells.filter((_, i) => !dropSet.has(i));
  const out: string[] = [];
  for (let i = 0; i < hi; i++) {
    out.push(allLines[i]!);
  }
  out.push(joinCsvRow(newHeader));
  for (let i = hi + 1; i < allLines.length; i++) {
    const ln = allLines[i];
    if (!ln?.trim()) {
      if (ln !== undefined) {
        out.push(ln);
      }
      continue;
    }
    const cells = parseCsvLine(ln);
    while (cells.length < headerCells.length) {
      cells.push("");
    }
    const newCells = cells.filter((_, j) => !dropSet.has(j));
    out.push(joinCsvRow(newCells));
  }
  fs.writeFileSync(filePath, out.join("\n").replace(/\n*$/, "") + "\n", "utf8");
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

/** First column index whose normalized header matches one of the candidates. */
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

export type CoachColIdx = {
  coachId: number;
  clubName: number;
  coachName: number;
  sex: number;
  dateOfBirth: number;
  joinedDate: number;
  homeAddress: number;
  country: number;
  email: number;
  phone: number;
  remark: number;
  hourlyRate: number;
  status: number;
  createdDate: number;
  lastUpdateDate: number;
};

export function resolveCoachColumnIndices(headerCells: string[]): CoachColIdx {
  return {
    coachId: colIndex(headerCells, [
      "coach_id",
      "CoachID",
      "coachid",
      "Coach ID",
      "coach id",
    ]),
    clubName: colIndex(headerCells, [
      "club_name",
      "Club Name",
      "ClubName",
      "club name",
      "club",
    ]),
    coachName: colIndex(headerCells, [
      "full_name",
      "Full Name",
      "CoachName",
      "Full name",
      "full name",
    ]),
    sex: colIndex(headerCells, ["sex", "Sex"]),
    dateOfBirth: colIndex(headerCells, [
      "date_of_birth",
      "Date of Birth",
      "date of birth",
      "DOB",
      "dob",
    ]),
    joinedDate: colIndex(headerCells, [
      "joined_date",
      "Joined Date",
      "joined date",
      "Join Date",
      "join date",
    ]),
    homeAddress: colIndex(headerCells, [
      "home_address",
      "Home Address",
      "home address",
    ]),
    country: colIndex(headerCells, ["country", "Country"]),
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
      "Contact number",
    ]),
    remark: colIndex(headerCells, ["remark", "Remark", "Specialty", "specialty"]),
    hourlyRate: colIndex(headerCells, [
      "hourly_rate (HKD)",
      "Hourly Rate (HKD)",
      "hourly_rate",
      "Hourly rate",
      "hourly rate",
    ]),
    status: colIndex(headerCells, ["status", "Status"]),
    createdDate: colIndex(headerCells, [
      "creation_date",
      "created_at",
      "Created At",
      "created_date",
      "Created at",
      "created at",
    ]),
    lastUpdateDate: colIndex(headerCells, [
      "lastUpdate_date",
      "Last Update Date",
      "last update date",
      "last_update_date",
      "Last update date",
    ]),
  };
}

function ensureIndices(idx: CoachColIdx): void {
  if (idx.coachId < 0 || idx.coachName < 0) {
    throw new Error(
      "UserList_Coach (CSV import): need coach_id (or CoachID) and a name column (full_name or CoachName).",
    );
  }
}

/** Parse a legacy `UserList_Coach.csv` file (used for one-time migration). */
export function parseAllCoachesFromCsvFilePath(p: string): CoachCsvRow[] {
  if (!fs.existsSync(p)) {
    return [];
  }
  const raw = fs.readFileSync(p, "utf8");
  const lines = raw.split(/\r?\n/).filter((l) => l.trim());
  if (lines.length < 2) {
    return [];
  }
  const headerLine = lines[0]!.replace(/^\uFEFF/, "");
  const headerCells = parseCsvLine(headerLine);
  const idx = resolveCoachColumnIndices(headerCells);
  ensureIndices(idx);
  const out: CoachCsvRow[] = [];
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
    const coachId = get(cells, idx.coachId);
    if (!coachId) {
      continue;
    }
    out.push({
      coachId,
      clubName: get(cells, idx.clubName),
      coachName: get(cells, idx.coachName),
      sex: get(cells, idx.sex),
      dateOfBirth: get(cells, idx.dateOfBirth),
      joinedDate: get(cells, idx.joinedDate),
      homeAddress: get(cells, idx.homeAddress),
      country: get(cells, idx.country),
      email: get(cells, idx.email),
      phone: get(cells, idx.phone),
      remark: get(cells, idx.remark),
      hourlyRate: get(cells, idx.hourlyRate),
      status: get(cells, idx.status) || "ACTIVE",
      createdDate: get(cells, idx.createdDate),
      lastUpdateDate: get(cells, idx.lastUpdateDate),
    });
  }
  return out;
}

export function ensureCoachListFile(clubId: string): void {
  if (!isValidClubFolderId(clubId)) {
    throw new Error("Invalid club ID.");
  }
  const clubDir = path.join(dataClubRoot(), clubId.trim());
  if (!fs.existsSync(clubDir)) {
    fs.mkdirSync(clubDir, { recursive: true });
  }
  migrateLegacyCoachCsvToJson(clubId);
  const p = coachListPath(clubId);
  if (!fs.existsSync(p)) {
    const srcJson = path.join(dataClubRoot(), "Src", COACH_LIST_FILENAME);
    const srcCsv = path.join(dataClubRoot(), "Src", COACH_LIST_CSV_LEGACY);
    if (fs.existsSync(srcJson)) {
      fs.copyFileSync(srcJson, p);
    } else if (fs.existsSync(srcCsv)) {
      const coaches = parseAllCoachesFromCsvFilePath(srcCsv);
      writeCoachListJsonAtPath(p, coaches);
    } else {
      writeCoachListJsonAtPath(p, []);
    }
  }
  stripCredentialKeysFromCoachJsonFile(p);
  upgradeCoachListJsonColumns(clubId);
}

/** Parsed coaches from `data_club/{clubId}/UserList_Coach.json`. */
export function loadCoaches(clubId: string): CoachCsvRow[] {
  if (!isValidClubFolderId(clubId)) {
    return [];
  }
  migrateLegacyCoachCsvToJson(clubId);
  const p = coachListPath(clubId);
  if (!fs.existsSync(p)) {
    return [];
  }
  return readCoachListJsonFromPath(p);
}

/**
 * Uppercase trimmed CoachID → club folder id. First occurrence in `data_club` readdir order
 * wins (same semantics as the legacy linear scan).
 */
const coachIdToClubId = new Map<string, string>();
let coachIdClubIndexReady = false;

/**
 * Rebuilds the CoachID → club folder index from disk (all `UserList_Coach.json` files).
 */
export function rebuildCoachIdClubIndex(): void {
  const next = new Map<string, string>();
  const root = dataClubRoot();
  if (fs.existsSync(root)) {
    for (const name of fs.readdirSync(root)) {
      if (!isValidClubFolderId(name)) {
        continue;
      }
      const coaches = loadCoaches(name);
      for (const c of coaches) {
        const u = c.coachId.replace(/^\uFEFF/, "").trim().toUpperCase();
        if (!u || next.has(u)) {
          continue;
        }
        next.set(u, name);
      }
    }
  }
  coachIdToClubId.clear();
  for (const [k, v] of next) {
    coachIdToClubId.set(k, v);
  }
  coachIdClubIndexReady = true;
}

function ensureCoachIdClubIndex(): void {
  if (!coachIdClubIndexReady) {
    rebuildCoachIdClubIndex();
  }
}

function registerCoachIdInIndex(coachId: string, clubId: string): void {
  const u = coachId.replace(/^\uFEFF/, "").trim().toUpperCase();
  if (!u) {
    return;
  }
  if (!coachIdToClubId.has(u)) {
    coachIdToClubId.set(u, clubId);
  }
}

/** First club folder id whose UserList_Coach.json lists this CoachID, or null. */
export function findClubUidForCoachId(coachId: string): string | null {
  const uid = coachId.trim();
  if (!uid) {
    return null;
  }
  ensureCoachIdClubIndex();
  return coachIdToClubId.get(uid.toUpperCase()) ?? null;
}

function normEqCoachField(a: string, b: string): boolean {
  return (
    String(a ?? "").trim().toLowerCase() === String(b ?? "").trim().toLowerCase()
  );
}

/** Exact case-insensitive match on coach name and/or email (both provided = AND). */
export function searchCoachesInClub(
  clubId: string,
  coachName?: string,
  email?: string,
): CoachCsvRow[] {
  const nq = (coachName ?? "").trim();
  const eq = (email ?? "").trim();
  if (!nq && !eq) {
    return [];
  }
  if (!isValidClubFolderId(clubId)) {
    return [];
  }
  return loadCoaches(clubId).filter((row) => {
    if (nq && !normEqCoachField(row.coachName, nq)) {
      return false;
    }
    if (eq && !normEqCoachField(row.email, eq)) {
      return false;
    }
    return true;
  });
}

export type CoachListRaw = {
  /** e.g. data_club/C0001/UserList_Coach.json */
  relativePath: string;
  headers: string[];
  rows: string[][];
};

/**
 * Tabular view of `data_club/{clubId}/UserList_Coach.json` for API / raw CSV-style UI.
 */
export function loadCoachListRaw(clubId: string): CoachListRaw {
  const id = clubId.trim();
  const relativePath = `data_club/${id}/${COACH_LIST_FILENAME}`;
  if (!isValidClubFolderId(id)) {
    return { relativePath, headers: [], rows: [] };
  }
  ensureCoachListFile(id);
  const coaches = loadCoaches(id);
  const headers = [...COACH_LIST_COLUMNS];
  const rows = coaches.map((c) => {
    const rec = coachRowToRecord(c);
    return COACH_LIST_COLUMNS.map((col) => rec[col] ?? "");
  });
  return { relativePath, headers, rows };
}

export function allocateNextCoachId(clubId: string): string {
  ensureCoachListFile(clubId);
  return nextCoachId(loadCoaches(clubId));
}

function nextCoachId(rows: CoachCsvRow[]): string {
  let max = 0;
  for (const r of rows) {
    const n = coachIdSequenceNumber(r.coachId);
    if (n != null && n > max) {
      max = n;
    }
  }
  return `C${String(max + 1).padStart(COACH_NEW_ID_PAD, "0")}`;
}

/** Bump `C######` by one (Mongo `userLogin.uid` is global across clubs). */
export function bumpNumericCoachLoginStyleId(current: string): string {
  const s = String(current ?? "").replace(/^\uFEFF/, "").trim();
  const m = s.match(/^C(\d+)$/i);
  if (!m) {
    return s;
  }
  const n = Number.parseInt(m[1]!, 10);
  if (Number.isNaN(n) || n < 0) {
    return s;
  }
  return `C${String(n + 1).padStart(COACH_NEW_ID_PAD, "0")}`;
}

function sanitizeCell(s: string): string {
  return String(s ?? "").replace(/,/g, " ").trim();
}

/** Match CoachID from CSV vs request (trim, strip BOM, ignore case). */
export function coachIdsEqual(a: string, b: string): boolean {
  const x = String(a ?? "")
    .replace(/^\uFEFF/, "")
    .trim();
  const y = String(b ?? "")
    .replace(/^\uFEFF/, "")
    .trim();
  return x.length > 0 && x.toUpperCase() === y.toUpperCase();
}

/**
 * Numeric coach sequence for this club: `CH…`, bare `C…`, or `{club}-C…` roster ids vs
 * coach role-login `uid` (typically `C######`). Used so sign-in matches roster when formats differ.
 */
function rosterCoachNumericTailForClub(
  clubFolderUid: string,
  coachId: string,
): number | null {
  const club = clubFolderUid.replace(/^\uFEFF/, "").trim();
  const s = String(coachId ?? "").replace(/^\uFEFF/, "").trim();
  if (!club || !s) {
    return null;
  }
  const plainSeq = coachIdSequenceNumber(s);
  if (plainSeq != null) {
    return plainSeq;
  }
  const norm = normalizePrefixedCoachIdForClub(s, club);
  if (!norm) {
    return null;
  }
  const m = norm.match(/-C(\d+)$/i);
  if (!m) {
    return null;
  }
  const n = Number.parseInt(m[1]!, 10);
  return Number.isNaN(n) ? null : n;
}

/** True when roster `CoachID` and coach login `uid` refer to the same coach within `clubFolderUid`. */
export function coachLoginUidMatchesRosterCoachId(
  clubFolderUid: string,
  rosterCoachId: string,
  loginUid: string,
): boolean {
  if (coachIdsEqual(rosterCoachId, loginUid)) {
    return true;
  }
  const a = rosterCoachNumericTailForClub(clubFolderUid, rosterCoachId);
  const b = rosterCoachNumericTailForClub(clubFolderUid, loginUid);
  return a != null && b != null && a === b;
}

export function appendCoachRow(
  clubId: string,
  clubName: string,
  input: {
    coachName: string;
    email: string;
    phone: string;
    sex?: string;
    dateOfBirth?: string;
    joinedDate?: string;
    homeAddress?: string;
    country?: string;
    remark?: string;
    hourlyRate?: string;
    status?: string;
    /** When set (e.g. pre-allocated for userLogin), must not already exist in the coach list. */
    coachId?: string;
  }
): { ok: true; coachId: string } | { ok: false; error: string } {
  const name = sanitizeCell(input.coachName);
  if (!name) {
    return { ok: false, error: "full_name is required." };
  }
  const email = sanitizeCell(input.email);
  ensureCoachListFile(clubId);
  const coaches = loadCoaches(clubId);
  const requested = input.coachId?.trim();
  let coachId: string;
  if (requested) {
    const prefixed = normalizePrefixedCoachIdForClub(requested, clubId);
    const normalized = normalizeCoachIdInput(requested);
    const chosen = prefixed ?? normalized;
    if (!chosen) {
      return {
        ok: false,
        error:
          "Invalid CoachID format (expected CH####, C######, or {Club_ID}-C#####).",
      };
    }
    if (coaches.some((r) => coachIdsEqual(r.coachId, chosen))) {
      return { ok: false, error: "CoachID already exists in coach list." };
    }
    coachId = chosen;
  } else {
    coachId = nextCoachId(coaches);
  }
  const today = new Date().toISOString().slice(0, 10);
  const status = sanitizeCell(input.status || "ACTIVE") || "ACTIVE";
  coaches.push({
    coachId,
    clubName: sanitizeCell(clubName),
    coachName: name,
    sex: sanitizeCell(input.sex ?? ""),
    dateOfBirth: sanitizeCell(input.dateOfBirth ?? ""),
    joinedDate: sanitizeCell(input.joinedDate ?? ""),
    homeAddress: sanitizeCell(input.homeAddress ?? ""),
    country: sanitizeCell(input.country ?? ""),
    email,
    phone: sanitizeCell(input.phone),
    remark: sanitizeCell(input.remark ?? ""),
    hourlyRate: sanitizeCell(input.hourlyRate ?? ""),
    status,
    createdDate: today,
    lastUpdateDate: today,
  });
  writeCoachListFile(clubId, coaches);
  registerCoachIdInIndex(coachId, clubId);
  return { ok: true, coachId };
}

export function updateCoachRow(
  clubId: string,
  clubName: string,
  coachId: string,
  input: {
    coachName: string;
    email: string;
    phone: string;
    sex?: string;
    dateOfBirth?: string;
    joinedDate?: string;
    homeAddress?: string;
    country?: string;
    remark?: string;
    hourlyRate?: string;
    status?: string;
  }
): { ok: true } | { ok: false; error: string } {
  const id = coachId.trim();
  if (!id) {
    return { ok: false, error: "CoachID is required." };
  }
  const name = sanitizeCell(input.coachName);
  if (!name) {
    return { ok: false, error: "full_name is required." };
  }
  const email = sanitizeCell(input.email);
  ensureCoachListFile(clubId);
  const coaches = loadCoaches(clubId);
  const idx = coaches.findIndex((c) => coachIdsEqual(c.coachId, id));
  if (idx < 0) {
    return { ok: false, error: "Coach not found." };
  }
  const today = new Date().toISOString().slice(0, 10);
  const status = sanitizeCell(input.status || "ACTIVE") || "ACTIVE";
  const prev = coaches[idx]!;
  coaches[idx] = {
    ...prev,
    clubName: sanitizeCell(clubName),
    coachName: name,
    sex: sanitizeCell(input.sex ?? ""),
    dateOfBirth: sanitizeCell(input.dateOfBirth ?? ""),
    joinedDate: sanitizeCell(input.joinedDate ?? ""),
    homeAddress: sanitizeCell(input.homeAddress ?? ""),
    country: sanitizeCell(input.country ?? ""),
    email,
    phone: sanitizeCell(input.phone),
    remark: sanitizeCell(input.remark ?? ""),
    hourlyRate: sanitizeCell(input.hourlyRate ?? ""),
    status,
    lastUpdateDate: today,
  };
  writeCoachListFile(clubId, coaches);
  return { ok: true };
}

export function removeCoachRow(
  clubId: string,
  coachId: string
): { ok: true } | { ok: false; error: string } {
  const id = coachId.trim();
  if (!id) {
    return { ok: false, error: "CoachID is required." };
  }
  ensureCoachListFile(clubId);
  const coaches = loadCoaches(clubId);
  const today = new Date().toISOString().slice(0, 10);
  let found = false;
  const next = coaches.map((c) => {
    if (!coachIdsEqual(c.coachId, id)) {
      return c;
    }
    found = true;
    return { ...c, status: "INACTIVE", lastUpdateDate: today };
  });
  if (!found) {
    return { ok: false, error: "Coach not found." };
  }
  writeCoachListFile(clubId, next);
  return { ok: true };
}

/** Deletes the coach data row from UserList_Coach.json (disk only; no index rebuild). */
function purgeCoachRowOnDisk(
  clubId: string,
  coachId: string,
): { ok: true } | { ok: false; error: string } {
  const id = coachId.trim();
  if (!id) {
    return { ok: false, error: "CoachID is required." };
  }
  ensureCoachListFile(clubId);
  const coaches = loadCoaches(clubId);
  const before = coaches.length;
  const next = coaches.filter((c) => !coachIdsEqual(c.coachId, id));
  if (next.length === before) {
    return { ok: false, error: "Coach not found." };
  }
  writeCoachListFile(clubId, next);
  return { ok: true };
}

/** Deletes the coach data row from UserList_Coach.json (row removed, not marked INACTIVE). */
export function purgeCoachRow(
  clubId: string,
  coachId: string,
): { ok: true } | { ok: false; error: string } {
  const r = purgeCoachRowOnDisk(clubId, coachId);
  if (r.ok) {
    rebuildCoachIdClubIndex();
  }
  return r;
}

const PURGE_COACH_SKIP_ERRORS = new Set([
  "Coach not found.",
  "Coach list not found.",
  "UserList_Coach.json: missing coach_id column.",
  "Invalid CSV header.",
]);

/**
 * Removes the coach row from every `data_club/{clubFolderId}/UserList_Coach.json` that contains this CoachID.
 * Returns `{ ok: true, updatedClubIds: [] }` when no roster contained this CoachID (not an error — login/main cleanup may still be needed).
 */
export function purgeCoachRowFromAllClubFolders(
  coachId: string,
):
  | { ok: true; updatedClubIds: string[] }
  | { ok: false; error: string } {
  const id = coachId.trim();
  if (!id) {
    return { ok: false, error: "CoachID is required." };
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
    const r = purgeCoachRowOnDisk(folder, id);
    if (r.ok) {
      updatedClubIds.push(folder);
      continue;
    }
    if (PURGE_COACH_SKIP_ERRORS.has(r.error)) {
      continue;
    }
    return r;
  }
  if (updatedClubIds.length > 0) {
    rebuildCoachIdClubIndex();
  }
  return { ok: true, updatedClubIds };
}
