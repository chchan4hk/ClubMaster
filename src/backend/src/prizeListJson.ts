import fs from "fs";
import path from "path";
import {
  parseCsvLine,
  isValidClubFolderId,
  getDataClubRootPath,
} from "./coachListCsv";
import { findUserByUid } from "./userlistCsv";
import { isMongoConfigured } from "./db/DBConnection";
import { userLoginCsvReadFallbackEnabled } from "./userListMongo";

const PRIZE_ID_RE = /^PR(\d+)$/i;
/** Legacy global `PR…` prize ids: numeric width for new allocations. */
export const PRIZE_ID_NUM_WIDTH = 5;
const LEGACY_CSV_NAME = "PrizeList.csv";

/**
 * Prize list: `backend/data_club/{folder}/PrizeList.json` only (no PrizeList.csv writes).
 * Each prize includes `ClubID` (session UID) and `Club_name` (manager account club name).
 * Default folder is the session club UID. Optional override:
 * `PRIZE_LIST_STORAGE_CLUB_ID=CM00000001` → read/write that folder instead.
 */
function resolvePrizeStorageClubId(requestClubId: string): string {
  const raw = process.env.PRIZE_LIST_STORAGE_CLUB_ID;
  if (raw === undefined || raw === "") {
    return requestClubId.trim();
  }
  const t = String(raw).trim();
  if (t.toLowerCase() === "session" || t === "0") {
    return requestClubId.trim();
  }
  if (isValidClubFolderId(t)) {
    return t;
  }
  return requestClubId.trim();
}

export type PrizeCsvRow = {
  prizeId: string;
  /** Same as coach-manager / club folder UID (JSON key ClubID). */
  clubId: string;
  /** Display name from coach manager account (JSON key Club_name). */
  clubName: string;
  sportType: string;
  year: string;
  association: string;
  competition: string;
  ageGroup: string;
  prizeType: string;
  studentName: string;
  ranking: string;
  status: string;
  createdAt: string;
  lastUpdatedDate: string;
  verifiedBy: string;
  remarks: string;
};

export const PRIZE_LIST_FILENAME = "PrizeList.json";

/** Column order for API / raw table fallback (matches former CSV). */
export const PRIZE_LIST_COLUMNS: string[] = [
  "PrizeID",
  "ClubID",
  "Club_name",
  "SportType",
  "Year",
  "Association",
  "Competition",
  "Age_group",
  "Prize_type",
  "StudentName",
  "Ranking",
  "Status",
  "Created_at",
  "LastUpdated_Date",
  "VerifiedBy",
  "Remarks",
];

export function prizeCsvRowToApiFields(p: PrizeCsvRow): Record<string, string> {
  return {
    PrizeID: p.prizeId,
    ClubID: p.clubId,
    Club_name: p.clubName,
    SportType: p.sportType,
    Year: p.year,
    Association: p.association,
    Competition: p.competition,
    Age_group: p.ageGroup,
    Prize_type: p.prizeType,
    StudentName: p.studentName,
    Ranking: p.ranking,
    Status: p.status,
    Created_at: p.createdAt,
    LastUpdated_Date: p.lastUpdatedDate,
    VerifiedBy: p.verifiedBy,
    Remarks: p.remarks,
  };
}

function dataClubRoot(): string {
  return getDataClubRootPath();
}

export function prizeListPath(clubId: string): string {
  return path.join(
    dataClubRoot(),
    resolvePrizeStorageClubId(clubId),
    PRIZE_LIST_FILENAME,
  );
}

function legacyPrizeCsvPath(clubId: string): string {
  return path.join(
    dataClubRoot(),
    resolvePrizeStorageClubId(clubId),
    LEGACY_CSV_NAME,
  );
}

/** Folder id used for PrizeList.json (after optional PRIZE_LIST_STORAGE_CLUB_ID pin). */
export function prizeListStorageClubId(requestClubId: string): string {
  const id = requestClubId.trim();
  if (!isValidClubFolderId(id)) {
    return id;
  }
  return resolvePrizeStorageClubId(id);
}

export function prizeListResolvedPath(clubId: string): string {
  const id = clubId.trim();
  if (!isValidClubFolderId(id)) {
    return "";
  }
  return path.normalize(prizeListPath(id));
}

function rowToJsonObject(p: PrizeCsvRow): Record<string, string> {
  return prizeCsvRowToApiFields(p);
}

function rowFromJsonObject(o: Record<string, unknown>): PrizeCsvRow | null {
  const g = (k: string) => String(o[k] ?? "").trim();
  const first = (...keys: string[]) => {
    for (const k of keys) {
      const v = g(k);
      if (v) {
        return v;
      }
    }
    return "";
  };
  const prizeId = first("PrizeID", "prize_id");
  const studentName = first("StudentName", "student_name");
  if (!prizeId || !studentName) {
    return null;
  }
  return {
    prizeId,
    clubId: first("ClubID", "club_id"),
    clubName: first("Club_name", "club_name"),
    sportType: first("SportType", "sport_type"),
    year: first("Year", "year"),
    association: first("Association", "association"),
    competition: first("Competition", "competition"),
    ageGroup: first("Age_group", "age_group"),
    prizeType: first("Prize_type", "prize_type"),
    studentName,
    ranking: first("Ranking", "ranking"),
    status: first("Status", "status") || "ACTIVE",
    createdAt: first("Created_at", "Creation_date", "created_at"),
    lastUpdatedDate: first(
      "LastUpdated_Date",
      "lastUpdate_date",
      "last_updated_date",
    ),
    verifiedBy: first("VerifiedBy", "verified_by"),
    remarks: first("Remarks", "remarks"),
  };
}

function readPrizeListDocument(clubId: string): PrizeCsvRow[] {
  const p = prizeListPath(clubId); // resolves storage folder
  if (!fs.existsSync(p)) {
    return [];
  }
  let raw: unknown;
  try {
    raw = JSON.parse(fs.readFileSync(p, "utf8"));
  } catch {
    throw new Error("PrizeList.json is not valid JSON.");
  }
  if (!raw || typeof raw !== "object") {
    return [];
  }
  const prizes = (raw as { prizes?: unknown }).prizes;
  if (!Array.isArray(prizes)) {
    return [];
  }
  const out: PrizeCsvRow[] = [];
  for (const item of prizes) {
    if (!item || typeof item !== "object") {
      continue;
    }
    const row = rowFromJsonObject(item as Record<string, unknown>);
    if (row) {
      out.push(row);
    }
  }
  return out;
}

function writePrizeListDocument(clubId: string, rows: PrizeCsvRow[]): void {
  const p = prizeListPath(clubId);
  const doc = { prizes: rows.map(rowToJsonObject) };
  fs.writeFileSync(p, `${JSON.stringify(doc, null, 2)}\n`, "utf8");
  // Prizes are JSON-only; never keep a parallel PrizeList.csv after any save.
  const csvP = legacyPrizeCsvPath(clubId);
  if (fs.existsSync(csvP)) {
    try {
      fs.unlinkSync(csvP);
    } catch {
      /* ignore */
    }
  }
}

/* --- legacy CSV migration (one-time per club folder) --- */

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

type CsvColIdx = {
  prizeId: number;
  clubId: number;
  clubName: number;
  sportType: number;
  year: number;
  association: number;
  competition: number;
  ageGroup: number;
  prizeType: number;
  studentName: number;
  ranking: number;
  status: number;
  createdAt: number;
  lastUpdatedDate: number;
  verifiedBy: number;
  remarks: number;
};

function resolveCsvColumnIndices(headerCells: string[]): CsvColIdx {
  return {
    prizeId: colIndex(headerCells, ["PrizeID", "prize id", "Prize Id"]),
    clubId: colIndex(headerCells, ["ClubID", "Club Id", "club id", "clubid"]),
    clubName: colIndex(headerCells, [
      "Club_name",
      "Club name",
      "club_name",
      "Club Name",
    ]),
    sportType: colIndex(headerCells, ["SportType", "sport type", "Sport Type"]),
    year: colIndex(headerCells, ["Year", "year"]),
    association: colIndex(headerCells, ["Association", "association"]),
    competition: colIndex(headerCells, ["Competition", "competition"]),
    ageGroup: colIndex(headerCells, [
      "Age_group",
      "Age group",
      "age_group",
      "Age Group",
    ]),
    prizeType: colIndex(headerCells, [
      "Prize_type",
      "Prize type",
      "prize_type",
      "Prize Type",
    ]),
    studentName: colIndex(headerCells, [
      "StudentName",
      "Student Name",
      "student name",
    ]),
    ranking: colIndex(headerCells, ["Ranking", "ranking"]),
    status: colIndex(headerCells, ["Status", "status"]),
    createdAt: colIndex(headerCells, [
      "Created_at",
      "Created At",
      "created_at",
      "Created at",
    ]),
    lastUpdatedDate: colIndex(headerCells, [
      "LastUpdated_Date",
      "Last Updated Date",
      "lastupdated_date",
      "Last Update Date",
      "last update date",
    ]),
    verifiedBy: colIndex(headerCells, [
      "VerifiedBy",
      "Verified By",
      "verified by",
    ]),
    remarks: colIndex(headerCells, ["Remarks", "remarks", "Remark", "remark"]),
  };
}

function parseLegacyCsvFile(
  csvPath: string,
  defaultClubId: string,
): PrizeCsvRow[] {
  if (!fs.existsSync(csvPath)) {
    return [];
  }
  const raw = fs.readFileSync(csvPath, "utf8");
  const lines = raw.split(/\r?\n/).filter((l) => l.trim());
  if (lines.length < 2) {
    return [];
  }
  const headerCells = parseCsvLine(lines[0]!.replace(/^\uFEFF/, ""));
  const idx = resolveCsvColumnIndices(headerCells);
  if (idx.prizeId < 0 || idx.studentName < 0) {
    return [];
  }
  const get = (cells: string[], ix: number) =>
    ix >= 0 && ix < cells.length ? (cells[ix] ?? "").trim() : "";
  const out: PrizeCsvRow[] = [];
  for (let i = 1; i < lines.length; i++) {
    const cells = parseCsvLine(lines[i]!);
    while (cells.length < headerCells.length) {
      cells.push("");
    }
    const prizeId = get(cells, idx.prizeId);
    if (!prizeId) {
      continue;
    }
    const cid =
      idx.clubId >= 0 ? get(cells, idx.clubId) : "";
    const cname = idx.clubName >= 0 ? get(cells, idx.clubName) : "";
    out.push({
      prizeId,
      clubId: cid || defaultClubId,
      clubName: cname,
      sportType: get(cells, idx.sportType),
      year: get(cells, idx.year),
      association: get(cells, idx.association),
      competition: get(cells, idx.competition),
      ageGroup: get(cells, idx.ageGroup),
      prizeType: get(cells, idx.prizeType),
      studentName: get(cells, idx.studentName),
      ranking: get(cells, idx.ranking),
      status: get(cells, idx.status) || "ACTIVE",
      createdAt: get(cells, idx.createdAt),
      lastUpdatedDate: get(cells, idx.lastUpdatedDate),
      verifiedBy: get(cells, idx.verifiedBy),
      remarks: get(cells, idx.remarks),
    });
  }
  return out;
}

function migrateLegacyCsvIfPresent(clubId: string): void {
  const jsonP = prizeListPath(clubId);
  const csvP = legacyPrizeCsvPath(clubId);
  if (fs.existsSync(jsonP) || !fs.existsSync(csvP)) {
    return;
  }
  const rows = parseLegacyCsvFile(
    csvP,
    resolvePrizeStorageClubId(clubId),
  );
  writePrizeListDocument(clubId, rows);
  try {
    fs.unlinkSync(csvP);
  } catch {
    /* keep csv if delete fails */
  }
}

/**
 * If PrizeList.json exists but has no prize rows while PrizeList.csv still has data
 * (legacy split state), import CSV into JSON and drop CSV. If JSON already has rows,
 * remove stray CSV only. JSON is always the source of truth after this runs.
 */
function reconcileLegacyPrizeCsvWithJson(clubId: string): void {
  const csvP = legacyPrizeCsvPath(clubId);
  if (!fs.existsSync(csvP)) {
    return;
  }
  let jsonRows: PrizeCsvRow[] = [];
  try {
    jsonRows = readPrizeListDocument(clubId);
  } catch {
    return;
  }
  if (jsonRows.length > 0) {
    try {
      fs.unlinkSync(csvP);
    } catch {
      /* ignore */
    }
    return;
  }
  const fromCsv = parseLegacyCsvFile(
    csvP,
    resolvePrizeStorageClubId(clubId),
  );
  if (fromCsv.length > 0) {
    writePrizeListDocument(clubId, fromCsv);
  } else {
    try {
      fs.unlinkSync(csvP);
    } catch {
      /* ignore */
    }
  }
}

export function ensurePrizeListFile(clubId: string): void {
  if (!isValidClubFolderId(clubId)) {
    throw new Error("Invalid club ID.");
  }
  const storageId = resolvePrizeStorageClubId(clubId);
  const clubDir = path.join(dataClubRoot(), storageId);
  if (!fs.existsSync(clubDir)) {
    fs.mkdirSync(clubDir, { recursive: true });
  }
  migrateLegacyCsvIfPresent(clubId);
  const p = prizeListPath(clubId);
  if (!fs.existsSync(p)) {
    writePrizeListDocument(clubId, []);
  }
  reconcileLegacyPrizeCsvWithJson(clubId);
}

export function loadPrizes(clubId: string): PrizeCsvRow[] {
  if (!isValidClubFolderId(clubId)) {
    return [];
  }
  ensurePrizeListFile(clubId);
  const sessionUid = clubId.trim();
  const rows = readPrizeListDocument(clubId);
  const mgr =
    isMongoConfigured() && !userLoginCsvReadFallbackEnabled()
      ? undefined
      : findUserByUid(sessionUid);
  const defaultClubName =
    mgr?.clubName && mgr.clubName.trim() && mgr.clubName.trim() !== "—"
      ? mgr.clubName.trim()
      : "";
  return rows.map((r) => {
    let next =
      r.clubId && r.clubId.trim() ? r : { ...r, clubId: sessionUid };
    if (!next.clubName?.trim() && defaultClubName) {
      next = { ...next, clubName: defaultClubName };
    }
    return next;
  });
}

/** Uppercase PrizeID → club folder UID (row `ClubID`, or session folder when missing). */
const prizeIdToClubId = new Map<string, string>();
let prizeIdClubIndexReady = false;

/**
 * Rebuilds PrizeID → club UID from all distinct `PrizeList.json` files (dedupes storage-pin).
 */
export function rebuildPrizeIdClubIndex(): void {
  const next = new Map<string, string>();
  const root = dataClubRoot();
  if (fs.existsSync(root)) {
    const seenFiles = new Set<string>();
    for (const name of fs.readdirSync(root)) {
      if (!isValidClubFolderId(name)) {
        continue;
      }
      const abs = path.normalize(prizeListPath(name));
      if (seenFiles.has(abs)) {
        continue;
      }
      if (!fs.existsSync(abs)) {
        continue;
      }
      seenFiles.add(abs);
      const rows = readPrizeListDocument(name);
      for (const r of rows) {
        const u = r.prizeId.replace(/^\uFEFF/, "").trim().toUpperCase();
        if (!u || next.has(u)) {
          continue;
        }
        const logical = (r.clubId && r.clubId.trim()) || name;
        next.set(u, logical);
      }
    }
  }
  prizeIdToClubId.clear();
  for (const [k, v] of next) {
    prizeIdToClubId.set(k, v);
  }
  prizeIdClubIndexReady = true;
}

function ensurePrizeIdClubIndex(): void {
  if (!prizeIdClubIndexReady) {
    rebuildPrizeIdClubIndex();
  }
}

function registerPrizeIdInIndex(prizeId: string, clubUid: string): void {
  const u = prizeId.replace(/^\uFEFF/, "").trim().toUpperCase();
  const c = clubUid.trim();
  if (!u || !c) {
    return;
  }
  if (!prizeIdToClubId.has(u)) {
    prizeIdToClubId.set(u, c);
  }
}

function upsertPrizeIdInIndex(prizeId: string, clubUid: string): void {
  const u = prizeId.replace(/^\uFEFF/, "").trim().toUpperCase();
  const c = clubUid.trim();
  if (!u || !c) {
    return;
  }
  prizeIdToClubId.set(u, c);
}

function removePrizeIdFromIndex(prizeId: string): void {
  const u = prizeId.replace(/^\uFEFF/, "").trim().toUpperCase();
  if (u) {
    prizeIdToClubId.delete(u);
  }
}

/** Club folder UID for this PrizeID (from row ClubID / first roster match), or null. */
export function findClubUidForPrizeId(prizeId: string): string | null {
  const id = prizeId.trim();
  if (!id) {
    return null;
  }
  ensurePrizeIdClubIndex();
  return prizeIdToClubId.get(id.toUpperCase()) ?? null;
}

export type PrizeListRaw = {
  relativePath: string;
  headers: string[];
  rows: string[][];
};

/** Tabular raw view from in-memory rows (e.g. after student-roster name enrichment). */
export function prizeCsvRowsToPrizeListRaw(
  clubId: string,
  prizes: PrizeCsvRow[],
): PrizeListRaw {
  const id = clubId.trim();
  const storageId = isValidClubFolderId(id)
    ? resolvePrizeStorageClubId(id)
    : id;
  const relativePath = `data_club/${storageId}/${PRIZE_LIST_FILENAME}`;
  if (!isValidClubFolderId(id)) {
    return { relativePath, headers: [], rows: [] };
  }
  const headers = PRIZE_LIST_COLUMNS;
  const rows = prizes.map((p) => {
    const api = prizeCsvRowToApiFields(p);
    return headers.map((h) => api[h] ?? "");
  });
  return { relativePath, headers, rows };
}

/** Tabular shape for UI fallback (same as former CSV raw view). */
export function loadPrizeListRaw(clubId: string): PrizeListRaw {
  const id = clubId.trim();
  const storageId = isValidClubFolderId(id)
    ? resolvePrizeStorageClubId(id)
    : id;
  const relativePath = `data_club/${storageId}/${PRIZE_LIST_FILENAME}`;
  if (!isValidClubFolderId(id)) {
    return { relativePath, headers: [], rows: [] };
  }
  ensurePrizeListFile(clubId);
  let prizes: PrizeCsvRow[] = [];
  try {
    prizes = loadPrizes(clubId);
  } catch {
    return {
      relativePath,
      headers: PRIZE_LIST_COLUMNS,
      rows: [],
    };
  }
  const headers = PRIZE_LIST_COLUMNS;
  const rows = prizes.map((p) => {
    const api = prizeCsvRowToApiFields(p);
    return headers.map((h) => api[h] ?? "");
  });
  return { relativePath, headers, rows };
}

function nextPrizeId(rows: PrizeCsvRow[]): string {
  let max = 0;
  for (const r of rows) {
    const m = r.prizeId.match(PRIZE_ID_RE);
    if (m) {
      const n = Number.parseInt(m[1]!, 10);
      if (!Number.isNaN(n) && n > max) {
        max = n;
      }
    }
  }
  return `PR${String(max + 1).padStart(PRIZE_ID_NUM_WIDTH, "0")}`;
}

export function allocateNextPrizeId(clubId: string): string {
  ensurePrizeListFile(clubId);
  return nextPrizeId(loadPrizes(clubId));
}

function sanitizeCell(s: string): string {
  return String(s ?? "").replace(/,/g, " ").trim();
}

function prizeIdsEqual(a: string, b: string): boolean {
  const x = String(a ?? "")
    .replace(/^\uFEFF/, "")
    .trim();
  const y = String(b ?? "")
    .replace(/^\uFEFF/, "")
    .trim();
  return x.length > 0 && x.toUpperCase() === y.toUpperCase();
}

export function appendPrizeRow(
  clubId: string,
  input: {
    clubName: string;
    sportType: string;
    year: string;
    association: string;
    competition: string;
    ageGroup: string;
    prizeType: string;
    studentName: string;
    ranking: string;
    status?: string;
    verifiedBy: string;
    remarks: string;
    prizeId?: string;
  },
): { ok: true; prizeId: string } | { ok: false; error: string } {
  const studentName = sanitizeCell(input.studentName);
  if (!studentName) {
    return { ok: false, error: "StudentName is required." };
  }
  const competition = sanitizeCell(input.competition);
  if (!competition) {
    return { ok: false, error: "Competition is required." };
  }
  ensurePrizeListFile(clubId);
  const rows = readPrizeListDocument(clubId);
  const requested = input.prizeId?.trim();
  let prizeId: string;
  if (requested) {
    if (!PRIZE_ID_RE.test(requested)) {
      return {
        ok: false,
        error: `Invalid PrizeID format (expected PR + ${PRIZE_ID_NUM_WIDTH} digits).`,
      };
    }
    const normalized = `PR${requested.match(PRIZE_ID_RE)![1]!.padStart(PRIZE_ID_NUM_WIDTH, "0")}`;
    if (rows.some((r) => prizeIdsEqual(r.prizeId, normalized))) {
      return { ok: false, error: "PrizeID already exists in prize list." };
    }
    prizeId = normalized;
  } else {
    prizeId = nextPrizeId(rows);
  }
  const today = new Date().toISOString().slice(0, 10);
  const status = sanitizeCell(input.status || "ACTIVE") || "ACTIVE";
  /** Logged-in club UID (same as backend/data_club/{UID}/ folder for session). */
  const sessionUid = clubId.trim();
  const clubNameStored = String(input.clubName ?? "").trim();
  rows.push({
    prizeId,
    clubId: sessionUid,
    clubName: clubNameStored,
    sportType: sanitizeCell(input.sportType),
    year: sanitizeCell(input.year),
    association: sanitizeCell(input.association),
    competition,
    ageGroup: sanitizeCell(input.ageGroup),
    prizeType: sanitizeCell(input.prizeType),
    studentName,
    ranking: sanitizeCell(input.ranking),
    status,
    createdAt: today,
    lastUpdatedDate: today,
    verifiedBy: sanitizeCell(input.verifiedBy),
    remarks: sanitizeCell(input.remarks),
  });
  writePrizeListDocument(clubId, rows);
  registerPrizeIdInIndex(prizeId, sessionUid);
  return { ok: true, prizeId };
}

export function updatePrizeRow(
  clubId: string,
  prizeId: string,
  input: {
    clubName: string;
    sportType: string;
    year: string;
    association: string;
    competition: string;
    ageGroup: string;
    prizeType: string;
    studentName: string;
    ranking: string;
    status?: string;
    verifiedBy: string;
    remarks: string;
  },
): { ok: true } | { ok: false; error: string } {
  const id = prizeId.trim();
  if (!id) {
    return { ok: false, error: "PrizeID is required." };
  }
  const studentName = sanitizeCell(input.studentName);
  if (!studentName) {
    return { ok: false, error: "StudentName is required." };
  }
  const competition = sanitizeCell(input.competition);
  if (!competition) {
    return { ok: false, error: "Competition is required." };
  }
  ensurePrizeListFile(clubId);
  const rows = readPrizeListDocument(clubId);
  const today = new Date().toISOString().slice(0, 10);
  const status = sanitizeCell(input.status || "ACTIVE") || "ACTIVE";
  let found = false;
  const next = rows.map((r) => {
    if (!prizeIdsEqual(r.prizeId, id)) {
      return r;
    }
    found = true;
    const uid = clubId.trim();
    const cn = String(input.clubName ?? "").trim();
    return {
      ...r,
      clubId: r.clubId && r.clubId.trim() ? r.clubId.trim() : uid,
      clubName: cn || r.clubName || "",
      sportType: sanitizeCell(input.sportType),
      year: sanitizeCell(input.year),
      association: sanitizeCell(input.association),
      competition,
      ageGroup: sanitizeCell(input.ageGroup),
      prizeType: sanitizeCell(input.prizeType),
      studentName,
      ranking: sanitizeCell(input.ranking),
      status,
      lastUpdatedDate: today,
      verifiedBy: sanitizeCell(input.verifiedBy),
      remarks: sanitizeCell(input.remarks),
    };
  });
  if (!found) {
    return { ok: false, error: "Prize not found." };
  }
  writePrizeListDocument(clubId, next);
  const updated = next.find((r) => prizeIdsEqual(r.prizeId, id));
  if (updated) {
    const uid =
      (updated.clubId && updated.clubId.trim()) || clubId.trim();
    upsertPrizeIdInIndex(updated.prizeId, uid);
  }
  return { ok: true };
}

export function deletePrizeRow(
  clubId: string,
  prizeId: string,
): { ok: true } | { ok: false; error: string } {
  const id = prizeId.trim();
  if (!id) {
    return { ok: false, error: "PrizeID is required." };
  }
  if (!isValidClubFolderId(clubId.trim())) {
    return { ok: false, error: "Invalid club ID." };
  }
  ensurePrizeListFile(clubId);
  const rows = readPrizeListDocument(clubId);
  const next = rows.filter((r) => !prizeIdsEqual(r.prizeId, id));
  if (next.length === rows.length) {
    return { ok: false, error: "Prize not found." };
  }
  writePrizeListDocument(clubId, next);
  removePrizeIdFromIndex(id);
  return { ok: true };
}
