import fs from "fs";
import path from "path";
import {
  parseCsvLine,
  clubDataDir,
  isValidClubFolderId,
  getDataClubRootPath,
} from "./coachListCsv";
import { invalidateDataFileCache, readFileCached } from "./dataFileCache";

const LESSON_ID_RE = /^LE(\d+)$/i;

/** Legacy filename; migrated to JSON on first access. */
const LEGACY_LESSON_LIST_CSV = "LessonList.csv";

export type LessonCsvRow = {
  lessonId: string;
  /** Data-club folder UID; exposed as ClubID in API. */
  clubUid: string;
  sportType: string;
  year: string;
  classId: string;
  classInfo: string;
  classTime: string;
  classFee: string;
  classSun: string;
  classMon: string;
  classTue: string;
  classWed: string;
  classThur: string;
  classFri: string;
  classSat: string;
  ageGroup: string;
  maxNumber: string;
  /** Non-negative integer as string; stored as ReservedNumber in JSON. */
  reservedNumber: string;
  frequency: string;
  lessonStartDate: string;
  lessonEndDate: string;
  sportCenter: string;
  courtNo: string;
  coachName: string;
  status: string;
  createdAt: string;
  lastUpdatedDate: string;
  remarks: string;
};

export const LESSON_LIST_FILENAME = "LessonList.json";

/** Column labels (same as former CSV header) for API raw-table export. */
export const LESSON_LIST_COLUMNS: string[] = [
  "LessonID",
  "ClubID",
  "SportType",
  "Year",
  "class_id",
  "class_info",
  "class_time",
  "class_fee",
  "class_sun",
  "class_mon",
  "class_tue",
  "class_wed",
  "class_thur",
  "class_fri",
  "class_sat",
  "Age_group",
  "max_number",
  "ReservedNumber",
  "Frequency",
  "lesson_start_date",
  "lesson_end_date",
  "Sport_center",
  "court_no",
  "Coach Name",
  "status",
  "Created_at",
  "LastUpdated_Date",
  "Remarks",
];

const LESSON_ROW_KEYS: (keyof LessonCsvRow)[] = [
  "lessonId",
  "clubUid",
  "sportType",
  "year",
  "classId",
  "classInfo",
  "classTime",
  "classFee",
  "classSun",
  "classMon",
  "classTue",
  "classWed",
  "classThur",
  "classFri",
  "classSat",
  "ageGroup",
  "maxNumber",
  "reservedNumber",
  "frequency",
  "lessonStartDate",
  "lessonEndDate",
  "sportCenter",
  "courtNo",
  "coachName",
  "status",
  "createdAt",
  "lastUpdatedDate",
  "remarks",
];

export const LESSON_LIST_HEADER = LESSON_LIST_COLUMNS.join(",");

type LessonListJsonFile = {
  version?: number;
  lessons: LessonCsvRow[];
};

function dataClubRoot(): string {
  return getDataClubRootPath();
}

export function lessonListPath(clubId: string): string {
  const dir = clubDataDir(clubId);
  return dir ? path.join(dir, LESSON_LIST_FILENAME) : "";
}

export function lessonListResolvedPath(clubId: string): string {
  const id = clubId.trim();
  if (!isValidClubFolderId(id)) {
    return "";
  }
  return path.normalize(lessonListPath(id));
}

/**
 * Folder under backend/data_club/ that holds LessonList.json for API read/write.
 * Defaults to the signed-in club UID (authedClubId).
 * Set LESSON_LIST_CLUB_ID to pin reads/writes to another folder (dev only).
 */
export function resolveLessonFileClubId(authedClubId: string): string {
  const env = process.env.LESSON_LIST_CLUB_ID?.trim();
  if (env && isValidClubFolderId(env)) {
    return env;
  }
  const auth = authedClubId.trim();
  if (isValidClubFolderId(auth)) {
    return auth;
  }
  return auth;
}

function defaultEmptyLessonRow(): LessonCsvRow {
  return {
    lessonId: "",
    clubUid: "",
    sportType: "",
    year: "",
    classId: "",
    classInfo: "",
    classTime: "",
    classFee: "",
    classSun: "N",
    classMon: "N",
    classTue: "N",
    classWed: "N",
    classThur: "N",
    classFri: "N",
    classSat: "N",
    ageGroup: "",
    maxNumber: "",
    reservedNumber: "0",
    frequency: "",
    lessonStartDate: "",
    lessonEndDate: "",
    sportCenter: "",
    courtNo: "",
    coachName: "",
    status: "ACTIVE",
    createdAt: "",
    lastUpdatedDate: "",
    remarks: "",
  };
}

function pickStr(o: Record<string, unknown>, ...keys: string[]): string {
  for (const k of keys) {
    if (!Object.prototype.hasOwnProperty.call(o, k)) {
      continue;
    }
    const v = o[k];
    if (v === undefined || v === null) {
      continue;
    }
    const s = String(v).trim();
    if (s !== "") {
      return s;
    }
  }
  return "";
}

function sanitizeReservedNumber(s: string): string {
  const t = String(s ?? "").trim();
  if (t === "" || !/^\d+$/.test(t)) {
    return "0";
  }
  return t;
}

function reservedNumberFromUnknown(o: Record<string, unknown>): string {
  const s = pickStr(o, "reservedNumber", "ReservedNumber", "reserved_number");
  return sanitizeReservedNumber(s);
}

function ynFromUnknown(v: unknown): string {
  if (v === true || v === 1) {
    return "Y";
  }
  if (v === false || v === 0) {
    return "N";
  }
  const t = String(v ?? "")
    .trim()
    .toUpperCase();
  if (t === "Y" || t === "1" || t === "TRUE" || t === "YES") {
    return "Y";
  }
  if (t === "N" || t === "0" || t === "FALSE" || t === "NO" || t === "") {
    return "N";
  }
  return "N";
}

export function lessonFromUnknown(o: unknown): LessonCsvRow {
  if (!o || typeof o !== "object") {
    return defaultEmptyLessonRow();
  }
  const x = o as Record<string, unknown>;
  const d = defaultEmptyLessonRow();
  return {
    lessonId: pickStr(x, "lessonId", "LessonID", "lesson_id"),
    clubUid: pickStr(x, "clubUid", "ClubID", "club_uid"),
    sportType: pickStr(x, "sportType", "SportType"),
    year: pickStr(x, "year", "Year"),
    classId: pickStr(x, "classId", "class_id"),
    classInfo: pickStr(x, "classInfo", "class_info"),
    classTime: pickStr(x, "classTime", "class_time"),
    classFee: pickStr(x, "classFee", "class_fee"),
    classSun: pickStr(x, "classSun", "class_sun") || ynFromUnknown(x.class_sun ?? x.classSun),
    classMon: pickStr(x, "classMon", "class_mon") || ynFromUnknown(x.class_mon ?? x.classMon),
    classTue: pickStr(x, "classTue", "class_tue") || ynFromUnknown(x.class_tue ?? x.classTue),
    classWed: pickStr(x, "classWed", "class_wed") || ynFromUnknown(x.class_wed ?? x.classWed),
    classThur: pickStr(x, "classThur", "class_thur", "class_thu") ||
      ynFromUnknown(x.class_thur ?? x.classThur ?? x.class_thu),
    classFri: pickStr(x, "classFri", "class_fri") || ynFromUnknown(x.class_fri ?? x.classFri),
    classSat: pickStr(x, "classSat", "class_sat") || ynFromUnknown(x.class_sat ?? x.classSat),
    ageGroup: pickStr(x, "ageGroup", "Age_group", "age_group"),
    maxNumber: pickStr(x, "maxNumber", "max_number"),
    reservedNumber: reservedNumberFromUnknown(x),
    frequency: pickStr(x, "frequency", "Frequency"),
    lessonStartDate: pickStr(x, "lessonStartDate", "lesson_start_date"),
    lessonEndDate: pickStr(x, "lessonEndDate", "lesson_end_date"),
    sportCenter: pickStr(x, "sportCenter", "Sport_center", "sport_center"),
    courtNo: pickStr(x, "courtNo", "court_no", "Court_no"),
    coachName: pickStr(x, "coachName", "Coach Name", "coach_name"),
    status: pickStr(x, "status", "Status") || "ACTIVE",
    createdAt: pickStr(x, "createdAt", "Created_at", "created_at"),
    lastUpdatedDate: pickStr(
      x,
      "lastUpdatedDate",
      "LastUpdated_Date",
      "lastupdated_date",
    ),
    remarks: pickStr(x, "remarks", "Remarks"),
  };
}

function parseLessonsJsonRaw(raw: string): LessonCsvRow[] {
  let data: unknown;
  try {
    data = JSON.parse(raw);
  } catch {
    return [];
  }
  if (Array.isArray(data)) {
    return data.map((x) => lessonFromUnknown(x));
  }
  if (data && typeof data === "object" && Array.isArray((data as LessonListJsonFile).lessons)) {
    return (data as LessonListJsonFile).lessons.map((x) => lessonFromUnknown(x));
  }
  return [];
}

function readLessonsFromJsonFile(p: string): LessonCsvRow[] {
  if (!fs.existsSync(p)) {
    return [];
  }
  return readFileCached(p, (raw) => parseLessonsJsonRaw(raw), []);
}

function lessonRowToStoredJson(r: LessonCsvRow): Record<string, unknown> {
  const { clubUid, reservedNumber, ...rest } = r;
  return { ...rest, ClubID: clubUid, ReservedNumber: reservedNumber };
}

/**
 * Lessons are JSON-only (LessonList.json). Remove a parallel LessonList.csv so
 * editors and tools do not treat CSV as the live store (same idea as PrizeList).
 */
function retireLegacyLessonListCsvBesideJson(jsonPath: string): void {
  const pCsv = path.join(path.dirname(jsonPath), LEGACY_LESSON_LIST_CSV);
  if (!fs.existsSync(pCsv)) {
    return;
  }
  try {
    fs.unlinkSync(pCsv);
  } catch {
    try {
      fs.renameSync(pCsv, `${pCsv}.retired.${Date.now()}.bak`);
    } catch {
      /* ignore */
    }
  }
}

function writeLessonsToJsonFile(p: string, lessons: LessonCsvRow[]): void {
  const payload = {
    version: 1,
    lessons: lessons.map(lessonRowToStoredJson),
  };
  fs.writeFileSync(p, `${JSON.stringify(payload, null, 2)}\n`, "utf8");
  /** So the next `loadLessons` sees this write (avoids stale cache on back-to-back increments). */
  invalidateDataFileCache(p);
  retireLegacyLessonListCsvBesideJson(p);
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

type LessonColIdx = {
  lessonId: number;
  clubUid: number;
  sportType: number;
  year: number;
  classId: number;
  classInfo: number;
  classTime: number;
  classFee: number;
  classSun: number;
  classMon: number;
  classTue: number;
  classWed: number;
  classThur: number;
  classFri: number;
  classSat: number;
  ageGroup: number;
  maxNumber: number;
  reservedNumber: number;
  frequency: number;
  lessonStartDate: number;
  lessonEndDate: number;
  sportCenter: number;
  courtNo: number;
  coachName: number;
  status: number;
  createdAt: number;
  lastUpdatedDate: number;
  remarks: number;
};

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

function resolveLessonColumnIndices(headerCells: string[]): LessonColIdx {
  return {
    lessonId: colIndex(headerCells, [
      "LessonID",
      "lessonid",
      "Lesson ID",
      "lesson id",
    ]),
    clubUid: colIndex(headerCells, [
      "ClubID",
      "clubid",
      "Club ID",
      "club id",
      "club_uid",
      "clubUid",
    ]),
    sportType: colIndex(headerCells, ["SportType", "sport type", "sporttype"]),
    year: colIndex(headerCells, ["Year", "year"]),
    classId: colIndex(headerCells, ["class_id", "Class ID", "class id"]),
    classInfo: colIndex(headerCells, [
      "class_info",
      "Class Info",
      "class info",
    ]),
    classTime: colIndex(headerCells, [
      "class_time",
      "Class Time",
      "class time",
    ]),
    classFee: colIndex(headerCells, [
      "class_fee",
      "Class Fee",
      "class fee",
    ]),
    classSun: colIndex(headerCells, ["class_sun", "Class Sun", "class sun"]),
    classMon: colIndex(headerCells, ["class_mon", "Class Mon", "class mon"]),
    classTue: colIndex(headerCells, ["class_tue", "Class Tue", "class tue"]),
    classWed: colIndex(headerCells, ["class_wed", "Class Wed", "class wed"]),
    classThur: colIndex(headerCells, [
      "class_thur",
      "class_thu",
      "Class Thur",
      "class thur",
    ]),
    classFri: colIndex(headerCells, ["class_fri", "Class Fri", "class fri"]),
    classSat: colIndex(headerCells, ["class_sat", "Class Sat", "class sat"]),
    ageGroup: colIndex(headerCells, ["Age_group", "Age group", "age group"]),
    maxNumber: colIndex(headerCells, [
      "max_number",
      "Max number",
      "max number",
    ]),
    reservedNumber: colIndex(headerCells, [
      "ReservedNumber",
      "reservednumber",
      "Reserved number",
      "reserved number",
      "reserved_number",
    ]),
    frequency: colIndex(headerCells, ["Frequency", "frequency"]),
    lessonStartDate: colIndex(headerCells, [
      "lesson_start_date",
      "Lesson start date",
      "lesson start date",
    ]),
    lessonEndDate: colIndex(headerCells, [
      "lesson_end_date",
      "Lesson end date",
      "lesson end date",
    ]),
    sportCenter: colIndex(headerCells, [
      "Sport_center",
      "sport_center",
      "Sport center",
      "sport center",
    ]),
    courtNo: colIndex(headerCells, [
      "court_no",
      "Court_no",
      "Court no",
      "court no",
      "Court No",
    ]),
    coachName: colIndex(headerCells, [
      "Coach Name",
      "coach name",
      "CoachName",
      "coach_name",
    ]),
    status: colIndex(headerCells, ["status", "Status"]),
    createdAt: colIndex(headerCells, [
      "Created_at",
      "created_at",
      "Created at",
      "created at",
    ]),
    lastUpdatedDate: colIndex(headerCells, [
      "LastUpdated_Date",
      "lastupdated_date",
      "Last Updated Date",
      "last updated date",
    ]),
    remarks: colIndex(headerCells, ["Remarks", "remarks", "Remark", "remark"]),
  };
}

function ensureLessonIndices(idx: LessonColIdx): void {
  if (idx.lessonId < 0 || idx.sportType < 0) {
    throw new Error(
      "LessonList: need LessonID and SportType columns in legacy CSV migration.",
    );
  }
}

/** One-time migration from legacy LessonList.csv to LessonList.json */
function parseAllLessonsFromLegacyCsvPath(csvPath: string): LessonCsvRow[] {
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
  const idx = resolveLessonColumnIndices(headerCells);
  ensureLessonIndices(idx);
  const out: LessonCsvRow[] = [];
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
    const lessonId = get(cells, idx.lessonId);
    if (!lessonId) {
      continue;
    }
    out.push({
      lessonId,
      clubUid: idx.clubUid >= 0 ? get(cells, idx.clubUid) : "",
      sportType: get(cells, idx.sportType),
      year: get(cells, idx.year),
      classId: get(cells, idx.classId),
      classInfo: get(cells, idx.classInfo),
      classTime: get(cells, idx.classTime),
      classFee: get(cells, idx.classFee),
      classSun: get(cells, idx.classSun) || "N",
      classMon: get(cells, idx.classMon) || "N",
      classTue: get(cells, idx.classTue) || "N",
      classWed: get(cells, idx.classWed) || "N",
      classThur: get(cells, idx.classThur) || "N",
      classFri: get(cells, idx.classFri) || "N",
      classSat: get(cells, idx.classSat) || "N",
      ageGroup: get(cells, idx.ageGroup),
      maxNumber: get(cells, idx.maxNumber),
      reservedNumber: sanitizeReservedNumber(
        idx.reservedNumber >= 0 ? get(cells, idx.reservedNumber) : "",
      ),
      frequency: get(cells, idx.frequency),
      lessonStartDate: get(cells, idx.lessonStartDate),
      lessonEndDate: get(cells, idx.lessonEndDate),
      sportCenter: get(cells, idx.sportCenter),
      courtNo: get(cells, idx.courtNo),
      coachName: get(cells, idx.coachName),
      status: get(cells, idx.status) || "ACTIVE",
      createdAt: get(cells, idx.createdAt),
      lastUpdatedDate: get(cells, idx.lastUpdatedDate),
      remarks: get(cells, idx.remarks),
    });
  }
  return out;
}

function migrateLegacyCsvToJson(clubId: string): void {
  const dir = clubDataDir(clubId.trim());
  if (!dir) {
    return;
  }
  const pJson = lessonListPath(clubId);
  if (!pJson || fs.existsSync(pJson)) {
    return;
  }
  const pCsv = path.join(dir, LEGACY_LESSON_LIST_CSV);
  if (!fs.existsSync(pCsv)) {
    return;
  }
  const id = clubId.trim();
  const lessons = parseAllLessonsFromLegacyCsvPath(pCsv).map((r) => ({
    ...r,
    clubUid: r.clubUid || id,
  }));
  writeLessonsToJsonFile(pJson, lessons);
  try {
    fs.renameSync(pCsv, `${pCsv}.bak`);
  } catch {
    /* keep csv if rename fails */
  }
}

export function lessonCsvRowToApiFields(r: LessonCsvRow): Record<string, string> {
  return {
    LessonID: r.lessonId,
    ClubID: r.clubUid,
    SportType: r.sportType,
    Year: r.year,
    class_id: r.classId,
    class_info: r.classInfo,
    class_time: r.classTime,
    class_fee: r.classFee,
    class_sun: r.classSun,
    class_mon: r.classMon,
    class_tue: r.classTue,
    class_wed: r.classWed,
    class_thur: r.classThur,
    class_fri: r.classFri,
    class_sat: r.classSat,
    Age_group: r.ageGroup,
    max_number: r.maxNumber,
    ReservedNumber: r.reservedNumber,
    Frequency: r.frequency,
    lesson_start_date: r.lessonStartDate,
    lesson_end_date: r.lessonEndDate,
    Sport_center: r.sportCenter,
    court_no: r.courtNo,
    "Coach Name": r.coachName,
    status: r.status,
    Created_at: r.createdAt,
    LastUpdated_Date: r.lastUpdatedDate,
    Remarks: r.remarks,
  };
}

function lessonRowToStringArray(r: LessonCsvRow): string[] {
  return LESSON_ROW_KEYS.map((k) => String(r[k] ?? ""));
}

export function ensureLessonListFile(clubId: string): void {
  if (!isValidClubFolderId(clubId)) {
    throw new Error("Invalid club ID.");
  }
  const clubDir = path.join(dataClubRoot(), clubId.trim());
  if (!fs.existsSync(clubDir)) {
    fs.mkdirSync(clubDir, { recursive: true });
  }
  const p = lessonListPath(clubId);
  if (!p) {
    throw new Error("Invalid club ID.");
  }
  migrateLegacyCsvToJson(clubId);
  if (!fs.existsSync(p)) {
    writeLessonsToJsonFile(p, []);
  }
  if (fs.existsSync(p)) {
    retireLegacyLessonListCsvBesideJson(p);
  }
}

export function loadLessons(clubId: string): LessonCsvRow[] {
  if (!isValidClubFolderId(clubId)) {
    return [];
  }
  const p = lessonListPath(clubId);
  if (!p || !fs.existsSync(p)) {
    return [];
  }
  const id = clubId.trim();
  return readLessonsFromJsonFile(p).map((r) => ({
    ...r,
    clubUid: r.clubUid || id,
    reservedNumber: sanitizeReservedNumber(r.reservedNumber),
  }));
}

/** Uppercase LessonID → club folder UID (`clubUid` on row, or session folder). */
const lessonIdToClubId = new Map<string, string>();
let lessonIdClubIndexReady = false;

/**
 * Rebuilds LessonID → club UID from distinct `LessonList.json` files (dedupes LESSON_LIST_CLUB_ID pin).
 */
export function rebuildLessonIdClubIndex(): void {
  const next = new Map<string, string>();
  const root = dataClubRoot();
  if (fs.existsSync(root)) {
    const seenFiles = new Set<string>();
    for (const name of fs.readdirSync(root)) {
      if (!isValidClubFolderId(name)) {
        continue;
      }
      const fileClub = resolveLessonFileClubId(name);
      const p = lessonListPath(fileClub);
      if (!p) {
        continue;
      }
      const abs = path.normalize(p);
      if (seenFiles.has(abs)) {
        continue;
      }
      if (!fs.existsSync(p)) {
        continue;
      }
      seenFiles.add(abs);
      const lessons = loadLessons(fileClub);
      for (const row of lessons) {
        const u = row.lessonId.replace(/^\uFEFF/, "").trim().toUpperCase();
        if (!u || next.has(u)) {
          continue;
        }
        const logical = (row.clubUid && row.clubUid.trim()) || fileClub;
        next.set(u, logical);
      }
    }
  }
  lessonIdToClubId.clear();
  for (const [k, v] of next) {
    lessonIdToClubId.set(k, v);
  }
  lessonIdClubIndexReady = true;
}

function ensureLessonIdClubIndex(): void {
  if (!lessonIdClubIndexReady) {
    rebuildLessonIdClubIndex();
  }
}

function registerLessonIdInIndex(lessonId: string, clubUid: string): void {
  const u = lessonId.replace(/^\uFEFF/, "").trim().toUpperCase();
  const c = clubUid.trim();
  if (!u || !c) {
    return;
  }
  if (!lessonIdToClubId.has(u)) {
    lessonIdToClubId.set(u, c);
  }
}

function upsertLessonIdInIndex(lessonId: string, clubUid: string): void {
  const u = lessonId.replace(/^\uFEFF/, "").trim().toUpperCase();
  const c = clubUid.trim();
  if (!u || !c) {
    return;
  }
  lessonIdToClubId.set(u, c);
}

/** Club folder UID for this LessonID (from row ClubID / first roster match), or null. */
export function findClubUidForLessonId(lessonId: string): string | null {
  const id = lessonId.trim();
  if (!id) {
    return null;
  }
  ensureLessonIdClubIndex();
  return lessonIdToClubId.get(id.toUpperCase()) ?? null;
}

export function searchLessonsInClub(
  clubId: string,
  classInfo?: string,
  sportType?: string,
): LessonCsvRow[] {
  const ci = (classInfo ?? "").trim();
  const st = (sportType ?? "").trim();
  if (!ci && !st) {
    return [];
  }
  if (!isValidClubFolderId(clubId)) {
    return [];
  }
  return loadLessons(clubId).filter((row) => {
    if (ci && !normEqLessonField(row.classInfo, ci)) {
      return false;
    }
    if (st && !normEqLessonField(row.sportType, st)) {
      return false;
    }
    return true;
  });
}

function normEqLessonField(a: string, b: string): boolean {
  return (
    String(a ?? "").trim().toLowerCase() === String(b ?? "").trim().toLowerCase()
  );
}

export type LessonListRaw = {
  relativePath: string;
  headers: string[];
  rows: string[][];
};

export function loadLessonListRaw(clubId: string): LessonListRaw {
  const id = clubId.trim();
  const relativePath = `data_club/${id}/${LESSON_LIST_FILENAME}`;
  if (!isValidClubFolderId(id)) {
    return { relativePath, headers: [], rows: [] };
  }
  const lessons = loadLessons(id);
  const headers = [...LESSON_LIST_COLUMNS];
  const rows = lessons.map(lessonRowToStringArray);
  const maxW = Math.max(headers.length, ...rows.map((r) => r.length), 1);
  const pad = (cells: string[]): string[] => {
    const x = cells.slice();
    while (x.length < maxW) {
      x.push("");
    }
    return x;
  };
  return {
    relativePath,
    headers: pad(headers),
    rows: rows.map(pad),
  };
}

export function allocateNextLessonId(clubId: string): string {
  ensureLessonListFile(clubId);
  return nextLessonId(loadLessons(clubId));
}

function nextLessonId(rows: LessonCsvRow[]): string {
  let max = 0;
  for (const r of rows) {
    const m = r.lessonId.match(LESSON_ID_RE);
    if (m) {
      const n = Number.parseInt(m[1]!, 10);
      if (!Number.isNaN(n) && n > max) {
        max = n;
      }
    }
  }
  return `LE${String(max + 1).padStart(6, "0")}`;
}

function sanitizeCell(s: string): string {
  return String(s ?? "").replace(/,/g, " ").trim();
}

function dayYnFromInput(s: string | boolean | number | undefined | null): string {
  if (s === true || s === 1) {
    return "Y";
  }
  if (s === false || s === 0) {
    return "N";
  }
  const t = String(s ?? "").trim().toUpperCase();
  if (t === "Y" || t === "1" || t === "TRUE" || t === "YES") {
    return "Y";
  }
  return "N";
}

export function lessonIdsEqual(a: string, b: string): boolean {
  const x = String(a ?? "")
    .replace(/^\uFEFF/, "")
    .trim();
  const y = String(b ?? "")
    .replace(/^\uFEFF/, "")
    .trim();
  return x.length > 0 && x.toUpperCase() === y.toUpperCase();
}

function mergeInputIntoRow(
  row: LessonCsvRow,
  input: {
    sportType: string;
    year?: string;
    classId?: string;
    classInfo?: string;
    classTime?: string;
    classFee?: string;
    classSun?: string | boolean | number;
    classMon?: string | boolean | number;
    classTue?: string | boolean | number;
    classWed?: string | boolean | number;
    classThur?: string | boolean | number;
    classFri?: string | boolean | number;
    classSat?: string | boolean | number;
    ageGroup?: string;
    maxNumber?: string;
    reservedNumber?: string;
    frequency?: string;
    lessonStartDate?: string;
    lessonEndDate?: string;
    sportCenter?: string;
    courtNo?: string;
    coachName?: string;
    remarks?: string;
    status?: string;
  },
  touchLastUpdated: boolean,
): LessonCsvRow {
  const today = new Date().toISOString().slice(0, 10);
  const status = sanitizeCell(input.status || row.status || "ACTIVE") || "ACTIVE";
  return {
    ...row,
    sportType: sanitizeCell(input.sportType),
    year: sanitizeCell(input.year ?? row.year),
    classId: sanitizeCell(input.classId ?? row.classId),
    classInfo: sanitizeCell(input.classInfo ?? row.classInfo),
    classTime: sanitizeCell(input.classTime ?? row.classTime),
    classFee: sanitizeCell(input.classFee ?? row.classFee),
    classSun: dayYnFromInput(input.classSun ?? row.classSun),
    classMon: dayYnFromInput(input.classMon ?? row.classMon),
    classTue: dayYnFromInput(input.classTue ?? row.classTue),
    classWed: dayYnFromInput(input.classWed ?? row.classWed),
    classThur: dayYnFromInput(input.classThur ?? row.classThur),
    classFri: dayYnFromInput(input.classFri ?? row.classFri),
    classSat: dayYnFromInput(input.classSat ?? row.classSat),
    ageGroup: sanitizeCell(input.ageGroup ?? row.ageGroup),
    maxNumber: sanitizeCell(input.maxNumber ?? row.maxNumber),
    reservedNumber:
      input.reservedNumber !== undefined
        ? sanitizeReservedNumber(input.reservedNumber)
        : sanitizeReservedNumber(row.reservedNumber),
    frequency: sanitizeCell(input.frequency ?? row.frequency),
    lessonStartDate: sanitizeCell(input.lessonStartDate ?? row.lessonStartDate),
    lessonEndDate: sanitizeCell(input.lessonEndDate ?? row.lessonEndDate),
    sportCenter: sanitizeCell(input.sportCenter ?? row.sportCenter),
    courtNo: sanitizeCell(input.courtNo ?? row.courtNo),
    coachName: sanitizeCell(input.coachName ?? row.coachName),
    remarks: sanitizeCell(input.remarks ?? row.remarks),
    status,
    createdAt: row.createdAt || today,
    lastUpdatedDate: touchLastUpdated ? today : row.lastUpdatedDate,
  };
}

export function appendLessonRow(
  clubId: string,
  input: {
    sportType: string;
    year?: string;
    classId?: string;
    classInfo?: string;
    classTime?: string;
    classFee?: string;
    classSun?: string | boolean | number;
    classMon?: string | boolean | number;
    classTue?: string | boolean | number;
    classWed?: string | boolean | number;
    classThur?: string | boolean | number;
    classFri?: string | boolean | number;
    classSat?: string | boolean | number;
    ageGroup?: string;
    maxNumber?: string;
    reservedNumber?: string;
    frequency?: string;
    lessonStartDate?: string;
    lessonEndDate?: string;
    sportCenter?: string;
    courtNo?: string;
    coachName?: string;
    remarks?: string;
    status?: string;
    lessonId?: string;
  },
  /** Signed-in club UID stored as ClubID (may differ from clubId when LESSON_LIST_CLUB_ID pins file path). */
  ownerClubUid?: string,
): { ok: true; lessonId: string } | { ok: false; error: string } {
  const sport = sanitizeCell(input.sportType);
  if (!sport) {
    return { ok: false, error: "SportType is required." };
  }
  ensureLessonListFile(clubId);
  const p = lessonListPath(clubId);
  if (!p) {
    return { ok: false, error: "Invalid club." };
  }
  const lessons = loadLessons(clubId);
  const requested = input.lessonId?.trim();
  let lessonId: string;
  if (requested) {
    if (!LESSON_ID_RE.test(requested)) {
      return { ok: false, error: "Invalid LessonID format (expected LE######)." };
    }
    const normalized = `LE${requested.match(LESSON_ID_RE)![1]!.padStart(6, "0")}`;
    if (lessons.some((r) => lessonIdsEqual(r.lessonId, normalized))) {
      return { ok: false, error: "LessonID already exists in lesson list." };
    }
    lessonId = normalized;
  } else {
    lessonId = nextLessonId(lessons);
  }
  const blank = defaultEmptyLessonRow();
  const newRow = mergeInputIntoRow(
    { ...blank, lessonId, createdAt: "", lastUpdatedDate: "" },
    input,
    true,
  );
  const today = new Date().toISOString().slice(0, 10);
  newRow.createdAt = newRow.createdAt || today;
  newRow.lastUpdatedDate = today;
  newRow.clubUid = sanitizeCell((ownerClubUid ?? clubId).trim());
  newRow.reservedNumber = "0";
  lessons.push(newRow);
  writeLessonsToJsonFile(p, lessons);
  registerLessonIdInIndex(
    lessonId,
    (newRow.clubUid && newRow.clubUid.trim()) || clubId.trim(),
  );
  return { ok: true, lessonId };
}

export function updateLessonRow(
  clubId: string,
  lessonId: string,
  input: {
    sportType: string;
    year?: string;
    classId?: string;
    classInfo?: string;
    classTime?: string;
    classFee?: string;
    classSun?: string | boolean | number;
    classMon?: string | boolean | number;
    classTue?: string | boolean | number;
    classWed?: string | boolean | number;
    classThur?: string | boolean | number;
    classFri?: string | boolean | number;
    classSat?: string | boolean | number;
    ageGroup?: string;
    maxNumber?: string;
    reservedNumber?: string;
    frequency?: string;
    lessonStartDate?: string;
    lessonEndDate?: string;
    sportCenter?: string;
    courtNo?: string;
    coachName?: string;
    remarks?: string;
    status?: string;
  },
  ownerClubUid?: string,
): { ok: true } | { ok: false; error: string } {
  const id = lessonId.trim();
  if (!id) {
    return { ok: false, error: "LessonID is required." };
  }
  const sport = sanitizeCell(input.sportType);
  if (!sport) {
    return { ok: false, error: "SportType is required." };
  }
  ensureLessonListFile(clubId);
  const p = lessonListPath(clubId);
  if (!p || !fs.existsSync(p)) {
    return { ok: false, error: "Lesson list not found." };
  }
  const lessons = loadLessons(clubId);
  let found = false;
  const next = lessons.map((row) => {
    if (!lessonIdsEqual(row.lessonId, id)) {
      return row;
    }
    found = true;
    const merged = mergeInputIntoRow(row, input, true);
    return {
      ...merged,
      clubUid: sanitizeCell((ownerClubUid ?? merged.clubUid ?? clubId).trim()),
    };
  });
  if (!found) {
    return { ok: false, error: "Lesson not found." };
  }
  writeLessonsToJsonFile(p, next);
  const upd = next.find((r) => lessonIdsEqual(r.lessonId, id));
  if (upd) {
    upsertLessonIdInIndex(
      upd.lessonId,
      (upd.clubUid && upd.clubUid.trim()) || clubId.trim(),
    );
  }
  return { ok: true };
}

/** Bump ReservedNumber by 1 for an existing lesson row. */
export function incrementLessonReservedNumber(
  clubId: string,
  lessonId: string,
): { ok: true; newReserved: string } | { ok: false; error: string } {
  const id = lessonId.trim();
  if (!id) {
    return { ok: false, error: "LessonID is required." };
  }
  ensureLessonListFile(clubId);
  const p = lessonListPath(clubId);
  if (!p || !fs.existsSync(p)) {
    return { ok: false, error: "Lesson list not found." };
  }
  const lessons = loadLessons(clubId);
  let found = false;
  const today = new Date().toISOString().slice(0, 10);
  const next = lessons.map((row) => {
    if (!lessonIdsEqual(row.lessonId, id)) {
      return row;
    }
    found = true;
    const n = Math.max(0, Number.parseInt(row.reservedNumber, 10) || 0) + 1;
    return {
      ...row,
      reservedNumber: String(n),
      lastUpdatedDate: today,
    };
  });
  if (!found) {
    return { ok: false, error: "Lesson not found." };
  }
  writeLessonsToJsonFile(p, next);
  const updated = next.find((r) => lessonIdsEqual(r.lessonId, id))!;
  return { ok: true, newReserved: updated.reservedNumber };
}

/** Decrease ReservedNumber by 1 for an existing lesson row (floors at 0). */
export function decrementLessonReservedNumber(
  clubId: string,
  lessonId: string,
): { ok: true; newReserved: string } | { ok: false; error: string } {
  const id = lessonId.trim();
  if (!id) {
    return { ok: false, error: "LessonID is required." };
  }
  ensureLessonListFile(clubId);
  const p = lessonListPath(clubId);
  if (!p || !fs.existsSync(p)) {
    return { ok: false, error: "Lesson list not found." };
  }
  const lessons = loadLessons(clubId);
  let found = false;
  const today = new Date().toISOString().slice(0, 10);
  const next = lessons.map((row) => {
    if (!lessonIdsEqual(row.lessonId, id)) {
      return row;
    }
    found = true;
    const cur = Math.max(0, Number.parseInt(row.reservedNumber, 10) || 0);
    const n = Math.max(0, cur - 1);
    return {
      ...row,
      reservedNumber: String(n),
      lastUpdatedDate: today,
    };
  });
  if (!found) {
    return { ok: false, error: "Lesson not found." };
  }
  writeLessonsToJsonFile(p, next);
  const updated = next.find((r) => lessonIdsEqual(r.lessonId, id))!;
  return { ok: true, newReserved: updated.reservedNumber };
}

export function removeLessonRow(
  clubId: string,
  lessonId: string,
): { ok: true } | { ok: false; error: string } {
  const id = lessonId.trim();
  if (!id) {
    return { ok: false, error: "LessonID is required." };
  }
  const p = lessonListPath(clubId);
  if (!p || !fs.existsSync(p)) {
    return { ok: false, error: "Lesson list not found." };
  }
  const lessons = loadLessons(clubId);
  const today = new Date().toISOString().slice(0, 10);
  let found = false;
  const next = lessons.map((row) => {
    if (!lessonIdsEqual(row.lessonId, id)) {
      return row;
    }
    found = true;
    return {
      ...row,
      status: "INACTIVE",
      lastUpdatedDate: today,
    };
  });
  if (!found) {
    return { ok: false, error: "Lesson not found." };
  }
  writeLessonsToJsonFile(p, next);
  return { ok: true };
}
