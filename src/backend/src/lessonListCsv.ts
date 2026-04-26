import fs from "fs";
import path from "path";
import {
  parseCsvLine,
  clubDataDir,
  isValidClubFolderId,
  getDataClubRootPath,
} from "./coachListCsv";
import { invalidateDataFileCache, readFileCached } from "./dataFileCache";
import {
  findLessonListClubDocument,
  iterateLessonListClubDocuments,
  lessonListUsesMongo,
  replaceLessonListForClub,
} from "./lessonListMongo";
import { LESSON_LIST_COLLECTION } from "./db/DBConnection";

function escapeRegExpLiteral(s: string): string {
  return s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

/** New catalog lesson IDs: `{ClubFolderUid}-LE0000001` (7-digit tail), for any valid `data_club` folder id. */
function maxLessonSequenceNumber(rows: LessonCsvRow[]): number {
  let max = 0;
  for (const r of rows) {
    const id = r.lessonId.replace(/^\uFEFF/, "").trim();
    const suff = id.match(/-LE(\d+)$/i);
    if (suff) {
      const n = Number.parseInt(suff[1]!, 10);
      if (!Number.isNaN(n) && n > max) {
        max = n;
      }
      continue;
    }
    const leg = id.match(/^LE(\d+)$/i);
    if (leg) {
      const n = Number.parseInt(leg[1]!, 10);
      if (!Number.isNaN(n) && n > max) {
        max = n;
      }
    }
  }
  return max;
}

function nextAllocatedLessonId(clubUid: string, rows: LessonCsvRow[]): string {
  const max = maxLessonSequenceNumber(rows);
  const club = clubUid.replace(/^\uFEFF/, "").trim();
  if (isValidClubFolderId(club)) {
    const c = club.toUpperCase();
    return `${c}-LE${String(max + 1).padStart(7, "0")}`;
  }
  return `LE${String(max + 1).padStart(6, "0")}`;
}

function normalizeRequestedLessonId(
  clubFolderUid: string,
  requested: string,
):
  | { ok: true; lessonId: string }
  | { ok: false; error: string } {
  const r = requested.replace(/^\uFEFF/, "").trim();
  if (!r) {
    return { ok: false, error: "LessonID is empty." };
  }
  const club = clubFolderUid.replace(/^\uFEFF/, "").trim();
  const isFolder = isValidClubFolderId(club);
  const prefixed = r.match(
    new RegExp(`^${escapeRegExpLiteral(club)}-LE(\\d+)$`, "i"),
  );
  if (prefixed) {
    const n = Number.parseInt(prefixed[1]!, 10);
    if (Number.isNaN(n) || n < 0) {
      return { ok: false, error: "Invalid LessonID numeric part." };
    }
    return {
      ok: true,
      lessonId: `${club.toUpperCase()}-LE${String(n).padStart(7, "0")}`,
    };
  }
  const legacy = r.match(/^LE(\d+)$/i);
  if (legacy) {
    const n = Number.parseInt(legacy[1]!, 10);
    if (Number.isNaN(n) || n < 0) {
      return { ok: false, error: "Invalid LessonID numeric part." };
    }
    if (isFolder) {
      return {
        ok: true,
        lessonId: `${club.toUpperCase()}-LE${String(n).padStart(7, "0")}`,
      };
    }
    return { ok: true, lessonId: `LE${String(n).padStart(6, "0")}` };
  }
  return {
    ok: false,
    error:
      isFolder
        ? "Invalid LessonID format (expected {ClubFolderUid}-LE####### or legacy LE######)."
        : "Invalid LessonID format (expected LE###### or {ClubFolderUid}-LE#######).",
  };
}

async function persistLessons(
  clubId: string,
  lessons: LessonCsvRow[],
): Promise<void> {
  const id = clubId.trim();
  if (lessonListUsesMongo()) {
    await replaceLessonListForClub(
      id,
      lessons.map((row) => lessonRowToStoredJson(row)),
    );
    return;
  }
  const p = lessonListPath(id);
  if (!p) {
    throw new Error("Invalid club ID.");
  }
  writeLessonsToJsonFile(p, lessons);
}

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
  /** Optional: Mongo / imports may set `student_coach` (assigned coach display name). */
  studentCoach: string;
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
  "student_coach",
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
  "studentCoach",
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
    studentCoach: "",
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

/**
 * Normalises lesson `status` from Mongo/JSON (booleans, "true", empty) to `ACTIVE` / `INACTIVE`
 * or another explicit string so fee-allocation and list filters behave consistently.
 */
function normalizeLessonListStatus(x: Record<string, unknown>): string {
  const raw =
    x.status ?? x.Status ?? x.LESSON_STATUS ?? x.lesson_status;
  if (raw === true || raw === 1) {
    return "ACTIVE";
  }
  if (raw === false || raw === 0) {
    return "INACTIVE";
  }
  const s = String(raw ?? "").trim();
  if (!s) {
    return "ACTIVE";
  }
  const u = s.toUpperCase();
  if (u === "TRUE" || u === "1" || u === "YES") {
    return "ACTIVE";
  }
  if (u === "FALSE" || u === "0" || u === "NO") {
    return "INACTIVE";
  }
  return s;
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
    studentCoach: pickStr(
      x,
      "studentCoach",
      "student_coach",
      "Student_coach",
      "Student Coach",
      "UserCoach",
      "user_coach",
    ),
    status: normalizeLessonListStatus(x),
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
  const { clubUid, reservedNumber, studentCoach, ...rest } = r;
  const out: Record<string, unknown> = {
    ...rest,
    ClubID: clubUid,
    ReservedNumber: reservedNumber,
  };
  const sc = String(studentCoach ?? "").trim();
  if (sc) {
    out.student_coach = sc;
  }
  return out;
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
  studentCoach: number;
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
    studentCoach: colIndex(headerCells, [
      "student_coach",
      "Student_coach",
      "Student Coach",
      "student coach",
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
      studentCoach: get(cells, idx.studentCoach),
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
    student_coach: r.studentCoach,
    status: r.status,
    Created_at: r.createdAt,
    LastUpdated_Date: r.lastUpdatedDate,
    Remarks: r.remarks,
  };
}

function lessonRowToStringArray(r: LessonCsvRow): string[] {
  return LESSON_ROW_KEYS.map((k) => String(r[k] ?? ""));
}

export async function ensureLessonListFile(clubId: string): Promise<void> {
  if (!isValidClubFolderId(clubId)) {
    throw new Error("Invalid club ID.");
  }
  const id = clubId.trim();
  const clubDir = path.join(dataClubRoot(), id);
  if (!fs.existsSync(clubDir)) {
    fs.mkdirSync(clubDir, { recursive: true });
  }
  if (lessonListUsesMongo()) {
    const existing = await findLessonListClubDocument(id);
    if (existing) {
      return;
    }
    migrateLegacyCsvToJson(clubId);
    const p = lessonListPath(clubId);
    let lessons: LessonCsvRow[] = [];
    if (p && fs.existsSync(p)) {
      lessons = readLessonsFromJsonFile(p).map((r) => ({
        ...r,
        clubUid: r.clubUid || id,
        reservedNumber: sanitizeReservedNumber(r.reservedNumber),
      }));
      if (fs.existsSync(p)) {
        retireLegacyLessonListCsvBesideJson(p);
      }
    }
    await replaceLessonListForClub(
      id,
      lessons.map((row) => lessonRowToStoredJson(row)),
    );
    return;
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

export async function loadLessons(clubId: string): Promise<LessonCsvRow[]> {
  if (!isValidClubFolderId(clubId)) {
    return [];
  }
  const id = clubId.trim();
  if (lessonListUsesMongo()) {
    await ensureLessonListFile(clubId);
    const doc = await findLessonListClubDocument(id);
    if (!doc || !Array.isArray(doc.lessons)) {
      return [];
    }
    return doc.lessons.map((x) => lessonFromUnknown(x)).map((r) => ({
      ...r,
      clubUid: r.clubUid || id,
      reservedNumber: sanitizeReservedNumber(r.reservedNumber),
    }));
  }
  const p = lessonListPath(clubId);
  if (!p || !fs.existsSync(p)) {
    return [];
  }
  return readLessonsFromJsonFile(p).map((r) => ({
    ...r,
    clubUid: r.clubUid || id,
    reservedNumber: sanitizeReservedNumber(r.reservedNumber),
  }));
}

/** Uppercase LessonID → club folder UID (`clubUid` on row, or session folder). */
const lessonIdToClubId = new Map<string, string>();
let lessonIdClubIndexReady = false;

let lessonIdClubIndexRebuild: Promise<void> | null = null;

/**
 * Rebuilds LessonID → club UID from Mongo `LessonList` when configured, else from distinct
 * `LessonList.json` files (dedupes LESSON_LIST_CLUB_ID pin).
 */
export async function rebuildLessonIdClubIndex(): Promise<void> {
  const next = new Map<string, string>();
  if (lessonListUsesMongo()) {
    const docs = await iterateLessonListClubDocuments();
    for (const doc of docs) {
      const name = String(doc._id ?? "").trim();
      if (!isValidClubFolderId(name)) {
        continue;
      }
      const fileClub = resolveLessonFileClubId(name);
      const lessons = (Array.isArray(doc.lessons) ? doc.lessons : []).map((x) =>
        lessonFromUnknown(x),
      );
      for (const row of lessons) {
        const u = row.lessonId.replace(/^\uFEFF/, "").trim().toUpperCase();
        if (!u || next.has(u)) {
          continue;
        }
        const logical = (row.clubUid && row.clubUid.trim()) || fileClub;
        next.set(u, logical);
      }
    }
  } else {
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
        const lessons = readLessonsFromJsonFile(p).map((r) => ({
          ...r,
          clubUid: r.clubUid || fileClub,
          reservedNumber: sanitizeReservedNumber(r.reservedNumber),
        }));
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
  }
  lessonIdToClubId.clear();
  for (const [k, v] of next) {
    lessonIdToClubId.set(k, v);
  }
  lessonIdClubIndexReady = true;
}

async function ensureLessonIdClubIndex(): Promise<void> {
  if (lessonIdClubIndexReady) {
    return;
  }
  if (!lessonIdClubIndexRebuild) {
    lessonIdClubIndexRebuild = rebuildLessonIdClubIndex().finally(() => {
      lessonIdClubIndexRebuild = null;
    });
  }
  await lessonIdClubIndexRebuild;
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
export async function findClubUidForLessonId(
  lessonId: string,
): Promise<string | null> {
  const id = lessonId.trim();
  if (!id) {
    return null;
  }
  await ensureLessonIdClubIndex();
  return lessonIdToClubId.get(id.toUpperCase()) ?? null;
}

export async function searchLessonsInClub(
  clubId: string,
  classInfo?: string,
  sportType?: string,
): Promise<LessonCsvRow[]> {
  const ci = (classInfo ?? "").trim();
  const st = (sportType ?? "").trim();
  if (!ci && !st) {
    return [];
  }
  if (!isValidClubFolderId(clubId)) {
    return [];
  }
  const rows = await loadLessons(clubId);
  return rows.filter((row) => {
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

export async function loadLessonListRaw(clubId: string): Promise<LessonListRaw> {
  const id = clubId.trim();
  const relativePath = lessonListUsesMongo()
    ? `mongodb:${LESSON_LIST_COLLECTION}/${id}`
    : `data_club/${id}/${LESSON_LIST_FILENAME}`;
  if (!isValidClubFolderId(id)) {
    return { relativePath, headers: [], rows: [] };
  }
  const lessons = await loadLessons(id);
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

export async function allocateNextLessonId(clubId: string): Promise<string> {
  await ensureLessonListFile(clubId);
  const rows = await loadLessons(clubId);
  return nextAllocatedLessonId(clubId.trim(), rows);
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
    studentCoach?: string;
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
    studentCoach: sanitizeCell(
      input.studentCoach !== undefined ? input.studentCoach : row.studentCoach,
    ),
    remarks: sanitizeCell(input.remarks ?? row.remarks),
    status,
    createdAt: row.createdAt || today,
    lastUpdatedDate: touchLastUpdated ? today : row.lastUpdatedDate,
  };
}

export async function appendLessonRow(
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
    studentCoach?: string;
    remarks?: string;
    status?: string;
    lessonId?: string;
  },
  /** Signed-in club UID stored as ClubID (may differ from clubId when LESSON_LIST_CLUB_ID pins file path). */
  ownerClubUid?: string,
): Promise<{ ok: true; lessonId: string } | { ok: false; error: string }> {
  const sport = sanitizeCell(input.sportType);
  if (!sport) {
    return { ok: false, error: "SportType is required." };
  }
  await ensureLessonListFile(clubId);
  const owner = (ownerClubUid ?? clubId).trim();
  const lessons = await loadLessons(clubId);
  const requested = input.lessonId?.trim();
  let lessonId: string;
  if (requested) {
    const norm = normalizeRequestedLessonId(owner, requested);
    if (!norm.ok) {
      return norm;
    }
    lessonId = norm.lessonId;
    if (lessons.some((r) => lessonIdsEqual(r.lessonId, lessonId))) {
      return { ok: false, error: "LessonID already exists in lesson list." };
    }
  } else {
    lessonId = nextAllocatedLessonId(owner, lessons);
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
  await persistLessons(clubId, lessons);
  registerLessonIdInIndex(
    lessonId,
    (newRow.clubUid && newRow.clubUid.trim()) || clubId.trim(),
  );
  return { ok: true, lessonId };
}

/**
 * Duplicates an existing lesson row with a newly allocated Lesson ID.
 * Copies all editable fields except lesson start/end dates (left blank for the coach to set).
 * Persists via {@link appendLessonRow} (Mongo `LessonList` when configured, else JSON).
 */
export async function cloneLessonCatalogRow(
  clubId: string,
  sourceLessonId: string,
  ownerClubUid?: string,
): Promise<{ ok: true; lessonId: string } | { ok: false; error: string }> {
  await ensureLessonListFile(clubId);
  const lessons = await loadLessons(clubId);
  const src = lessons.find((r) => lessonIdsEqual(r.lessonId, sourceLessonId));
  if (!src) {
    return { ok: false, error: "Source lesson not found." };
  }
  return appendLessonRow(
    clubId,
    {
      sportType: src.sportType,
      year: src.year,
      classId: src.classId,
      classInfo: src.classInfo,
      classTime: src.classTime,
      classFee: src.classFee,
      classSun: src.classSun,
      classMon: src.classMon,
      classTue: src.classTue,
      classWed: src.classWed,
      classThur: src.classThur,
      classFri: src.classFri,
      classSat: src.classSat,
      ageGroup: src.ageGroup,
      maxNumber: src.maxNumber,
      frequency: src.frequency,
      lessonStartDate: "",
      lessonEndDate: "",
      sportCenter: src.sportCenter,
      courtNo: src.courtNo,
      coachName: src.coachName,
      studentCoach: src.studentCoach,
      remarks: src.remarks,
      status: src.status,
    },
    ownerClubUid,
  );
}

export async function updateLessonRow(
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
    studentCoach?: string;
    remarks?: string;
    status?: string;
  },
  ownerClubUid?: string,
): Promise<{ ok: true } | { ok: false; error: string }> {
  const id = lessonId.trim();
  if (!id) {
    return { ok: false, error: "LessonID is required." };
  }
  const sport = sanitizeCell(input.sportType);
  if (!sport) {
    return { ok: false, error: "SportType is required." };
  }
  await ensureLessonListFile(clubId);
  if (!lessonListUsesMongo()) {
    const p = lessonListPath(clubId);
    if (!p || !fs.existsSync(p)) {
      return { ok: false, error: "Lesson list not found." };
    }
  }
  const lessons = await loadLessons(clubId);
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
  await persistLessons(clubId, next);
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
export async function incrementLessonReservedNumber(
  clubId: string,
  lessonId: string,
): Promise<{ ok: true; newReserved: string } | { ok: false; error: string }> {
  const id = lessonId.trim();
  if (!id) {
    return { ok: false, error: "LessonID is required." };
  }
  await ensureLessonListFile(clubId);
  if (!lessonListUsesMongo()) {
    const p = lessonListPath(clubId);
    if (!p || !fs.existsSync(p)) {
      return { ok: false, error: "Lesson list not found." };
    }
  }
  const lessons = await loadLessons(clubId);
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
  await persistLessons(clubId, next);
  const updated = next.find((r) => lessonIdsEqual(r.lessonId, id))!;
  return { ok: true, newReserved: updated.reservedNumber };
}

/** Decrease ReservedNumber by 1 for an existing lesson row (floors at 0). */
export async function decrementLessonReservedNumber(
  clubId: string,
  lessonId: string,
): Promise<{ ok: true; newReserved: string } | { ok: false; error: string }> {
  const id = lessonId.trim();
  if (!id) {
    return { ok: false, error: "LessonID is required." };
  }
  await ensureLessonListFile(clubId);
  if (!lessonListUsesMongo()) {
    const p = lessonListPath(clubId);
    if (!p || !fs.existsSync(p)) {
      return { ok: false, error: "Lesson list not found." };
    }
  }
  const lessons = await loadLessons(clubId);
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
  await persistLessons(clubId, next);
  const updated = next.find((r) => lessonIdsEqual(r.lessonId, id))!;
  return { ok: true, newReserved: updated.reservedNumber };
}

export async function removeLessonRow(
  clubId: string,
  lessonId: string,
): Promise<{ ok: true } | { ok: false; error: string }> {
  const id = lessonId.trim();
  if (!id) {
    return { ok: false, error: "LessonID is required." };
  }
  await ensureLessonListFile(clubId);
  if (!lessonListUsesMongo()) {
    const p = lessonListPath(clubId);
    if (!p || !fs.existsSync(p)) {
      return { ok: false, error: "Lesson list not found." };
    }
  }
  const lessons = await loadLessons(clubId);
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
  await persistLessons(clubId, next);
  return { ok: true };
}
