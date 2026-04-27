import { isValidClubFolderId } from "./coachListCsv";
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

/** Scoped lesson IDs: `{ClubFolderUid}-LE000001` (6-digit tail); legacy `LE00001` (5-digit). */
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
    return `${c}-LE${String(max + 1).padStart(6, "0")}`;
  }
  return `LE${String(max + 1).padStart(5, "0")}`;
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
      lessonId: `${club.toUpperCase()}-LE${String(n).padStart(6, "0")}`,
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
        lessonId: `${club.toUpperCase()}-LE${String(n).padStart(6, "0")}`,
      };
    }
    return { ok: true, lessonId: `LE${String(n).padStart(5, "0")}` };
  }
  return {
    ok: false,
    error:
      isFolder
        ? "Invalid LessonID format (expected {ClubFolderUid}-LE###### or legacy LE#####)."
        : "Invalid LessonID format (expected LE##### or {ClubFolderUid}-LE######).",
  };
}

async function persistLessons(
  clubId: string,
  lessons: LessonCsvRow[],
): Promise<void> {
  if (!lessonListUsesMongo()) {
    throw new Error(
      "MongoDB is required for LessonList (ClubMaster_DB.LessonList). Configure MONGODB_URI / MONGO_URI.",
    );
  }
  const id = clubId.trim();
  await replaceLessonListForClub(
    id,
    lessons.map((row) => lessonRowToStoredJson(row)),
  );
}

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

/** Virtual path for diagnostics / API metadata (lessons live in MongoDB only). */
export function lessonListPath(clubId: string): string {
  const id = clubId.trim();
  if (!isValidClubFolderId(id)) {
    return "";
  }
  return `mongodb:${LESSON_LIST_COLLECTION}/${encodeURIComponent(id)}`;
}

export function lessonListResolvedPath(clubId: string): string {
  return lessonListPath(clubId);
}

/**
 * Club folder UID used for LessonList in MongoDB.
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
  if (!lessonListUsesMongo()) {
    throw new Error(
      "MongoDB is required for LessonList (ClubMaster_DB.LessonList). Configure MONGODB_URI / MONGO_URI.",
    );
  }
  const id = clubId.trim();
  const existing = await findLessonListClubDocument(id);
  if (existing) {
    return;
  }
  await replaceLessonListForClub(id, []);
}

export async function loadLessons(clubId: string): Promise<LessonCsvRow[]> {
  if (!isValidClubFolderId(clubId)) {
    return [];
  }
  if (!lessonListUsesMongo()) {
    throw new Error(
      "MongoDB is required for LessonList (ClubMaster_DB.LessonList). Configure MONGODB_URI / MONGO_URI.",
    );
  }
  const id = clubId.trim();
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

/** Uppercase LessonID → club folder UID (`clubUid` on row, or session folder). */
const lessonIdToClubId = new Map<string, string>();
let lessonIdClubIndexReady = false;

let lessonIdClubIndexRebuild: Promise<void> | null = null;

/**
 * Rebuilds LessonID → club UID from Mongo `LessonList`.
 */
export async function rebuildLessonIdClubIndex(): Promise<void> {
  const next = new Map<string, string>();
  if (!lessonListUsesMongo()) {
    lessonIdToClubId.clear();
    lessonIdClubIndexReady = true;
    return;
  }
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
  const relativePath = `mongodb:${LESSON_LIST_COLLECTION}/${id}`;
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
