import fs from "fs";
import { Router, type Request } from "express";
import { findStudentRoleLoginByUid } from "../coachStudentLoginCsv";
import { requireAuth, requireRole } from "../middleware/requireAuth";
import {
  coachManagerClubContextAsync,
  findCoachManagerUserRowForClubUid,
} from "../coachManagerSession";
import { sportCoachDebugOn } from "../sportCoachDebug";
import {
  csvCoachFieldMatchesLoggedCoach,
  filterRawRowsByIdColumn,
  findCoachRosterRow,
} from "../coachSelfFilter";
import {
  findClubUidForCoachId,
  getDataClubRootPath,
  isValidClubFolderId,
  loadCoaches,
} from "../coachListCsv";
import {
  appendLessonRow,
  ensureLessonListFile,
  decrementLessonReservedNumber,
  incrementLessonReservedNumber,
  LESSON_LIST_FILENAME,
  lessonCsvRowToApiFields,
  lessonIdsEqual,
  lessonListPath,
  lessonListResolvedPath,
  loadLessonListRaw,
  loadLessons,
  removeLessonRow,
  resolveLessonFileClubId,
  searchLessonsInClub,
  updateLessonRow,
  type LessonCsvRow,
  type LessonListRaw,
} from "../lessonListCsv";
import {
  appendLessonReservation,
  ensureLessonReserveListFile,
  hasActiveReservationForStudentLesson,
  loadLessonReservations,
  removeActiveReservationForStudentLesson,
  removeLessonReservationByReserveId,
} from "../lessonReserveList";
import { loadStudents, resolveStudentClubSession } from "../studentListCsv";
import { listActiveSportCenterNames } from "../sportCenterListCsv";
import { createCoachManagerSalaryRouter } from "./coachManagerSalaryRoutes";

function readDayYn(b: Record<string, unknown>, key: string): string {
  if (!Object.prototype.hasOwnProperty.call(b, key)) {
    return "N";
  }
  const v = b[key];
  if (v === true || v === "true" || v === "Y" || v === "y" || v === 1) {
    return "Y";
  }
  if (v === false || v === "false" || v === "N" || v === "n" || v === 0) {
    return "N";
  }
  const t = String(v).trim().toUpperCase();
  if (t === "Y" || t === "1" || t === "TRUE" || t === "YES") {
    return "Y";
  }
  return "N";
}

type LessonWriteBody = {
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
  reservedNumber: string;
  frequency: string;
  lessonStartDate: string;
  lessonEndDate: string;
  sportCenter: string;
  courtNo: string;
  coachName: string;
  remarks: string;
  status: string;
};

function readLessonWriteBody(body: unknown): LessonWriteBody {
  const b =
    body != null && typeof body === "object"
      ? (body as Record<string, unknown>)
      : {};
  return {
    sportType: String(b?.SportType ?? b?.sportType ?? "").trim(),
    year: String(b?.Year ?? b?.year ?? "").trim(),
    classId: String(b?.class_id ?? b?.classId ?? "").trim(),
    classInfo: String(b?.class_info ?? b?.classInfo ?? "").trim(),
    classTime: String(b?.class_time ?? b?.classTime ?? "").trim(),
    classFee: String(b?.class_fee ?? b?.classFee ?? "").trim(),
    classSun: readDayYn(b, "class_sun"),
    classMon: readDayYn(b, "class_mon"),
    classTue: readDayYn(b, "class_tue"),
    classWed: readDayYn(b, "class_wed"),
    classThur: readDayYn(b, "class_thur"),
    classFri: readDayYn(b, "class_fri"),
    classSat: readDayYn(b, "class_sat"),
    ageGroup: String(b?.Age_group ?? b?.age_group ?? b?.ageGroup ?? "").trim(),
    maxNumber: String(b?.max_number ?? b?.maxNumber ?? "").trim(),
    reservedNumber: String(
      b?.ReservedNumber ?? b?.reservedNumber ?? "",
    ).trim(),
    frequency: String(b?.Frequency ?? b?.frequency ?? "").trim(),
    lessonStartDate: String(
      b?.lesson_start_date ?? b?.lessonStartDate ?? "",
    ).trim(),
    lessonEndDate: String(
      b?.lesson_end_date ?? b?.lessonEndDate ?? "",
    ).trim(),
    sportCenter: String(
      b?.Sport_center ?? b?.sport_center ?? b?.sportCenter ?? "",
    ).trim(),
    courtNo: String(
      b?.court_no ?? b?.Court_no ?? b?.courtNo ?? "",
    ).trim(),
    coachName: String(
      b?.["Coach Name"] ?? b?.coach_name ?? b?.coachName ?? "",
    ).trim(),
    remarks: String(b?.Remarks ?? b?.remarks ?? "").trim(),
    status: String(b?.status ?? b?.Status ?? "ACTIVE").trim(),
  };
}

function bodyHasOwn(body: unknown, key: string): boolean {
  return (
    body != null &&
    typeof body === "object" &&
    Object.prototype.hasOwnProperty.call(body as object, key)
  );
}

/** If the client omits class_time / class_fee / weekday keys, keep CSV values (avoid defaulting days to N). */
function mergeLessonPutWithExisting(
  rawBody: unknown,
  w: LessonWriteBody,
  existing: LessonCsvRow | undefined,
): LessonWriteBody {
  if (!existing) {
    return w;
  }
  const pickTime =
    bodyHasOwn(rawBody, "class_time") || bodyHasOwn(rawBody, "classTime");
  const pickFee =
    bodyHasOwn(rawBody, "class_fee") || bodyHasOwn(rawBody, "classFee");
  const pickReserved =
    bodyHasOwn(rawBody, "ReservedNumber") ||
    bodyHasOwn(rawBody, "reservedNumber");
  return {
    ...w,
    classTime: pickTime ? w.classTime : existing.classTime,
    classFee: pickFee ? w.classFee : existing.classFee,
    reservedNumber: pickReserved ? w.reservedNumber : existing.reservedNumber,
    classSun: bodyHasOwn(rawBody, "class_sun") ? w.classSun : existing.classSun,
    classMon: bodyHasOwn(rawBody, "class_mon") ? w.classMon : existing.classMon,
    classTue: bodyHasOwn(rawBody, "class_tue") ? w.classTue : existing.classTue,
    classWed: bodyHasOwn(rawBody, "class_wed") ? w.classWed : existing.classWed,
    classThur:
      bodyHasOwn(rawBody, "class_thur") || bodyHasOwn(rawBody, "class_thu")
        ? w.classThur
        : existing.classThur,
    classFri: bodyHasOwn(rawBody, "class_fri") ? w.classFri : existing.classFri,
    classSat: bodyHasOwn(rawBody, "class_sat") ? w.classSat : existing.classSat,
  };
}

function coachManagerLessonDebugSnapshot(
  req: Request,
  lessonStorageClubId?: string,
): Record<string, unknown> {
  const jwtSub = String(req.user?.sub ?? "").trim();
  const root = getDataClubRootPath();
  const storageId =
    (lessonStorageClubId ?? "").trim() ||
    (jwtSub ? resolveLessonFileClubId(jwtSub) : "");
  const idOk = storageId ? isValidClubFolderId(storageId) : false;
  const resolved = idOk ? lessonListResolvedPath(storageId) : "";
  const pathForFile = idOk ? lessonListPath(storageId) : "";
  let lessonFileExistsOnDisk = false;
  let fileSizeBytes: number | null = null;
  let fileHeadPreview = "";
  let fileReadError: string | null = null;
  if (pathForFile) {
    try {
      lessonFileExistsOnDisk = fs.existsSync(pathForFile);
      if (lessonFileExistsOnDisk) {
        fileSizeBytes = fs.statSync(pathForFile).size;
        fileHeadPreview = fs.readFileSync(pathForFile, "utf8").slice(0, 500);
      }
    } catch (e) {
      fileReadError = e instanceof Error ? e.message : String(e);
    }
  }
  return {
    route: "GET /api/coach-manager/lessons",
    cwd: process.cwd(),
    dataClubRoot: root,
    jwtSub,
    lessonStorageClubId: storageId || null,
    jwtRole: req.user?.role ?? null,
    uidMatchesDataClubPattern: idOk,
    lessonCsvResolvedPath: resolved || null,
    lessonFileExistsOnDisk,
    fileSizeBytes,
    fileHeadPreview,
    fileReadError,
  };
}

function lessonLoadDebugExtra(
  clubId: string,
  lessonCsv: LessonListRaw,
  lessonsLen: number,
): Record<string, unknown> {
  return {
    lessonCsvHeaderCount: lessonCsv.headers.length,
    lessonCsvRowCount: lessonCsv.rows.length,
    lessonsParsedCount: lessonsLen,
    firstHeadersSample: lessonCsv.headers.slice(0, 20),
  };
}

async function resolveLessonClubContextAsync(
  req: Request,
):
  Promise<
    | { ok: true; clubId: string; clubName: string }
    | { ok: false; status: number; error: string }
  > {
  const role = String(req.user?.role ?? "");
  if (role === "CoachManager") {
    return coachManagerClubContextAsync(req);
  }
  if (role === "Coach") {
    const coachId = String(req.user?.sub ?? "").trim();
    if (!coachId) {
      return { ok: false, status: 403, error: "Invalid session." };
    }
    const clubId = findClubUidForCoachId(coachId);
    if (!clubId) {
      return {
        ok: false,
        status: 403,
        error: "No club roster found for this coach account.",
      };
    }
    const managerRow = await findCoachManagerUserRowForClubUid(clubId);
    if (!managerRow || managerRow.role !== "CoachManager") {
      return { ok: false, status: 403, error: "Invalid club for lesson access." };
    }
    const clubName = (managerRow.clubName && managerRow.clubName.trim()) || "";
    if (!clubName || clubName === "—") {
      return {
        ok: false,
        status: 400,
        error: "Your club has no name configured; contact an administrator.",
      };
    }
    const inRoster = loadCoaches(clubId).some(
      (c) => c.coachId.trim().toUpperCase() === coachId.toUpperCase(),
    );
    if (!inRoster) {
      return { ok: false, status: 403, error: "Coach not in club roster." };
    }
    return { ok: true, clubId, clubName };
  }
  if (role === "Student") {
    const studentId = String(req.user?.sub ?? "").trim();
    if (!studentId) {
      return { ok: false, status: 403, error: "Invalid session." };
    }
    const session = resolveStudentClubSession(studentId);
    if (!session.ok) {
      return { ok: false, status: 403, error: session.error };
    }
    const { clubId } = session;
    const managerRow = await findCoachManagerUserRowForClubUid(clubId);
    if (!managerRow || managerRow.role !== "CoachManager") {
      return { ok: false, status: 403, error: "Invalid club for lesson access." };
    }
    const clubName = (managerRow.clubName && managerRow.clubName.trim()) || "";
    return { ok: true, clubId, clubName };
  }
  return { ok: false, status: 403, error: "Forbidden" };
}

const LESSON_LIST_PAGE_MAX = 10;

/**
 * When `page` and/or `limit` (or `pageSize`) is present, return at most 10 lessons per page.
 * Omit both to return the full list (e.g. lesson timetable and legacy clients).
 */
function parseLessonListPagination(req: Request): { page: number; limit: number } | null {
  const q = req.query;
  const hasPage = q.page != null && String(q.page).trim() !== "";
  const hasLimit =
    (q.limit != null && String(q.limit).trim() !== "") ||
    (q.pageSize != null && String(q.pageSize).trim() !== "");
  if (!hasPage && !hasLimit) {
    return null;
  }
  const page = Math.max(1, parseInt(String(q.page ?? "1"), 10) || 1);
  const limRaw =
    q.limit != null && String(q.limit).trim() !== ""
      ? String(q.limit)
      : String(q.pageSize ?? String(LESSON_LIST_PAGE_MAX));
  const limit = Math.min(
    LESSON_LIST_PAGE_MAX,
    Math.max(1, parseInt(limRaw, 10) || LESSON_LIST_PAGE_MAX),
  );
  return { page, limit };
}

export function createCoachManagerLessonRouter(): Router {
  const r = Router();

  r.use(requireAuth);

  /**
   * Coach salary + lesson fee allocation (Coach Manager). Nested here so the same
   * `/api/coach-manager/lessons` mount that already works in production always reaches this API.
   */
  r.use("/coach-salary-data", createCoachManagerSalaryRouter());

  /** Lightweight list for lesson forms (Sport Center column, ACTIVE rows only). */
  r.get(
    "/sport-center-options",
    requireRole("CoachManager", "Coach"),
    async (req, res) => {
    const ctx = await resolveLessonClubContextAsync(req);
    if (!ctx.ok) {
      res.status(ctx.status).json({ ok: false, error: ctx.error });
      return;
    }
    try {
      const names = listActiveSportCenterNames(
        resolveLessonFileClubId(ctx.clubId),
      );
      res.json({ ok: true, names });
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      console.warn("[coach-manager/lessons/sport-center-options]", msg);
      res.status(500).json({ ok: false, error: msg });
    }
  },
  );

  /** Student: ACTIVE rows in LessonReserveList.json for JWT sub, joined with LessonList.json. */
  r.get("/student-bookings", requireRole("Student"), async (req, res) => {
    const ctx = await resolveLessonClubContextAsync(req);
    if (!ctx.ok) {
      res.status(ctx.status).json({ ok: false, error: ctx.error });
      return;
    }
    const qClubParam = String(req.query?.clubId ?? "").trim();
    if (qClubParam && qClubParam !== ctx.clubId) {
      res.status(403).json({
        ok: false,
        error: "clubId does not match your club folder.",
      });
      return;
    }
    const studentId = String(req.user?.sub ?? "").trim();
    const fileClub = resolveLessonFileClubId(ctx.clubId);
    try {
      ensureLessonReserveListFile(fileClub);
      ensureLessonListFile(fileClub);
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      res.status(500).json({ ok: false, error: msg });
      return;
    }
    const reservations = loadLessonReservations(fileClub).filter(
      (r) =>
        r.student_id.trim().toUpperCase() === studentId.toUpperCase() &&
        r.status.toUpperCase() === "ACTIVE",
    );
    let lessons: ReturnType<typeof loadLessons> = [];
    try {
      lessons = loadLessons(fileClub);
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      res.status(500).json({ ok: false, error: msg });
      return;
    }
    const byId = new Map(
      lessons.map((l) => [l.lessonId.trim().toUpperCase(), l]),
    );
    const merged: Record<string, unknown>[] = [];
    for (const resv of reservations) {
      const row = byId.get(resv.lessonId.trim().toUpperCase());
      if (!row) {
        continue;
      }
      merged.push({
        ...lessonCsvRowToApiFields(row),
        lessonReserveId: resv.lessonReserveId,
        Payment_Status: resv.Payment_Status,
        Payment_Confirm: resv.Payment_Confirm,
      });
    }
    res.json({
      ok: true,
      clubId: ctx.clubId,
      clubName: ctx.clubName,
      lessonStorageClubId: fileClub,
      lessons: merged,
    });
  });

  r.get("/", requireRole("CoachManager", "Coach", "Student"), async (_req, res) => {
    const ctx = await resolveLessonClubContextAsync(_req);
    if (!ctx.ok) {
      const body: Record<string, unknown> = { ok: false, error: ctx.error };
      if (sportCoachDebugOn()) {
        body.debug = coachManagerLessonDebugSnapshot(_req);
        console.warn("[SPORT_COACH_DEBUG] coach-manager lessons context failed", body);
      }
      res.status(ctx.status).json(body);
      return;
    }
    const qClubParam = String(_req.query?.clubId ?? "").trim();
    if (qClubParam && qClubParam !== ctx.clubId) {
      res.status(403).json({
        ok: false,
        error: "clubId does not match your club folder.",
      });
      return;
    }
    try {
      const fileClub = resolveLessonFileClubId(ctx.clubId);
      ensureLessonListFile(fileClub);
      let lessonCsv = loadLessonListRaw(fileClub);
      let lessons: ReturnType<typeof loadLessons> = [];
      let lessonsParseWarning: string | null = null;
      try {
        lessons = loadLessons(fileClub);
      } catch (parseErr) {
        lessonsParseWarning =
          parseErr instanceof Error ? parseErr.message : String(parseErr);
      }
      let lessonCsvOut = lessonCsv;
      /** Coach: pass clubId= matching your folder to list every lesson in that club (browse). Omit clubId to list only your rows (Coach Name match). */
      const coachBrowseAll =
        _req.user?.role === "Coach" &&
        qClubParam !== "" &&
        qClubParam === ctx.clubId;
      if (_req.user?.role === "Student") {
        /* full club lesson list for browse + reserve */
      } else if (_req.user?.role === "Coach" && !coachBrowseAll) {
        const crow = findCoachRosterRow(ctx.clubId, String(_req.user.sub));
        const uname = String(_req.user.username ?? "");
        if (crow) {
          lessons = lessons.filter((x) =>
            csvCoachFieldMatchesLoggedCoach(x.coachName, crow, uname),
          );
        } else {
          lessons = [];
        }
        const keep = new Set(
          lessons.map((l) => l.lessonId.trim().toUpperCase()),
        );
        lessonCsvOut = {
          ...lessonCsv,
          rows: filterRawRowsByIdColumn(
            lessonCsv,
            ["LessonID", "lessonid", "LESSONID"],
            keep,
          ).rows,
        };
      }
      const listPag = parseLessonListPagination(_req);
      const totalLessonsForList = lessons.length;
      let pageLessons = lessons;
      if (listPag) {
        const start = (listPag.page - 1) * listPag.limit;
        pageLessons = lessons.slice(start, start + listPag.limit);
      }
      const pageLessonIdSet = new Set(
        pageLessons.map((l) => l.lessonId.trim().toUpperCase()),
      );
      const idEnc = encodeURIComponent(fileClub);
      const fileEnc = encodeURIComponent(LESSON_LIST_FILENAME);
      let activeSportCenters: string[] = [];
      let activeSportCentersLoadError: string | null = null;
      try {
        activeSportCenters = listActiveSportCenterNames(fileClub);
      } catch (e) {
        activeSportCenters = [];
        activeSportCentersLoadError =
          e instanceof Error ? e.message : String(e);
        console.warn(
          "[coach-manager/lessons] listActiveSportCenterNames failed",
          activeSportCentersLoadError,
        );
      }
      /** Distinct coach display names (`full_name`) from ACTIVE rows in UserList_Coach.json. */
      let coachNameOptions: string[] = [];
      let coachNameOptionsLoadError: string | null = null;
      try {
        const seen = new Set<string>();
        for (const c of loadCoaches(fileClub)) {
          if (String(c.status ?? "").trim().toUpperCase() !== "ACTIVE") {
            continue;
          }
          const n = String(c.coachName ?? "").trim();
          if (!n || seen.has(n)) {
            continue;
          }
          seen.add(n);
          coachNameOptions.push(n);
        }
        coachNameOptions.sort((a, b) => a.localeCompare(b));
      } catch (e) {
        coachNameOptions = [];
        coachNameOptionsLoadError =
          e instanceof Error ? e.message : String(e);
        console.warn(
          "[coach-manager/lessons] coachNameOptions from UserList_Coach failed",
          coachNameOptionsLoadError,
        );
      }
      const studentSub = String(_req.user?.sub ?? "").trim();
      let lessonReservationsPayload: {
        lessonId: string;
        lessonReserveId: string;
        student_id: string;
        Student_Name: string;
        status: string;
        Payment_Status: string;
        Payment_Confirm: boolean;
      }[] = [];
      if (_req.user?.role === "CoachManager" || _req.user?.role === "Coach") {
        try {
          ensureLessonReserveListFile(fileClub);
          let resvList = loadLessonReservations(fileClub);
          /** Coach (narrow list): only reservations for lessons this coach teaches — smaller payload & less client work. */
          if (_req.user?.role === "Coach" && !coachBrowseAll) {
            const allowedLessonIds = new Set(
              lessons.map((l) => l.lessonId.trim().toUpperCase()),
            );
            resvList = resvList.filter((r) =>
              allowedLessonIds.has(r.lessonId.trim().toUpperCase()),
            );
          }
          lessonReservationsPayload = resvList.map((resv) => ({
            lessonId: resv.lessonId,
            lessonReserveId: resv.lessonReserveId,
            student_id: resv.student_id,
            Student_Name: resv.Student_Name,
            status: resv.status,
            Payment_Status: resv.Payment_Status,
            Payment_Confirm: resv.Payment_Confirm,
          }));
        } catch (resvErr) {
          console.warn(
            "[coach-manager/lessons] load reservations failed",
            resvErr instanceof Error ? resvErr.message : String(resvErr),
          );
          lessonReservationsPayload = [];
        }
      }
      if (listPag) {
        lessonReservationsPayload = lessonReservationsPayload.filter((r) =>
          pageLessonIdSet.has(r.lessonId.trim().toUpperCase()),
        );
      }
      let lessonPayload: Record<string, string | boolean>[] = pageLessons.map((x) =>
        lessonCsvRowToApiFields(x),
      );
      if (_req.user?.role === "Student" && studentSub) {
        ensureLessonReserveListFile(fileClub);
        const reservations = loadLessonReservations(fileClub);
        const activeLessonIds = new Set(
          reservations
            .filter(
              (r) =>
                r.student_id.trim().toUpperCase() === studentSub.toUpperCase() &&
                r.status.toUpperCase() === "ACTIVE",
            )
            .map((r) => r.lessonId.trim().toUpperCase()),
        );
        lessonPayload = pageLessons.map((x) => ({
          ...lessonCsvRowToApiFields(x),
          studentHasActiveReservation: activeLessonIds.has(
            x.lessonId.trim().toUpperCase(),
          ),
        }));
      }
      const includeFullLessonCsv = listPag == null || listPag.page <= 1;
      const lessonListRawForResponse = includeFullLessonCsv
        ? lessonCsvOut
        : {
            ...lessonCsvOut,
            rows: [] as string[][],
            truncated: true,
          };
      const payload: Record<string, unknown> = {
        ok: true,
        clubId: ctx.clubId,
        clubName: ctx.clubName,
        lessonStorageClubId: fileClub,
        lessonCsvFileUrl: `/backend/data_club/${idEnc}/${fileEnc}`,
        lessonCsvResolvedPath: lessonListResolvedPath(fileClub),
        lessons: lessonPayload,
        ...(_req.user?.role === "CoachManager" || _req.user?.role === "Coach"
          ? { lessonReservations: lessonReservationsPayload }
          : {}),
        /** Tabular mirror of LessonList.json columns (headers + rows). */
        lessonListRaw: lessonListRawForResponse,
        /** @deprecated Prefer lessonListRaw; same value for older clients. */
        lessonCsv: lessonListRawForResponse,
        activeSportCenters,
        ...(activeSportCentersLoadError
          ? { activeSportCentersLoadError }
          : {}),
        coachNameOptions,
        ...(coachNameOptionsLoadError
          ? { coachNameOptionsLoadError }
          : {}),
        ...(lessonsParseWarning ? { lessonsParseWarning } : {}),
      };
      if (listPag) {
        payload.lessonTotal = totalLessonsForList;
        payload.lessonPage = listPag.page;
        payload.lessonPageSize = listPag.limit;
      }
      if (sportCoachDebugOn()) {
        const dbg = {
          ...coachManagerLessonDebugSnapshot(_req, fileClub),
          ...lessonLoadDebugExtra(fileClub, lessonCsv, lessons.length),
        };
        payload.debug = dbg;
        console.log("[SPORT_COACH_DEBUG] coach-manager lessons OK", {
          clubId: ctx.clubId,
          lessonStorageClubId: fileClub,
          lessonCsvHeaderCount: lessonCsv.headers.length,
          lessonCsvRowCount: lessonCsv.rows.length,
          path: dbg.lessonCsvResolvedPath,
          fileExists: dbg.lessonFileExistsOnDisk,
        });
      }
      res.json(payload);
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      const body: Record<string, unknown> = { ok: false, error: msg };
      if (sportCoachDebugOn()) {
        body.debug = {
          ...coachManagerLessonDebugSnapshot(_req),
          loadException: msg,
        };
      }
      res.status(500).json(body);
    }
  });

  r.post("/reserve", requireRole("Student"), async (req, res) => {
    const ctx = await resolveLessonClubContextAsync(req);
    if (!ctx.ok) {
      res.status(ctx.status).json({ ok: false, error: ctx.error });
      return;
    }
    const lessonId = String(
      req.body?.lessonId ?? req.body?.LessonID ?? "",
    ).trim();
    if (!lessonId) {
      res.status(400).json({ ok: false, error: "lessonId is required." });
      return;
    }
    const studentId = String(req.user?.sub ?? "").trim();
    const fileClub = resolveLessonFileClubId(ctx.clubId);
    ensureLessonListFile(fileClub);
    let lessons: ReturnType<typeof loadLessons> = [];
    try {
      lessons = loadLessons(fileClub);
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      res.status(500).json({ ok: false, error: msg });
      return;
    }
    const row = lessons.find((x) => lessonIdsEqual(x.lessonId, lessonId));
    if (!row) {
      res.status(404).json({ ok: false, error: "Lesson not found." });
      return;
    }
    if (row.status.toUpperCase() !== "ACTIVE") {
      res.status(400).json({ ok: false, error: "Lesson is not ACTIVE." });
      return;
    }
    const maxN = Math.max(0, Number.parseInt(row.maxNumber, 10) || 0);
    const resN = Math.max(0, Number.parseInt(row.reservedNumber, 10) || 0);
    if (maxN > 0 && resN >= maxN) {
      res.status(400).json({
        ok: false,
        error: "Lesson is full (max reservations reached).",
      });
      return;
    }
    if (hasActiveReservationForStudentLesson(fileClub, lessonId, studentId)) {
      res.status(409).json({
        ok: false,
        error: "You already have an ACTIVE reservation for this lesson.",
      });
      return;
    }
    const roster = loadStudents(fileClub);
    const stu = roster.find(
      (s) => s.studentId.trim().toUpperCase() === studentId.toUpperCase(),
    );
    const login = findStudentRoleLoginByUid(studentId);
    const studentName =
      (login?.fullName && login.fullName.trim()) ||
      (stu?.studentName && stu.studentName.trim()) ||
      "—";

    ensureLessonReserveListFile(fileClub);
    const append = appendLessonReservation(fileClub, {
      lessonId,
      ClubID: ctx.clubId,
      student_id: studentId,
      Student_Name: studentName,
      status: "ACTIVE",
    });
    if (!append.ok) {
      res.status(400).json({ ok: false, error: append.error });
      return;
    }
    const inc = incrementLessonReservedNumber(fileClub, lessonId);
    if (!inc.ok) {
      res.status(500).json({ ok: false, error: inc.error });
      return;
    }
    res.json({
      ok: true,
      lessonReserveId: append.lessonReserveId,
      ReservedNumber: inc.newReserved,
      message: "Reservation created.",
    });
  });

  /**
   * Coach Manager: create ACTIVE lesson reservations for selected roster students
   * (UserList_Student.json). Skips invalid / inactive / already-reserved / when full.
   */
  r.post("/assign-students", requireRole("CoachManager"), async (req, res) => {
    const ctx = await resolveLessonClubContextAsync(req);
    if (!ctx.ok) {
      res.status(ctx.status).json({ ok: false, error: ctx.error });
      return;
    }
    if (String(req.user?.role ?? "") !== "CoachManager") {
      res.status(403).json({ ok: false, error: "Coach Manager only." });
      return;
    }
    const lessonId = String(
      req.body?.lessonId ?? req.body?.LessonID ?? "",
    ).trim();
    const rawIds = req.body?.studentIds ?? req.body?.StudentIDs;
    if (!lessonId) {
      res.status(400).json({ ok: false, error: "lessonId is required." });
      return;
    }
    if (!Array.isArray(rawIds) || rawIds.length === 0) {
      res
        .status(400)
        .json({ ok: false, error: "studentIds must be a non-empty array." });
      return;
    }
    const fileClub = resolveLessonFileClubId(ctx.clubId);
    ensureLessonListFile(fileClub);
    ensureLessonReserveListFile(fileClub);
    let lessons: ReturnType<typeof loadLessons> = [];
    try {
      lessons = loadLessons(fileClub);
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      res.status(500).json({ ok: false, error: msg });
      return;
    }
    const row = lessons.find((x) => lessonIdsEqual(x.lessonId, lessonId));
    if (!row) {
      res.status(404).json({ ok: false, error: "Lesson not found." });
      return;
    }
    if (row.status.toUpperCase() !== "ACTIVE") {
      res.status(400).json({ ok: false, error: "Lesson is not ACTIVE." });
      return;
    }
    const roster = loadStudents(fileClub);
    const rosterByUpper = new Map(
      roster.map((s) => [s.studentId.trim().toUpperCase(), s]),
    );
    const seen = new Set<string>();
    const idList: string[] = [];
    for (const x of rawIds) {
      const sid = String(x ?? "").trim();
      if (!sid) {
        continue;
      }
      const up = sid.toUpperCase();
      if (seen.has(up)) {
        continue;
      }
      seen.add(up);
      idList.push(sid);
    }
    if (idList.length === 0) {
      res.status(400).json({
        ok: false,
        error: "No valid student IDs in studentIds.",
      });
      return;
    }
    let resN = Math.max(0, Number.parseInt(row.reservedNumber, 10) || 0);
    const maxN = Math.max(0, Number.parseInt(row.maxNumber, 10) || 0);
    const reserved: {
      studentId: string;
      lessonReserveId: string;
      Student_Name: string;
    }[] = [];
    const skipped: { studentId: string; reason: string }[] = [];
    for (const sid of idList) {
      const up = sid.trim().toUpperCase();
      const stu = rosterByUpper.get(up);
      if (!stu) {
        skipped.push({ studentId: sid, reason: "Not in club roster." });
        continue;
      }
      if (stu.status.trim().toUpperCase() !== "ACTIVE") {
        skipped.push({ studentId: sid, reason: "Student is not ACTIVE." });
        continue;
      }
      if (hasActiveReservationForStudentLesson(fileClub, lessonId, stu.studentId)) {
        skipped.push({
          studentId: stu.studentId,
          reason: "Already has an ACTIVE reservation for this lesson.",
        });
        continue;
      }
      if (maxN > 0 && resN >= maxN) {
        skipped.push({
          studentId: stu.studentId,
          reason: "Lesson is full (max reservations reached).",
        });
        continue;
      }
      const login = findStudentRoleLoginByUid(stu.studentId);
      const studentName =
        (login?.fullName && login.fullName.trim()) ||
        (stu.studentName && stu.studentName.trim()) ||
        "—";
      const append = appendLessonReservation(fileClub, {
        lessonId,
        ClubID: ctx.clubId,
        student_id: stu.studentId,
        Student_Name: studentName,
        status: "ACTIVE",
      });
      if (!append.ok) {
        skipped.push({ studentId: stu.studentId, reason: append.error });
        continue;
      }
      const inc = incrementLessonReservedNumber(fileClub, lessonId);
      if (!inc.ok) {
        removeLessonReservationByReserveId(fileClub, append.lessonReserveId);
        skipped.push({ studentId: stu.studentId, reason: inc.error });
        continue;
      }
      resN = Math.max(0, Number.parseInt(inc.newReserved, 10) || resN + 1);
      reserved.push({
        studentId: stu.studentId,
        lessonReserveId: append.lessonReserveId,
        Student_Name: studentName,
      });
    }
    res.json({
      ok: true,
      lessonId,
      reserved,
      skipped,
      newReservedNumber: String(resN),
    });
  });

  r.post("/cancel-reserve", requireRole("Student"), async (req, res) => {
    const ctx = await resolveLessonClubContextAsync(req);
    if (!ctx.ok) {
      res.status(ctx.status).json({ ok: false, error: ctx.error });
      return;
    }
    const lessonId = String(
      req.body?.lessonId ?? req.body?.LessonID ?? "",
    ).trim();
    if (!lessonId) {
      res.status(400).json({ ok: false, error: "lessonId is required." });
      return;
    }
    const studentId = String(req.user?.sub ?? "").trim();
    const fileClub = resolveLessonFileClubId(ctx.clubId);
    ensureLessonListFile(fileClub);
    let lessons: ReturnType<typeof loadLessons> = [];
    try {
      lessons = loadLessons(fileClub);
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      res.status(500).json({ ok: false, error: msg });
      return;
    }
    const row = lessons.find((x) => lessonIdsEqual(x.lessonId, lessonId));
    if (!row) {
      res.status(404).json({ ok: false, error: "Lesson not found." });
      return;
    }
    const removed = removeActiveReservationForStudentLesson(
      fileClub,
      lessonId,
      studentId,
    );
    if (!removed.ok) {
      res.status(404).json({ ok: false, error: removed.error });
      return;
    }
    const dec = decrementLessonReservedNumber(fileClub, lessonId);
    if (!dec.ok) {
      res.status(500).json({ ok: false, error: dec.error });
      return;
    }
    res.json({
      ok: true,
      lessonReserveId: removed.lessonReserveId,
      ReservedNumber: dec.newReserved,
      message: "Booking cancelled.",
    });
  });

  r.post("/", requireRole("CoachManager"), async (req, res) => {
    const ctx = await coachManagerClubContextAsync(req);
    if (!ctx.ok) {
      res.status(ctx.status).json({ ok: false, error: ctx.error });
      return;
    }
    const w = readLessonWriteBody(req.body);
    const fileClub = resolveLessonFileClubId(ctx.clubId);
    const result = appendLessonRow(
      fileClub,
      {
        sportType: w.sportType,
        year: w.year,
        classId: w.classId,
        classInfo: w.classInfo,
        classTime: w.classTime,
        classFee: w.classFee,
        classSun: w.classSun,
        classMon: w.classMon,
        classTue: w.classTue,
        classWed: w.classWed,
        classThur: w.classThur,
        classFri: w.classFri,
        classSat: w.classSat,
        ageGroup: w.ageGroup,
        maxNumber: w.maxNumber,
        frequency: w.frequency,
        lessonStartDate: w.lessonStartDate,
        lessonEndDate: w.lessonEndDate,
        sportCenter: w.sportCenter,
        courtNo: w.courtNo,
        coachName: w.coachName,
        remarks: w.remarks,
        status: w.status,
      },
      ctx.clubId,
    );
    if (!result.ok) {
      res.status(400).json({ ok: false, error: result.error });
      return;
    }
    res.json({ ok: true, lessonId: result.lessonId, message: "Lesson created." });
  });

  r.put("/", requireRole("CoachManager"), async (req, res) => {
    const ctx = await coachManagerClubContextAsync(req);
    if (!ctx.ok) {
      res.status(ctx.status).json({ ok: false, error: ctx.error });
      return;
    }
    const lessonId = String(
      req.body?.LessonID ?? req.body?.lessonId ?? "",
    ).trim();
    const fileClub = resolveLessonFileClubId(ctx.clubId);
    ensureLessonListFile(fileClub);
    const existingRow = loadLessons(fileClub).find(
      (x) => x.lessonId.trim().toUpperCase() === lessonId.toUpperCase(),
    );
    const w = readLessonWriteBody(req.body);
    const merged = mergeLessonPutWithExisting(req.body, w, existingRow);
    const result = updateLessonRow(
      fileClub,
      lessonId,
      {
        sportType: merged.sportType,
        year: merged.year,
        classId: merged.classId,
        classInfo: merged.classInfo,
        classTime: merged.classTime,
        classFee: merged.classFee,
        classSun: merged.classSun,
        classMon: merged.classMon,
        classTue: merged.classTue,
        classWed: merged.classWed,
        classThur: merged.classThur,
        classFri: merged.classFri,
        classSat: merged.classSat,
        ageGroup: merged.ageGroup,
        maxNumber: merged.maxNumber,
        reservedNumber: merged.reservedNumber,
        frequency: merged.frequency,
        lessonStartDate: merged.lessonStartDate,
        lessonEndDate: merged.lessonEndDate,
        sportCenter: merged.sportCenter,
        courtNo: merged.courtNo,
        coachName: merged.coachName,
        remarks: merged.remarks,
        status: merged.status,
      },
      ctx.clubId,
    );
    if (!result.ok) {
      res.status(400).json({ ok: false, error: result.error });
      return;
    }
    res.json({ ok: true, message: "Lesson updated." });
  });

  r.post("/remove", requireRole("CoachManager"), async (req, res) => {
    const ctx = await coachManagerClubContextAsync(req);
    if (!ctx.ok) {
      res.status(ctx.status).json({ ok: false, error: ctx.error });
      return;
    }
    const lessonId = String(
      req.body?.LessonID ?? req.body?.lessonId ?? "",
    ).trim();
    const result = removeLessonRow(resolveLessonFileClubId(ctx.clubId), lessonId);
    if (!result.ok) {
      res.status(400).json({ ok: false, error: result.error });
      return;
    }
    res.json({
      ok: true,
      message: "Lesson marked INACTIVE.",
      LastUpdated_Date: new Date().toISOString().slice(0, 10),
    });
  });

  r.post("/search", requireRole("CoachManager"), async (req, res) => {
    const ctx = await coachManagerClubContextAsync(req);
    if (!ctx.ok) {
      res.status(ctx.status).json({ ok: false, error: ctx.error });
      return;
    }
    const classInfo = String(req.body?.class_info ?? req.body?.classInfo ?? "").trim();
    const sportType = String(req.body?.SportType ?? req.body?.sportType ?? "").trim();
    if (!classInfo && !sportType) {
      res.status(400).json({
        ok: false,
        error:
          "Enter at least one: class_info and/or SportType (LessonList.json for your club).",
      });
      return;
    }
    try {
      const fileClub = resolveLessonFileClubId(ctx.clubId);
      ensureLessonListFile(fileClub);
      const list = searchLessonsInClub(
        fileClub,
        classInfo || undefined,
        sportType || undefined,
      );
      res.json({
        ok: true,
        results: list.map((x) => lessonCsvRowToApiFields(x)),
      });
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      res.status(500).json({ ok: false, error: msg });
    }
  });

  r.post("/activate", requireRole("CoachManager"), async (req, res) => {
    const ctx = await coachManagerClubContextAsync(req);
    if (!ctx.ok) {
      res.status(ctx.status).json({ ok: false, error: ctx.error });
      return;
    }
    const lessonId = String(
      req.body?.LessonID ?? req.body?.lessonId ?? "",
    ).trim();
    if (!lessonId) {
      res.status(400).json({ ok: false, error: "LessonID is required." });
      return;
    }
    const fileClub = resolveLessonFileClubId(ctx.clubId);
    ensureLessonListFile(fileClub);
    const lessons = loadLessons(fileClub);
    const row = lessons.find(
      (x) => x.lessonId.trim().toUpperCase() === lessonId.trim().toUpperCase(),
    );
    if (!row) {
      res.status(404).json({ ok: false, error: "Lesson not found in your list." });
      return;
    }
    if (String(row.status).trim().toUpperCase() === "ACTIVE") {
      res.status(400).json({ ok: false, error: "Lesson is already ACTIVE." });
      return;
    }
    const result = updateLessonRow(
      fileClub,
      row.lessonId,
      {
        sportType: row.sportType,
        year: row.year,
        classId: row.classId,
        classInfo: row.classInfo,
        classTime: row.classTime,
        classFee: row.classFee,
        classSun: row.classSun,
        classMon: row.classMon,
        classTue: row.classTue,
        classWed: row.classWed,
        classThur: row.classThur,
        classFri: row.classFri,
        classSat: row.classSat,
        ageGroup: row.ageGroup,
        maxNumber: row.maxNumber,
        reservedNumber: row.reservedNumber,
        frequency: row.frequency,
        lessonStartDate: row.lessonStartDate,
        lessonEndDate: row.lessonEndDate,
        sportCenter: row.sportCenter,
        courtNo: row.courtNo,
        coachName: row.coachName,
        remarks: row.remarks,
        status: "ACTIVE",
      },
      ctx.clubId,
    );
    if (!result.ok) {
      res.status(400).json({ ok: false, error: result.error });
      return;
    }
    res.json({
      ok: true,
      message: "Marked ACTIVE in LessonList.json.",
      LastUpdated_Date: new Date().toISOString().slice(0, 10),
    });
  });

  r.post("/remove-by-lookup", requireRole("CoachManager"), async (req, res) => {
    const ctx = await coachManagerClubContextAsync(req);
    if (!ctx.ok) {
      res.status(ctx.status).json({ ok: false, error: ctx.error });
      return;
    }
    const classInfo = String(req.body?.class_info ?? req.body?.classInfo ?? "").trim();
    const sportType = String(req.body?.SportType ?? req.body?.sportType ?? "").trim();
    if (!classInfo && !sportType) {
      res.status(400).json({
        ok: false,
        error: "Enter at least one: class_info and/or SportType.",
      });
      return;
    }
    const fileClub = resolveLessonFileClubId(ctx.clubId);
    ensureLessonListFile(fileClub);
    const list = searchLessonsInClub(
      fileClub,
      classInfo || undefined,
      sportType || undefined,
    );
    if (list.length === 0) {
      res.status(404).json({
        ok: false,
        error:
          "No lesson found in backend/data_club/" +
          fileClub +
          "/LessonList.json for that class_info and/or SportType.",
      });
      return;
    }
    if (list.length > 1) {
      res.status(400).json({
        ok: false,
        error: "Multiple rows matched; narrow with class_info and SportType.",
      });
      return;
    }
    const target = list[0]!;
    if (String(target.status).trim().toUpperCase() === "INACTIVE") {
      res.status(400).json({
        ok: false,
        error: "This lesson is already INACTIVE.",
      });
      return;
    }
    const result = removeLessonRow(fileClub, target.lessonId);
    if (!result.ok) {
      res.status(400).json({ ok: false, error: result.error });
      return;
    }
    res.json({
      ok: true,
      message: "Lesson marked INACTIVE.",
      status: "INACTIVE",
      LastUpdated_Date: new Date().toISOString().slice(0, 10),
    });
  });

  return r;
}
