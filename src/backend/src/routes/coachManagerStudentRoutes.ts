import fs from "fs";
import { Router, type Request } from "express";
import { requireAuth, requireRole } from "../middleware/requireAuth";
import {
  getCoachManagerExpiryDateForClubFolderUid,
  removeMainUserlistCoachOrStudentByUid,
} from "../userlistCsv";
import { isMongoConfigured, USER_LIST_STUDENT_COLLECTION } from "../db/DBConnection";
import {
  findUserByUidPreferred,
  findUsersByUidsPreferred,
  getCoachManagerExpiryDateForClubFolderUidMongo,
} from "../userListMongo";
import {
  coachManagerClubContextAsync,
  resolveClubFolderRoleContextAsync,
  resolveClubFolderUidForCoachRequest,
} from "../coachManagerSession";
import {
  appendStudentRoleLoginRow,
  deleteRoleLoginByUidOrMissing,
  findStudentRoleLoginByUid,
  studentRoleLoginExistsForStudentIdAndClub,
  usernameTakenForNewLoginPreferred,
} from "../coachStudentLoginCsv";
import { sportCoachDebugOn } from "../sportCoachDebug";
import {
  csvCoachFieldMatchesLoggedCoach,
  filterRawRowsByIdColumn,
  findCoachRosterRow,
} from "../coachSelfFilter";
import { getDataClubRootPath, isValidClubFolderId } from "../coachListCsv";
import {
  allocateNextStudentId,
  appendStudentRow,
  bumpClubScopedStudentId,
  ensureStudentListFile,
  loadStudentListRaw,
  loadStudents,
  purgeStudentRowFromAllClubFolders,
  STUDENT_LIST_FILENAME,
  studentCsvRowToApiFields,
  studentIdsEqual,
  studentListPath,
  studentListResolvedPath,
  updateStudentRow,
  type StudentListRaw,
} from "../studentListCsv";

function readStudentWriteBody(body: unknown): {
  studentName: string;
  email: string;
  phone: string;
  sex: string;
  dateOfBirth: string;
  joinedDate: string;
  homeAddress: string;
  country: string;
  username: string;
  guardian: string;
  guardianContact: string;
  school: string;
  studentCoach: string;
  remark: string;
  status: string;
  password: string;
} {
  const b = body as Record<string, unknown>;
  return {
    studentName: String(
      b?.full_name ?? b?.StudentName ?? b?.studentName ?? "",
    ).trim(),
    email: String(b?.email ?? b?.Email ?? "").trim(),
    phone: String(
      b?.contact_number ?? b?.Phone ?? b?.phone ?? "",
    ).trim(),
    sex: String(b?.sex ?? b?.Sex ?? "").trim(),
    dateOfBirth: String(
      b?.date_of_birth ?? b?.dateOfBirth ?? b?.DateOfBirth ?? "",
    ).trim(),
    joinedDate: String(
      b?.joined_date ?? b?.joinedDate ?? b?.JoinedDate ?? "",
    ).trim(),
    homeAddress: String(
      b?.home_address ?? b?.homeAddress ?? b?.HomeAddress ?? "",
    ).trim(),
    country: String(b?.country ?? b?.Country ?? "").trim(),
    username: String(b?.username ?? b?.Username ?? "").trim(),
    guardian: String(b?.guardian ?? b?.Guardian ?? "").trim(),
    guardianContact: String(
      b?.guardian_contact ?? b?.guardianContact ?? b?.GuardianContact ?? "",
    ).trim(),
    school: String(b?.school ?? b?.School ?? "").trim(),
    studentCoach: String(
      b?.student_coach ?? b?.studentCoach ?? b?.StudentCoach ?? "",
    ).trim(),
    remark: String(b?.remark ?? b?.Remark ?? "").trim(),
    status: String(b?.status ?? b?.Status ?? "ACTIVE").trim(),
    password: String(
      b?.default_password ?? b?.defaultPassword ?? b?.password ?? "",
    ).trim(),
  };
}

function coachManagerStudentDebugSnapshot(req: Request): Record<string, unknown> {
  const jwtSub = String(req.user?.sub ?? "").trim();
  const root = getDataClubRootPath();
  const idOk = jwtSub ? isValidClubFolderId(jwtSub) : false;
  const resolved = idOk ? studentListResolvedPath(jwtSub) : "";
  const pathForFile = idOk ? studentListPath(jwtSub) : "";
  let fileExistsOnDisk = false;
  let fileSizeBytes: number | null = null;
  let fileHeadPreview = "";
  let fileReadError: string | null = null;
  if (pathForFile) {
    try {
      fileExistsOnDisk = fs.existsSync(pathForFile);
      if (fileExistsOnDisk) {
        fileSizeBytes = fs.statSync(pathForFile).size;
        fileHeadPreview = fs.readFileSync(pathForFile, "utf8").slice(0, 500);
      }
    } catch (e) {
      fileReadError = e instanceof Error ? e.message : String(e);
    }
  }
  return {
    route: "GET /api/coach-manager/students",
    cwd: process.cwd(),
    dataClubRoot: root,
    jwtSub,
    jwtRole: req.user?.role ?? null,
    uidMatchesDataClubPattern: idOk,
    studentCsvResolvedPath: resolved || null,
    studentFileExistsOnDisk: fileExistsOnDisk,
    fileSizeBytes,
    fileHeadPreview,
    fileReadError,
  };
}

function studentLoadDebugExtra(
  clubId: string,
  studentCsv: StudentListRaw,
  studentsLen: number,
): Record<string, unknown> {
  return {
    studentCsvHeaderCount: studentCsv.headers.length,
    studentCsvRowCount: studentCsv.rows.length,
    studentsParsedCount: studentsLen,
    firstHeadersSample: studentCsv.headers.slice(0, 20),
  };
}

async function resolveStudentClubContextAsync(
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
  if (role !== "Coach") {
    return { ok: false, status: 403, error: "Forbidden" };
  }
  const coachId = String(req.user?.sub ?? "").trim();
  if (!coachId) {
    return { ok: false, status: 403, error: "Invalid session." };
  }
  const clubId = await resolveClubFolderUidForCoachRequest(req);
  if (!clubId) {
    return {
      ok: false,
      status: 403,
      error: "No club roster found for this coach account.",
    };
  }
  if ((await findCoachRosterRow(clubId, coachId)) == null) {
    return { ok: false, status: 403, error: "Coach not in club roster." };
  }
  const folderCtx = await resolveClubFolderRoleContextAsync(clubId, "student");
  if (!folderCtx.ok) {
    return folderCtx;
  }
  const clubName = folderCtx.clubName;
  if (!clubName || clubName === "—") {
    return {
      ok: false,
      status: 400,
      error: "Your club has no name configured; contact an administrator.",
    };
  }
  return { ok: true, clubId, clubName };
}

function parseCoachManagerStudentListQuery(req: Request): {
  compact: boolean;
  page: number | null;
  limit: number | null;
} {
  const q = req.query;
  const compact =
    String(q.compact ?? "").trim() === "1" ||
    String(q.compact ?? "").trim().toLowerCase() === "true";
  const hasPage = q.page != null && String(q.page).trim() !== "";
  const hasLimit =
    (q.limit != null && String(q.limit).trim() !== "") ||
    (q.pageSize != null && String(q.pageSize).trim() !== "");
  if (compact && !hasPage && !hasLimit) {
    return { compact: true, page: null, limit: null };
  }
  if (hasPage || hasLimit) {
    const page = Math.max(1, parseInt(String(q.page ?? "1"), 10) || 1);
    const limRaw =
      q.limit != null && String(q.limit).trim() !== ""
        ? String(q.limit)
        : String(q.pageSize ?? "10");
    const limit = Math.min(10, Math.max(1, parseInt(limRaw, 10) || 10));
    return { compact: false, page, limit };
  }
  return { compact: false, page: null, limit: null };
}

export function createCoachManagerStudentRouter(): Router {
  const r = Router();

  r.use(requireAuth, requireRole("CoachManager", "Coach"));

  r.get("/", async (_req, res) => {
    const ctx = await resolveStudentClubContextAsync(_req);
    if (!ctx.ok) {
      const body: Record<string, unknown> = { ok: false, error: ctx.error };
      if (sportCoachDebugOn()) {
        body.debug = coachManagerStudentDebugSnapshot(_req);
        console.warn("[SPORT_COACH_DEBUG] coach-manager students context failed", body);
      }
      res.status(ctx.status).json(body);
      return;
    }
    try {
      ensureStudentListFile(ctx.clubId);
      let studentCsv = await loadStudentListRaw(ctx.clubId);
      let students: Awaited<ReturnType<typeof loadStudents>> = [];
      let studentsParseWarning: string | null = null;
      try {
        students = await loadStudents(ctx.clubId);
      } catch (parseErr) {
        studentsParseWarning =
          parseErr instanceof Error ? parseErr.message : String(parseErr);
      }
      if (_req.user?.role === "Coach") {
        const crow = await findCoachRosterRow(
          ctx.clubId,
          String(_req.user.sub),
        );
        const uname = String(_req.user.username ?? "");
        if (crow) {
          students = students.filter((s) =>
            csvCoachFieldMatchesLoggedCoach(s.studentCoach, crow, uname),
          );
        } else {
          students = [];
        }
        const keep = new Set(
          students.map((s) => s.studentId.trim().toUpperCase()),
        );
        studentCsv = {
          ...studentCsv,
          rows: filterRawRowsByIdColumn(
            studentCsv,
            ["StudentID", "student_id", "studentid", "STUDENTID"],
            keep,
          ).rows,
        };
      }
      const idEnc = encodeURIComponent(ctx.clubId);
      const fileEnc = encodeURIComponent(STUDENT_LIST_FILENAME);
      const rosterMongo = isMongoConfigured();
      const listQ = parseCoachManagerStudentListQuery(_req);

      if (listQ.compact) {
        const loginByStudentId = await findUsersByUidsPreferred(
          students.map((s) => s.studentId),
        );
        const slim = students.map((s) => {
          const fields = studentCsvRowToApiFields(s);
          const ul = loginByStudentId.get(s.studentId.trim().toUpperCase());
          return {
            student_id: fields.student_id ?? fields.StudentID ?? s.studentId,
            full_name: fields.full_name ?? "",
            username: ul?.username ?? "",
          };
        });
        res.json({
          ok: true,
          compact: true,
          clubId: ctx.clubId,
          clubName: ctx.clubName,
          studentTotal: students.length,
          students: slim,
          ...(studentsParseWarning ? { studentsParseWarning } : {}),
        });
        return;
      }

      const totalStudents = students.length;
      let pageStudents = students;
      if (listQ.page != null && listQ.limit != null) {
        const start = (listQ.page - 1) * listQ.limit;
        pageStudents = students.slice(start, start + listQ.limit);
      }
      const loginByStudentId = await findUsersByUidsPreferred(
        pageStudents.map((s) => s.studentId),
      );
      const includeFullCsv =
        listQ.page == null ||
        listQ.limit == null ||
        listQ.page <= 1;
      const studentCsvOut = includeFullCsv
        ? studentCsv
        : {
            headers: studentCsv.headers,
            rows: [] as typeof studentCsv.rows,
            truncated: true,
          };
      const payload: Record<string, unknown> = {
        ok: true,
        clubId: ctx.clubId,
        clubName: ctx.clubName,
        studentRosterSource: rosterMongo ? "mongo" : "disk",
        studentCsvFileUrl: rosterMongo
          ? null
          : `/backend/data_club/${idEnc}/${fileEnc}`,
        mongoStudentCollection: rosterMongo ? USER_LIST_STUDENT_COLLECTION : null,
        studentCsvResolvedPath: rosterMongo
          ? `MongoDB/${USER_LIST_STUDENT_COLLECTION}/${ctx.clubId}`
          : studentListResolvedPath(ctx.clubId),
        students: pageStudents.map((s) => {
          const fields = studentCsvRowToApiFields(s);
          const ul = loginByStudentId.get(s.studentId.trim().toUpperCase());
          return {
            ...fields,
            club_name: ctx.clubName,
            username: ul?.username ?? "",
          };
        }),
        studentCsv: studentCsvOut,
        ...(studentsParseWarning ? { studentsParseWarning } : {}),
      };
      if (listQ.page != null && listQ.limit != null) {
        payload.studentTotal = totalStudents;
        payload.studentPage = listQ.page;
        payload.studentPageSize = listQ.limit;
      }
      if (sportCoachDebugOn()) {
        const dbg = {
          ...coachManagerStudentDebugSnapshot(_req),
          ...studentLoadDebugExtra(ctx.clubId, studentCsv, students.length),
        };
        payload.debug = dbg;
        console.log("[SPORT_COACH_DEBUG] coach-manager students OK", {
          clubId: ctx.clubId,
          studentCsvHeaderCount: studentCsv.headers.length,
          studentCsvRowCount: studentCsv.rows.length,
          path: dbg.studentCsvResolvedPath,
          fileExists: dbg.studentFileExistsOnDisk,
        });
      }
      res.json(payload);
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      const body: Record<string, unknown> = { ok: false, error: msg };
      if (sportCoachDebugOn()) {
        body.debug = {
          ...coachManagerStudentDebugSnapshot(_req),
          loadException: msg,
        };
      }
      res.status(500).json(body);
    }
  });

  r.post("/", async (req, res) => {
    const ctx = await resolveStudentClubContextAsync(req);
    if (!ctx.ok) {
      res.status(ctx.status).json({ ok: false, error: ctx.error });
      return;
    }
    const w = readStudentWriteBody(req.body);
    let studentCoach = w.studentCoach;
    if (req.user?.role === "Coach") {
      const crow = await findCoachRosterRow(ctx.clubId, String(req.user.sub));
      if (!crow) {
        res.status(403).json({ ok: false, error: "Coach roster row not found." });
        return;
      }
      studentCoach = (crow.coachName && crow.coachName.trim()) || studentCoach;
    }
    const rosterStudents = await loadStudents(ctx.clubId);
    const rosterHasStudentId = (id: string) =>
      rosterStudents.some((r) => studentIdsEqual(r.studentId, id));
    let studentId = await allocateNextStudentId(ctx.clubId);
    const maxUidAttempts = 10_000;
    for (let attempt = 0; attempt < maxUidAttempts; attempt++) {
      if (!rosterHasStudentId(studentId) && !(await findUserByUidPreferred(studentId))) {
        break;
      }
      const nextId = bumpClubScopedStudentId(studentId);
      if (nextId === studentId) {
        res.status(500).json({
          ok: false,
          error:
            "Could not allocate a student ID (unexpected ID format after collisions).",
        });
        return;
      }
      studentId = nextId;
    }
    if (rosterHasStudentId(studentId) || (await findUserByUidPreferred(studentId))) {
      res.status(409).json({
        ok: false,
        error:
          "Could not allocate a free student UID after many attempts; check userLogin and roster consistency.",
      });
      return;
    }
    const result = await appendStudentRow(ctx.clubId, ctx.clubName, {
      studentName: w.studentName,
      email: w.email,
      phone: w.phone,
      sex: w.sex || "N/A",
      dateOfBirth: w.dateOfBirth,
      joinedDate: w.joinedDate,
      homeAddress: w.homeAddress,
      country: w.country,
      guardian: w.guardian,
      guardianContact: w.guardianContact,
      school: w.school,
      studentCoach,
      remark: w.remark,
      status: w.status,
      studentId,
    });
    if (!result.ok) {
      res.status(400).json({ ok: false, error: result.error });
      return;
    }
    res.json({
      ok: true,
      studentId: result.studentId,
      message:
        "Student roster row created. No entry was added to userLogin.csv — use New Login Account (Coach Manager) or an admin tool to add login when needed.",
    });
  });

  /** Standalone student login row in userLogin_Student only (Coach Manager). */
  r.post("/role-login-account", async (req, res) => {
    if (req.user?.role !== "CoachManager") {
      res.status(403).json({
        ok: false,
        error: "Only Coach Manager can create a standalone student login account.",
      });
      return;
    }
    const ctx = await coachManagerClubContextAsync(req);
    if (!ctx.ok) {
      res.status(ctx.status).json({ ok: false, error: ctx.error });
      return;
    }
    const b = req.body as Record<string, unknown>;
    const fullName = String(b?.fullName ?? b?.full_name ?? "").trim();
    const username = String(b?.username ?? "").trim();
    const password = String(b?.password ?? "").trim();
    const passwordConfirm = String(
      b?.passwordConfirm ?? b?.re_enter_password ?? b?.password_confirm ?? "",
    ).trim();
    if (!fullName) {
      res.status(400).json({ ok: false, error: "Full name is required." });
      return;
    }
    if (!username) {
      res.status(400).json({ ok: false, error: "Username is required." });
      return;
    }
    if (!password) {
      res.status(400).json({ ok: false, error: "Password is required." });
      return;
    }
    if (password !== passwordConfirm) {
      res.status(400).json({ ok: false, error: "Passwords do not match." });
      return;
    }
    if (password.length < 6) {
      res.status(400).json({
        ok: false,
        error: "Password must be at least 6 characters.",
      });
      return;
    }
    if (await usernameTakenForNewLoginPreferred(username)) {
      res.status(400).json({
        ok: false,
        error: "The user already existed !",
      });
      return;
    }

    function normPersonName(s: string): string {
      return String(s ?? "")
        .trim()
        .toLowerCase()
        .replace(/\s+/g, " ");
    }
    const nameKey = normPersonName(fullName);
    const rosterIds: string[] = [];
    if (nameKey) {
      for (const s of await loadStudents(ctx.clubId)) {
        if (normPersonName(s.studentName) === nameKey) {
          rosterIds.push(String(s.studentId ?? "").trim());
        }
      }
    }
    const uniqueRosterIds = [...new Set(rosterIds.filter(Boolean))];
    if (uniqueRosterIds.length > 1) {
      res.status(400).json({
        ok: false,
        error:
          "Multiple students in this club share that full name; cannot determine which roster row to link.",
      });
      return;
    }
    if (uniqueRosterIds.length === 1) {
      const rosterSid = uniqueRosterIds[0]!;
      if (
        studentRoleLoginExistsForStudentIdAndClub(rosterSid, ctx.clubName)
      ) {
        res.status(400).json({
          ok: false,
          error: "The student user's login was created before!",
        });
        return;
      }
    }

    let expiryDate = getCoachManagerExpiryDateForClubFolderUid(ctx.clubId);
    if (isMongoConfigured()) {
      try {
        const e = await getCoachManagerExpiryDateForClubFolderUidMongo(
          ctx.clubId,
        );
        if (e) {
          expiryDate = e;
        }
      } catch {
        /* keep CSV-derived expiry */
      }
    }
    const out = await appendStudentRoleLoginRow({
      username,
      password,
      fullName,
      clubName: ctx.clubName,
      clubFolderUid: ctx.clubId,
      expiryDate,
    });
    if (!out.ok) {
      res.status(400).json({ ok: false, error: out.error });
      return;
    }
    res.json({
      ok: true,
      message: isMongoConfigured()
        ? "Student login account created in MongoDB (userLogin)."
        : "Student login account created in userLogin_Student.",
      uid: out.uid,
      clubId: ctx.clubId,
      clubName: ctx.clubName,
    });
  });

  r.put("/", async (req, res) => {
    const ctx = await resolveStudentClubContextAsync(req);
    if (!ctx.ok) {
      res.status(ctx.status).json({ ok: false, error: ctx.error });
      return;
    }
    const studentId = String(
      req.body?.student_id ??
      req.body?.StudentID ??
      req.body?.studentId ??
      "",
    ).trim();
    const w = readStudentWriteBody(req.body);
    if (req.user?.role === "Coach") {
      const crow = await findCoachRosterRow(ctx.clubId, String(req.user.sub));
      if (!crow) {
        res.status(403).json({ ok: false, error: "Coach roster row not found." });
        return;
      }
      const list = await loadStudents(ctx.clubId);
      const existing = list.find(
        (s) => s.studentId.trim().toUpperCase() === studentId.toUpperCase(),
      );
      if (
        !existing ||
        !csvCoachFieldMatchesLoggedCoach(
          existing.studentCoach,
          crow,
          String(req.user.username ?? ""),
        )
      ) {
        res.status(403).json({
          ok: false,
          error: "You can only update students assigned to you (student_coach).",
        });
        return;
      }
    }
    const result = await updateStudentRow(ctx.clubId, ctx.clubName, studentId, {
      studentName: w.studentName,
      email: w.email,
      phone: w.phone,
      sex: w.sex,
      dateOfBirth: w.dateOfBirth,
      joinedDate: w.joinedDate,
      homeAddress: w.homeAddress,
      country: w.country,
      guardian: w.guardian,
      guardianContact: w.guardianContact,
      school: w.school,
      studentCoach: w.studentCoach,
      remark: w.remark,
      status: w.status,
    });
    if (!result.ok) {
      res.status(400).json({ ok: false, error: result.error });
      return;
    }
    res.json({ ok: true, message: "Student updated." });
  });

  r.post("/remove", async (req, res) => {
    const ctx = await resolveStudentClubContextAsync(req);
    if (!ctx.ok) {
      res.status(ctx.status).json({ ok: false, error: ctx.error });
      return;
    }
    if (req.user?.role === "Coach") {
      res.status(403).json({
        ok: false,
        error: "Only Coach Manager can remove a student account.",
      });
      return;
    }
    const studentId = String(
      req.body?.student_id ??
      req.body?.StudentID ??
      req.body?.studentId ??
      "",
    ).trim();
    if (!studentId) {
      res.status(400).json({ ok: false, error: "student_id is required." });
      return;
    }
    const loginBefore = !!findStudentRoleLoginByUid(studentId);
    const mainBefore = Boolean(await findUserByUidPreferred(studentId));
    const purge = await purgeStudentRowFromAllClubFolders(studentId);
    if (!purge.ok) {
      res.status(400).json({ ok: false, error: purge.error });
      return;
    }
    const delRole = deleteRoleLoginByUidOrMissing(studentId, "Student");
    if (!delRole.ok) {
      res.status(400).json({ ok: false, error: delRole.error });
      return;
    }
    const delMain = removeMainUserlistCoachOrStudentByUid(studentId, "Student");
    if (!delMain.ok) {
      res.status(400).json({ ok: false, error: delMain.error });
      return;
    }
    if (
      purge.updatedClubIds.length === 0 &&
      !loginBefore &&
      !mainBefore
    ) {
      res.status(404).json({
        ok: false,
        error:
          "Student not found in club rosters, userLogin_Student, or main user list.",
      });
      return;
    }
    res.json({
      ok: true,
      message:
        "Student removed from MongoDB UserList_Student (or disk roster where used), userLogin_Student, and main userLogin when present.",
      purgedFromClubFolders: purge.updatedClubIds,
    });
  });

  return r;
}
