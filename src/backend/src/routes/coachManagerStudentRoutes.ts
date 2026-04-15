import fs from "fs";
import { Router, type Request } from "express";
import { requireAuth, requireRole } from "../middleware/requireAuth";
import {
  findUserByUid,
  getCoachManagerExpiryDateForClubFolderUid,
  removeMainUserlistCoachOrStudentByUid,
} from "../userlistCsv";
import {
  appendStudentRoleLoginRow,
  deleteRoleLoginByUidOrMissing,
  findStudentRoleLoginByUid,
  studentRoleLoginExistsForStudentIdAndClub,
  usernameTakenForNewLogin,
} from "../coachStudentLoginCsv";
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
  allocateNextStudentId,
  appendStudentRow,
  ensureStudentListFile,
  loadStudentListRaw,
  loadStudents,
  purgeStudentRowFromAllClubFolders,
  STUDENT_LIST_FILENAME,
  studentCsvRowToApiFields,
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

function coachManagerClubContext(req: Request):
  | { ok: true; clubId: string; clubName: string }
  | { ok: false; status: number; error: string } {
  const clubId = String(req.user?.sub ?? "").trim();
  if (!clubId || !isValidClubFolderId(clubId)) {
    return { ok: false, status: 403, error: "Invalid club session." };
  }
  const row = findUserByUid(clubId);
  if (!row || row.role !== "CoachManager") {
    return { ok: false, status: 403, error: "Coach Manager access only." };
  }
  const clubName = (row.clubName && row.clubName.trim()) || "";
  if (!clubName || clubName === "—") {
    return {
      ok: false,
      status: 400,
      error: "Your account has no club name; contact an administrator.",
    };
  }
  return { ok: true, clubId, clubName };
}

function resolveStudentClubContext(req: Request):
  | { ok: true; clubId: string; clubName: string }
  | { ok: false; status: number; error: string } {
  const role = String(req.user?.role ?? "");
  if (role === "CoachManager") {
    return coachManagerClubContext(req);
  }
  if (role !== "Coach") {
    return { ok: false, status: 403, error: "Forbidden" };
  }
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
  const managerRow = findUserByUid(clubId);
  if (!managerRow || managerRow.role !== "CoachManager") {
    return { ok: false, status: 403, error: "Invalid club for student access." };
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
    (c) => c.coachId.trim().toUpperCase() === coachId.toUpperCase()
  );
  if (!inRoster) {
    return { ok: false, status: 403, error: "Coach not in club roster." };
  }
  return { ok: true, clubId, clubName };
}

export function createCoachManagerStudentRouter(): Router {
  const r = Router();

  r.use(requireAuth, requireRole("CoachManager", "Coach"));

  r.get("/", (_req, res) => {
    const ctx = resolveStudentClubContext(_req);
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
      let studentCsv = loadStudentListRaw(ctx.clubId);
      let students: ReturnType<typeof loadStudents> = [];
      let studentsParseWarning: string | null = null;
      try {
        students = loadStudents(ctx.clubId);
      } catch (parseErr) {
        studentsParseWarning =
          parseErr instanceof Error ? parseErr.message : String(parseErr);
      }
      if (_req.user?.role === "Coach") {
        const crow = findCoachRosterRow(ctx.clubId, String(_req.user.sub));
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
            ["StudentID", "studentid", "STUDENTID"],
            keep,
          ).rows,
        };
      }
      const idEnc = encodeURIComponent(ctx.clubId);
      const fileEnc = encodeURIComponent(STUDENT_LIST_FILENAME);
      const payload: Record<string, unknown> = {
        ok: true,
        clubId: ctx.clubId,
        clubName: ctx.clubName,
        studentCsvFileUrl: `/backend/data_club/${idEnc}/${fileEnc}`,
        studentCsvResolvedPath: studentListResolvedPath(ctx.clubId),
        students: students.map((s) => {
          const fields = studentCsvRowToApiFields(s);
          const ul = findUserByUid(s.studentId);
          return { ...fields, username: ul?.username ?? "" };
        }),
        studentCsv,
        ...(studentsParseWarning ? { studentsParseWarning } : {}),
      };
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

  r.post("/", (req, res) => {
    const ctx = resolveStudentClubContext(req);
    if (!ctx.ok) {
      res.status(ctx.status).json({ ok: false, error: ctx.error });
      return;
    }
    const w = readStudentWriteBody(req.body);
    let studentCoach = w.studentCoach;
    if (req.user?.role === "Coach") {
      const crow = findCoachRosterRow(ctx.clubId, String(req.user.sub));
      if (!crow) {
        res.status(403).json({ ok: false, error: "Coach roster row not found." });
        return;
      }
      studentCoach = (crow.coachName && crow.coachName.trim()) || studentCoach;
    }
    const studentId = allocateNextStudentId(ctx.clubId);
    if (findUserByUid(studentId)) {
      res.status(409).json({
        ok: false,
        error: "Student UID already exists in user list; cannot allocate a new ID.",
      });
      return;
    }
    const result = appendStudentRow(ctx.clubId, ctx.clubName, {
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
        "Student roster row created. No entry was added to userLogin.json — use New Login Account (Coach Manager) or an admin tool to add login when needed.",
    });
  });

  /** Standalone student login row in userLogin_Student only (Coach Manager). */
  r.post("/role-login-account", (req, res) => {
    if (req.user?.role !== "CoachManager") {
      res.status(403).json({
        ok: false,
        error: "Only Coach Manager can create a standalone student login account.",
      });
      return;
    }
    const ctx = coachManagerClubContext(req);
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
    if (usernameTakenForNewLogin(username)) {
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
      for (const s of loadStudents(ctx.clubId)) {
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

    const out = appendStudentRoleLoginRow({
      username,
      password,
      fullName,
      clubName: ctx.clubName,
      clubFolderUid: ctx.clubId,
      expiryDate: getCoachManagerExpiryDateForClubFolderUid(ctx.clubId),
    });
    if (!out.ok) {
      res.status(400).json({ ok: false, error: out.error });
      return;
    }
    res.json({
      ok: true,
      message: "Student login account created in userLogin_Student.",
      uid: out.uid,
      clubId: ctx.clubId,
      clubName: ctx.clubName,
    });
  });

  r.put("/", (req, res) => {
    const ctx = resolveStudentClubContext(req);
    if (!ctx.ok) {
      res.status(ctx.status).json({ ok: false, error: ctx.error });
      return;
    }
    const studentId = String(
      req.body?.StudentID ?? req.body?.studentId ?? "",
    ).trim();
    const w = readStudentWriteBody(req.body);
    if (req.user?.role === "Coach") {
      const crow = findCoachRosterRow(ctx.clubId, String(req.user.sub));
      if (!crow) {
        res.status(403).json({ ok: false, error: "Coach roster row not found." });
        return;
      }
      const list = loadStudents(ctx.clubId);
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
    const result = updateStudentRow(ctx.clubId, ctx.clubName, studentId, {
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

  r.post("/remove", (req, res) => {
    const ctx = resolveStudentClubContext(req);
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
      req.body?.StudentID ?? req.body?.studentId ?? "",
    ).trim();
    if (!studentId) {
      res.status(400).json({ ok: false, error: "StudentID is required." });
      return;
    }
    const loginBefore = !!findStudentRoleLoginByUid(studentId);
    const mainBefore = !!findUserByUid(studentId);
    const purge = purgeStudentRowFromAllClubFolders(studentId);
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
        "Student removed from UserList_Student.json under all data_club folders (where present), userLogin_Student, and main userLogin when present.",
      purgedFromClubFolders: purge.updatedClubIds,
    });
  });

  return r;
}
