import fs from "fs";
import { Router, type Request } from "express";
import { requireAuth, requireRole } from "../middleware/requireAuth";
import {
  findUserByUid,
  getCoachManagerExpiryDateForClubFolderUid,
  removeMainUserlistCoachOrStudentByUid,
  setCoachPasswordByUid,
  setLoginUsernameByUid,
} from "../userlistCsv";
import {
  appendCoachRoleLoginRow,
  coachRoleLoginExistsForCoachIdAndClub,
  deleteRoleLoginByUidOrMissing,
  findCoachRoleLoginByUid,
  usernameTakenForNewLogin,
} from "../coachStudentLoginCsv";
import { sportCoachDebugOn } from "../sportCoachDebug";
import {
  allocateNextCoachId,
  appendCoachRow,
  COACH_LIST_FILENAME,
  coachCsvRowToApiFields,
  coachListPath,
  coachListResolvedPath,
  ensureCoachListFile,
  findClubUidForCoachId,
  getDataClubRootPath,
  isValidClubFolderId,
  loadCoachListRaw,
  loadCoaches,
  purgeCoachRowFromAllClubFolders,
  removeCoachRow,
  searchCoachesInClub,
  updateCoachRow,
  type CoachListRaw,
} from "../coachListCsv";
import { resolveStudentClubSession } from "../studentListCsv";

function readCoachWriteBody(body: unknown): {
  coachName: string;
  email: string;
  phone: string;
  sex: string;
  dateOfBirth: string;
  joinedDate: string;
  homeAddress: string;
  country: string;
  username: string;
  remark: string;
  hourlyRate: string;
  status: string;
  /** For create flow only; ignored on update. */
  password: string;
} {
  const b = body as Record<string, unknown>;
  return {
    coachName: String(
      b?.full_name ?? b?.CoachName ?? b?.coachName ?? "",
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
    remark: String(b?.remark ?? b?.Remark ?? "").trim(),
    hourlyRate: String(
      b?.["hourly_rate (HKD)"] ?? b?.hourlyRate ?? b?.hourly_rate ?? "",
    ).trim(),
    status: String(b?.status ?? b?.Status ?? "ACTIVE").trim(),
    password: String(
      b?.default_password ?? b?.defaultPassword ?? b?.password ?? "",
    ).trim(),
  };
}


function coachManagerDebugSnapshot(req: Request): Record<string, unknown> {
  const jwtSub = String(req.user?.sub ?? "").trim();
  const row = jwtSub ? findUserByUid(jwtSub) : null;
  const root = getDataClubRootPath();
  const idOk = jwtSub ? isValidClubFolderId(jwtSub) : false;
  const resolved = idOk ? coachListResolvedPath(jwtSub) : "";
  const pathForFile = idOk ? coachListPath(jwtSub) : "";
  let coachFileExistsOnDisk = false;
  let fileSizeBytes: number | null = null;
  let fileHeadPreview = "";
  let fileReadError: string | null = null;
  if (pathForFile) {
    try {
      coachFileExistsOnDisk = fs.existsSync(pathForFile);
      if (coachFileExistsOnDisk) {
        fileSizeBytes = fs.statSync(pathForFile).size;
        fileHeadPreview = fs.readFileSync(pathForFile, "utf8").slice(0, 500);
      }
    } catch (e) {
      fileReadError = e instanceof Error ? e.message : String(e);
    }
  }
  return {
    route: "GET /api/coach-manager/coaches",
    cwd: process.cwd(),
    dataClubRoot: root,
    env_DATA_CLUB_ROOT: process.env.DATA_CLUB_ROOT ?? null,
    jwtSub,
    jwtRole: req.user?.role ?? null,
    jwtUsername: req.user?.username ?? null,
    uidMatchesDataClubPattern: idOk,
    userlistRowFound: Boolean(row),
    userlistRole: row?.role ?? null,
    userlistClubNamePreview: row?.clubName ?? null,
    coachCsvResolvedPath: resolved || null,
    coachFileExistsOnDisk,
    fileSizeBytes,
    fileHeadPreview,
    fileReadError,
  };
}

function coachLoadDebugExtra(
  clubId: string,
  coachCsv: CoachListRaw,
  coachesLen: number,
): Record<string, unknown> {
  return {
    coachCsvHeaderCount: coachCsv.headers.length,
    coachCsvRowCount: coachCsv.rows.length,
    coachesParsedCount: coachesLen,
    firstHeadersSample: coachCsv.headers.slice(0, 20),
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

/** Coach Manager, Coach, or Student (read-only roster). */
function resolveCoachListReadContext(req: Request):
  | { ok: true; clubId: string; clubName: string }
  | { ok: false; status: number; error: string } {
  const role = String(req.user?.role ?? "");
  if (role === "CoachManager") {
    return coachManagerClubContext(req);
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
    const managerRow = findUserByUid(clubId);
    if (!managerRow || managerRow.role !== "CoachManager") {
      return { ok: false, status: 403, error: "Invalid club for coach access." };
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
    const managerRow = findUserByUid(clubId);
    if (!managerRow || managerRow.role !== "CoachManager") {
      return { ok: false, status: 403, error: "Invalid club for student access." };
    }
    const clubName = (managerRow.clubName && managerRow.clubName.trim()) || "";
    return { ok: true, clubId, clubName };
  }
  return { ok: false, status: 403, error: "Forbidden" };
}

export function createCoachManagerCoachRouter(): Router {
  const r = Router();

  r.use(requireAuth);

  r.get("/", requireRole("CoachManager", "Coach", "Student"), (_req, res) => {
    const ctx = resolveCoachListReadContext(_req);
    if (!ctx.ok) {
      const body: Record<string, unknown> = { ok: false, error: ctx.error };
      if (sportCoachDebugOn()) {
        body.debug = coachManagerDebugSnapshot(_req);
        console.warn("[SPORT_COACH_DEBUG] coach-manager coaches context failed", body);
      }
      res.status(ctx.status).json(body);
      return;
    }
    try {
      ensureCoachListFile(ctx.clubId);
      /** Raw table always returned; structured coaches may be empty if headers don’t match expected columns. */
      const coachCsv = loadCoachListRaw(ctx.clubId);
      let coaches: ReturnType<typeof loadCoaches> = [];
      let coachesParseWarning: string | null = null;
      try {
        coaches = loadCoaches(ctx.clubId);
      } catch (parseErr) {
        coachesParseWarning =
          parseErr instanceof Error ? parseErr.message : String(parseErr);
      }
      const idEnc = encodeURIComponent(ctx.clubId);
      const fileEnc = encodeURIComponent(COACH_LIST_FILENAME);
      const payload: Record<string, unknown> = {
        ok: true,
        clubId: ctx.clubId,
        clubName: ctx.clubName,
        coachCsvFileUrl: `/backend/data_club/${idEnc}/${fileEnc}`,
        coachCsvResolvedPath: coachListResolvedPath(ctx.clubId),
        coaches: coaches.map((c) => {
          const fields = coachCsvRowToApiFields(c);
          const ul = findUserByUid(c.coachId);
          return { ...fields, username: ul?.username ?? "" };
        }),
        coachCsv,
        ...(coachesParseWarning ? { coachesParseWarning } : {}),
      };
      if (sportCoachDebugOn()) {
        const dbg = {
          ...coachManagerDebugSnapshot(_req),
          ...coachLoadDebugExtra(ctx.clubId, coachCsv, coaches.length),
        };
        payload.debug = dbg;
        console.log("[SPORT_COACH_DEBUG] coach-manager coaches OK", {
          clubId: ctx.clubId,
          coachCsvHeaderCount: coachCsv.headers.length,
          coachCsvRowCount: coachCsv.rows.length,
          path: dbg.coachCsvResolvedPath,
          fileExists: dbg.coachFileExistsOnDisk,
        });
      }
      res.json(payload);
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      const body: Record<string, unknown> = { ok: false, error: msg };
      if (sportCoachDebugOn()) {
        body.debug = {
          ...coachManagerDebugSnapshot(_req),
          loadException: msg,
        };
      }
      res.status(500).json(body);
    }
  });

  r.post("/", requireRole("CoachManager"), (req, res) => {
    const ctx = coachManagerClubContext(req);
    if (!ctx.ok) {
      res.status(ctx.status).json({ ok: false, error: ctx.error });
      return;
    }
    const w = readCoachWriteBody(req.body);
    const coachId = allocateNextCoachId(ctx.clubId);
    if (findUserByUid(coachId)) {
      res.status(409).json({
        ok: false,
        error: "Coach UID already exists in user list; cannot allocate a new ID.",
      });
      return;
    }
    const result = appendCoachRow(ctx.clubId, ctx.clubName, {
      coachName: w.coachName,
      email: w.email,
      phone: w.phone,
      sex: w.sex || "N/A",
      dateOfBirth: w.dateOfBirth,
      joinedDate: w.joinedDate,
      homeAddress: w.homeAddress,
      country: w.country,
      remark: w.remark,
      hourlyRate: w.hourlyRate,
      status: w.status,
      coachId,
    });
    if (!result.ok) {
      res.status(400).json({ ok: false, error: result.error });
      return;
    }
    res.json({
      ok: true,
      coachId: result.coachId,
      message:
        "Coach roster row created. No entry was added to userLogin.json — use New Login Account or an admin tool to add login when needed.",
    });
  });

  /** Standalone coach login row in userLogin_Coach only (not main userLogin / no roster row). */
  r.post("/role-login-account", requireRole("CoachManager"), (req, res) => {
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
    const rosterCoachIds: string[] = [];
    if (nameKey) {
      for (const c of loadCoaches(ctx.clubId)) {
        if (normPersonName(c.coachName) === nameKey) {
          rosterCoachIds.push(String(c.coachId ?? "").trim());
        }
      }
    }
    const uniqueCoachIds = [...new Set(rosterCoachIds.filter(Boolean))];
    if (uniqueCoachIds.length > 1) {
      res.status(400).json({
        ok: false,
        error:
          "Multiple coaches in this club share that full name; cannot determine which roster row to link.",
      });
      return;
    }
    if (uniqueCoachIds.length === 1) {
      const rosterCid = uniqueCoachIds[0]!;
      if (coachRoleLoginExistsForCoachIdAndClub(rosterCid, ctx.clubName)) {
        res.status(400).json({
          ok: false,
          error: "The Coach's User login was created before!",
        });
        return;
      }
    }

    const out = appendCoachRoleLoginRow({
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
      message: "Coach login account created in userLogin_Coach.",
      uid: out.uid,
      clubId: ctx.clubId,
      clubName: ctx.clubName,
    });
  });

  r.put("/", requireRole("CoachManager"), (req, res) => {
    const ctx = coachManagerClubContext(req);
    if (!ctx.ok) {
      res.status(ctx.status).json({ ok: false, error: ctx.error });
      return;
    }
    const coachId = String(req.body?.CoachID ?? req.body?.coachId ?? "").trim();
    const w = readCoachWriteBody(req.body);
    const result = updateCoachRow(ctx.clubId, ctx.clubName, coachId, {
      coachName: w.coachName,
      email: w.email,
      phone: w.phone,
      sex: w.sex,
      dateOfBirth: w.dateOfBirth,
      joinedDate: w.joinedDate,
      homeAddress: w.homeAddress,
      country: w.country,
      remark: w.remark,
      hourlyRate: w.hourlyRate,
      status: w.status,
    });
    if (!result.ok) {
      res.status(400).json({ ok: false, error: result.error });
      return;
    }
    if (w.username) {
      const ur = setLoginUsernameByUid(coachId, w.username, "Coach");
      if (!ur.ok) {
        res.status(400).json({
          ok: false,
          error:
            ur.error +
            " Coach details in UserList_Coach.json were saved; login username was not changed.",
        });
        return;
      }
    }
    const newPwd = String(
      (req.body as Record<string, unknown>)?.default_password ??
        (req.body as Record<string, unknown>)?.defaultPassword ??
        (req.body as Record<string, unknown>)?.password ??
        "",
    ).trim();
    if (newPwd) {
      const pr = setCoachPasswordByUid(coachId, newPwd);
      if (!pr.ok) {
        res.status(400).json({
          ok: false,
          error:
            pr.error +
            " Coach details in UserList_Coach.json were saved; password was not changed.",
        });
        return;
      }
    }
    res.json({ ok: true, message: "Coach updated." });
  });

  r.post("/remove", requireRole("CoachManager"), (req, res) => {
    const ctx = coachManagerClubContext(req);
    if (!ctx.ok) {
      res.status(ctx.status).json({ ok: false, error: ctx.error });
      return;
    }
    const coachId = String(req.body?.CoachID ?? req.body?.coachId ?? "").trim();
    if (!coachId) {
      res.status(400).json({ ok: false, error: "CoachID is required." });
      return;
    }
    const loginBefore = !!findCoachRoleLoginByUid(coachId);
    const mainBefore = !!findUserByUid(coachId);
    const purge = purgeCoachRowFromAllClubFolders(coachId);
    if (!purge.ok) {
      res.status(400).json({ ok: false, error: purge.error });
      return;
    }
    const delRole = deleteRoleLoginByUidOrMissing(coachId, "Coach");
    if (!delRole.ok) {
      res.status(400).json({ ok: false, error: delRole.error });
      return;
    }
    const delMain = removeMainUserlistCoachOrStudentByUid(coachId, "Coach");
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
          "Coach not found in club rosters, userLogin_Coach, or main user list.",
      });
      return;
    }
    res.json({
      ok: true,
      message:
        "Coach removed from UserList_Coach.json under all data_club folders (where present), userLogin_Coach, and main userLogin when present.",
      purgedFromClubFolders: purge.updatedClubIds,
    });
  });

  r.post("/search", requireRole("CoachManager"), (req, res) => {
    const ctx = coachManagerClubContext(req);
    if (!ctx.ok) {
      res.status(ctx.status).json({ ok: false, error: ctx.error });
      return;
    }
    const coachName = String(req.body?.coachName ?? "").trim();
    const email = String(req.body?.email ?? "").trim();
    if (!coachName && !email) {
      res.status(400).json({
        ok: false,
        error:
          "Enter at least one: coach name and/or email (UserList_Coach.json for your club).",
      });
      return;
    }
    try {
      ensureCoachListFile(ctx.clubId);
      const list = searchCoachesInClub(
        ctx.clubId,
        coachName || undefined,
        email || undefined,
      );
      res.json({
        ok: true,
        results: list.map((c) => coachCsvRowToApiFields(c)),
      });
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      res.status(500).json({ ok: false, error: msg });
    }
  });

  r.post("/activate", requireRole("CoachManager"), (req, res) => {
    const ctx = coachManagerClubContext(req);
    if (!ctx.ok) {
      res.status(ctx.status).json({ ok: false, error: ctx.error });
      return;
    }
    const coachId = String(req.body?.CoachID ?? req.body?.coachId ?? "").trim();
    if (!coachId) {
      res.status(400).json({ ok: false, error: "CoachID is required." });
      return;
    }
    ensureCoachListFile(ctx.clubId);
    const coaches = loadCoaches(ctx.clubId);
    const c = coaches.find(
      (x) => x.coachId.trim().toUpperCase() === coachId.trim().toUpperCase(),
    );
    if (!c) {
      res.status(404).json({ ok: false, error: "Coach not found in your list." });
      return;
    }
    if (String(c.status).trim().toUpperCase() === "ACTIVE") {
      res.status(400).json({ ok: false, error: "Coach is already ACTIVE." });
      return;
    }
    const result = updateCoachRow(ctx.clubId, ctx.clubName, c.coachId, {
      coachName: c.coachName,
      email: c.email,
      phone: c.phone,
      sex: c.sex,
      dateOfBirth: c.dateOfBirth,
      joinedDate: c.joinedDate,
      homeAddress: c.homeAddress,
      country: c.country,
      remark: c.remark,
      hourlyRate: c.hourlyRate,
      status: "ACTIVE",
    });
    if (!result.ok) {
      res.status(400).json({ ok: false, error: result.error });
      return;
    }
    res.json({
      ok: true,
      message: "Marked ACTIVE in UserList_Coach.json.",
      lastUpdate_date: new Date().toISOString().slice(0, 10),
    });
  });

  r.post("/remove-by-lookup", requireRole("CoachManager"), (req, res) => {
    const ctx = coachManagerClubContext(req);
    if (!ctx.ok) {
      res.status(ctx.status).json({ ok: false, error: ctx.error });
      return;
    }
    const coachName = String(req.body?.coachName ?? "").trim();
    const email = String(req.body?.email ?? "").trim();
    if (!coachName && !email) {
      res.status(400).json({
        ok: false,
        error: "Enter at least one: coach name and/or email.",
      });
      return;
    }
    ensureCoachListFile(ctx.clubId);
    const list = searchCoachesInClub(
      ctx.clubId,
      coachName || undefined,
      email || undefined,
    );
    if (list.length === 0) {
      res.status(404).json({
        ok: false,
        error:
          "No coach found in backend/data_club/" +
          ctx.clubId +
          "/UserList_Coach.json for that name and/or email.",
      });
      return;
    }
    if (list.length > 1) {
      res.status(400).json({
        ok: false,
        error: "Multiple rows matched; narrow with coach name and email.",
      });
      return;
    }
    const target = list[0]!;
    if (String(target.status).trim().toUpperCase() === "INACTIVE") {
      res.status(400).json({
        ok: false,
        error: "This coach is already INACTIVE.",
      });
      return;
    }
    const result = removeCoachRow(ctx.clubId, target.coachId);
    if (!result.ok) {
      res.status(400).json({ ok: false, error: result.error });
      return;
    }
    res.json({
      ok: true,
      message: "Coach marked INACTIVE.",
      status: "INACTIVE",
      lastUpdate_date: new Date().toISOString().slice(0, 10),
    });
  });

  return r;
}
