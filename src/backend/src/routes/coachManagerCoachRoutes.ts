import fs from "fs";
import { Router, type Request } from "express";
import { requireAuth, requireRole } from "../middleware/requireAuth";
import {
  getCoachManagerExpiryDateForClubFolderUid,
  removeMainUserlistCoachOrStudentByUid,
  setCoachPasswordByUid,
  setLoginUsernameByUid,
} from "../userlistCsv";
import {
  isMongoConfigured,
  resolveUserListRosterDatabaseName,
  USER_LIST_COACH_COLLECTION,
} from "../db/DBConnection";
import {
  findUserByUidPreferred,
  findUsersByUidsPreferred,
  getCoachManagerExpiryDateForClubFolderUidMongo,
} from "../userListMongo";
import {
  coachManagerClubContextAsync,
  resolveClubFolderRoleContextAsync,
  resolveClubFolderUidForCoachRequest,
  resolveStudentClubSessionFromRequest,
} from "../coachManagerSession";
import {
  allocateNextCoachIdPreferred,
  appendCoachRowMongo,
  bumpCoachUidForCollisionPreferred,
  loadCoachListRawPreferred,
  loadCoachesPreferred,
  purgeCoachRowFromAllPreferred,
  removeCoachRowMongo,
  searchCoachesInClubPreferred,
  updateCoachRowMongo,
} from "../coachListMongo";
import {
  appendCoachRoleLoginRow,
  coachRoleLoginExistsForCoachIdAndClubPreferred,
  deleteRoleLoginByUidOrMissing,
  findCoachRoleLoginByUid,
  usernameTakenForNewLoginPreferred,
} from "../coachStudentLoginCsv";
import { sportCoachDebugOn } from "../sportCoachDebug";
import {
  allocateNextCoachId,
  appendCoachRow,
  bumpNumericCoachLoginStyleId,
  coachIdsEqual,
  COACH_LIST_FILENAME,
  coachCsvRowToApiFields,
  coachListPath,
  coachListResolvedPath,
  ensureCoachListFile,
  getDataClubRootPath,
  isValidClubFolderId,
  purgeCoachRowFromAllClubFolders,
  removeCoachRow,
  updateCoachRow,
  type CoachListRaw,
} from "../coachListCsv";

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


async function coachManagerDebugSnapshot(
  req: Request,
): Promise<Record<string, unknown>> {
  const jwtSub = String(req.user?.sub ?? "").trim();
  const row = jwtSub ? await findUserByUidPreferred(jwtSub) : null;
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

/** Coach Manager, Coach, or Student (read-only roster). */
async function resolveCoachListReadContextAsync(
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
    const clubId = await resolveClubFolderUidForCoachRequest(req);
    if (!clubId) {
      return {
        ok: false,
        status: 403,
        error: "No club roster found for this coach account.",
      };
    }
    const inRoster = (await loadCoachesPreferred(clubId)).some(
      (c) => c.coachId.trim().toUpperCase() === coachId.toUpperCase(),
    );
    if (!inRoster) {
      return { ok: false, status: 403, error: "Coach not in club roster." };
    }
    const folderCtx = await resolveClubFolderRoleContextAsync(clubId, "coach");
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
  if (role === "Student") {
    const studentId = String(req.user?.sub ?? "").trim();
    if (!studentId) {
      return { ok: false, status: 403, error: "Invalid session." };
    }
    const session = await resolveStudentClubSessionFromRequest(req);
    if (!session.ok) {
      return { ok: false, status: 403, error: session.error };
    }
    const { clubId } = session;
    const folderCtx = await resolveClubFolderRoleContextAsync(clubId, "student");
    if (!folderCtx.ok) {
      return folderCtx;
    }
    return { ok: true, clubId, clubName: folderCtx.clubName };
  }
  return { ok: false, status: 403, error: "Forbidden" };
}

/** `compact=1`: full roster, minimal JSON (no `coachCsv`). `page`/`limit`: paginated table + Mongo logins only for that page. */
function parseCoachManagerListQuery(req: Request): {
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

export function createCoachManagerCoachRouter(): Router {
  const r = Router();

  r.use(requireAuth);

  r.get("/", requireRole("CoachManager", "Coach", "Student"), async (_req, res) => {
    const ctx = await resolveCoachListReadContextAsync(_req);
    if (!ctx.ok) {
      const body: Record<string, unknown> = { ok: false, error: ctx.error };
      if (sportCoachDebugOn()) {
        body.debug = await coachManagerDebugSnapshot(_req);
        console.warn("[SPORT_COACH_DEBUG] coach-manager coaches context failed", body);
      }
      res.status(ctx.status).json(body);
      return;
    }
    const qClub = String(
      _req.query?.club_id ?? _req.query?.clubId ?? "",
    ).trim();
    if (qClub && qClub !== ctx.clubId) {
      res.status(403).json({
        ok: false,
        error: "club_id does not match your session club.",
      });
      return;
    }
    try {
      /** Raw table always returned; structured coaches may be empty if headers don’t match expected columns. */
      const coachCsv = await loadCoachListRawPreferred(ctx.clubId);
      let coaches: Awaited<ReturnType<typeof loadCoachesPreferred>> = [];
      let coachesParseWarning: string | null = null;
      try {
        coaches = await loadCoachesPreferred(ctx.clubId);
      } catch (parseErr) {
        coachesParseWarning =
          parseErr instanceof Error ? parseErr.message : String(parseErr);
      }
      const idEnc = encodeURIComponent(ctx.clubId);
      const fileEnc = encodeURIComponent(COACH_LIST_FILENAME);
      const listQ = parseCoachManagerListQuery(_req);

      if (listQ.compact) {
        const loginByCoachId = await findUsersByUidsPreferred(
          coaches.map((c) => c.coachId),
        );
        const slim = coaches.map((c) => {
          const fields = coachCsvRowToApiFields(c);
          const ul = loginByCoachId.get(c.coachId.trim().toUpperCase());
          return {
            coach_id: fields.coach_id ?? fields.CoachID ?? c.coachId,
            full_name: fields.full_name ?? "",
            username: ul?.username ?? "",
            status: fields.status ?? c.status ?? "ACTIVE",
          };
        });
        res.json({
          ok: true,
          compact: true,
          clubId: ctx.clubId,
          clubName: ctx.clubName,
          rosterStorage: isMongoConfigured() ? "mongodb" : "json_file",
          coachTotal: coaches.length,
          coaches: slim,
          ...(coachesParseWarning ? { coachesParseWarning } : {}),
        });
        return;
      }

      const totalCoaches = coaches.length;
      let pageCoaches = coaches;
      if (listQ.page != null && listQ.limit != null) {
        const start = (listQ.page - 1) * listQ.limit;
        pageCoaches = coaches.slice(start, start + listQ.limit);
      }
      const loginByCoachId = await findUsersByUidsPreferred(
        pageCoaches.map((c) => c.coachId),
      );
      const includeFullCsv =
        listQ.page == null ||
        listQ.limit == null ||
        listQ.page <= 1;
      const coachCsvOut = includeFullCsv
        ? coachCsv
        : {
            headers: coachCsv.headers,
            rows: [] as typeof coachCsv.rows,
            truncated: true,
          };
      const payload: Record<string, unknown> = {
        ok: true,
        clubId: ctx.clubId,
        clubName: ctx.clubName,
        rosterStorage: isMongoConfigured() ? "mongodb" : "json_file",
        coachCsvFileUrl: isMongoConfigured()
          ? null
          : `/backend/data_club/${idEnc}/${fileEnc}`,
        coachCsvResolvedPath: isMongoConfigured()
          ? `mongodb:${resolveUserListRosterDatabaseName()}/${USER_LIST_COACH_COLLECTION}`
          : coachListResolvedPath(ctx.clubId),
        coaches: pageCoaches.map((c) => {
          const fields = coachCsvRowToApiFields(c);
          const ul = loginByCoachId.get(c.coachId.trim().toUpperCase());
          return { ...fields, username: ul?.username ?? "" };
        }),
        coachCsv: coachCsvOut,
        ...(coachesParseWarning ? { coachesParseWarning } : {}),
      };
      if (listQ.page != null && listQ.limit != null) {
        payload.coachTotal = totalCoaches;
        payload.coachPage = listQ.page;
        payload.coachPageSize = listQ.limit;
      }
      if (sportCoachDebugOn()) {
        const dbg = {
          ...(await coachManagerDebugSnapshot(_req)),
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
          ...(await coachManagerDebugSnapshot(_req)),
          loadException: msg,
        };
      }
      res.status(500).json(body);
    }
  });

  r.post("/", requireRole("CoachManager"), async (req, res) => {
    const ctx = await coachManagerClubContextAsync(req);
    if (!ctx.ok) {
      res.status(ctx.status).json({ ok: false, error: ctx.error });
      return;
    }
    const w = readCoachWriteBody(req.body);
    const rosterSnapshot = await loadCoachesPreferred(ctx.clubId);
    const rosterHasCoachId = (id: string) =>
      rosterSnapshot.some((r) => coachIdsEqual(r.coachId, id));
    let coachId = isMongoConfigured()
      ? await allocateNextCoachIdPreferred(ctx.clubId)
      : allocateNextCoachId(ctx.clubId);
    const maxUidAttempts = 10_000;
    for (let attempt = 0; attempt < maxUidAttempts; attempt++) {
      if (!rosterHasCoachId(coachId) && !(await findUserByUidPreferred(coachId))) {
        break;
      }
      const nextId = isMongoConfigured()
        ? bumpCoachUidForCollisionPreferred(ctx.clubId, coachId)
        : bumpNumericCoachLoginStyleId(coachId);
      if (nextId === coachId) {
        res.status(500).json({
          ok: false,
          error:
            "Could not allocate a coach ID (unexpected ID format after collisions).",
        });
        return;
      }
      coachId = nextId;
    }
    if (rosterHasCoachId(coachId) || (await findUserByUidPreferred(coachId))) {
      res.status(409).json({
        ok: false,
        error:
          "Could not allocate a free coach UID after many attempts; check userLogin and roster consistency.",
      });
      return;
    }
    const result = isMongoConfigured()
      ? await appendCoachRowMongo(ctx.clubId, ctx.clubName, {
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
        })
      : appendCoachRow(ctx.clubId, ctx.clubName, {
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
        "Coach roster row created. No entry was added to userLogin.csv — use New Login Account or an admin tool to add login when needed.",
    });
  });

  /** Standalone coach login row in userLogin_Coach only (not main userLogin / no roster row). */
  r.post("/role-login-account", requireRole("CoachManager"), async (req, res) => {
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
    const rosterCoachIds: string[] = [];
    if (nameKey) {
      for (const c of await loadCoachesPreferred(ctx.clubId)) {
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
      if (
        await coachRoleLoginExistsForCoachIdAndClubPreferred(
          rosterCid,
          ctx.clubId,
          ctx.clubName,
        )
      ) {
        res.status(400).json({
          ok: false,
          error: "The Coach's User login was created before!",
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
    const out = await appendCoachRoleLoginRow({
      username,
      password,
      fullName,
      clubName: ctx.clubName,
      clubFolderUid: ctx.clubId,
      ...(uniqueCoachIds.length === 1
        ? { rosterCoachId: uniqueCoachIds[0]! }
        : {}),
      expiryDate,
    });
    if (!out.ok) {
      res.status(400).json({ ok: false, error: out.error });
      return;
    }
    res.json({
      ok: true,
      message: isMongoConfigured()
        ? "Coach login account created in MongoDB (userLogin)."
        : "Coach login account created in userLogin_Coach.",
      uid: out.uid,
      coach_id:
        uniqueCoachIds.length === 1 ? uniqueCoachIds[0]! : undefined,
      clubId: ctx.clubId,
      clubName: ctx.clubName,
    });
  });

  r.put("/", requireRole("CoachManager"), async (req, res) => {
    const ctx = await coachManagerClubContextAsync(req);
    if (!ctx.ok) {
      res.status(ctx.status).json({ ok: false, error: ctx.error });
      return;
    }
    const coachId = String(
      req.body?.coach_id ?? req.body?.CoachID ?? req.body?.coachId ?? "",
    ).trim();
    const w = readCoachWriteBody(req.body);
    const result = isMongoConfigured()
      ? await updateCoachRowMongo(ctx.clubId, ctx.clubName, coachId, {
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
        })
      : updateCoachRow(ctx.clubId, ctx.clubName, coachId, {
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
            (isMongoConfigured()
              ? " Coach details in MongoDB UserList_Coach were saved; login username was not changed."
              : " Coach details in UserList_Coach.json were saved; login username was not changed."),
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
            (isMongoConfigured()
              ? " Coach details in MongoDB UserList_Coach were saved; password was not changed."
              : " Coach details in UserList_Coach.json were saved; password was not changed."),
        });
        return;
      }
    }
    res.json({ ok: true, message: "Coach updated." });
  });

  r.post("/remove", requireRole("CoachManager"), async (req, res) => {
    const ctx = await coachManagerClubContextAsync(req);
    if (!ctx.ok) {
      res.status(ctx.status).json({ ok: false, error: ctx.error });
      return;
    }
    const coachId = String(
      req.body?.coach_id ?? req.body?.CoachID ?? req.body?.coachId ?? "",
    ).trim();
    if (!coachId) {
      res.status(400).json({ ok: false, error: "coach_id is required." });
      return;
    }
    const loginBefore = !!findCoachRoleLoginByUid(coachId);
    const mainBefore = Boolean(await findUserByUidPreferred(coachId));
    const purge = isMongoConfigured()
      ? await purgeCoachRowFromAllPreferred(coachId)
      : purgeCoachRowFromAllClubFolders(coachId);
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
    const mongoRowsRemoved =
      isMongoConfigured() &&
      purge.ok &&
      "mongoDeleted" in purge &&
      (purge as { mongoDeleted: number }).mongoDeleted > 0;
    if (
      purge.updatedClubIds.length === 0 &&
      !mongoRowsRemoved &&
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
      message: isMongoConfigured()
        ? "Coach removed from MongoDB UserList_Coach (and club JSON files where present), userLogin_Coach, and main userLogin when present."
        : "Coach removed from UserList_Coach.json under all data_club folders (where present), userLogin_Coach, and main userLogin when present.",
      purgedFromClubFolders: purge.updatedClubIds,
      ...(isMongoConfigured() && "mongoDeleted" in purge
        ? { mongoCoachRowsDeleted: (purge as { mongoDeleted: number }).mongoDeleted }
        : {}),
    });
  });

  r.post("/search", requireRole("CoachManager"), async (req, res) => {
    const ctx = await coachManagerClubContextAsync(req);
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
          "Enter at least one: coach name and/or email (coach roster for your club).",
      });
      return;
    }
    try {
      if (!isMongoConfigured()) {
        ensureCoachListFile(ctx.clubId);
      }
      const list = await searchCoachesInClubPreferred(
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

  r.post("/activate", requireRole("CoachManager"), async (req, res) => {
    const ctx = await coachManagerClubContextAsync(req);
    if (!ctx.ok) {
      res.status(ctx.status).json({ ok: false, error: ctx.error });
      return;
    }
    const coachId = String(
      req.body?.coach_id ?? req.body?.CoachID ?? req.body?.coachId ?? "",
    ).trim();
    if (!coachId) {
      res.status(400).json({ ok: false, error: "coach_id is required." });
      return;
    }
    if (!isMongoConfigured()) {
      ensureCoachListFile(ctx.clubId);
    }
    const coaches = await loadCoachesPreferred(ctx.clubId);
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
    const result = isMongoConfigured()
      ? await updateCoachRowMongo(ctx.clubId, ctx.clubName, c.coachId, {
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
        })
      : updateCoachRow(ctx.clubId, ctx.clubName, c.coachId, {
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
      message: isMongoConfigured()
        ? "Marked ACTIVE in MongoDB UserList_Coach."
        : "Marked ACTIVE in UserList_Coach.json.",
      lastUpdate_date: new Date().toISOString().slice(0, 10),
    });
  });

  r.post("/remove-by-lookup", requireRole("CoachManager"), async (req, res) => {
    const ctx = await coachManagerClubContextAsync(req);
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
    if (!isMongoConfigured()) {
      ensureCoachListFile(ctx.clubId);
    }
    const list = await searchCoachesInClubPreferred(
      ctx.clubId,
      coachName || undefined,
      email || undefined,
    );
    if (list.length === 0) {
      res.status(404).json({
        ok: false,
        error: isMongoConfigured()
          ? "No coach found in MongoDB UserList_Coach for that name and/or email."
          : "No coach found in backend/data_club/" +
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
    const result = isMongoConfigured()
      ? await removeCoachRowMongo(ctx.clubId, target.coachId)
      : removeCoachRow(ctx.clubId, target.coachId);
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
