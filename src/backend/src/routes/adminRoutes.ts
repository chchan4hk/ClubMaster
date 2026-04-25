import { Router } from "express";
import fs from "fs";
import path from "path";
import multer from "multer";
import { requireAuth, requireRole } from "../middleware/requireAuth";
import { clubPhotoPublicUrl } from "./userAccountRoutes";
import {
  activateCoachRoleLogin,
  activateStudentRoleLogin,
  appendCoachRoleLoginRow,
  appendStudentRoleLoginRow,
  deactivateCoachRoleLogin,
  deactivateStudentRoleLogin,
  loadCoachRoleLogins,
  loadStudentRoleLogins,
  searchCoachRoleByUsernameOrClub,
  searchStudentRoleByUsernameOrClub,
  deleteRoleLoginByUid,
  removeRoleLoginRowsForCoachManagerFolderUid,
  setCoachRoleLoginActiveByUid,
  setRoleLoginPasswordByUid,
  setStudentRoleLoginActiveByUid,
  setRoleLoginExpiryByUid,
  updateRoleLoginProfileByUid,
} from "../coachStudentLoginCsv";
import { allocateNextClubUid } from "../coachListCsv";
import { invalidateDataFileCacheUnderDir } from "../dataFileCache";
export { allocateNextClubUid };
import {
  activateCoachManager,
  appendCoachManagerRow,
  deactivateCoachManager,
  findCoachManagerClubUidByClubName,
  getCoachManagerExpiryDateForClubFolderUid,
  findUserByUsername,
  loadUsersFromCsv,
  searchCoachManagers,
  setMainLoginPasswordByUid,
  setMainUserlistActivationByUid,
  setMainUserlistExpiryByUid,
  removeCoachManagerFromUserLoginStore,
  updateMainUserlistProfileByUid,
} from "../userlistCsv";
import { isMongoConfigured } from "../db/DBConnection";
import { patchClubInfoFields } from "../clubInfoMongo";
import { todaySlashYmd } from "../clubInfoJson";
import { clubCurrencyFromCountry } from "../countryCurrency";
import { readBasicInfoFromMongo } from "../basicInfoMongo";
import { lookupCountryCodeForBasicInfoName } from "../basicInfoCsv";
import {
  activateCoachManagerMongo,
  activateCoachRoleLoginMongo,
  activateStudentRoleLoginMongo,
  assertUsernameFreeForUidMongo,
  allocateNextCoachLoginUidMongo,
  allocateNextStudentLoginUidMongo,
  deactivateCoachManagerMongo,
  deactivateCoachRoleLoginMongo,
  deactivateStudentRoleLoginMongo,
  deleteCoachStudentForClubFolderMongo,
  deleteMainLoginMongo,
  deleteRoleLoginMongo,
  findCoachManagerUidByClubNameMongo,
  getCoachManagerExpiryDateForClubFolderUidMongo,
  userLoginUidExistsMongo,
  insertCoachManagerMongo,
  insertCoachRoleMongo,
  insertStudentRoleMongo,
  listAdminLoginAccountsFromMongo,
  searchCoachManagersMongo,
  searchCoachRoleByUsernameOrClubMongo,
  searchStudentRoleByUsernameOrClubMongo,
  setMainActivationMongo,
  setMainExpiryMongo,
  setMainPasswordMongo,
  setRoleActivationMongo,
  setRolePasswordMongo,
  setRoleExpiryOnlyMongo,
  updateMainProfileMongo,
  updateRoleProfileMongo,
} from "../userListMongo";

function dataClubRoot(): string {
  return path.join(__dirname, "..", "..", "data_club");
}

const CLUB_ID_NUM_PAD = 7;

function resolveClubTemplateDir(root: string): string {
  // Preferred: repo-root Backup template (requested)
  // When server runs from `src/backend`, repo root is `../..`.
  const fromCwd = path.resolve(
    process.cwd(),
    "..",
    "..",
    "Backup",
    "src",
    "data_club",
    "src",
  );
  if (fs.existsSync(fromCwd)) {
    return fromCwd;
  }
  // Fallback: legacy template under `data_club/Src`
  const legacy = path.join(root, "Src");
  if (fs.existsSync(legacy)) {
    return legacy;
  }
  return fromCwd; // return preferred path for error messaging
}

function resolveBackupDataClubRoot(): string {
  // Repo root from `src/backend`
  return path.resolve(process.cwd(), "..", "..", "Backup", "src", "data_club");
}

async function resolveCountryCodeForClubId(countryName: string): Promise<string> {
  const name = String(countryName ?? "").trim();
  if (!name) {
    return "";
  }
  if (isMongoConfigured()) {
    try {
      const bi = await readBasicInfoFromMongo();
      const rows = bi?.countries ?? [];
      const hit = rows.find(
        (r) => String(r.name ?? "").trim().toLowerCase() === name.toLowerCase(),
      );
      const code = String(hit?.country_code ?? "").trim();
      if (code) {
        return code.toUpperCase();
      }
    } catch {
      /* ignore */
    }
  }
  const fallback = lookupCountryCodeForBasicInfoName(name);
  return fallback ? fallback.toUpperCase() : "";
}

function nextClubIdForCountryCode(root: string, countryCode: string): string {
  const cc = String(countryCode ?? "").trim().toUpperCase();
  if (!/^[A-Z]{2,3}$/.test(cc)) {
    // Fall back to legacy allocator (CM00000001…) when no usable country code.
    return allocateNextClubUid();
  }
  let max = 0;
  try {
    const entries = fs.readdirSync(root, { withFileTypes: true });
    const re = new RegExp(`^${cc}(\\d{${CLUB_ID_NUM_PAD}})$`, "i");
    for (const e of entries) {
      if (!e.isDirectory()) continue;
      const m = re.exec(e.name);
      if (!m) continue;
      const n = parseInt(m[1], 10);
      if (Number.isFinite(n) && n > max) {
        max = n;
      }
    }
  } catch {
    /* ignore */
  }
  const next = max + 1;
  return `${cc}${String(next).padStart(CLUB_ID_NUM_PAD, "0")}`;
}

/** Empty string, or YYYY-MM-DD from admin form field `Expiry_Date`. */
function parseExpiryDateField(v: unknown): { ok: true; value: string } | { ok: false } {
  const t = String(v ?? "").trim();
  if (!t) {
    return { ok: true, value: "" };
  }
  if (!/^\d{4}-\d{2}-\d{2}$/.test(t)) {
    return { ok: false };
  }
  return { ok: true, value: t };
}

/** Resolved path to `data_club/{uid}` only when `uid` is a single safe segment (no traversal). */
function safeClubDataDirForUid(uid: string, root: string): string | null {
  const t = uid.trim();
  if (!t || t === "." || t === "..") {
    return null;
  }
  if (t.toLowerCase() === "src") {
    return null;
  }
  if (/[/\\]/.test(t)) {
    return null;
  }
  const base = path.resolve(root);
  const resolved = path.resolve(path.join(root, t));
  const rel = path.relative(base, resolved);
  if (rel.startsWith("..") || path.isAbsolute(rel) || rel === "") {
    return null;
  }
  return resolved;
}

const upload = multer();

async function assertUsernameFreeForUid(
  username: string,
  exceptUid: string,
): Promise<{ ok: true } | { ok: false; error: string }> {
  if (isMongoConfigured()) {
    return assertUsernameFreeForUidMongo(username, exceptUid);
  }
  const uq = username.trim();
  if (!uq) {
    return { ok: false, error: "Username is required." };
  }
  const lower = uq.toLowerCase();
  const ex = exceptUid.trim().toUpperCase();
  for (const r of loadUsersFromCsv()) {
    if (r.username.trim().toLowerCase() !== lower) {
      continue;
    }
    if (r.uid.trim().toUpperCase() === ex) {
      continue;
    }
    return { ok: false, error: "That username is already in use." };
  }
  for (const r of loadCoachRoleLogins()) {
    if (r.username.trim().toLowerCase() !== lower) {
      continue;
    }
    if (r.uid.trim().toUpperCase() === ex) {
      continue;
    }
    return { ok: false, error: "That username is already in use." };
  }
  for (const r of loadStudentRoleLogins()) {
    if (r.username.trim().toLowerCase() !== lower) {
      continue;
    }
    if (r.uid.trim().toUpperCase() === ex) {
      continue;
    }
    return { ok: false, error: "That username is already in use." };
  }
  return { ok: true };
}

export function createAdminRouter(): Router {
  const r = Router();

  r.get("/login-accounts", requireAuth, requireRole("Admin"), async (req, res) => {
    if (isMongoConfigured()) {
      try {
        const pickInt = (name: string): number | undefined => {
          const v = req.query[name];
          if (v == null || (Array.isArray(v) && v.length === 0)) {
            return undefined;
          }
          const raw = Array.isArray(v) ? v[0] : v;
          const n = parseInt(String(raw).trim(), 10);
          return Number.isFinite(n) ? n : undefined;
        };
        const mongoOpts: {
          userLoginPage?: number;
          userLoginPageSize?: number;
          coachPage?: number;
          coachPageSize?: number;
          studentPage?: number;
          studentPageSize?: number;
        } = {};
        const ulP = pickInt("userLoginPage");
        const ulS =
          pickInt("userLoginPageSize") ?? pickInt("userLoginLimit");
        if (ulP != null && ulS != null) {
          mongoOpts.userLoginPage = ulP;
          mongoOpts.userLoginPageSize = ulS;
        }
        const chP = pickInt("coachPage");
        const chS = pickInt("coachPageSize") ?? pickInt("coachLimit");
        if (chP != null && chS != null) {
          mongoOpts.coachPage = chP;
          mongoOpts.coachPageSize = chS;
        }
        const stP = pickInt("studentPage");
        const stS = pickInt("studentPageSize") ?? pickInt("studentLimit");
        if (stP != null && stS != null) {
          mongoOpts.studentPage = stP;
          mongoOpts.studentPageSize = stS;
        }
        const hasMongoPaging = Object.keys(mongoOpts).length > 0;
        const {
          userLogin,
          coach,
          student,
          pagination,
        } = await listAdminLoginAccountsFromMongo(
          hasMongoPaging ? mongoOpts : undefined,
        );
        res.json({
          ok: true,
          userLogin,
          coach,
          student,
          ...(pagination ? { pagination } : {}),
        });
      } catch (e) {
        const msg = e instanceof Error ? e.message : String(e);
        res.status(503).json({ ok: false, error: msg });
      }
      return;
    }
    const userLogin = loadUsersFromCsv().map((u) => ({
      uid: u.uid,
      usertype: u.usertype,
      role: u.role,
      username: u.username,
      fullName: u.fullName,
      clubName: u.clubName,
      status: u.status,
      isActivated: u.isActivated,
      creationDate: u.creationDate,
      lastUpdateDate: u.lastUpdateDate,
      expiryDate: u.expiryDate ?? "",
    }));
    const coach = loadCoachRoleLogins().map((row) => ({
      uid: row.uid,
      username: row.username,
      fullName: row.fullName,
      clubName: row.clubName,
      status: row.status,
      isActivated: row.isActivated,
      creationDate: row.creationDate,
      lastUpdateDate: row.lastUpdateDate,
      expiryDate: row.expiryDate ?? "",
    }));
    const student = loadStudentRoleLogins().map((row) => ({
      uid: row.uid,
      username: row.username,
      fullName: row.fullName,
      clubName: row.clubName,
      status: row.status,
      isActivated: row.isActivated,
      creationDate: row.creationDate,
      lastUpdateDate: row.lastUpdateDate,
      expiryDate: row.expiryDate ?? "",
    }));
    res.json({ ok: true, userLogin, coach, student });
  });

  r.post(
    "/login-accounts/set-status",
    requireAuth,
    requireRole("Admin"),
    async (req, res) => {
      const store = String(req.body?.store ?? "").trim().toLowerCase();
      const uid = String(req.body?.uid ?? "").trim();
      if (!uid) {
        res.status(400).json({ ok: false, error: "uid is required." });
        return;
      }
      if (
        store !== "userlogin" &&
        store !== "main" &&
        store !== "coach" &&
        store !== "student"
      ) {
        res.status(400).json({
          ok: false,
          error: 'store must be "userLogin", "coach", or "student".',
        });
        return;
      }

      const hasProfile =
        typeof req.body?.username === "string" &&
        typeof req.body?.fullName === "string" &&
        typeof req.body?.clubName === "string";

      let expiryUpdated = false;
      const countryRaw =
        typeof req.body?.country === "string" ? String(req.body.country).trim() : "";

      if (hasProfile) {
        const username = String(req.body.username).trim();
        const fullName = String(req.body.fullName);
        const clubName = String(req.body.clubName);
        const expiryParsed = parseExpiryDateField(
          req.body?.expiryDate ?? req.body?.Expiry_date,
        );
        if (!expiryParsed.ok) {
          res.status(400).json({
            ok: false,
            error:
              "expiryDate / Expiry_date must be empty or a valid date (YYYY-MM-DD).",
          });
          return;
        }
        const nameCheck = await assertUsernameFreeForUid(username, uid);
        if (!nameCheck.ok) {
          res.status(400).json({ ok: false, error: nameCheck.error });
          return;
        }
        let profileResult:
          | { ok: true }
          | { ok: false; error: string };
        if (isMongoConfigured()) {
          if (store === "userlogin" || store === "main") {
            profileResult = await updateMainProfileMongo(uid, {
              username,
              fullName,
              clubName,
              expiryDate: expiryParsed.value,
            });
          } else if (store === "coach") {
            profileResult = await updateRoleProfileMongo(uid, "coach", {
              username,
              fullName,
              clubName,
              expiryDate: expiryParsed.value,
            });
          } else {
            profileResult = await updateRoleProfileMongo(uid, "student", {
              username,
              fullName,
              clubName,
              expiryDate: expiryParsed.value,
            });
          }
        } else if (store === "userlogin" || store === "main") {
          profileResult = updateMainUserlistProfileByUid(uid, {
            username,
            fullName,
            clubName,
            expiryDate: expiryParsed.value,
          });
        } else if (store === "coach") {
          profileResult = updateRoleLoginProfileByUid(uid, "Coach", {
            username,
            fullName,
            clubName,
            expiryDate: expiryParsed.value,
          });
        } else {
          profileResult = updateRoleLoginProfileByUid(uid, "Student", {
            username,
            fullName,
            clubName,
            expiryDate: expiryParsed.value,
          });
        }
        if (!profileResult.ok) {
          res.status(400).json({ ok: false, error: profileResult.error });
          return;
        }
        if (
          isMongoConfigured() &&
          countryRaw &&
          (store === "userlogin" || store === "main")
        ) {
          const cur = clubCurrencyFromCountry(countryRaw);
          try {
            await patchClubInfoFields(
              uid,
              clubName,
              { country: countryRaw, Currency: cur.currencyCode },
              todaySlashYmd(),
            );
          } catch {
            /* ignore clubInfo update errors; login profile already updated */
          }
        }
      } else {
        const expOnly = req.body?.expiryDate ?? req.body?.Expiry_date;
        if (typeof expOnly === "string") {
          const expiryParsed = parseExpiryDateField(expOnly);
          if (!expiryParsed.ok) {
            res.status(400).json({
              ok: false,
              error:
                "expiryDate / Expiry_date must be empty or a valid date (YYYY-MM-DD).",
            });
            return;
          }
          let onlyResult:
            | { ok: true }
            | { ok: false; error: string };
          if (isMongoConfigured()) {
            if (store === "userlogin" || store === "main") {
              onlyResult = await setMainExpiryMongo(uid, expiryParsed.value);
            } else if (store === "coach") {
              onlyResult = await setRoleExpiryOnlyMongo(
                uid,
                "coach",
                expiryParsed.value,
              );
            } else {
              onlyResult = await setRoleExpiryOnlyMongo(
                uid,
                "student",
                expiryParsed.value,
              );
            }
          } else if (store === "userlogin" || store === "main") {
            onlyResult = setMainUserlistExpiryByUid(uid, expiryParsed.value);
          } else if (store === "coach") {
            onlyResult = setRoleLoginExpiryByUid(uid, "Coach", expiryParsed.value);
          } else {
            onlyResult = setRoleLoginExpiryByUid(
              uid,
              "Student",
              expiryParsed.value,
            );
          }
          if (!onlyResult.ok) {
            res.status(400).json({ ok: false, error: onlyResult.error });
            return;
          }
          expiryUpdated = true;
        }
      }

      const pwd = String(req.body?.password ?? "");
      const pwd2 = String(req.body?.passwordConfirm ?? "");
      let passwordUpdated = false;
      if (pwd.length > 0 || pwd2.length > 0) {
        if (pwd !== pwd2) {
          res.status(400).json({
            ok: false,
            error: "Password and re-entered password do not match.",
          });
          return;
        }
        if (pwd.length < 6) {
          res.status(400).json({
            ok: false,
            error: "Password must be at least 6 characters.",
          });
          return;
        }
        let pwdResult:
          | { ok: true }
          | { ok: false; error: string };
        if (isMongoConfigured()) {
          if (store === "userlogin" || store === "main") {
            pwdResult = await setMainPasswordMongo(uid, pwd);
          } else if (store === "coach") {
            pwdResult = await setRolePasswordMongo(uid, "coach", pwd);
          } else {
            pwdResult = await setRolePasswordMongo(uid, "student", pwd);
          }
        } else if (store === "userlogin" || store === "main") {
          pwdResult = setMainLoginPasswordByUid(uid, pwd);
        } else if (store === "coach") {
          pwdResult = setRoleLoginPasswordByUid(uid, "Coach", pwd);
        } else {
          pwdResult = setRoleLoginPasswordByUid(uid, "Student", pwd);
        }
        if (!pwdResult.ok) {
          res.status(400).json({ ok: false, error: pwdResult.error });
          return;
        }
        passwordUpdated = true;
      }

      const activeRaw = req.body?.active;
      const activeProvided =
        activeRaw !== undefined &&
        activeRaw !== null &&
        activeRaw !== "";
      if (!activeProvided) {
        const today = new Date().toISOString().slice(0, 10);
        const parts: string[] = [];
        if (hasProfile) {
          parts.push("profile");
        }
        if (expiryUpdated && !hasProfile) {
          parts.push("Expiry_date");
        }
        if (passwordUpdated) {
          parts.push("password");
        }
        res.json({
          ok: true,
          message:
            parts.length > 0
              ? `Updated ${parts.join(" and ")}.`
              : "No changes applied.",
          lastUpdate_date: today,
        });
        return;
      }

      const active =
        activeRaw === true ||
        activeRaw === "true" ||
        activeRaw === 1 ||
        activeRaw === "1";
      let result:
        | { ok: true }
        | { ok: false; error: string };
      if (isMongoConfigured()) {
        if (store === "userlogin" || store === "main") {
          result = await setMainActivationMongo(uid, active);
        } else if (store === "coach") {
          result = await setRoleActivationMongo(uid, "coach", active);
        } else {
          result = await setRoleActivationMongo(uid, "student", active);
        }
      } else if (store === "userlogin" || store === "main") {
        result = setMainUserlistActivationByUid(uid, active);
      } else if (store === "coach") {
        result = setCoachRoleLoginActiveByUid(uid, active);
      } else {
        result = setStudentRoleLoginActiveByUid(uid, active);
      }
      if (!result.ok) {
        res.status(400).json({ ok: false, error: result.error });
        return;
      }
      const today = new Date().toISOString().slice(0, 10);
      const extra: string[] = [];
      if (hasProfile) {
        extra.push("profile");
      }
      if (expiryUpdated && !hasProfile) {
        extra.push("Expiry_date");
      }
      if (passwordUpdated) {
        extra.push("password");
      }
      const prefix =
        extra.length > 0 ? `Updated ${extra.join(" and ")}; ` : "";
      res.json({
        ok: true,
        message: `${prefix}${active ? "Account marked active." : "Account marked inactive."}`,
        status: active ? "ACTIVE" : "INACTIVE",
        lastUpdate_date: today,
      });
    },
  );

  r.post("/login-accounts/remove", requireAuth, requireRole("Admin"), async (req, res) => {
    const store = String(req.body?.store ?? "").trim().toLowerCase();
    const uid = String(req.body?.uid ?? "").trim();
    if (!uid) {
      res.status(400).json({ ok: false, error: "uid is required." });
      return;
    }
    let result: { ok: true } | { ok: false; error: string };
    let message: string;
    let deleteCoachManagerFolder = false;

    if (isMongoConfigured()) {
      if (store === "coach") {
        result = await deleteRoleLoginMongo(uid, "coach");
        message = "Coach login removed from MongoDB userLogin.";
      } else if (store === "student") {
        result = await deleteRoleLoginMongo(uid, "student");
        message = "Student login removed from MongoDB userLogin.";
      } else if (store === "userlogin" || store === "main") {
        const permanent =
          req.body?.deleteCoachManager === true ||
          req.body?.deleteCoachManager === "true";
        if (permanent) {
          deleteCoachManagerFolder = true;
          const purge = await deleteCoachStudentForClubFolderMongo(uid);
          result = await deleteMainLoginMongo(uid);
          if (!result.ok) {
            res.status(400).json({ ok: false, error: result.error });
            return;
          }
          message =
            `Coach Manager removed from userLogin; removed ${purge.removedCoach} coach and ${purge.removedStudent} student role login(s) for this club ID; club data folder deleted when present.`;
        } else {
          result = await setMainActivationMongo(uid, false);
          message = "Account marked inactive in userLogin.";
        }
      } else {
        res.status(400).json({
          ok: false,
          error: 'store must be "userLogin", "coach", or "student".',
        });
        return;
      }
      if (!result.ok) {
        res.status(400).json({ ok: false, error: result.error });
        return;
      }
      if (deleteCoachManagerFolder) {
        const clubDir = safeClubDataDirForUid(uid, dataClubRoot());
        if (clubDir && fs.existsSync(clubDir)) {
          try {
            fs.rmSync(clubDir, { recursive: true, force: true });
            invalidateDataFileCacheUnderDir(clubDir);
          } catch (e) {
            const detail =
              e instanceof Error ? e.message : "Unknown error deleting folder.";
            res.status(500).json({
              ok: false,
              error: `userLogin row was removed but club folder could not be deleted: ${detail}`,
            });
            return;
          }
        }
      }
      res.json({ ok: true, message });
      return;
    }

    if (store === "coach") {
      result = deleteRoleLoginByUid(uid, "Coach");
      message = "Coach login removed from userLogin_Coach.";
    } else if (store === "student") {
      result = deleteRoleLoginByUid(uid, "Student");
      message = "Student login removed from userLogin_Student.";
    } else if (store === "userlogin" || store === "main") {
      const permanent =
        req.body?.deleteCoachManager === true ||
        req.body?.deleteCoachManager === "true";
      if (permanent) {
        deleteCoachManagerFolder = true;
        const purge = removeRoleLoginRowsForCoachManagerFolderUid(uid);
        if (!purge.ok) {
          res.status(400).json({ ok: false, error: purge.error });
          return;
        }
        result = removeCoachManagerFromUserLoginStore(uid);
        message =
          `Coach Manager removed from UserLogin; removed ${purge.removedCoach} coach and ${purge.removedStudent} student role login(s) for this club ID; club data folder deleted when present.`;
      } else {
        result = setMainUserlistActivationByUid(uid, false);
        message = "Account marked inactive in UserLogin.";
      }
    } else {
      res.status(400).json({
        ok: false,
        error: 'store must be "userLogin", "coach", or "student".',
      });
      return;
    }
    if (!result.ok) {
      res.status(400).json({ ok: false, error: result.error });
      return;
    }
    if (deleteCoachManagerFolder) {
      const clubDir = safeClubDataDirForUid(uid, dataClubRoot());
      if (clubDir && fs.existsSync(clubDir)) {
        try {
          fs.rmSync(clubDir, { recursive: true, force: true });
          invalidateDataFileCacheUnderDir(clubDir);
        } catch (e) {
          const detail =
            e instanceof Error ? e.message : "Unknown error deleting folder.";
          res.status(500).json({
            ok: false,
            error: `UserLogin row was removed but club folder could not be deleted: ${detail}`,
          });
          return;
        }
      }
    }
    res.json({ ok: true, message });
  });

  r.post(
    "/coach-managers",
    requireAuth,
    requireRole("Admin"),
    upload.none(),
    async (req, res) => {
      const username = String(req.body?.username ?? "").trim();
      const password = String(req.body?.password ?? "").trim();
      const clubName = String(req.body?.clubName ?? "").trim();
      const fullName = String(req.body?.full_name ?? "").trim();
      const country = String(req.body?.country ?? "").trim();
      const sportType = String(req.body?.sportType ?? req.body?.Sport_type ?? "").trim();
      const clubTheme = String(req.body?.club_theme ?? "").trim();
      const userRole = String(req.body?.userRole ?? "Coach Manager").trim();
      const expiryParsed = parseExpiryDateField(req.body?.Expiry_Date);
      if (!expiryParsed.ok) {
        res.status(400).json({
          ok: false,
          error: "Expiry_Date must be empty or a valid date (YYYY-MM-DD).",
        });
        return;
      }
      const expiryDate = expiryParsed.value;

      if (!username) {
        res.status(400).json({ ok: false, error: "Username is required." });
        return;
      }
      if (!password) {
        res.status(400).json({ ok: false, error: "Default password is required." });
        return;
      }
      if (!fullName) {
        res.status(400).json({ ok: false, error: "Full name is required." });
        return;
      }

      const validRoles = ["Coach Manager", "Coach", "Student"];
      if (!validRoles.includes(userRole)) {
        res.status(400).json({
          ok: false,
          error: 'User role must be "Coach Manager", "Coach", or "Student".',
        });
        return;
      }

      if (userRole === "Coach" || userRole === "Student") {
        if (!clubName) {
          res.status(400).json({ ok: false, error: "Club name is required." });
          return;
        }
        let cmUidForClub: string | null = null;
        if (isMongoConfigured()) {
          try {
            cmUidForClub =
              (await findCoachManagerUidByClubNameMongo(clubName)) ||
              findCoachManagerClubUidByClubName(clubName);
          } catch {
            cmUidForClub = findCoachManagerClubUidByClubName(clubName);
          }
        } else {
          cmUidForClub = findCoachManagerClubUidByClubName(clubName);
        }
        if (!cmUidForClub) {
          res.status(400).json({
            ok: false,
            error: "No Coach Manager matches that club name.",
          });
          return;
        }
        let cmExpiry = "";
        if (isMongoConfigured()) {
          cmExpiry = await getCoachManagerExpiryDateForClubFolderUidMongo(
            cmUidForClub,
          );
        }
        if (!cmExpiry) {
          cmExpiry = getCoachManagerExpiryDateForClubFolderUid(cmUidForClub);
        }
        const roleLoginExpiry = cmExpiry || expiryDate;

        if (isMongoConfigured()) {
          const taken = await assertUsernameFreeForUidMongo(username, "");
          if (!taken.ok) {
            res.status(409).json({
              ok: false,
              error: "The user already existed !",
            });
            return;
          }
          const uid =
            userRole === "Coach"
              ? await allocateNextCoachLoginUidMongo()
              : await allocateNextStudentLoginUidMongo();
          const uidCollide = await userLoginUidExistsMongo(uid);
          if (uidCollide) {
            res.status(400).json({
              ok: false,
              error: "Could not allocate a new login UID; try again.",
            });
            return;
          }
          const ins =
            userRole === "Coach"
              ? await insertCoachRoleMongo({
                  uid,
                  username,
                  password,
                  fullName,
                  clubName,
                  clubFolderUid: cmUidForClub,
                  expiryDate: roleLoginExpiry,
                })
              : await insertStudentRoleMongo({
                  uid,
                  username,
                  password,
                  fullName,
                  clubName,
                  clubFolderUid: cmUidForClub,
                  expiryDate: roleLoginExpiry,
                });
          if (!ins.ok) {
            const low = ins.error.toLowerCase();
            const status =
              low.includes("duplicate") || low.includes("e11000") ? 409 : 400;
            res.status(status).json({ ok: false, error: ins.error });
            return;
          }
          res.json({
            ok: true,
            loginUid: uid,
            message:
              userRole === "Coach"
                ? `Coach login saved (UID ${uid}).`
                : `Student login saved (UID ${uid}).`,
          });
          return;
        }

        const csvResult =
          userRole === "Coach"
            ? await appendCoachRoleLoginRow({
                username,
                password,
                fullName,
                clubName,
                expiryDate: roleLoginExpiry,
              })
            : await appendStudentRoleLoginRow({
                username,
                password,
                fullName,
                clubName,
                expiryDate: roleLoginExpiry,
              });
        if (!csvResult.ok) {
          const status =
            csvResult.error === "The user already existed !" ? 409 : 400;
          res.status(status).json({ ok: false, error: csvResult.error });
          return;
        }
        res.json({
          ok: true,
          loginUid: csvResult.uid,
          message:
            userRole === "Coach"
              ? `Coach login saved (UID ${csvResult.uid}).`
              : `Student login saved (UID ${csvResult.uid}).`,
        });
        return;
      }

      if (!clubName) {
        res.status(400).json({ ok: false, error: "Club name is required." });
        return;
      }

      if (isMongoConfigured()) {
        const taken = await assertUsernameFreeForUidMongo(username, "");
        if (!taken.ok) {
          res.status(409).json({
            ok: false,
            error: "The user already existed !",
          });
          return;
        }
      } else if (findUserByUsername(username)) {
        res.status(409).json({
          ok: false,
          error: "The user already existed !",
        });
        return;
      }

      const root = dataClubRoot();
      const cc = await resolveCountryCodeForClubId(country);
      const clubId = cc ? nextClubIdForCountryCode(root, cc) : allocateNextClubUid();
      const srcDir = resolveClubTemplateDir(root);
      const clubDir = path.join(root, clubId);
      const imageDir = path.join(clubDir, "image");
      const backupRoot = resolveBackupDataClubRoot();
      const backupClubDir = path.join(backupRoot, clubId);

      if (!fs.existsSync(srcDir)) {
        res.status(500).json({
          ok: false,
          error:
            "Template folder is missing. Expected Backup/src/data_club/src (preferred) or backend/data_club/Src (legacy).",
        });
        return;
      }

      if (fs.existsSync(clubDir)) {
        res.status(409).json({
          ok: false,
          error: `Club folder ${clubId} already exists. Refusing to overwrite.`,
        });
        return;
      }

      if (fs.existsSync(backupClubDir)) {
        res.status(409).json({
          ok: false,
          error: `Backup club folder ${clubId} already exists under Backup/src/data_club. Refusing to overwrite.`,
        });
        return;
      }

      const clubPhotoRel = "";
      try {
        fs.mkdirSync(clubDir, { recursive: true });
        fs.cpSync(srcDir, clubDir, { recursive: true });
        fs.mkdirSync(imageDir, { recursive: true });

        // Keep a mirrored copy under Backup/src/data_club/<clubId> for templates/backups.
        fs.mkdirSync(backupRoot, { recursive: true });
        fs.mkdirSync(backupClubDir, { recursive: true });
        fs.cpSync(srcDir, backupClubDir, { recursive: true });
      } catch (e) {
        try {
          fs.rmSync(clubDir, { recursive: true, force: true });
          invalidateDataFileCacheUnderDir(clubDir);
        } catch {
          /* ignore */
        }
        try {
          fs.rmSync(backupClubDir, { recursive: true, force: true });
        } catch {
          /* ignore */
        }
        const msg = e instanceof Error ? e.message : String(e);
        res.status(500).json({ ok: false, error: `Could not create club data: ${msg}` });
        return;
      }

      if (isMongoConfigured()) {
        const mongoResult = await insertCoachManagerMongo({
          uid: clubId,
          username,
          password,
          fullName,
          clubName,
          clubPhoto: clubPhotoRel,
          expiryDate,
        });
        if (!mongoResult.ok) {
          try {
            fs.rmSync(clubDir, { recursive: true, force: true });
            invalidateDataFileCacheUnderDir(clubDir);
          } catch {
            /* ignore */
          }
          res.status(400).json({ ok: false, error: mongoResult.error });
          return;
        }
        try {
          const patch: Record<string, string> = {
            Sport_type: sportType,
            club_theme: clubTheme || "Black Gold",
          };
          if (country) {
            const cur = clubCurrencyFromCountry(country);
            patch.country = country;
            patch.Currency = cur.currencyCode;
          }
          await patchClubInfoFields(clubId, clubName, patch, todaySlashYmd());
        } catch {
          /* ignore clubInfo patch errors; account created */
        }
        res.json({
          ok: true,
          clubId,
          message: `Coach Manager created with club ${clubId}.`,
        });
        return;
      }

      const csvResult = appendCoachManagerRow({
        uid: clubId,
        username,
        password,
        fullName,
        clubName,
        clubPhoto: clubPhotoRel,
        expiryDate,
      });

      if (!csvResult.ok) {
        try {
          fs.rmSync(clubDir, { recursive: true, force: true });
          invalidateDataFileCacheUnderDir(clubDir);
        } catch {
          /* ignore */
        }
        res.status(400).json({ ok: false, error: csvResult.error });
        return;
      }

      res.json({
        ok: true,
        clubId,
        message: `Coach Manager created with club ${clubId}.`,
      });
    }
  );

  r.post("/coach-managers/search", requireAuth, requireRole("Admin"), async (req, res) => {
    const username = String(req.body?.username ?? "").trim();
    const clubName = String(req.body?.clubName ?? "").trim();
    const userRole = String(req.body?.userRole ?? "Coach Manager").trim();
    if (!username && !clubName) {
      res.status(400).json({
        ok: false,
        error: "Enter username and/or club name.",
      });
      return;
    }
    if (userRole === "Coach") {
      const rows = isMongoConfigured()
        ? await searchCoachRoleByUsernameOrClubMongo(username, clubName)
        : searchCoachRoleByUsernameOrClub(username, clubName);
      const results = rows.map((row) => ({
        uid: row.uid,
        username: row.username,
        fullName: row.fullName || "—",
        clubName: row.clubName,
        status: row.status || (row.isActivated ? "ACTIVE" : "INACTIVE"),
        lastUpdate_date: row.lastUpdateDate || "—",
        creation_date: row.creationDate || "—",
      }));
      res.json({ ok: true, results });
      return;
    }
    if (userRole === "Student") {
      const rows = isMongoConfigured()
        ? await searchStudentRoleByUsernameOrClubMongo(username, clubName)
        : searchStudentRoleByUsernameOrClub(username, clubName);
      const results = rows.map((row) => ({
        uid: row.uid,
        username: row.username,
        fullName: row.fullName || "—",
        clubName: row.clubName,
        status: row.status || (row.isActivated ? "ACTIVE" : "INACTIVE"),
        lastUpdate_date: row.lastUpdateDate || "—",
        creation_date: row.creationDate || "—",
      }));
      res.json({ ok: true, results });
      return;
    }
    if (userRole !== "Coach Manager") {
      res.status(400).json({
        ok: false,
        error: 'User role must be "Coach Manager", "Coach", or "Student".',
      });
      return;
    }
    const rows = isMongoConfigured()
      ? await searchCoachManagersMongo(username, clubName)
      : searchCoachManagers(username, clubName);
    const results = rows.map((row) => {
      return {
        uid: row.uid,
        username: row.username,
        fullName: row.fullName || "—",
        clubName: row.clubName,
        clubPhoto: row.clubPhoto,
        clubPhotoUrl: clubPhotoPublicUrl(row.uid, row.clubPhoto),
        status: row.status || (row.isActivated ? "ACTIVE" : "INACTIVE"),
        lastUpdate_date: row.lastUpdateDate || "—",
        creation_date: row.creationDate || "—",
      };
    });
    res.json({ ok: true, results });
  });

  r.post("/coach-managers/activate", requireAuth, requireRole("Admin"), async (req, res) => {
    const username = String(req.body?.username ?? "").trim();
    const clubName = String(req.body?.clubName ?? "").trim();
    const userRole = String(req.body?.userRole ?? "Coach Manager").trim();
    if (!username && !clubName) {
      res.status(400).json({
        ok: false,
        error: "Enter username and/or club name.",
      });
      return;
    }
    const today = new Date().toISOString().slice(0, 10);
    if (userRole === "Coach") {
      const result = isMongoConfigured()
        ? await activateCoachRoleLoginMongo(username, clubName)
        : activateCoachRoleLogin(username, clubName);
      if (!result.ok) {
        res.status(400).json({ ok: false, error: result.error });
        return;
      }
      res.json({
        ok: true,
        message: "Account marked active.",
        status: "ACTIVE",
        lastUpdate_date: today,
      });
      return;
    }
    if (userRole === "Student") {
      const result = isMongoConfigured()
        ? await activateStudentRoleLoginMongo(username, clubName)
        : activateStudentRoleLogin(username, clubName);
      if (!result.ok) {
        res.status(400).json({ ok: false, error: result.error });
        return;
      }
      res.json({
        ok: true,
        message: "Account marked active.",
        status: "ACTIVE",
        lastUpdate_date: today,
      });
      return;
    }
    if (userRole !== "Coach Manager") {
      res.status(400).json({
        ok: false,
        error: 'User role must be "Coach Manager", "Coach", or "Student".',
      });
      return;
    }
    const result = isMongoConfigured()
      ? await activateCoachManagerMongo(username, clubName)
      : activateCoachManager(username, clubName);
    if (!result.ok) {
      res.status(400).json({ ok: false, error: result.error });
      return;
    }
    res.json({
      ok: true,
      message: "Account marked active.",
      status: "ACTIVE",
      lastUpdate_date: today,
    });
  });

  r.post("/coach-managers/remove", requireAuth, requireRole("Admin"), async (req, res) => {
    const username = String(req.body?.username ?? "").trim();
    const clubName = String(req.body?.clubName ?? "").trim();
    const userRole = String(req.body?.userRole ?? "Coach Manager").trim();
    if (!username && !clubName) {
      res.status(400).json({
        ok: false,
        error: "Enter username and/or club name.",
      });
      return;
    }
    const today = new Date().toISOString().slice(0, 10);
    if (userRole === "Coach") {
      const result = isMongoConfigured()
        ? await deactivateCoachRoleLoginMongo(username, clubName)
        : deactivateCoachRoleLogin(username, clubName);
      if (!result.ok) {
        res.status(400).json({ ok: false, error: result.error });
        return;
      }
      res.json({
        ok: true,
        message: "Account marked inactive.",
        status: "INACTIVE",
        lastUpdate_date: today,
      });
      return;
    }
    if (userRole === "Student") {
      const result = isMongoConfigured()
        ? await deactivateStudentRoleLoginMongo(username, clubName)
        : deactivateStudentRoleLogin(username, clubName);
      if (!result.ok) {
        res.status(400).json({ ok: false, error: result.error });
        return;
      }
      res.json({
        ok: true,
        message: "Account marked inactive.",
        status: "INACTIVE",
        lastUpdate_date: today,
      });
      return;
    }
    if (userRole !== "Coach Manager") {
      res.status(400).json({
        ok: false,
        error: 'User role must be "Coach Manager", "Coach", or "Student".',
      });
      return;
    }
    const result = isMongoConfigured()
      ? await deactivateCoachManagerMongo(username, clubName)
      : deactivateCoachManager(username, clubName);
    if (!result.ok) {
      res.status(400).json({ ok: false, error: result.error });
      return;
    }
    res.json({
      ok: true,
      message: "Account marked inactive.",
      status: "INACTIVE",
      lastUpdate_date: today,
    });
  });

  return r;
}
