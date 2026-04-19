import { Router, type Response } from "express";
import jwt from "jsonwebtoken";
import {
  findCoachRoleLoginByUsername,
  findStudentRoleLoginByUsername,
  verifyRoleLoginPassword,
} from "../coachStudentLoginCsv";
import {
  findClubUidForCoachId,
  isValidClubFolderId,
  loadCoaches,
} from "../coachListCsv";
import { findClubUidForStudentId } from "../studentListCsv";
import {
  distinctClubNamesFromUserlist,
  findCoachManagerClubUidByClubName,
  findUserByUsername,
  verifyMainLoginPassword,
} from "../userlistCsv";
import { isMongoConfigured } from "../db/DBConnection";
import {
  findCoachManagerUidByClubNameMongo,
  findCoachRoleLoginByUsernameMongo,
  findStudentRoleLoginByUsernameMongo,
  findUserByUsernameMongo,
} from "../userListMongo";
import { jwtSecret } from "../middleware/requireAuth";
import { isLoginExpiryDatePast } from "../accountExpiry";

const ROLE_FORM_TO_JWT: Record<string, string> = {
  "Coach Manager": "CoachManager",
  Coach: "Coach",
  Student: "Student",
  Admin: "Admin",
};

function normEqText(a: string, b: string): boolean {
  return a.trim().toLowerCase() === b.trim().toLowerCase();
}

/** Club folder UID from file, else Mongo `userLogin` when configured. */
async function resolveCoachManagerClubUid(clubName: string): Promise<string | null> {
  const trimmed = clubName.trim();
  if (!trimmed) {
    return null;
  }
  const fromFile = findCoachManagerClubUidByClubName(trimmed);
  if (fromFile) {
    return fromFile;
  }
  if (!isMongoConfigured()) {
    return null;
  }
  try {
    return await findCoachManagerUidByClubNameMongo(trimmed);
  } catch {
    return null;
  }
}

function sendExpiredLogin(
  res: Response,
  u: {
    username: string;
    role: string;
    usertype: string;
    uid: string;
    expiry_date: string;
  },
): void {
  res.json({
    ok: true,
    accountExpired: true,
    user: {
      username: u.username,
      role: u.role,
      usertype: u.usertype,
      uid: u.uid,
      expiry_date: u.expiry_date,
    },
  });
}

export function createAuthRouter(): Router {
  const r = Router();

  /** Public list of club names from userLogin.csv (for sign-in page dropdown). */
  r.get("/club-names", (_req, res) => {
    try {
      res.json({ ok: true, names: distinctClubNamesFromUserlist() });
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      res.status(500).json({ ok: false, error: msg });
    }
  });

  /**
   * Login with explicit role + optional club name (sign-in page).
   * When `MONGODB_URI` / `MONGO_PASSWORD` is set, credentials are checked against MongoDB
   * `userLogin` via `getAuthUserLoginCollection()` (default DB `ClubMaster_DB`; env `MONGO_AUTH_USERLOGIN_DB`).
   * Otherwise: `userLogin.json` / CSV + club roster files.
   */
  r.post("/login-with-context", async (req, res) => {
    const username = String(req.body?.username ?? "").trim();
    const password = String(req.body?.password ?? "");
    const roleInput = String(req.body?.role ?? "").trim();
    const clubName = String(req.body?.clubName ?? "").trim();

    if (!username || !password) {
      res.status(400).json({ ok: false, error: "Username and password required." });
      return;
    }

    const expectedRole = ROLE_FORM_TO_JWT[roleInput];
    if (!expectedRole) {
      res.status(400).json({
        ok: false,
        error: 'Invalid role. Use "Coach Manager", "Coach", "Student", or "Admin".',
      });
      return;
    }

    const signAndSend = (
      payload: {
        sub: string;
        username: string;
        role: string;
        usertype: string;
      },
      clubCtx?: { club_folder_uid: string; club_name?: string },
    ) => {
      const token = jwt.sign(payload, jwtSecret(), { expiresIn: "12h" });
      const user: Record<string, string> = {
        uid: payload.sub,
        username: payload.username,
        role: payload.role,
        usertype: payload.usertype,
      };
      if (clubCtx?.club_folder_uid) {
        user.club_folder_uid = clubCtx.club_folder_uid;
      }
      const cn = clubCtx?.club_name?.trim();
      if (cn && cn !== "—") {
        user.club_name = cn;
      }
      res.json({
        ok: true,
        token,
        user,
      });
    };

    if (expectedRole === "Admin" || expectedRole === "CoachManager") {
      let row: ReturnType<typeof findUserByUsername>;
      try {
        if (isMongoConfigured()) {
          const mongoRow = await findUserByUsernameMongo(username);
          row = mongoRow ?? undefined;
        } else {
          row = findUserByUsername(username);
        }
      } catch (e) {
        const msg = e instanceof Error ? e.message : String(e);
        res.status(503).json({
          ok: false,
          error: `Login database unavailable: ${msg}`,
        });
        return;
      }
      if (!row || !verifyMainLoginPassword(row, password)) {
        res.status(401).json({ ok: false, error: "Invalid credentials." });
        return;
      }
      if (row.role !== expectedRole) {
        res.status(403).json({
          ok: false,
          error: "That user is not the selected role for this account.",
        });
        return;
      }
      if (!row.isActivated || row.status.toUpperCase() !== "ACTIVE") {
        res.status(403).json({
          ok: false,
          error: "Account is not activated or is INACTIVE.",
        });
        return;
      }
      if (expectedRole === "CoachManager" && clubName) {
        if (!normEqText(row.clubName, clubName)) {
          res.status(403).json({
            ok: false,
            error: "Club name does not match this Coach Manager account.",
          });
          return;
        }
      }
      if (isLoginExpiryDatePast(row.expiryDate)) {
        sendExpiredLogin(res, {
          username: row.username,
          role: row.role,
          usertype: row.usertype,
          uid: row.uid,
          expiry_date: String(row.expiryDate ?? "").trim() || "—",
        });
        return;
      }
      signAndSend({
        sub: row.uid,
        username: row.username,
        role: row.role,
        usertype: row.usertype,
      });
      return;
    }

    if (expectedRole === "Coach") {
      let login: ReturnType<typeof findCoachRoleLoginByUsername> | undefined;
      try {
        if (isMongoConfigured()) {
          login = (await findCoachRoleLoginByUsernameMongo(username)) ?? undefined;
        } else {
          login = findCoachRoleLoginByUsername(username);
        }
      } catch (e) {
        const msg = e instanceof Error ? e.message : String(e);
        res.status(503).json({
          ok: false,
          error: `Login database unavailable: ${msg}`,
        });
        return;
      }
      if (!login || !verifyRoleLoginPassword(login, password)) {
        res.status(401).json({
          ok: false,
          error: "Invalid coach username or password.",
        });
        return;
      }
      const loginFolderCoach = (login.clubFolderUid ?? "").trim();
      let clubUid: string | null = null;
      if (loginFolderCoach && isValidClubFolderId(loginFolderCoach)) {
        clubUid = loginFolderCoach;
      } else {
        const reqClub = clubName.trim();
        if (reqClub) {
          clubUid = await resolveCoachManagerClubUid(reqClub);
        } else {
          const fromRow = login.clubName.trim();
          if (fromRow) {
            clubUid = await resolveCoachManagerClubUid(fromRow);
          }
          if (!clubUid) {
            clubUid = findClubUidForCoachId(login.uid);
          }
        }
      }
      if (!clubUid) {
        res.status(404).json({
          ok: false,
          error:
            "No Coach Manager / club folder found for that club name (user login / data_club).",
        });
        return;
      }
      const coaches = loadCoaches(clubUid);
      const coach = coaches.find(
        (c) => c.coachId.trim().toUpperCase() === login.uid.trim().toUpperCase(),
      );
      if (!coach) {
        res.status(403).json({
          ok: false,
          error:
            "Coach login exists in userLogin_Coach but there is no matching CoachID row in UserList_Coach.json for this club.",
        });
        return;
      }
      if (coach.status.toUpperCase() !== "ACTIVE") {
        res.status(403).json({
          ok: false,
          error: "Coach status is not ACTIVE in UserList_Coach.json.",
        });
        return;
      }
      if (!login.isActivated || login.status.toUpperCase() !== "ACTIVE") {
        res.status(403).json({
          ok: false,
          error: "Coach account is inactive in userLogin_Coach.",
        });
        return;
      }
      if (isLoginExpiryDatePast(login.expiryDate)) {
        sendExpiredLogin(res, {
          username: login.username,
          role: "Coach",
          usertype: "Coach",
          uid: coach.coachId,
          expiry_date: String(login.expiryDate ?? "").trim() || "—",
        });
        return;
      }
      signAndSend(
        {
          sub: coach.coachId,
          username: login.username,
          role: "Coach",
          usertype: "Coach",
        },
        { club_folder_uid: clubUid, club_name: login.clubName },
      );
      return;
    }

    if (expectedRole === "Student") {
      let login: ReturnType<typeof findStudentRoleLoginByUsername> | undefined;
      try {
        if (isMongoConfigured()) {
          login = (await findStudentRoleLoginByUsernameMongo(username)) ?? undefined;
        } else {
          login = findStudentRoleLoginByUsername(username);
        }
      } catch (e) {
        const msg = e instanceof Error ? e.message : String(e);
        res.status(503).json({
          ok: false,
          error: `Login database unavailable: ${msg}`,
        });
        return;
      }
      if (!login || !verifyRoleLoginPassword(login, password)) {
        res.status(401).json({
          ok: false,
          error: "Invalid student username or password (userLogin_Student).",
        });
        return;
      }
      /** JWT subject: `StudentID` from login row when set, else `uid`. */
      const rosterStudentId = String(login.studentId ?? login.uid).trim();
      if (!rosterStudentId) {
        res.status(403).json({
          ok: false,
          error: "Student login is missing roster StudentID / uid.",
        });
        return;
      }
      const loginFolderStudent = (login.clubFolderUid ?? "").trim();
      let clubUid: string | null = null;
      if (loginFolderStudent && isValidClubFolderId(loginFolderStudent)) {
        clubUid = loginFolderStudent;
      } else {
        const reqClub = clubName.trim();
        if (reqClub) {
          clubUid = await resolveCoachManagerClubUid(reqClub);
        } else {
          const fromRow = login.clubName.trim();
          if (fromRow) {
            clubUid = await resolveCoachManagerClubUid(fromRow);
          }
          if (!clubUid) {
            clubUid = findClubUidForStudentId(rosterStudentId);
          }
        }
      }
      if (!clubUid) {
        res.status(404).json({
          ok: false,
          error:
            "No Coach Manager / club folder found for that club name (user login / data_club).",
        });
        return;
      }
      if (!login.isActivated || login.status.toUpperCase() !== "ACTIVE") {
        res.status(403).json({
          ok: false,
          error: "Student account is inactive in userLogin_Student.",
        });
        return;
      }
      if (isLoginExpiryDatePast(login.expiryDate)) {
        sendExpiredLogin(res, {
          username: login.username,
          role: "Student",
          usertype: "Student",
          uid: rosterStudentId,
          expiry_date: String(login.expiryDate ?? "").trim() || "—",
        });
        return;
      }
      signAndSend(
        {
          sub: rosterStudentId,
          username: login.username,
          role: "Student",
          usertype: "Student",
        },
        { club_folder_uid: clubUid, club_name: login.clubName },
      );
      return;
    }

    res.status(400).json({ ok: false, error: "Unsupported role." });
  });

  r.post("/login", async (req, res) => {
    const username = String(
      req.body?.username ?? req.body?.userID ?? req.body?.email ?? ""
    ).trim();
    const password = String(req.body?.password || "");
    if (!username || !password) {
      res.status(400).json({ ok: false, error: "Username and password required" });
      return;
    }
    let row: ReturnType<typeof findUserByUsername>;
    try {
      if (isMongoConfigured()) {
        const mongoRow = await findUserByUsernameMongo(username);
        row = mongoRow ?? undefined;
      } else {
        row = findUserByUsername(username);
      }
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      res.status(503).json({
        ok: false,
        error: `Login database unavailable: ${msg}`,
      });
      return;
    }
    if (!row) {
      res.status(401).json({ ok: false, error: "Invalid credentials" });
      return;
    }
    if (!row.isActivated) {
      res.status(403).json({ ok: false, error: "Account is not activated" });
      return;
    }
    if (!verifyMainLoginPassword(row, password)) {
      res.status(401).json({ ok: false, error: "Invalid credentials" });
      return;
    }
    if (isLoginExpiryDatePast(row.expiryDate)) {
      sendExpiredLogin(res, {
        username: row.username,
        role: row.role,
        usertype: row.role === "Coach" ? "Coach" : row.usertype,
        uid: row.uid,
        expiry_date: String(row.expiryDate ?? "").trim() || "—",
      });
      return;
    }
    const usertypeOut =
      row.role === "Coach" ? "Coach" : row.usertype;
    const token = jwt.sign(
      {
        sub: row.uid,
        username: row.username,
        role: row.role,
        usertype: usertypeOut,
      },
      jwtSecret(),
      { expiresIn: "12h" }
    );
    res.json({
      ok: true,
      token,
      user: {
        uid: row.uid,
        username: row.username,
        role: row.role,
        usertype: usertypeOut,
      },
    });
  });

  r.post("/logout", (_req, res) => {
    res.clearCookie("token");
    res.json({ ok: true });
  });

  return r;
}
