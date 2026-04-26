import fs from "fs";
import path from "path";
import { Router } from "express";
import { requireAuth } from "../middleware/requireAuth";
import {
  clubDataDir,
  isValidClubFolderId,
  updateCoachRow,
} from "../coachListCsv";
import {
  findCoachRoleLoginByUid,
  findStudentRoleLoginByUid,
  updateCoachLoginPasswordInCsv,
  updateStudentLoginPasswordInCsv,
} from "../coachStudentLoginCsv";
import type { CsvUser } from "../userlistCsv";
import {
  findCoachManagerClubUidByClubName,
  findUserByUid,
  updateUserPasswordInCsv,
} from "../userlistCsv";
import {
  changeAuthenticatedUserLoginPasswordMongo,
  isMongoConfigured,
  updateCoachManagerContactMongo,
  userLoginCsvReadFallbackEnabled,
} from "../userListMongo";
import {
  findClubUidForCoachIdPreferred,
  updateCoachRowMongo,
} from "../coachListMongo";
import { patchStudentSelfContact } from "../studentListCsv";
import { patchStudentSelfProfile } from "../studentListCsv";

function looksLikeValidEmail(s: string): boolean {
  const t = String(s ?? "").trim();
  if (!t) {
    return true;
  }
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(t);
}

/**
 * Turn CSV values that may be absolute file paths or file:// URLs into a path
 * relative to the club folder (e.g. image/PuiLap.jpg).
 */
export function normalizeClubPhotoRel(uid: string, raw: string): string {
  const id = String(uid || "").trim();
  if (!raw || !String(raw).trim()) {
    return "";
  }
  let s = String(raw).replace(/\\/g, "/").trim();
  if (/^file:/i.test(s)) {
    s = s.replace(/^file:/i, "").replace(/^\/+/, "");
    if (s.startsWith("/") && /^\/[a-zA-Z]:/.test(s)) {
      s = s.slice(1);
    }
  }
  if (/^[a-zA-Z]:\//.test(s)) {
    s = s.replace(/^[a-zA-Z]:\//, "/");
  }
  const lower = s.toLowerCase();
  const needle = "data_club/";
  const pos = lower.indexOf(needle);
  if (pos >= 0) {
    let rest = s.slice(pos + needle.length).replace(/^\/+/, "");
    const segments = rest.split("/").filter(Boolean);
    if (segments.length >= 2) {
      return segments.slice(1).join("/");
    }
    if (segments.length === 1) {
      const one = segments[0]!;
      return /\.[a-z0-9]+$/i.test(one) ? one : "";
    }
    return "";
  }
  s = s.replace(/^\/+/, "");
  if (id && s.toLowerCase().startsWith(id.toLowerCase() + "/")) {
    s = s.slice(id.length + 1);
  }
  return s.replace(/^\/+/, "");
}

/** Site-root URL path for static file serving (not file:// or drive paths). */
export function clubPhotoPublicUrl(uid: string, clubPhotoRel: string): string | null {
  const rel = normalizeClubPhotoRel(uid, clubPhotoRel);
  if (!rel) {
    return null;
  }
  return `/backend/data_club/${encodeURIComponent(uid)}/${rel
    .split("/")
    .map((seg) => encodeURIComponent(seg))
    .join("/")}`;
}

const DEFAULT_CLUB_LOGO_RELS = [
  "Image/club_logo.jpg",
  "image/club_logo.jpg",
  "Image/club_logo.jpeg",
  "image/club_logo.jpeg",
  "Image/club_logo.png",
  "image/club_logo.png",
  "Image/club_logo.JPG",
  "image/club_logo.JPG",
  "Image/logo.jpg",
  "Image/logo.jpeg",
  "Image/logo.png",
  "image/logo.jpg",
  "image/logo.jpeg",
  "image/logo.png",
  "Image/logo.JPG",
  "image/logo.JPG",
  "Image/Logo.jpg",
  "image/Logo.jpg",
];

function isSafeRelSegments(segments: string[]): boolean {
  return segments.every((s) => s !== ".." && s !== "." && !path.isAbsolute(s));
}

/**
 * Pick a club photo path that exists on disk: CSV value first, then default logos
 * (Image/club_logo.jpg, Image/logo.jpg, … under backend/data_club/{uid}/).
 */
export function resolveClubPhotoRelForServing(
  uid: string,
  rawCsv: string
): string {
  const id = String(uid || "").trim();
  const base = clubDataDir(id);
  if (!base || !fs.existsSync(base)) {
    return "";
  }
  const primaryRel = normalizeClubPhotoRel(id, rawCsv);
  if (primaryRel) {
    const segs = primaryRel.split("/").filter(Boolean);
    if (isSafeRelSegments(segs)) {
      const full = path.join(base, ...segs);
      if (fs.existsSync(full)) {
        return primaryRel;
      }
    }
  }
  for (const c of DEFAULT_CLUB_LOGO_RELS) {
    const segs = c.split("/").filter(Boolean);
    if (!isSafeRelSegments(segs)) {
      continue;
    }
    if (fs.existsSync(path.join(base, ...segs))) {
      return c;
    }
  }
  return "";
}

export function createUserAccountRouter(): Router {
  const r = Router();

  /**
   * Student-only: persist email + contact to club roster (`UserList_Student` in Mongo
   * `ClubMaster_DB`, or `UserList_Student.json` on disk when Mongo is off).
   */
  r.patch("/student-contact", requireAuth, async (req, res) => {
    if (req.user?.role !== "Student") {
      res.status(403).json({ ok: false, error: "Only students may update this profile." });
      return;
    }
    const uid = req.user?.sub;
    if (uid == null || String(uid).trim() === "") {
      res.status(401).json({ ok: false, error: "Invalid session." });
      return;
    }
    const email = String(req.body?.email_address ?? req.body?.email ?? "").trim();
    const phone = String(
      req.body?.contact_number ?? req.body?.phone ?? req.body?.contactNumber ?? "",
    ).trim();
    const school = String(req.body?.school ?? "").trim();
    const home_address = String(
      req.body?.home_address ?? req.body?.homeAddress ?? "",
    ).trim();
    if (!looksLikeValidEmail(email)) {
      res.status(400).json({ ok: false, error: "Invalid email address." });
      return;
    }
    try {
      const out =
        school || home_address
          ? await patchStudentSelfProfile(String(uid).trim(), {
              email_address: email,
              contact_number: phone,
              school,
              home_address,
            })
          : await patchStudentSelfContact(String(uid).trim(), email, phone);
      if (!out.ok) {
        res.status(400).json({ ok: false, error: out.error });
        return;
      }
      res.json({
        ok: true,
        message: "Profile updated.",
        email_address: email || "—",
        contact_number: phone || "—",
        school: school || "—",
        home_address: home_address || "—",
      });
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      res.status(503).json({ ok: false, error: msg });
    }
  });

  /**
   * Coach-only: persist email + contact to club roster (`UserList_Coach` in Mongo
   * `ClubMaster_DB`, or `UserList_Coach.json` on disk when Mongo is off).
   */
  r.patch("/coach-contact", requireAuth, async (req, res) => {
    if (req.user?.role !== "Coach") {
      res.status(403).json({
        ok: false,
        error: "Only coaches may update roster contact here.",
      });
      return;
    }
    const uid = String(req.user?.sub ?? "").trim();
    if (!uid) {
      res.status(401).json({ ok: false, error: "Invalid session." });
      return;
    }
    const email_address = String(req.body?.email_address ?? req.body?.email ?? "").trim();
    const contact_number = String(
      req.body?.contact_number ?? req.body?.phone ?? req.body?.contactNumber ?? "",
    ).trim();
    const home_address = String(
      req.body?.home_address ?? req.body?.homeAddress ?? req.body?.address ?? "",
    ).trim();
    if (!looksLikeValidEmail(email_address)) {
      res.status(400).json({ ok: false, error: "Invalid email address." });
      return;
    }
    const jwtFolder = String(req.user?.club_folder_uid ?? "").trim();
    const username = String(req.user?.username ?? "").trim();

    type CoachLoginSlice = NonNullable<
      ReturnType<typeof coachProfileFromUserLoginCoachCsv>
    >;
    let coachLogin: CoachLoginSlice | null = null;
    if (isMongoConfigured()) {
      try {
        const { loadMeProfileFromUserLoginMongo } = await import(
          "../userLoginCollectionMongo"
        );
        const mongoMe = await loadMeProfileFromUserLoginMongo(uid, "Coach", {
          username,
          clubFolderUid: jwtFolder,
        });
        if (mongoMe?.coachLogin) {
          coachLogin = mongoMe.coachLogin;
        }
      } catch {
        /* ignore */
      }
    }
    const allowFile = !isMongoConfigured() || userLoginCsvReadFallbackEnabled();
    if (!coachLogin && allowFile) {
      coachLogin = coachProfileFromUserLoginCoachCsv(uid);
    }
    const clubNameTrimmed =
      coachLogin?.club_name &&
      String(coachLogin.club_name).trim() !== "" &&
      String(coachLogin.club_name).trim() !== "—"
        ? String(coachLogin.club_name).trim()
        : "";
    let club_folder_uid: string | null = null;
    const fromLoginCoach = (coachLogin?.club_folder_uid ?? "").trim();
    if (fromLoginCoach && isValidClubFolderId(fromLoginCoach)) {
      club_folder_uid = fromLoginCoach;
    } else if (jwtFolder && isValidClubFolderId(jwtFolder)) {
      club_folder_uid = jwtFolder;
    } else if (clubNameTrimmed) {
      const u = findCoachManagerClubUidByClubName(clubNameTrimmed);
      club_folder_uid = u && isValidClubFolderId(u) ? u : null;
    }
    if (!club_folder_uid) {
      const fromRoster = await findClubUidForCoachIdPreferred(uid);
      if (fromRoster && isValidClubFolderId(fromRoster)) {
        club_folder_uid = fromRoster;
      }
    }
    if (!club_folder_uid) {
      res.status(400).json({
        ok: false,
        error:
          "Could not resolve your club folder. Ask the club to link your login to a club folder.",
      });
      return;
    }

    const { findCoachRosterRow } = await import("../coachSelfFilter");
    const crow = await findCoachRosterRow(club_folder_uid, uid);
    if (!crow) {
      res.status(404).json({
        ok: false,
        error: "No coach roster row found for this account in UserList_Coach.",
      });
      return;
    }
    const clubName =
      String(crow.clubName ?? "").trim() || clubNameTrimmed || "—";
    const payload = {
      coachName: crow.coachName,
      email: email_address,
      phone: contact_number,
      sex: crow.sex,
      dateOfBirth: crow.dateOfBirth,
      joinedDate: crow.joinedDate,
      homeAddress: home_address,
      country: crow.country,
      remark: crow.remark,
      hourlyRate: crow.hourlyRate,
      status: crow.status,
    };
    try {
      const result = isMongoConfigured()
        ? await updateCoachRowMongo(club_folder_uid, clubName, crow.coachId, payload)
        : updateCoachRow(club_folder_uid, clubName, crow.coachId, payload);
      if (!result.ok) {
        res.status(400).json({ ok: false, error: result.error });
        return;
      }
      res.json({
        ok: true,
        message: "Profile updated.",
        coach_id: crow.coachId,
        email_address: email_address || "—",
        contact_number: contact_number || "—",
        home_address: home_address || "—",
      });
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      res.status(503).json({ ok: false, error: msg });
    }
  });

  r.post("/password", requireAuth, async (req, res) => {
    const uid = req.user?.sub;
    if (uid == null || String(uid).trim() === "") {
      res.status(401).json({ ok: false, error: "Invalid session." });
      return;
    }
    const oldPassword = String(req.body?.oldPassword ?? "");
    const newPassword = String(req.body?.newPassword ?? "");
    if (!oldPassword) {
      res.status(400).json({ ok: false, error: "Old password is required." });
      return;
    }
    if (!newPassword) {
      res.status(400).json({ ok: false, error: "New password is required." });
      return;
    }
    const role = req.user?.role ?? "";

    if (isMongoConfigured()) {
      try {
        const mongo = await changeAuthenticatedUserLoginPasswordMongo(
          String(uid),
          role,
          oldPassword,
          newPassword,
          String(req.user?.username ?? "").trim(),
        );
        if (mongo.ok) {
          res.json({ ok: true, message: "Password updated." });
          return;
        }
        res.status(400).json({ ok: false, error: mongo.error });
        return;
      } catch (e) {
        const msg = e instanceof Error ? e.message : String(e);
        res.status(503).json({
          ok: false,
          error: `Login database unavailable: ${msg}`,
        });
        return;
      }
    }

    const result =
      role === "Coach"
        ? updateCoachLoginPasswordInCsv(uid, oldPassword, newPassword)
        : role === "Student"
          ? updateStudentLoginPasswordInCsv(uid, oldPassword, newPassword)
          : updateUserPasswordInCsv(uid, oldPassword, newPassword);
    if (!result.ok) {
      res.status(400).json({ ok: false, error: result.error });
      return;
    }
    res.json({ ok: true, message: "Password updated." });
  });

  r.post("/contact", requireAuth, async (req, res) => {
    if (req.user?.role !== "CoachManager") {
      res.status(403).json({
        ok: false,
        error: "Only Coach Manager can update email and contact number here.",
      });
      return;
    }
    if (!isMongoConfigured()) {
      res.status(400).json({
        ok: false,
        error: "MongoDB is required to save your profile.",
      });
      return;
    }
    const uid = req.user?.sub;
    if (uid == null || String(uid).trim() === "") {
      res.status(401).json({ ok: false, error: "Invalid session." });
      return;
    }
    const email_address = String(req.body?.email_address ?? "");
    const contact_number = String(req.body?.contact_number ?? "");
    try {
      const out = await updateCoachManagerContactMongo(
        String(uid),
        String(req.user?.username ?? "").trim(),
        { email_address, contact_number },
      );
      if (!out.ok) {
        res.status(400).json({ ok: false, error: out.error });
        return;
      }
      res.json({ ok: true });
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      res.status(503).json({
        ok: false,
        error: `Login database unavailable: ${msg}`,
      });
    }
  });

  return r;
}

/** Attach to GET /me enrichment in server — exported for reuse */
export function accountPayloadForUid(uid: string | number) {
  const row = findUserByUid(uid);
  if (!row) {
    return null;
  }
  return accountPayloadFromCsvRow(row);
}

/** Club name + creation_date + full_name + expiry from `userLogin_Student.csv` for JWT sub = StudentID. */
export function studentProfileFromUserLoginStudentCsv(uid: string | number): {
  club_name: string;
  creation_date: string;
  full_name: string;
  expiry_date: string;
  /** `data_club/{club_folder_uid}/` when set on login row. */
  club_folder_uid: string;
} | null {
  const row = findStudentRoleLoginByUid(uid);
  if (!row) {
    return null;
  }
  const club = (row.clubName && String(row.clubName).trim()) || "";
  const created = (row.creationDate && String(row.creationDate).trim()) || "";
  const fullName = (row.fullName && String(row.fullName).trim()) || "";
  const expiry = (row.expiryDate && String(row.expiryDate).trim()) || "";
  const folderUid = (row.clubFolderUid ?? "").trim();
  return {
    club_name: club || "—",
    creation_date: created || "—",
    full_name: fullName || "—",
    expiry_date: expiry || "—",
    club_folder_uid: folderUid,
  };
}

/** `club_name` + `creation_date` + `full_name` + expiry from `userLogin_Coach.csv` for JWT sub = CoachID. */
export function coachProfileFromUserLoginCoachCsv(uid: string | number): {
  club_name: string;
  creation_date: string;
  full_name: string;
  expiry_date: string;
  club_folder_uid: string;
} | null {
  const row = findCoachRoleLoginByUid(uid);
  if (!row) {
    return null;
  }
  const club = (row.clubName && String(row.clubName).trim()) || "";
  const created = (row.creationDate && String(row.creationDate).trim()) || "";
  const fullName = (row.fullName && String(row.fullName).trim()) || "";
  const expiry = (row.expiryDate && String(row.expiryDate).trim()) || "";
  const folderUid = (row.clubFolderUid ?? "").trim();
  return {
    club_name: club || "—",
    creation_date: created || "—",
    full_name: fullName || "—",
    expiry_date: expiry || "—",
    club_folder_uid: folderUid,
  };
}

export function accountPayloadFromCsvRow(row: CsvUser) {
  const status =
    (row.status && String(row.status).trim()) ||
    (row.isActivated ? "ACTIVE" : "INACTIVE");
  const clubName = (row.clubName && String(row.clubName).trim()) || "";
  const clubPhoto = (row.clubPhoto && String(row.clubPhoto).trim()) || "";
  const photoRel = resolveClubPhotoRelForServing(row.uid, clubPhoto);
  const expiryRaw = (row.expiryDate && String(row.expiryDate).trim()) || "";
  return {
    uid: row.uid,
    username: row.username,
    full_name: (row.fullName && String(row.fullName).trim()) || "—",
    usertype: row.usertype,
    status,
    club_name: clubName || "—",
    club_photo: clubPhoto,
    club_photo_url: photoRel ? clubPhotoPublicUrl(row.uid, photoRel) : null,
    creation_date: row.creationDate.trim() || "—",
    expiry_date: expiryRaw || "—",
  };
}
