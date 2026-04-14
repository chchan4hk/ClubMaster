import fs from "fs";
import path from "path";
import { Router } from "express";
import { requireAuth } from "../middleware/requireAuth";
import { clubDataDir } from "../coachListCsv";
import {
  findCoachRoleLoginByUid,
  findStudentRoleLoginByUid,
  updateCoachLoginPasswordInCsv,
  updateStudentLoginPasswordInCsv,
} from "../coachStudentLoginCsv";
import type { CsvUser } from "../userlistCsv";
import { findUserByUid, updateUserPasswordInCsv } from "../userlistCsv";

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

  r.post("/password", requireAuth, (req, res) => {
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

/** Club name + creation_date + full_name + expiry from `userLogin_Student` (JSON or CSV) for JWT sub = StudentID. */
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

/** `club_name` + `creation_date` + `full_name` + expiry from `userLogin_Coach` (JSON or CSV) for JWT sub = CoachID. */
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
