import type { Document } from "mongodb";
import {
  ensureUserLoginCollection,
  getUserListCollection,
  getUserLoginCollection,
  isMongoConfigured,
  USER_TYPE_VALUES,
  type UserLoginInsert,
  type UserType,
} from "./db/DBConnection";

export type MigrateUserListResult = {
  scanned: number;
  inserted: number;
  skippedAlreadyInUserLogin: number;
  skippedInvalid: number;
  removedFromUserList: number;
  errors: Array<{ uid?: string; message: string }>;
};

function isUserType(v: string): v is UserType {
  return (USER_TYPE_VALUES as readonly string[]).includes(v);
}

/** Legacy strings that are not stored exactly as `UserType`. */
function normalizeLegacyUserType(raw: string): UserType | null {
  const t = raw.trim();
  if (!t) {
    return null;
  }
  if (isUserType(t)) {
    return t;
  }
  const key = t.replace(/\s+/g, " ").toLowerCase();
  const aliases: Record<string, UserType> = {
    coachmanager: "Coach Manager",
    "coach manager": "Coach Manager",
    admin: "Administrator",
    administrator: "Administrator",
  };
  const mapped = aliases[key];
  return mapped ?? null;
}

function inferUserType(doc: Document): UserType | null {
  const raw = String(doc.usertype ?? "").trim();
  const normalized = normalizeLegacyUserType(raw);
  if (normalized) {
    return normalized;
  }
  const store = String(doc.store ?? "").trim().toLowerCase();
  if (store === "coach") {
    return "Coach";
  }
  if (store === "student") {
    return "Student";
  }
  if (store === "main" || store === "userlogin") {
    return null;
  }
  return null;
}

function toDate(v: unknown, fallback: Date): Date {
  if (v instanceof Date && !Number.isNaN(v.getTime())) {
    return v;
  }
  if (typeof v === "string" && v.trim()) {
    const d = new Date(
      /^\d{4}-\d{2}-\d{2}$/.test(v.trim())
        ? `${v.trim()}T12:00:00.000Z`
        : v.trim(),
    );
    if (!Number.isNaN(d.getTime())) {
      return d;
    }
  }
  return fallback;
}

function toBool(v: unknown, fallback = true): boolean {
  if (typeof v === "boolean") {
    return v;
  }
  if (typeof v === "string") {
    const s = v.trim().toUpperCase();
    if (s === "ACTIVE" || s === "YES" || s === "Y" || s === "TRUE" || s === "1") {
      return true;
    }
    if (s === "INACTIVE" || s === "NO" || s === "N" || s === "FALSE" || s === "0") {
      return false;
    }
  }
  return fallback;
}

function toExpiry(v: unknown): Date | null {
  if (v == null || v === "") {
    return null;
  }
  if (v instanceof Date) {
    return Number.isNaN(v.getTime()) ? null : v;
  }
  if (typeof v === "string" && v.trim()) {
    const d = toDate(v, new Date(NaN));
    return Number.isNaN(d.getTime()) ? null : d;
  }
  return null;
}

/**
 * Map a legacy `userList` document into a `userLogin` insert payload.
 * Returns `null` if the row cannot be migrated.
 */
export function userListDocumentToUserLoginInsert(
  doc: Document,
): UserLoginInsert | null {
  const uid = String(doc.uid ?? "").trim();
  if (!uid) {
    return null;
  }
  const usertype = inferUserType(doc);
  if (!usertype) {
    return null;
  }
  const password = String(doc.password ?? "").trim();
  if (!password) {
    return null;
  }
  const now = new Date();
  const username = String(doc.username ?? "").trim();
  if (!username) {
    return null;
  }

  const base: UserLoginInsert = {
    uid,
    usertype,
    username,
    password,
    full_name: String(doc.full_name ?? "").trim(),
    is_activated: toBool(doc.is_activated, true),
    creation_date: toDate(doc.creation_date, now),
    club_name: String(doc.club_name ?? "").trim(),
    club_photo: String(doc.club_photo ?? "").trim(),
    status: toBool(doc.status, true),
    lastUpdate_date: toDate(doc.lastUpdate_date, now),
    Expiry_date: toExpiry(doc.Expiry_date),
  };

  const cfu = String(doc.club_folder_uid ?? "").trim();
  if (cfu) {
    base.club_folder_uid = cfu;
  }
  const coachId = String(doc.coach_id ?? "").trim();
  if (coachId) {
    base.coach_id = coachId;
  }
  const studentId = String(doc.student_id ?? "").trim();
  if (studentId) {
    base.student_id = studentId;
  }
  const classId = String(doc.class_id ?? "").trim();
  if (classId) {
    base.class_id = classId;
  }
  const clubId = String(doc.club_id ?? "").trim();
  if (clubId) {
    base.club_id = clubId;
  }

  return base;
}

export type RunMigrateUserListOptions = {
  /** Database name (same resolution as `getUserLoginCollection`). */
  databaseName?: string;
  /** After a successful insert, delete the source row from `userList` (by `_id`). */
  removeFromUserListAfterInsert?: boolean;
};

/**
 * Copies documents from the `userList` collection into `userLogin`, skipping UIDs
 * that already exist in `userLogin`. Legacy `store` is not written to `userLogin`.
 */
export async function runUserListToUserLoginMigration(
  options: RunMigrateUserListOptions = {},
): Promise<MigrateUserListResult> {
  if (!isMongoConfigured()) {
    throw new Error("MongoDB is not configured.");
  }
  const dbName = options.databaseName;
  await ensureUserLoginCollection(dbName);
  const listColl = await getUserListCollection(dbName);
  const loginColl = await getUserLoginCollection(dbName);

  const result: MigrateUserListResult = {
    scanned: 0,
    inserted: 0,
    skippedAlreadyInUserLogin: 0,
    skippedInvalid: 0,
    removedFromUserList: 0,
    errors: [],
  };

  const cursor = listColl.find({});
  for await (const doc of cursor) {
    result.scanned += 1;
    const mapped = userListDocumentToUserLoginInsert(doc);
    if (!mapped) {
      result.skippedInvalid += 1;
      result.errors.push({
        uid: String(doc.uid ?? "").trim() || undefined,
        message: "Could not map document (missing uid/username/password or unknown usertype).",
      });
      continue;
    }
    const exists = await loginColl.findOne({ uid: mapped.uid });
    if (exists) {
      result.skippedAlreadyInUserLogin += 1;
      continue;
    }
    try {
      await loginColl.insertOne(mapped);
      result.inserted += 1;
      if (options.removeFromUserListAfterInsert && doc._id != null) {
        const dr = await listColl.deleteOne({ _id: doc._id });
        if (dr.deletedCount) {
          result.removedFromUserList += 1;
        }
      }
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      result.errors.push({ uid: mapped.uid, message: msg });
    }
  }

  return result;
}
