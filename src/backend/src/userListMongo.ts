import type { Document, Filter } from "mongodb";
import {
  getAuthUserLoginCollection,
  isMongoConfigured,
  resolveAuthLoginDatabaseName,
  type UserLoginDocument,
  type UserLoginInsert,
} from "./db/DBConnection";
import { hashPassword, verifyPassword } from "./userLoginPassword";
import type { CsvUser } from "./userlistCsv";
import type { CoachStudentLoginRow } from "./coachStudentLoginCsv";
import { userLoginDocumentToCsvUser } from "./userLoginCollectionMongo";

export { isMongoConfigured };

/** Case-insensitive matching for string fields. */
const LOGIN_STRING_COLLATION = { locale: "en", strength: 2 } as const;

const COACH_LOGIN_UID_NUM_RE = /^(?:CH|C)(\d+)$/i;
const STUDENT_ID_NUM_RE = /^S(\d+)$/i;
const COACH_LOGIN_UID_PAD = 6;
const STUDENT_LOGIN_UID_PAD = 9;

function formatDateOnly(d: Date | undefined | null): string {
  if (!d || !(d instanceof Date) || Number.isNaN(d.getTime())) {
    return "";
  }
  return d.toISOString().slice(0, 10);
}

function parseYyyyMmDdToDate(s: string): Date | null {
  const t = String(s ?? "").trim();
  if (!t || !/^\d{4}-\d{2}-\d{2}$/.test(t)) {
    return null;
  }
  return new Date(`${t}T12:00:00.000Z`);
}

function nowDates(): { creation: Date; last: Date } {
  const d = new Date();
  return { creation: d, last: d };
}

function docToCsvUserForAdminMainTable(doc: UserLoginDocument): CsvUser | null {
  const ut = String(doc.usertype ?? "");
  if (ut !== "Administrator" && ut !== "Coach Manager") {
    return null;
  }
  return userLoginDocumentToCsvUser(doc);
}

function docToCoachStudentRow(doc: UserLoginDocument): CoachStudentLoginRow {
  const pwd = String(doc.password ?? "").trim();
  const base: CoachStudentLoginRow = {
    uid: String(doc.uid ?? "").trim(),
    username: String(doc.username ?? "").trim(),
    password: "",
    passwordHash: pwd || null,
    fullName: String(doc.full_name ?? "").trim(),
    isActivated: Boolean(doc.is_activated),
    clubName: String(doc.club_name ?? "").trim(),
    status: doc.status === true ? "ACTIVE" : "INACTIVE",
    creationDate: formatDateOnly(doc.creation_date as Date),
    lastUpdateDate: formatDateOnly(doc.lastUpdate_date as Date),
    expiryDate: formatDateOnly(doc.Expiry_date as Date | null),
  };
  const cfu = String(doc.club_folder_uid ?? "").trim();
  if (cfu) {
    base.clubFolderUid = cfu;
  }
  if (doc.usertype === "Coach") {
    base.coachId = String(doc.coach_id ?? doc.uid ?? "").trim() || base.uid;
  } else {
    base.studentId = String(doc.student_id ?? doc.uid ?? "").trim() || base.uid;
  }
  return base;
}

async function ensureIndexes(): Promise<void> {
  const coll = await getAuthUserLoginCollection();
  await coll.createIndex({ uid: 1 }, { unique: true });
  await coll.createIndex({ username: 1 });
  await coll.createIndex({ usertype: 1 });
}

let indexesEnsured = false;
async function collWithIndexes(): Promise<
  ReturnType<typeof getAuthUserLoginCollection>
> {
  const coll = await getAuthUserLoginCollection();
  if (!indexesEnsured) {
    try {
      await ensureIndexes();
    } catch {
      /* index may already exist with different options */
    }
    indexesEnsured = true;
  }
  return coll;
}

/** GET /admin/login-accounts payload (same shape as file-based). */
export async function listAdminLoginAccountsFromMongo(): Promise<{
  userLogin: Array<Record<string, unknown>>;
  coach: Array<Record<string, unknown>>;
  student: Array<Record<string, unknown>>;
}> {
  const coll = await collWithIndexes();
  const rows = (await coll.find({}).toArray()) as UserLoginDocument[];
  const userLogin = rows
    .filter((d) => d.usertype === "Administrator" || d.usertype === "Coach Manager")
    .map((u) => {
      const c = docToCsvUserForAdminMainTable(u);
      if (!c) {
        return null;
      }
      return {
        uid: c.uid,
        usertype: c.usertype,
        role: c.role,
        username: c.username,
        fullName: c.fullName,
        clubName: c.clubName,
        status: c.status,
        isActivated: c.isActivated,
        creationDate: c.creationDate,
        lastUpdateDate: c.lastUpdateDate,
        expiryDate: c.expiryDate ?? "",
      };
    })
    .filter(Boolean) as Array<Record<string, unknown>>;
  const coach = rows
    .filter((d) => d.usertype === "Coach")
    .map((r) => {
      const row = docToCoachStudentRow(r);
      return {
        uid: row.uid,
        username: row.username,
        fullName: row.fullName,
        clubName: row.clubName,
        status: row.status,
        isActivated: row.isActivated,
        creationDate: row.creationDate,
        lastUpdateDate: row.lastUpdateDate,
        expiryDate: row.expiryDate ?? "",
      };
    });
  const student = rows
    .filter((d) => d.usertype === "Student")
    .map((r) => {
      const row = docToCoachStudentRow(r);
      return {
        uid: row.uid,
        username: row.username,
        fullName: row.fullName,
        clubName: row.clubName,
        status: row.status,
        isActivated: row.isActivated,
        creationDate: row.creationDate,
        lastUpdateDate: row.lastUpdateDate,
        expiryDate: row.expiryDate ?? "",
      };
    });
  return { userLogin, coach, student };
}

export async function findUserByUsernameMongo(
  username: string,
): Promise<CsvUser | null> {
  const q = username.trim();
  if (!q) {
    return null;
  }
  const coll = await collWithIndexes();
  const doc = await coll.findOne(
    { username: q },
    { collation: LOGIN_STRING_COLLATION },
  );
  if (!doc) {
    return null;
  }
  return userLoginDocumentToCsvUser(doc as UserLoginDocument);
}

export async function findCoachManagerUidByClubNameMongo(
  clubName: string,
): Promise<string | null> {
  const q = clubName.trim();
  if (!q) {
    return null;
  }
  const coll = await collWithIndexes();
  const doc = await coll.findOne(
    {
      usertype: "Coach Manager",
      club_name: q,
    },
    { collation: LOGIN_STRING_COLLATION },
  );
  const uid = doc?.uid != null ? String(doc.uid).trim() : "";
  return uid || null;
}

/** Distinct non-empty `club_name` values from `userLogin` (sign-in club dropdown). */
export async function distinctClubNamesFromUserLoginMongo(): Promise<string[]> {
  const coll = await collWithIndexes();
  const raw = (await coll.distinct("club_name", {
    club_name: { $exists: true, $nin: ["", "—"] },
  })) as unknown[];
  const seen = new Set<string>();
  const out: string[] = [];
  for (const v of raw) {
    const cn = String(v ?? "").trim();
    if (!cn || cn === "—") {
      continue;
    }
    const key = cn.toLowerCase();
    if (seen.has(key)) {
      continue;
    }
    seen.add(key);
    out.push(cn);
  }
  out.sort((a, b) => a.localeCompare(b, undefined, { sensitivity: "base" }));
  return out;
}

/**
 * Administrator / Coach Manager row by UID (main admin table).
 */
export async function findMainUserByUidMongo(
  uid: string,
): Promise<CsvUser | null> {
  const key = String(uid ?? "").trim();
  if (!key) {
    return null;
  }
  const coll = await collWithIndexes();
  const doc = await coll.findOne({
    uid: key,
    ...MAIN_USER_TYPES_FILTER,
  });
  if (!doc) {
    return null;
  }
  return userLoginDocumentToCsvUser(doc as UserLoginDocument);
}

/** True if any `userLogin` document uses this `uid` (allocating coach/student IDs). */
export async function userLoginUidExistsMongo(uid: string): Promise<boolean> {
  const key = String(uid ?? "").trim();
  if (!key) {
    return false;
  }
  const coll = await collWithIndexes();
  const hit = await coll.findOne({ uid: key });
  return Boolean(hit);
}

export async function findUserByUsernameAnyStoreMongo(
  username: string,
): Promise<boolean> {
  const q = username.trim();
  if (!q) {
    return false;
  }
  const coll = await collWithIndexes();
  const doc = await coll.findOne({ username: q }, { collation: LOGIN_STRING_COLLATION });
  return Boolean(doc);
}

export async function assertUsernameFreeForUidMongo(
  username: string,
  exceptUid: string,
): Promise<{ ok: true } | { ok: false; error: string }> {
  const uq = username.trim();
  if (!uq) {
    return { ok: false, error: "Username is required." };
  }
  const ex = exceptUid.trim().toUpperCase();
  const coll = await collWithIndexes();
  const hit = await coll.findOne(
    { username: uq },
    { collation: LOGIN_STRING_COLLATION },
  );
  if (hit && String(hit.uid ?? "").trim().toUpperCase() !== ex) {
    return { ok: false, error: "That username is already in use." };
  }
  return { ok: true };
}

export async function findCoachRoleLoginByUsernameMongo(
  username: string,
): Promise<CoachStudentLoginRow | null> {
  const q = username.trim();
  if (!q) {
    return null;
  }
  const coll = await collWithIndexes();
  const doc = await coll.findOne(
    { usertype: "Coach", username: q },
    { collation: LOGIN_STRING_COLLATION },
  );
  return doc ? docToCoachStudentRow(doc as UserLoginDocument) : null;
}

export async function findStudentRoleLoginByUsernameMongo(
  username: string,
): Promise<CoachStudentLoginRow | null> {
  const q = username.trim();
  if (!q) {
    return null;
  }
  const coll = await collWithIndexes();
  const doc = await coll.findOne(
    { usertype: "Student", username: q },
    { collation: LOGIN_STRING_COLLATION },
  );
  return doc ? docToCoachStudentRow(doc as UserLoginDocument) : null;
}

async function maxCoachNumericFromMongo(): Promise<number> {
  const coll = await collWithIndexes();
  const rows = await coll
    .find({ usertype: "Coach" })
    .project({ uid: 1 })
    .toArray();
  let max = 0;
  for (const d of rows) {
    const m = String(d.uid ?? "")
      .trim()
      .match(COACH_LOGIN_UID_NUM_RE);
    if (m) {
      const n = Number.parseInt(m[1]!, 10);
      if (!Number.isNaN(n)) {
        max = Math.max(max, n);
      }
    }
  }
  return max;
}

async function maxStudentNumericFromMongo(): Promise<number> {
  const coll = await collWithIndexes();
  const rows = await coll
    .find({ usertype: "Student" })
    .project({ uid: 1 })
    .toArray();
  let max = 0;
  for (const d of rows) {
    const m = String(d.uid ?? "")
      .trim()
      .match(STUDENT_ID_NUM_RE);
    if (m) {
      const n = Number.parseInt(m[1]!, 10);
      if (!Number.isNaN(n)) {
        max = Math.max(max, n);
      }
    }
  }
  return max;
}

export async function allocateNextCoachLoginUidMongo(): Promise<string> {
  const n = (await maxCoachNumericFromMongo()) + 1;
  return `C${String(n).padStart(COACH_LOGIN_UID_PAD, "0")}`;
}

export async function allocateNextStudentLoginUidMongo(): Promise<string> {
  const n = (await maxStudentNumericFromMongo()) + 1;
  return `S${String(n).padStart(STUDENT_LOGIN_UID_PAD, "0")}`;
}

export async function getCoachManagerExpiryDateForClubFolderUidMongo(
  clubFolderUid: string,
): Promise<string> {
  const key = String(clubFolderUid ?? "").trim();
  if (!key) {
    return "";
  }
  const coll = await collWithIndexes();
  const doc = await coll.findOne({
    uid: key,
    usertype: "Coach Manager",
  });
  if (!doc) {
    return "";
  }
  const row = userLoginDocumentToCsvUser(doc as UserLoginDocument);
  if (!row || row.role !== "CoachManager") {
    return "";
  }
  return String(row.expiryDate ?? "").trim();
}

export async function updateMainProfileMongo(
  uid: string,
  input: { username: string; fullName: string; clubName: string; expiryDate: string },
): Promise<{ ok: true } | { ok: false; error: string }> {
  const coll = await collWithIndexes();
  const key = uid.trim();
  const exp = input.expiryDate.trim()
    ? parseYyyyMmDdToDate(input.expiryDate.trim())
    : null;
  const r = await coll.updateOne(
    {
      uid: key,
      ...MAIN_USER_TYPES_FILTER,
    },
    {
      $set: {
        username: input.username.trim(),
        full_name: input.fullName,
        club_name: input.clubName,
        Expiry_date: exp,
        lastUpdate_date: new Date(),
      },
    },
  );
  if (r.matchedCount === 0) {
    return { ok: false, error: "User not found in userLogin." };
  }
  return { ok: true };
}

export async function updateRoleProfileMongo(
  uid: string,
  store: "coach" | "student",
  input: { username: string; fullName: string; clubName: string; expiryDate: string },
): Promise<{ ok: true } | { ok: false; error: string }> {
  const coll = await collWithIndexes();
  const key = uid.trim().toUpperCase();
  const exp = input.expiryDate.trim()
    ? parseYyyyMmDdToDate(input.expiryDate.trim())
    : null;
  const r = await coll.updateOne(coachStudentUidMatchFilter(store, key), {
    $set: {
      username: input.username.trim(),
      full_name: input.fullName,
      club_name: input.clubName,
      Expiry_date: exp,
      lastUpdate_date: new Date(),
    },
  });
  if (r.matchedCount === 0) {
    return { ok: false, error: "Could not update row." };
  }
  return { ok: true };
}

function escapeRegex(s: string): string {
  return s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

/** Same credential resolution as login mapping: optional hash fields, else `password`. */
function storedCredentialFromUserLoginDoc(doc: UserLoginDocument): string {
  const o = doc as unknown as Record<string, unknown>;
  const fromHash = String(
    o.passwordHash ?? o.password_hash ?? "",
  ).trim();
  if (fromHash) {
    return fromHash;
  }
  return String(doc.password ?? "").trim();
}

/** Match coach/student login by login UID or legacy id field. */
function coachStudentUidMatchFilter(
  store: "coach" | "student",
  uidKey: string,
): Filter<Document> {
  const usertype = store === "coach" ? "Coach" : "Student";
  const key = uidKey.trim().toUpperCase();
  const orUid: Document[] = [{ uid: new RegExp(`^${escapeRegex(key)}$`, "i") }];
  if (store === "coach") {
    orUid.push({ coach_id: new RegExp(`^${escapeRegex(key)}$`, "i") });
  } else {
    orUid.push({ student_id: new RegExp(`^${escapeRegex(key)}$`, "i") });
  }
  return { usertype, $or: orUid };
}

const MAIN_USER_TYPES_FILTER = {
  usertype: { $in: ["Administrator", "Coach Manager"] as const },
} as const;

export async function setMainExpiryMongo(
  uid: string,
  expiryYyyyMmDd: string,
): Promise<{ ok: true } | { ok: false; error: string }> {
  const coll = await collWithIndexes();
  const exp = expiryYyyyMmDd.trim()
    ? parseYyyyMmDdToDate(expiryYyyyMmDd.trim())
    : null;
  const r = await coll.updateOne(
    { uid: uid.trim(), ...MAIN_USER_TYPES_FILTER },
    { $set: { Expiry_date: exp, lastUpdate_date: new Date() } },
  );
  if (r.matchedCount === 0) {
    return { ok: false, error: "User not found." };
  }
  return { ok: true };
}

/** Expiry-only update for coach/student rows. */
export async function setRoleExpiryOnlyMongo(
  uid: string,
  store: "coach" | "student",
  expiryYyyyMmDd: string,
): Promise<{ ok: true } | { ok: false; error: string }> {
  const coll = await collWithIndexes();
  const key = uid.trim().toUpperCase();
  const exp = expiryYyyyMmDd.trim()
    ? parseYyyyMmDdToDate(expiryYyyyMmDd.trim())
    : null;
  const r = await coll.updateOne(coachStudentUidMatchFilter(store, key), {
    $set: { Expiry_date: exp, lastUpdate_date: new Date() },
  });
  if (r.matchedCount === 0) {
    return { ok: false, error: "Could not update row." };
  }
  return { ok: true };
}

export async function setMainPasswordMongo(
  uid: string,
  plain: string,
): Promise<{ ok: true } | { ok: false; error: string }> {
  const coll = await collWithIndexes();
  const r = await coll.updateOne(
    { uid: uid.trim(), ...MAIN_USER_TYPES_FILTER },
    { $set: { password: hashPassword(plain), lastUpdate_date: new Date() } },
  );
  if (r.matchedCount === 0) {
    return { ok: false, error: "User not found." };
  }
  return { ok: true };
}

export async function setRolePasswordMongo(
  uid: string,
  store: "coach" | "student",
  plain: string,
): Promise<{ ok: true } | { ok: false; error: string }> {
  const coll = await collWithIndexes();
  const key = uid.trim().toUpperCase();
  const r = await coll.updateOne(coachStudentUidMatchFilter(store, key), {
    $set: { password: hashPassword(plain), lastUpdate_date: new Date() },
  });
  if (r.matchedCount === 0) {
    return { ok: false, error: `No ${store} login found with that UID.` };
  }
  return { ok: true };
}

/**
 * Self-service password change against Mongo `userLogin` on the auth database
 * ({@link resolveAuthLoginDatabaseName}, default `ClubMaster_DB`). Verifies `oldPassword` against the same
 * stored credential shape as sign-in (`password` and optional `passwordHash` / `password_hash`).
 *
 * When `loginUsername` is set and the main-login row is not found by `uid`, resolves by `username`
 * (Administrator / Coach Manager) so JWT `sub` quirks still hit the same `userLogin` document.
 */
export async function changeAuthenticatedUserLoginPasswordMongo(
  uid: string,
  role: string,
  oldPassword: string,
  newPassword: string,
  loginUsername?: string,
): Promise<
  | { ok: true }
  | { ok: false; error: string; notInMongo?: true }
> {
  const newPlain = String(newPassword ?? "").trim();
  if (!newPlain) {
    return { ok: false, error: "New password is required." };
  }
  const coll = await collWithIndexes();
  const uidStr = String(uid ?? "").trim();
  if (!uidStr) {
    return { ok: false, error: "Invalid session." };
  }

  let doc: UserLoginDocument | null = null;
  if (role === "Coach") {
    doc = await coll.findOne(
      coachStudentUidMatchFilter("coach", uidStr.toUpperCase()),
    );
  } else if (role === "Student") {
    doc = await coll.findOne(
      coachStudentUidMatchFilter("student", uidStr.toUpperCase()),
    );
  } else {
    doc = await coll.findOne({
      uid: uidStr,
      ...MAIN_USER_TYPES_FILTER,
    });
    const un = String(loginUsername ?? "").trim();
    if (!doc && un) {
      doc = await coll.findOne({
        username: new RegExp(`^${escapeRegex(un)}$`, "i"),
        ...MAIN_USER_TYPES_FILTER,
      });
    }
  }

  const dbName = resolveAuthLoginDatabaseName();
  if (!doc) {
    return {
      ok: false,
      error: `No matching account in MongoDB ${dbName}.userLogin for this session.`,
      notInMongo: true,
    };
  }

  const stored = storedCredentialFromUserLoginDoc(doc);
  if (!verifyPassword(oldPassword, stored)) {
    return {
      ok: false,
      error: `Old password does not match credentials in MongoDB ${dbName}.userLogin.`,
    };
  }

  const r = await coll.updateOne(
    { _id: doc._id },
    { $set: { password: hashPassword(newPlain), lastUpdate_date: new Date() } },
  );
  if (r.matchedCount === 0) {
    return { ok: false, error: "Could not update password." };
  }
  return { ok: true };
}

export async function setMainActivationMongo(
  uid: string,
  active: boolean,
): Promise<{ ok: true } | { ok: false; error: string }> {
  const coll = await collWithIndexes();
  const r = await coll.updateOne(
    { uid: uid.trim(), ...MAIN_USER_TYPES_FILTER },
    {
      $set: {
        is_activated: active,
        status: active,
        lastUpdate_date: new Date(),
      },
    },
  );
  if (r.matchedCount === 0) {
    return { ok: false, error: "Could not update user row." };
  }
  return { ok: true };
}

export async function setRoleActivationMongo(
  uid: string,
  store: "coach" | "student",
  active: boolean,
): Promise<{ ok: true } | { ok: false; error: string }> {
  const coll = await collWithIndexes();
  const key = uid.trim().toUpperCase();
  const r = await coll.updateOne(coachStudentUidMatchFilter(store, key), {
    $set: {
      is_activated: active,
      status: active,
      lastUpdate_date: new Date(),
    },
  });
  if (r.matchedCount === 0) {
    return { ok: false, error: "Could not update row." };
  }
  return { ok: true };
}

export async function deleteRoleLoginMongo(
  uid: string,
  store: "coach" | "student",
): Promise<{ ok: true } | { ok: false; error: string }> {
  const coll = await collWithIndexes();
  const key = uid.trim().toUpperCase();
  const r = await coll.deleteOne(coachStudentUidMatchFilter(store, key));
  if (r.deletedCount === 0) {
    return { ok: false, error: "No matching login to delete." };
  }
  return { ok: true };
}

export async function deleteMainLoginMongo(
  uid: string,
): Promise<{ ok: true } | { ok: false; error: string }> {
  const coll = await collWithIndexes();
  const r = await coll.deleteOne({
    uid: uid.trim(),
    usertype: "Coach Manager",
  });
  if (r.deletedCount === 0) {
    return { ok: false, error: "Coach Manager row not found." };
  }
  return { ok: true };
}

export async function deleteCoachStudentForClubFolderMongo(
  folderUid: string,
): Promise<{ ok: true; removedCoach: number; removedStudent: number }> {
  const id = folderUid.trim();
  const coll = await collWithIndexes();
  const coachR = await coll.deleteMany({
    usertype: "Coach",
    club_folder_uid: id,
  });
  const stuR = await coll.deleteMany({
    usertype: "Student",
    club_folder_uid: id,
  });
  return {
    ok: true,
    removedCoach: coachR.deletedCount ?? 0,
    removedStudent: stuR.deletedCount ?? 0,
  };
}

export async function insertCoachManagerMongo(input: {
  uid: string;
  username: string;
  password: string;
  fullName: string;
  clubName: string;
  clubPhoto: string;
  expiryDate?: string;
}): Promise<{ ok: true } | { ok: false; error: string }> {
  const { creation, last } = nowDates();
  const exp = input.expiryDate?.trim()
    ? parseYyyyMmDdToDate(input.expiryDate.trim())
    : null;
  const coll = await collWithIndexes();
  try {
    await coll.insertOne({
      uid: input.uid.trim(),
      usertype: "Coach Manager",
      username: input.username.trim(),
      password: hashPassword(input.password),
      full_name: input.fullName.trim(),
      is_activated: true,
      creation_date: creation,
      club_name: input.clubName.trim(),
      club_photo: String(input.clubPhoto ?? "").trim(),
      status: true,
      lastUpdate_date: last,
      Expiry_date: exp,
    });
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    return { ok: false, error: msg };
  }
  return { ok: true };
}

export async function insertCoachRoleMongo(input: {
  uid: string;
  username: string;
  password: string;
  fullName: string;
  clubName: string;
  clubFolderUid?: string;
  expiryDate?: string;
}): Promise<{ ok: true } | { ok: false; error: string }> {
  const { creation, last } = nowDates();
  const exp = input.expiryDate?.trim()
    ? parseYyyyMmDdToDate(input.expiryDate.trim())
    : null;
  const uid = input.uid.trim();
  const coll = await collWithIndexes();
  const cfuCoach = String(input.clubFolderUid ?? "").trim();
  const doc: UserLoginInsert = {
    uid,
    usertype: "Coach",
    username: input.username.trim(),
    password: hashPassword(input.password),
    full_name: input.fullName.trim(),
    is_activated: true,
    creation_date: creation,
    club_name: input.clubName.trim(),
    club_photo: "",
    status: true,
    lastUpdate_date: last,
    Expiry_date: exp,
    coach_id: uid,
    ...(cfuCoach ? { club_folder_uid: cfuCoach } : {}),
  };
  try {
    await coll.insertOne(doc);
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    return { ok: false, error: msg };
  }
  return { ok: true };
}

export async function insertStudentRoleMongo(input: {
  uid: string;
  username: string;
  password: string;
  fullName: string;
  clubName: string;
  clubFolderUid?: string;
  expiryDate?: string;
}): Promise<{ ok: true } | { ok: false; error: string }> {
  const { creation, last } = nowDates();
  const exp = input.expiryDate?.trim()
    ? parseYyyyMmDdToDate(input.expiryDate.trim())
    : null;
  const uid = input.uid.trim();
  const coll = await collWithIndexes();
  const cfuStu = String(input.clubFolderUid ?? "").trim();
  const doc: UserLoginInsert = {
    uid,
    usertype: "Student",
    username: input.username.trim(),
    password: hashPassword(input.password),
    full_name: input.fullName.trim(),
    is_activated: true,
    creation_date: creation,
    club_name: input.clubName.trim(),
    club_photo: "",
    status: true,
    lastUpdate_date: last,
    Expiry_date: exp,
    student_id: uid,
    ...(cfuStu ? { club_folder_uid: cfuStu } : {}),
  };
  try {
    await coll.insertOne(doc);
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    return { ok: false, error: msg };
  }
  return { ok: true };
}

function normEq(a: string, b: string): boolean {
  return a.trim().toLowerCase() === b.trim().toLowerCase();
}

export async function searchCoachManagersMongo(
  username?: string,
  clubName?: string,
): Promise<CsvUser[]> {
  const uq = (username ?? "").trim();
  const cq = (clubName ?? "").trim();
  if (!uq && !cq) {
    return [];
  }
  const coll = await collWithIndexes();
  const docs = await coll.find({ usertype: "Coach Manager" }).toArray();
  return docs
    .map((d) => userLoginDocumentToCsvUser(d as UserLoginDocument))
    .filter((c): c is CsvUser => Boolean(c))
    .filter((row) => {
      if (row.role !== "CoachManager") {
        return false;
      }
      if (uq && !normEq(row.username, uq)) {
        return false;
      }
      if (cq && !normEq(row.clubName, cq)) {
        return false;
      }
      return true;
    });
}

export async function searchCoachRoleByUsernameOrClubMongo(
  username: string,
  clubName: string,
): Promise<CoachStudentLoginRow[]> {
  const uq = username.trim();
  const cq = clubName.trim();
  if (!uq && !cq) {
    return [];
  }
  const coll = await collWithIndexes();
  const docs = await coll.find({ usertype: "Coach" }).toArray();
  return docs
    .map((d) => docToCoachStudentRow(d as UserLoginDocument))
    .filter((r) => {
      if (uq && !normEq(r.username, uq)) {
        return false;
      }
      if (cq && !normEq(r.clubName, cq)) {
        return false;
      }
      return true;
    });
}

export async function searchStudentRoleByUsernameOrClubMongo(
  username: string,
  clubName: string,
): Promise<CoachStudentLoginRow[]> {
  const uq = username.trim();
  const cq = clubName.trim();
  if (!uq && !cq) {
    return [];
  }
  const coll = await collWithIndexes();
  const docs = await coll.find({ usertype: "Student" }).toArray();
  return docs
    .map((d) => docToCoachStudentRow(d as UserLoginDocument))
    .filter((r) => {
      if (uq && !normEq(r.username, uq)) {
        return false;
      }
      if (cq && !normEq(r.clubName, cq)) {
        return false;
      }
      return true;
    });
}

export async function activateCoachManagerMongo(
  username: string,
  clubName: string,
): Promise<{ ok: true } | { ok: false; error: string }> {
  const matches = await searchCoachManagersMongo(username, clubName);
  if (matches.length === 0) {
    return {
      ok: false,
      error:
        "No Coach Manager found for that username and/or club name (club_name).",
    };
  }
  if (matches.length > 1) {
    return { ok: false, error: "Multiple rows matched; narrow with username and club name." };
  }
  const target = matches[0]!;
  const st = target.status.trim().toUpperCase();
  if (st === "ACTIVE" && target.isActivated) {
    return { ok: false, error: "This Coach Manager is already active." };
  }
  return setMainActivationMongo(target.uid, true);
}

export async function deactivateCoachManagerMongo(
  username: string,
  clubName: string,
): Promise<{ ok: true } | { ok: false; error: string }> {
  const matches = await searchCoachManagersMongo(username, clubName);
  if (matches.length === 0) {
    return {
      ok: false,
      error:
        "No Coach Manager found for that username and/or club name (club_name).",
    };
  }
  if (matches.length > 1) {
    return { ok: false, error: "Multiple rows matched; narrow with username and club name." };
  }
  const target = matches[0]!;
  if (target.status.trim().toUpperCase() === "INACTIVE") {
    return { ok: false, error: "This Coach Manager is already inactive." };
  }
  return setMainActivationMongo(target.uid, false);
}

export async function activateCoachRoleLoginMongo(
  username: string,
  clubName: string,
): Promise<{ ok: true } | { ok: false; error: string }> {
  const matches = await searchCoachRoleByUsernameOrClubMongo(username, clubName);
  if (matches.length === 0) {
    return {
      ok: false,
      error:
        "No Coach found for that username and/or club name (club_name).",
    };
  }
  if (matches.length > 1) {
    return { ok: false, error: "Multiple rows matched; narrow with username and club name." };
  }
  const t = matches[0]!;
  const st = t.status.trim().toUpperCase();
  if (st === "ACTIVE" && t.isActivated) {
    return { ok: false, error: "This Coach login is already active." };
  }
  return setRoleActivationMongo(t.uid, "coach", true);
}

export async function deactivateCoachRoleLoginMongo(
  username: string,
  clubName: string,
): Promise<{ ok: true } | { ok: false; error: string }> {
  const matches = await searchCoachRoleByUsernameOrClubMongo(username, clubName);
  if (matches.length === 0) {
    return {
      ok: false,
      error:
        "No Coach found for that username and/or club name (club_name).",
    };
  }
  if (matches.length > 1) {
    return { ok: false, error: "Multiple rows matched; narrow with username and club name." };
  }
  const t = matches[0]!;
  if (t.status.trim().toUpperCase() === "INACTIVE") {
    return { ok: false, error: "This Coach login is already inactive." };
  }
  return setRoleActivationMongo(t.uid, "coach", false);
}

export async function activateStudentRoleLoginMongo(
  username: string,
  clubName: string,
): Promise<{ ok: true } | { ok: false; error: string }> {
  const matches = await searchStudentRoleByUsernameOrClubMongo(username, clubName);
  if (matches.length === 0) {
    return {
      ok: false,
      error:
        "No Student found for that username and/or club name (club_name).",
    };
  }
  if (matches.length > 1) {
    return { ok: false, error: "Multiple rows matched; narrow with username and club name." };
  }
  const t = matches[0]!;
  const st = t.status.trim().toUpperCase();
  if (st === "ACTIVE" && t.isActivated) {
    return { ok: false, error: "This Student login is already active." };
  }
  return setRoleActivationMongo(t.uid, "student", true);
}

export async function deactivateStudentRoleLoginMongo(
  username: string,
  clubName: string,
): Promise<{ ok: true } | { ok: false; error: string }> {
  const matches = await searchStudentRoleByUsernameOrClubMongo(username, clubName);
  if (matches.length === 0) {
    return {
      ok: false,
      error:
        "No Student found for that username and/or club name (club_name).",
    };
  }
  if (matches.length > 1) {
    return { ok: false, error: "Multiple rows matched; narrow with username and club name." };
  }
  const t = matches[0]!;
  if (t.status.trim().toUpperCase() === "INACTIVE") {
    return { ok: false, error: "This Student login is already inactive." };
  }
  return setRoleActivationMongo(t.uid, "student", false);
}

/** Resolve full name + expiry for subscription payment record (Mongo). */
export async function resolveFullNameAndExpiryMongo(
  username: string,
  role: string,
): Promise<{ full_name: string; Expiry_date: string }> {
  const r = String(role || "").trim();
  if (r === "Coach") {
    const login = await findCoachRoleLoginByUsernameMongo(username);
    if (login) {
      return {
        full_name: login.fullName.trim(),
        Expiry_date: String(login.expiryDate ?? "").trim(),
      };
    }
    return { full_name: "", Expiry_date: "" };
  }
  if (r === "Student") {
    const login = await findStudentRoleLoginByUsernameMongo(username);
    if (login) {
      return {
        full_name: login.fullName.trim(),
        Expiry_date: String(login.expiryDate ?? "").trim(),
      };
    }
    return { full_name: "", Expiry_date: "" };
  }
  const row = await findUserByUsernameMongo(username);
  if (row) {
    return {
      full_name: row.fullName.trim(),
      Expiry_date: String(row.expiryDate ?? "").trim(),
    };
  }
  return { full_name: "", Expiry_date: "" };
}
