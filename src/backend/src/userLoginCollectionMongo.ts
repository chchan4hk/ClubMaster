import {
  getAuthUserLoginCollection,
  isMongoConfigured,
  type UserLoginDocument,
} from "./db/DBConnection";
import {
  findCoachManagerClubUidByClubName,
  mapUserTypeToRole,
  type CsvUser,
} from "./userlistCsv";
import { accountPayloadFromCsvRow } from "./routes/userAccountRoutes";
import type { CoachStudentLoginRow } from "./coachStudentLoginCsv";

function escapeRegex(s: string): string {
  return s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function formatDateOnly(d: Date | undefined | null): string {
  if (!d || !(d instanceof Date) || Number.isNaN(d.getTime())) {
    return "";
  }
  return d.toISOString().slice(0, 10);
}

/**
 * Map a Mongo `userLogin` document to the in-memory `CsvUser` shape used by
 * `accountPayloadFromCsvRow` and `/api/me`.
 */
export function userLoginDocumentToCsvUser(doc: UserLoginDocument): CsvUser | null {
  const role = mapUserTypeToRole(doc.usertype);
  if (!role) {
    return null;
  }
  const pwd = String(doc.password ?? "").trim();
  return {
    uid: String(doc.uid ?? "").trim(),
    lineIndex: 0,
    usertype: doc.usertype,
    username: String(doc.username ?? "").trim(),
    password: "",
    passwordHash: pwd || null,
    fullName: String(doc.full_name ?? "").trim(),
    isActivated: Boolean(doc.is_activated),
    role,
    creationDate: formatDateOnly(doc.creation_date),
    clubName: String(doc.club_name ?? "").trim(),
    clubPhoto: String(doc.club_photo ?? "").trim(),
    status: doc.status ? "ACTIVE" : "INACTIVE",
    lastUpdateDate: formatDateOnly(doc.lastUpdate_date),
    expiryDate: formatDateOnly(doc.Expiry_date),
  };
}

function userLoginDocToCoachStudentRow(doc: UserLoginDocument): CoachStudentLoginRow {
  const pwd = String(doc.password ?? "").trim();
  const base: CoachStudentLoginRow = {
    uid: String(doc.uid ?? "").trim(),
    username: String(doc.username ?? "").trim(),
    password: "",
    passwordHash: pwd || null,
    fullName: String(doc.full_name ?? "").trim(),
    isActivated: Boolean(doc.is_activated),
    clubName: String(doc.club_name ?? "").trim(),
    status: doc.status ? "ACTIVE" : "INACTIVE",
    creationDate: formatDateOnly(doc.creation_date),
    lastUpdateDate: formatDateOnly(doc.lastUpdate_date),
    expiryDate: formatDateOnly(doc.Expiry_date),
  };
  if (doc.usertype === "Coach") {
    base.coachId = String(doc.uid ?? "").trim();
  } else if (doc.usertype === "Student") {
    base.studentId = String(doc.uid ?? "").trim();
  }
  const fromDoc = String(doc.club_folder_uid ?? doc.club_id ?? "").trim();
  if (fromDoc) {
    base.clubFolderUid = fromDoc;
  } else {
    const club = (doc.club_name ?? "").trim();
    const folder = findCoachManagerClubUidByClubName(club);
    if (folder) {
      base.clubFolderUid = folder;
    }
  }
  return base;
}

function roleLoginProfileFromRow(row: CoachStudentLoginRow): {
  club_name: string;
  creation_date: string;
  full_name: string;
  expiry_date: string;
  club_folder_uid: string;
} {
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

export async function findUserLoginDocumentByUid(
  uid: string,
): Promise<UserLoginDocument | null> {
  if (!isMongoConfigured()) {
    return null;
  }
  const key = String(uid ?? "").trim();
  if (!key) {
    return null;
  }
  const coll = await getAuthUserLoginCollection();
  let doc = await coll.findOne({ uid: key });
  if (!doc) {
    doc = await coll.findOne({
      uid: new RegExp(`^${escapeRegex(key)}$`, "i"),
    });
  }
  return doc as UserLoginDocument | null;
}

export type MeProfileFromUserLogin = {
  fromCsv: ReturnType<typeof accountPayloadFromCsvRow> | null;
  coachLogin: ReturnType<typeof roleLoginProfileFromRow> | null;
  studentLogin: ReturnType<typeof roleLoginProfileFromRow> | null;
};

/**
 * Profile slices for GET /api/me from MongoDB `userLogin` when a document exists for JWT `sub`.
 */
export async function loadMeProfileFromUserLoginMongo(
  uid: string,
  jwtRole: string | undefined,
): Promise<MeProfileFromUserLogin | null> {
  if (!isMongoConfigured()) {
    return null;
  }
  const doc = await findUserLoginDocumentByUid(uid);
  if (!doc) {
    return null;
  }
  const csvUser = userLoginDocumentToCsvUser(doc);
  if (!csvUser) {
    return null;
  }
  const fromCsv = accountPayloadFromCsvRow(csvUser);
  const role = jwtRole ?? csvUser.role;

  let coachLogin: ReturnType<typeof roleLoginProfileFromRow> | null = null;
  let studentLogin: ReturnType<typeof roleLoginProfileFromRow> | null = null;

  if (role === "Coach") {
    coachLogin = roleLoginProfileFromRow(userLoginDocToCoachStudentRow(doc));
  } else if (role === "Student") {
    studentLogin = roleLoginProfileFromRow(userLoginDocToCoachStudentRow(doc));
  }

  return { fromCsv, coachLogin, studentLogin };
}
