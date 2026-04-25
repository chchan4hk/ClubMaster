import {
  getAuthUserLoginCollection,
  isMongoConfigured,
  type UserLoginDocument,
} from "./db/DBConnection";
import { isValidClubFolderId } from "./coachListCsv";
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
    base.coachId =
      String(doc.coach_id ?? doc.uid ?? "").trim() || base.uid;
  } else if (doc.usertype === "Student") {
    base.studentId =
      String(doc.student_id ?? doc.uid ?? "").trim() || base.uid;
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
  if (!doc) {
    const sidRe = new RegExp(`^${escapeRegex(key)}$`, "i");
    doc = await coll.findOne({
      usertype: "Student",
      student_id: sidRe,
    });
  }
  if (!doc) {
    const cidRe = new RegExp(`^${escapeRegex(key)}$`, "i");
    doc = await coll.findOne({
      usertype: "Coach",
      coach_id: cidRe,
    });
  }
  return doc as UserLoginDocument | null;
}

const LOGIN_COLlation = { locale: "en", strength: 2 } as const;

/**
 * Coach role-login row in Mongo `userLogin` for GET /api/me when JWT `sub` is login uid,
 * roster CoachID, or when {@link findUserLoginDocumentByUid} misses (e.g. legacy tokens).
 * Prefers match on `club_folder_uid` / `club_id` + `username` when JWT carries the club folder.
 */
async function findCoachUserLoginForProfileMongo(
  uid: string,
  opts?: { username?: string; clubFolderUid?: string },
): Promise<UserLoginDocument | null> {
  const coll = await getAuthUserLoginCollection();
  const uname = String(opts?.username ?? "").trim();
  const club = String(opts?.clubFolderUid ?? "").trim();
  const idKey = String(uid ?? "").trim();
  const hasClub = club.length > 0 && isValidClubFolderId(club);
  const folderRe = hasClub ? new RegExp(`^${escapeRegex(club)}$`, "i") : null;

  if (uname && folderRe) {
    const byClubUser = await coll.findOne(
      {
        usertype: "Coach",
        username: uname,
        $or: [{ club_folder_uid: folderRe }, { club_id: folderRe }],
      },
      { collation: LOGIN_COLlation },
    );
    if (byClubUser) {
      return byClubUser as UserLoginDocument;
    }
  }

  if (idKey) {
    const idRe = new RegExp(`^${escapeRegex(idKey)}$`, "i");
    const identityOr: object[] = [{ uid: idRe }, { coach_id: idRe }];
    if (folderRe) {
      const byClubId = await coll.findOne({
        usertype: "Coach",
        $and: [
          { $or: identityOr },
          { $or: [{ club_folder_uid: folderRe }, { club_id: folderRe }] },
        ],
      });
      if (byClubId) {
        return byClubId as UserLoginDocument;
      }
    }
    const byId = await coll.findOne({
      usertype: "Coach",
      $or: identityOr,
    });
    if (byId) {
      return byId as UserLoginDocument;
    }
  }

  if (uname) {
    const byName = await coll.findOne(
      { usertype: "Coach", username: uname },
      { collation: LOGIN_COLlation },
    );
    return (byName as UserLoginDocument) ?? null;
  }

  return null;
}

export type MeProfileFromUserLogin = {
  fromCsv: ReturnType<typeof accountPayloadFromCsvRow> | null;
  coachLogin: ReturnType<typeof roleLoginProfileFromRow> | null;
  studentLogin: ReturnType<typeof roleLoginProfileFromRow> | null;
};

/**
 * Profile slices for GET /api/me from MongoDB `ClubMaster_DB` / `userLogin`.
 * Uses JWT `sub` first; for Coach, also resolves by `club_folder_uid`/`club_id` + username
 * or roster `coach_id` when the login row is not keyed by `uid` alone.
 */
export async function loadMeProfileFromUserLoginMongo(
  uid: string,
  jwtRole: string | undefined,
  opts?: { username?: string; clubFolderUid?: string },
): Promise<MeProfileFromUserLogin | null> {
  if (!isMongoConfigured()) {
    return null;
  }
  let doc = await findUserLoginDocumentByUid(uid);
  if (!doc && jwtRole === "Coach") {
    doc = await findCoachUserLoginForProfileMongo(uid, opts);
  }
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
