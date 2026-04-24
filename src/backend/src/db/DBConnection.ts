/**
 * MongoDB client + collections (`userLogin`, `clubInfo`, `basicInfo`, `LessonList`, `LessonSeriesInfo`, `CoachSalary`, `PrizeList`, etc.) on the app database
 * (default name `ClubMaster_DB`, overridable via env — see `resolveUserLoginDatabaseName`).
 */
import {
  type Collection,
  type Db,
  type Document,
  MongoClient,
  type MongoClientOptions,
  type ObjectId,
} from "mongodb";

/** Default MongoDB database name when `MONGO_USERLOGIN_DB` / `MONGO_DATABASE` are unset. */
export const DEFAULT_MONGO_APP_DATABASE = "ClubMaster_DB";

/** Logical name for user login documents (MongoDB collection). */
export const USER_LOGIN_COLLECTION = "userLogin";

/** Admin + unified login accounts (main / coach / student) for Mongo-backed flows. */
export const USER_LIST_COLLECTION = "userList";

/** Per-club student roster rows (see `data_club/{clubId}/UserList_Student.json`). */
export const USER_LIST_STUDENT_COLLECTION = "UserList_Student";

/** Per-club coach roster rows (see `data_club/{clubId}/UserList_Coach.json`). */
export const USER_LIST_COACH_COLLECTION = "UserList_Coach";

/** Club profile + payment QR paths (Coach Manager uses Mongo `clubInfo`; image files may live under `data_club/{clubId}/Image/`). */
export const CLUB_INFO_COLLECTION = "clubInfo";

/** Reference lists (countries, sport types) seeded from `data/BasicInfo.json`. */
export const BASIC_INFO_COLLECTION = "basicInfo";

/** Fixed `_id` for the single lists document in `basicInfo`. */
export const BASIC_INFO_LISTS_DOC_ID = "basicInfoLists";

/** Per-club `LessonList.json` mirror in Mongo (`LessonList` collection, `_id` = club folder UID). */
export const LESSON_LIST_COLLECTION = "LessonList";

/**
 * Per-session / series rows for lessons (e.g. one document per club + lesson + date/time slot).
 * Lives in {@link DEFAULT_MONGO_APP_DATABASE} by default (see {@link resolveLessonSeriesInfoDatabaseName}).
 */
export const LESSON_SERIES_INFO_COLLECTION = "LessonSeriesInfo";

/** Per-club coach salary rows (mirror of `data_club/{clubId}/CoachSalary.json` entries). */
export const COACH_SALARY_COLLECTION = "CoachSalary";

/** Per-club prize rows (mirror of `data_club/{clubId}/PrizeList.json` entries, API field names). */
export const PRIZE_LIST_ROW_COLLECTION = "PrizeList";

export const USER_TYPE_VALUES = [
  "Administrator",
  "Coach Manager",
  "Coach",
  "Student",
] as const;

export type UserType = (typeof USER_TYPE_VALUES)[number];

/**
 * `userLogin` document shape. Dates are stored as BSON date (`Date` in Node).
 * Field names match your app data (`Expiry_date` capital E).
 */
export interface UserLoginDocument {
  _id?: ObjectId;
  uid: string;
  usertype: UserType;
  username: string;
  password: string;
  full_name: string;
  is_activated: boolean;
  creation_date: Date;
  club_name: string;
  club_photo: string;
  status: boolean;
  lastUpdate_date: Date;
  /** Subscription / account end; null or absent = no fixed expiry in UI. */
  Expiry_date: Date | null;
  /** Coach Manager folder UID for coach/student role rows (optional). */
  club_folder_uid?: string;
  coach_id?: string;
  student_id?: string;
  /** Optional class grouping (e.g. lesson roster / billing context). */
  class_id?: string;
  /**
   * Coach Manager club folder id for Coach/Student logins (e.g. `CM00000001`);
   * set together with `club_folder_uid` when the manager creates the login.
   */
  club_id?: string;
}

export type UserLoginInsert = Omit<UserLoginDocument, "_id">;

/**
 * One row per club in Mongo (`clubInfo`). String dates match legacy ClubInfo text fields.
 */
export interface ClubInfoDocument {
  _id?: ObjectId;
  club_id: string;
  Currency: string;
  Sport_type: string;
  Club_name: string;
  country: string;
  setup_date: string;
  /** Club contact person (Coach Manager / Club Master). */
  contact_point: string;
  /** Club contact email. */
  contact_email: string;
  club_desc: string;
  club_logo: string;
  club_payment_payme: string;
  club_payment_FPS: string;
  club_payment_wechat: string;
  club_payment_alipay: string;
  lastUpdate_date: string;
  /** Alipay channel label variant (same file as `clubInfoJson` extended keys). */
  club_payment_支付寶: string;
}

export type ClubInfoInsert = Omit<ClubInfoDocument, "_id">;

/**
 * One student row in Mongo `UserList_Student` (matches club folder JSON + `club_folder_uid` for scope).
 */
export interface UserListStudentDocument {
  _id?: ObjectId;
  /** Club folder UID (e.g. `CM00000003`); added when importing from `data_club/{id}/`. */
  club_folder_uid: string;
  student_id: string;
  /** Same as `club_id` in `UserList_Student.json` (folder UID, e.g. `CM00000003`). */
  club_id: string;
  full_name: string;
  sex: string;
  email: string;
  contact_number: string;
  guardian: string;
  guardian_contact: string;
  school: string;
  student_coach: string;
  status: string;
  creation_date: string;
  remark: string;
  lastUpdate_date: string;
  date_of_birth: string;
  joined_date: string;
  home_address: string;
  country: string;
}

export type UserListStudentInsert = Omit<UserListStudentDocument, "_id">;

/**
 * One canonical document in Mongo `basicInfo` (same shape as `BasicInfoLists` from CSV/JSON).
 */
export interface BasicInfoListsDocument {
  _id: string;
  countries: string[];
  sportTypes: string[];
  /** Set when imported via `mongo:seed-basicinfo-json`. */
  lastImportedAt?: Date;
}

/** MongoDB `$jsonSchema` validator for the `basicInfo` collection (lists document). */
export const basicInfoJsonSchema: Document = {
  bsonType: "object",
  required: ["_id", "countries", "sportTypes"],
  properties: {
    _id: { bsonType: "string" },
    countries: { bsonType: "array", items: { bsonType: "string" } },
    sportTypes: { bsonType: "array", items: { bsonType: "string" } },
    lastImportedAt: { bsonType: ["date", "null"] },
  },
  additionalProperties: true,
};

/**
 * One document per club: mirrors `data_club/{clubId}/LessonList.json` (`version` + `lessons`).
 */
export interface LessonListClubDocument {
  _id: string;
  club_id: string;
  version: number;
  lessons: Document[];
  lastImportedAt?: Date;
}

/** MongoDB `$jsonSchema` validator for the `LessonList` collection (per-club document). */
export const lessonListJsonSchema: Document = {
  bsonType: "object",
  required: ["_id", "club_id", "version", "lessons"],
  properties: {
    _id: { bsonType: "string" },
    club_id: { bsonType: "string" },
    version: { bsonType: ["int", "long", "double"] },
    lessons: { bsonType: "array" },
    lastImportedAt: { bsonType: ["date", "null"] },
  },
  additionalProperties: true,
};

/**
 * One row per lesson series / session instance in Mongo `LessonSeriesInfo`.
 * Field names match the requested API shape (`ClubID`, `lessonId`, …).
 */
export interface LessonSeriesInfoDocument {
  _id?: ObjectId;
  ClubID: string;
  lessonId: string;
  sportType: string;
  year: string;
  classId: string;
  /** Preferred format YYYY-MM-DD (stored as string for parity with file-backed lesson data). */
  lesson_date: string;
  lesson_time: string;
  sportCenter: string;
  courtNo: string;
  coachName: string;
  status: string;
  createdAt: string;
  lastUpdatedDate: string;
  /** Optional free text; omit or use "" when not needed. */
  remarks?: string;
  /**
   * Roster for this session row: prefer `string[]` in MongoDB; legacy docs may use a single
   * comma / newline separated string.
   */
  studentList?: string | string[];
}

export type LessonSeriesInfoInsert = Omit<LessonSeriesInfoDocument, "_id">;

/** MongoDB `$jsonSchema` validator for the `LessonSeriesInfo` collection. */
export const lessonSeriesInfoJsonSchema: Document = {
  bsonType: "object",
  required: [
    "ClubID",
    "lessonId",
    "sportType",
    "year",
    "classId",
    "lesson_date",
    "lesson_time",
    "sportCenter",
    "courtNo",
    "coachName",
    "status",
    "createdAt",
    "lastUpdatedDate",
  ],
  properties: {
    _id: { bsonType: "objectId" },
    ClubID: { bsonType: "string" },
    lessonId: { bsonType: "string" },
    sportType: { bsonType: "string" },
    year: { bsonType: "string" },
    classId: { bsonType: "string" },
    lesson_date: { bsonType: "string" },
    lesson_time: { bsonType: "string" },
    sportCenter: { bsonType: "string" },
    courtNo: { bsonType: "string" },
    coachName: { bsonType: "string" },
    status: { bsonType: "string" },
    createdAt: { bsonType: "string" },
    lastUpdatedDate: { bsonType: "string" },
    remarks: { bsonType: "string" },
    studentList: {
      bsonType: ["array", "string"],
      items: { bsonType: "string" },
    },
  },
  additionalProperties: true,
};

/**
 * One document per salary row (`CoachSalary.json` / API shape). Dates stay as strings
 * to match the JSON file; optional `lastImportedAt` for seed scripts.
 */
export interface CoachSalaryDocument {
  _id?: ObjectId;
  CoachSalaryID: string;
  lessonId: string;
  ClubID: string;
  club_name: string;
  coach_id: string;
  salary_amount: number;
  Payment_Method: string;
  Payment_Status: string;
  Payment_Confirm: boolean;
  createdAt: string;
  lastUpdatedDate: string;
  lastImportedAt?: Date;
}

export type CoachSalaryInsert = Omit<CoachSalaryDocument, "_id">;

/** MongoDB `$jsonSchema` validator for the `CoachSalary` collection. */
export const coachSalaryJsonSchema: Document = {
  bsonType: "object",
  required: [
    "CoachSalaryID",
    "lessonId",
    "ClubID",
    "club_name",
    "coach_id",
    "salary_amount",
    "Payment_Method",
    "Payment_Status",
    "Payment_Confirm",
    "createdAt",
    "lastUpdatedDate",
  ],
  properties: {
    _id: { bsonType: "objectId" },
    CoachSalaryID: { bsonType: "string" },
    lessonId: { bsonType: "string" },
    ClubID: { bsonType: "string" },
    club_name: { bsonType: "string" },
    coach_id: { bsonType: "string" },
    salary_amount: { bsonType: ["double", "int", "long", "decimal"] },
    Payment_Method: { bsonType: "string" },
    Payment_Status: { bsonType: "string" },
    Payment_Confirm: { bsonType: "bool" },
    createdAt: { bsonType: "string" },
    lastUpdatedDate: { bsonType: "string" },
    lastImportedAt: { bsonType: ["date", "null"] },
  },
  additionalProperties: true,
};

/**
 * One prize row (same logical fields as `prizeCsvRowToApiFields` / `PRIZE_LIST_COLUMNS`).
 * Optional `lastImportedAt` for seed scripts.
 */
export interface PrizeListRowDocument {
  _id?: ObjectId;
  PrizeID: string;
  ClubID: string;
  Club_name: string;
  SportType: string;
  Year: string;
  Association: string;
  Competition: string;
  Age_group: string;
  Prize_type: string;
  StudentName: string;
  Ranking: string;
  Status: string;
  Created_at: string;
  LastUpdated_Date: string;
  VerifiedBy: string;
  Remarks: string;
  lastImportedAt?: Date;
}

export type PrizeListRowInsert = Omit<PrizeListRowDocument, "_id">;

/** MongoDB `$jsonSchema` validator for the `PrizeList` collection (per-row documents). */
export const prizeListRowJsonSchema: Document = {
  bsonType: "object",
  required: [
    "PrizeID",
    "ClubID",
    "Club_name",
    "SportType",
    "Year",
    "Association",
    "Competition",
    "Age_group",
    "Prize_type",
    "StudentName",
    "Ranking",
    "Status",
    "Created_at",
    "LastUpdated_Date",
    "VerifiedBy",
    "Remarks",
  ],
  properties: {
    _id: { bsonType: "objectId" },
    PrizeID: { bsonType: "string" },
    ClubID: { bsonType: "string" },
    Club_name: { bsonType: "string" },
    SportType: { bsonType: "string" },
    Year: { bsonType: "string" },
    Association: { bsonType: "string" },
    Competition: { bsonType: "string" },
    Age_group: { bsonType: "string" },
    Prize_type: { bsonType: "string" },
    StudentName: { bsonType: "string" },
    Ranking: { bsonType: "string" },
    Status: { bsonType: "string" },
    Created_at: { bsonType: "string" },
    LastUpdated_Date: { bsonType: "string" },
    VerifiedBy: { bsonType: "string" },
    Remarks: { bsonType: "string" },
    lastImportedAt: { bsonType: ["date", "null"] },
  },
  additionalProperties: true,
};

/** MongoDB `$jsonSchema` validator for the `clubInfo` collection. */
export const clubInfoJsonSchema: Document = {
  bsonType: "object",
  required: [
    "club_id",
    "Currency",
    "Sport_type",
    "Club_name",
    "country",
    "setup_date",
    "club_desc",
    "club_logo",
    "club_payment_payme",
    "club_payment_FPS",
    "club_payment_wechat",
    "club_payment_alipay",
    "lastUpdate_date",
    "club_payment_支付寶",
  ],
  properties: {
    _id: { bsonType: "objectId" },
    club_id: { bsonType: "string" },
    Currency: { bsonType: "string" },
    Sport_type: { bsonType: "string" },
    Club_name: { bsonType: "string" },
    country: { bsonType: "string" },
    setup_date: { bsonType: "string" },
    contact_point: { bsonType: "string" },
    contact_email: { bsonType: "string" },
    club_desc: { bsonType: "string" },
    club_logo: { bsonType: "string" },
    club_payment_payme: { bsonType: "string" },
    club_payment_FPS: { bsonType: "string" },
    club_payment_wechat: { bsonType: "string" },
    club_payment_alipay: { bsonType: "string" },
    lastUpdate_date: { bsonType: "string" },
    club_payment_支付寶: { bsonType: "string" },
  },
  additionalProperties: false,
};

/** MongoDB `$jsonSchema` validator for the `userLogin` collection. */
export const userLoginJsonSchema: Document = {
  bsonType: "object",
  required: [
    "uid",
    "usertype",
    "username",
    "password",
    "full_name",
    "is_activated",
    "creation_date",
    "club_name",
    "club_photo",
    "status",
    "lastUpdate_date",
  ],
  properties: {
    _id: { bsonType: "objectId" },
    uid: { bsonType: "string" },
    usertype: {
      bsonType: "string",
      enum: [...USER_TYPE_VALUES],
    },
    username: { bsonType: "string" },
    password: { bsonType: "string" },
    full_name: { bsonType: "string" },
    is_activated: { bsonType: "bool" },
    creation_date: { bsonType: "date" },
    club_name: { bsonType: "string" },
    club_photo: { bsonType: "string" },
    status: { bsonType: "bool" },
    lastUpdate_date: { bsonType: "date" },
    Expiry_date: { bsonType: ["date", "null"] },
    club_folder_uid: { bsonType: "string" },
    coach_id: { bsonType: "string" },
    student_id: { bsonType: "string" },
    class_id: { bsonType: "string" },
    club_id: { bsonType: "string" },
  },
  additionalProperties: false,
};

/**
 * Zeabur Mongo defaults (override with `MONGO_HOST` / `MONGO_PORT` / `MONGO_USER`).
 * Secrets must come from `MONGODB_URI` / `MONGO_URI` or `MONGO_PASSWORD` (never commit passwords).
 */
export const MONGO_DEFAULT_HOST = "101.32.219.59";
export const MONGO_DEFAULT_PORT = 30791;
export const MONGO_DEFAULT_USERNAME = "mongo";

export function resolveMongoHost(): string {
  return process.env.MONGO_HOST?.trim() || MONGO_DEFAULT_HOST;
}

export function resolveMongoPort(): number {
  const raw = process.env.MONGO_PORT?.trim() || String(MONGO_DEFAULT_PORT);
  const n = Number.parseInt(raw, 10);
  return Number.isFinite(n) && n > 0 ? n : MONGO_DEFAULT_PORT;
}

export function resolveMongoUsername(): string {
  return process.env.MONGO_USER?.trim() || MONGO_DEFAULT_USERNAME;
}

function resolveMongoPassword(): string | undefined {
  return (
    process.env.MONGO_PASSWORD?.trim() ||
    process.env.MONGODB_PASSWORD?.trim() ||
    process.env.MONGODB_USER_PASSWORD?.trim()
  );
}

function resolveMongoUriFromEnv(): string | undefined {
  const u = process.env.MONGODB_URI?.trim() || process.env.MONGO_URI?.trim();
  return u || undefined;
}

/**
 * Connection string: prefer full URI from the platform (`MONGODB_URI` / `MONGO_URI`),
 * otherwise build from user/password/host/port.
 */
export function buildMongoUri(): string {
  const fromEnv = resolveMongoUriFromEnv();
  if (fromEnv) {
    return fromEnv;
  }
  const password = resolveMongoPassword();
  if (!password) {
    throw new Error(
      "MongoDB is not configured: set MONGODB_URI / MONGO_URI, or set MONGO_PASSWORD " +
        "(optional: MONGO_HOST, MONGO_PORT, MONGO_USER).",
    );
  }
  const user = encodeURIComponent(resolveMongoUsername());
  const pass = encodeURIComponent(password);
  const host = resolveMongoHost();
  const port = resolveMongoPort();
  return `mongodb://${user}:${pass}@${host}:${port}`;
}

export function isMongoConfigured(): boolean {
  return Boolean(resolveMongoUriFromEnv() || resolveMongoPassword());
}

function clientOptions(): MongoClientOptions {
  const maxPoolSize =
    Number.parseInt(process.env.MONGO_POOL_MAX || "10", 10) || 10;
  return {
    serverSelectionTimeoutMS: 15_000,
    maxPoolSize,
  };
}

let clientSingleton: MongoClient | null = null;
let connecting: Promise<MongoClient> | null = null;

/**
 * Shared Mongo client (Zeabur). Lazily connects; throws if credentials are missing.
 */
export async function getMongoClient(): Promise<MongoClient> {
  if (clientSingleton) {
    return clientSingleton;
  }
  if (!isMongoConfigured()) {
    throw new Error(
      "MongoDB is not configured: set MONGODB_URI / MONGO_URI or MONGO_PASSWORD (see DBConnection.ts).",
    );
  }
  if (connecting) {
    return connecting;
  }
  const uri = buildMongoUri();
  const client = new MongoClient(uri, clientOptions());
  connecting = client
    .connect()
    .then(() => {
      clientSingleton = client;
      connecting = null;
      return client;
    })
    .catch((err: unknown) => {
      connecting = null;
      throw err;
    });
  return connecting;
}

/** Returns a client only when URI or password is set; otherwise `null`. */
export async function getMongoClientOrNull(): Promise<MongoClient | null> {
  if (!isMongoConfigured()) {
    return null;
  }
  return getMongoClient();
}

/**
 * Default database name: explicit arg, else `MONGO_DATABASE`, else {@link DEFAULT_MONGO_APP_DATABASE}.
 */
export async function getMongoDb(databaseName?: string): Promise<Db> {
  const name =
    databaseName?.trim() ||
    process.env.MONGO_DATABASE?.trim() ||
    DEFAULT_MONGO_APP_DATABASE;
  const client = await getMongoClient();
  return client.db(name);
}

/**
 * Database that holds `userLogin`, `clubInfo`, etc.: explicit arg, else `MONGO_USERLOGIN_DB`,
 * else `MONGO_DATABASE`, else {@link DEFAULT_MONGO_APP_DATABASE}.
 */
export function resolveUserLoginDatabaseName(explicit?: string): string {
  const t = explicit?.trim();
  if (t) {
    return t;
  }
  return (
    process.env.MONGO_USERLOGIN_DB?.trim() ||
    process.env.MONGO_DATABASE?.trim() ||
    DEFAULT_MONGO_APP_DATABASE
  );
}

/**
 * Database for sign-in (`main.html` → `/auth/login-with-context`) and related `userLogin` / `clubInfo` reads.
 * Defaults to {@link DEFAULT_MONGO_APP_DATABASE} (`ClubMaster_DB`). Set `MONGO_AUTH_USERLOGIN_DB` only to
 * use a different DB (e.g. legacy `userLogin`). Does not read `MONGO_USERLOGIN_DB`.
 */
export function resolveAuthLoginDatabaseName(explicit?: string): string {
  const t = explicit?.trim();
  if (t) {
    return t;
  }
  return (
    process.env.MONGO_AUTH_USERLOGIN_DB?.trim() ||
    DEFAULT_MONGO_APP_DATABASE
  );
}

/**
 * Same collection as {@link getUserLoginCollection}; database from {@link resolveAuthLoginDatabaseName}.
 */
export async function getAuthUserLoginCollection(
  databaseName?: string,
): Promise<Collection<UserLoginDocument>> {
  const db = await getMongoDb(resolveAuthLoginDatabaseName(databaseName));
  return db.collection<UserLoginDocument>(USER_LOGIN_COLLECTION);
}

/**
 * Database for the `basicInfo` collection: explicit arg, else `MONGO_BASICINFO_TARGET_DB`,
 * else {@link DEFAULT_MONGO_APP_DATABASE} (`ClubMaster_DB`) so lists stay with the app DB name.
 */
export function resolveBasicInfoDatabaseName(explicit?: string): string {
  const t = explicit?.trim();
  if (t) {
    return t;
  }
  return (
    process.env.MONGO_BASICINFO_TARGET_DB?.trim() ||
    DEFAULT_MONGO_APP_DATABASE
  );
}

/**
 * Database for the `LessonList` collection: explicit arg, else `MONGO_LESSONLIST_TARGET_DB`,
 * else {@link DEFAULT_MONGO_APP_DATABASE}.
 */
export function resolveLessonListDatabaseName(explicit?: string): string {
  const t = explicit?.trim();
  if (t) {
    return t;
  }
  return (
    process.env.MONGO_LESSONLIST_TARGET_DB?.trim() ||
    DEFAULT_MONGO_APP_DATABASE
  );
}

/**
 * Database for the `LessonSeriesInfo` collection: explicit arg, else `MONGO_LESSON_SERIES_INFO_TARGET_DB`,
 * else {@link DEFAULT_MONGO_APP_DATABASE} (`ClubMaster_DB`).
 */
export function resolveLessonSeriesInfoDatabaseName(explicit?: string): string {
  const t = explicit?.trim();
  if (t) {
    return t;
  }
  return (
    process.env.MONGO_LESSON_SERIES_INFO_TARGET_DB?.trim() ||
    DEFAULT_MONGO_APP_DATABASE
  );
}

/**
 * Database for the `CoachSalary` collection: explicit arg, else `MONGO_COACH_SALARY_TARGET_DB`,
 * else {@link DEFAULT_MONGO_APP_DATABASE}.
 */
export function resolveCoachSalaryDatabaseName(explicit?: string): string {
  const t = explicit?.trim();
  if (t) {
    return t;
  }
  return (
    process.env.MONGO_COACH_SALARY_TARGET_DB?.trim() ||
    DEFAULT_MONGO_APP_DATABASE
  );
}

/**
 * Database for the `PrizeList` collection: explicit arg, else `MONGO_PRIZE_LIST_TARGET_DB`,
 * else {@link DEFAULT_MONGO_APP_DATABASE}.
 */
export function resolvePrizeListRowDatabaseName(explicit?: string): string {
  const t = explicit?.trim();
  if (t) {
    return t;
  }
  return (
    process.env.MONGO_PRIZE_LIST_TARGET_DB?.trim() ||
    DEFAULT_MONGO_APP_DATABASE
  );
}

/**
 * Database for club roster collections `UserList_Coach` / `UserList_Student` (seed + future API reads).
 * Explicit arg, else `MONGO_USERLIST_ROSTER_TARGET_DB`, else {@link DEFAULT_MONGO_APP_DATABASE}.
 */
export function resolveUserListRosterDatabaseName(explicit?: string): string {
  const t = explicit?.trim();
  if (t) {
    return t;
  }
  return (
    process.env.MONGO_USERLIST_ROSTER_TARGET_DB?.trim() ||
    DEFAULT_MONGO_APP_DATABASE
  );
}

/**
 * Typed handle to the `userLogin` collection (same database as `getMongoDb`).
 * Uses `resolveUserLoginDatabaseName` when `databaseName` is omitted.
 */
export async function getUserLoginCollection(
  databaseName?: string,
): Promise<Collection<UserLoginDocument>> {
  const db = await getMongoDb(resolveUserLoginDatabaseName(databaseName));
  return db.collection<UserLoginDocument>(USER_LOGIN_COLLECTION);
}

/** Typed handle to the `userList` collection (same DB as `getUserLoginCollection`). */
export async function getUserListCollection(
  databaseName?: string,
): Promise<Collection<Document>> {
  const db = await getMongoDb(resolveUserLoginDatabaseName(databaseName));
  return db.collection(USER_LIST_COLLECTION);
}

/**
 * Typed handle to `UserList_Student` (see {@link resolveUserListRosterDatabaseName}).
 */
export async function getUserListStudentCollection(
  databaseName?: string,
): Promise<Collection<UserListStudentDocument>> {
  const db = await getMongoDb(resolveUserListRosterDatabaseName(databaseName));
  return db.collection<UserListStudentDocument>(USER_LIST_STUDENT_COLLECTION);
}

/**
 * Typed handle to `UserList_Coach` (see {@link resolveUserListRosterDatabaseName}).
 */
export async function getUserListCoachCollection(
  databaseName?: string,
): Promise<Collection<Document>> {
  const db = await getMongoDb(resolveUserListRosterDatabaseName(databaseName));
  return db.collection(USER_LIST_COACH_COLLECTION);
}

/** Typed handle to the `clubInfo` collection (same DB as sign-in — {@link resolveAuthLoginDatabaseName}). */
export async function getClubInfoCollection(
  databaseName?: string,
): Promise<Collection<ClubInfoDocument>> {
  const db = await getMongoDb(resolveAuthLoginDatabaseName(databaseName));
  return db.collection<ClubInfoDocument>(CLUB_INFO_COLLECTION);
}

/**
 * Creates the `clubInfo` collection if missing, applies `$jsonSchema` validation,
 * and ensures a unique index on `club_id`.
 */
export async function ensureClubInfoCollection(
  databaseName?: string,
): Promise<void> {
  const db = await getMongoDb(resolveAuthLoginDatabaseName(databaseName));
  const name = CLUB_INFO_COLLECTION;
  const exists =
    (await db.listCollections({ name }, { nameOnly: true }).toArray()).length >
    0;
  const validator: Document = { $jsonSchema: clubInfoJsonSchema };

  if (!exists) {
    await db.createCollection(name, {
      validator,
      validationLevel: "strict",
      validationAction: "error",
    });
  } else {
    await db.command({
      collMod: name,
      validator,
      validationLevel: "strict",
      validationAction: "error",
    });
  }

  await db
    .collection<ClubInfoDocument>(name)
    .createIndex({ club_id: 1 }, { unique: true });
}

/** Typed handle to the `basicInfo` collection (see {@link resolveBasicInfoDatabaseName}). */
export async function getBasicInfoCollection(
  databaseName?: string,
): Promise<Collection<BasicInfoListsDocument>> {
  const db = await getMongoDb(resolveBasicInfoDatabaseName(databaseName));
  return db.collection<BasicInfoListsDocument>(BASIC_INFO_COLLECTION);
}

/**
 * Creates the `basicInfo` collection if missing and applies `$jsonSchema` validation
 * for the lists document shape.
 */
export async function ensureBasicInfoCollection(
  databaseName?: string,
): Promise<void> {
  const db = await getMongoDb(resolveBasicInfoDatabaseName(databaseName));
  const name = BASIC_INFO_COLLECTION;
  const exists =
    (await db.listCollections({ name }, { nameOnly: true }).toArray()).length >
    0;
  const validator: Document = { $jsonSchema: basicInfoJsonSchema };

  if (!exists) {
    await db.createCollection(name, {
      validator,
      validationLevel: "strict",
      validationAction: "error",
    });
  } else {
    await db.command({
      collMod: name,
      validator,
      validationLevel: "strict",
      validationAction: "error",
    });
  }
}

/** Typed handle to the `LessonList` collection (see {@link resolveLessonListDatabaseName}). */
export async function getLessonListCollection(
  databaseName?: string,
): Promise<Collection<LessonListClubDocument>> {
  const db = await getMongoDb(resolveLessonListDatabaseName(databaseName));
  return db.collection<LessonListClubDocument>(LESSON_LIST_COLLECTION);
}

/**
 * Creates the `LessonList` collection if missing and applies `$jsonSchema` validation.
 * Unique index on `club_id` (matches `_id` for seeded club documents).
 */
export async function ensureLessonListCollection(
  databaseName?: string,
): Promise<void> {
  const db = await getMongoDb(resolveLessonListDatabaseName(databaseName));
  const name = LESSON_LIST_COLLECTION;
  const exists =
    (await db.listCollections({ name }, { nameOnly: true }).toArray()).length >
    0;
  const validator: Document = { $jsonSchema: lessonListJsonSchema };

  if (!exists) {
    await db.createCollection(name, {
      validator,
      validationLevel: "strict",
      validationAction: "error",
    });
  } else {
    await db.command({
      collMod: name,
      validator,
      validationLevel: "strict",
      validationAction: "error",
    });
  }

  await db
    .collection<LessonListClubDocument>(name)
    .createIndex({ club_id: 1 }, { unique: true });
}

/** Typed handle to the `LessonSeriesInfo` collection (see {@link resolveLessonSeriesInfoDatabaseName}). */
export async function getLessonSeriesInfoCollection(
  databaseName?: string,
): Promise<Collection<LessonSeriesInfoDocument>> {
  const db = await getMongoDb(resolveLessonSeriesInfoDatabaseName(databaseName));
  return db.collection<LessonSeriesInfoDocument>(LESSON_SERIES_INFO_COLLECTION);
}

/**
 * Creates the `LessonSeriesInfo` collection if missing, applies `$jsonSchema` validation,
 * and ensures indexes for club + lesson + date queries.
 */
export async function ensureLessonSeriesInfoCollection(
  databaseName?: string,
): Promise<void> {
  const db = await getMongoDb(resolveLessonSeriesInfoDatabaseName(databaseName));
  const name = LESSON_SERIES_INFO_COLLECTION;
  const exists =
    (await db.listCollections({ name }, { nameOnly: true }).toArray()).length >
    0;
  const validator: Document = { $jsonSchema: lessonSeriesInfoJsonSchema };

  if (!exists) {
    await db.createCollection(name, {
      validator,
      validationLevel: "strict",
      validationAction: "error",
    });
  } else {
    await db.command({
      collMod: name,
      validator,
      validationLevel: "strict",
      validationAction: "error",
    });
  }

  const coll = db.collection<LessonSeriesInfoDocument>(name);
  await coll.createIndex(
    { ClubID: 1, lessonId: 1, lesson_date: 1, lesson_time: 1 },
    { name: "lessonSeries_club_lesson_date_time" },
  );
  await coll.createIndex({ ClubID: 1, status: 1 }, { name: "lessonSeries_club_status" });
}

/** Typed handle to the `CoachSalary` collection (see {@link resolveCoachSalaryDatabaseName}). */
export async function getCoachSalaryCollection(
  databaseName?: string,
): Promise<Collection<CoachSalaryDocument>> {
  const db = await getMongoDb(resolveCoachSalaryDatabaseName(databaseName));
  return db.collection<CoachSalaryDocument>(COACH_SALARY_COLLECTION);
}

/**
 * Creates the `CoachSalary` collection if missing, applies `$jsonSchema` validation,
 * and ensures a unique index on `CoachSalaryID`.
 */
export async function ensureCoachSalaryCollection(
  databaseName?: string,
): Promise<void> {
  const db = await getMongoDb(resolveCoachSalaryDatabaseName(databaseName));
  const name = COACH_SALARY_COLLECTION;
  const exists =
    (await db.listCollections({ name }, { nameOnly: true }).toArray()).length >
    0;
  const validator: Document = { $jsonSchema: coachSalaryJsonSchema };

  if (!exists) {
    await db.createCollection(name, {
      validator,
      validationLevel: "strict",
      validationAction: "error",
    });
  } else {
    await db.command({
      collMod: name,
      validator,
      validationLevel: "strict",
      validationAction: "error",
    });
  }

  await db
    .collection<CoachSalaryDocument>(name)
    .createIndex({ CoachSalaryID: 1 }, { unique: true });
}

/** Typed handle to the `PrizeList` collection (see {@link resolvePrizeListRowDatabaseName}). */
export async function getPrizeListRowCollection(
  databaseName?: string,
): Promise<Collection<PrizeListRowDocument>> {
  const db = await getMongoDb(resolvePrizeListRowDatabaseName(databaseName));
  return db.collection<PrizeListRowDocument>(PRIZE_LIST_ROW_COLLECTION);
}

/**
 * Creates the `PrizeList` collection if missing, applies `$jsonSchema` validation,
 * and ensures a unique compound index on `ClubID` + `PrizeID`.
 */
export async function ensurePrizeListRowCollection(
  databaseName?: string,
): Promise<void> {
  const db = await getMongoDb(resolvePrizeListRowDatabaseName(databaseName));
  const name = PRIZE_LIST_ROW_COLLECTION;
  const exists =
    (await db.listCollections({ name }, { nameOnly: true }).toArray()).length >
    0;
  const validator: Document = { $jsonSchema: prizeListRowJsonSchema };

  if (!exists) {
    await db.createCollection(name, {
      validator,
      validationLevel: "strict",
      validationAction: "error",
    });
  } else {
    await db.command({
      collMod: name,
      validator,
      validationLevel: "strict",
      validationAction: "error",
    });
  }

  await db
    .collection<PrizeListRowDocument>(name)
    .createIndex({ ClubID: 1, PrizeID: 1 }, { unique: true });
}

/**
 * Creates the `userLogin` collection if missing, applies `$jsonSchema` validation,
 * and ensures a unique index on `uid`. If the collection already exists, runs
 * `collMod` to attach the validator (existing documents must satisfy the schema).
 */
export async function ensureUserLoginCollection(
  databaseName?: string,
): Promise<void> {
  const db = await getMongoDb(resolveUserLoginDatabaseName(databaseName));
  const name = USER_LOGIN_COLLECTION;
  const exists =
    (await db.listCollections({ name }, { nameOnly: true }).toArray()).length >
    0;
  const validator: Document = { $jsonSchema: userLoginJsonSchema };

  if (!exists) {
    await db.createCollection(name, {
      validator,
      validationLevel: "strict",
      validationAction: "error",
    });
  } else {
    await db.command({
      collMod: name,
      validator,
      validationLevel: "strict",
      validationAction: "error",
    });
  }

  await db
    .collection<UserLoginDocument>(name)
    .createIndex({ uid: 1 }, { unique: true });
}

export async function closeMongoClient(): Promise<void> {
  if (clientSingleton) {
    await clientSingleton.close();
    clientSingleton = null;
  }
  connecting = null;
}

export async function testMongoConnection(): Promise<
  { ok: true; latencyMs: number } | { ok: false; error: string }
> {
  if (!isMongoConfigured()) {
    return { ok: false, error: "MongoDB not configured." };
  }
  const started = Date.now();
  try {
    const client = await getMongoClient();
    await client.db("admin").command({ ping: 1 });
    return { ok: true, latencyMs: Date.now() - started };
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    return { ok: false, error: msg };
  }
}
