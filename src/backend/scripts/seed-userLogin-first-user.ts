import fs from "fs";
import path from "path";
import { loadLocalEnvFile } from "../src/config/env";
import {
  USER_TYPE_VALUES,
  closeMongoClient,
  ensureUserLoginCollection,
  getUserLoginCollection,
  isMongoConfigured,
  resolveUserLoginDatabaseName,
  type UserLoginInsert,
  type UserType,
} from "../src/db/DBConnection";
import {
  ensureUserlistFileExists,
  loadUsersFromCsv,
  type CsvUser,
} from "../src/userlistCsv";

function parseDay(s: string): Date {
  const t = s.trim();
  if (/^\d{4}-\d{2}-\d{2}$/.test(t)) {
    return new Date(`${t}T00:00:00.000Z`);
  }
  const d = new Date(t);
  if (Number.isNaN(d.getTime())) {
    throw new Error(`Invalid date: ${s}`);
  }
  return d;
}

/** CSV rows may omit dates; use epoch instead of failing the seed. */
function parseDayOrEpoch(s: string): Date {
  const t = s.trim();
  if (!t) {
    return new Date(0);
  }
  return parseDay(t);
}

function parseExpiryOptional(s: string): Date | null {
  const t = s.trim();
  if (!t) {
    return null;
  }
  return parseDay(t);
}

function normalizeUserType(raw: string): UserType {
  const key = raw.trim().toLowerCase();
  const map: Record<string, UserType> = {
    administrator: "Administrator",
    "coach manager": "Coach Manager",
    coach: "Coach",
    student: "Student",
  };
  const v = map[key];
  if (v) {
    return v;
  }
  if ((USER_TYPE_VALUES as readonly string[]).includes(raw.trim())) {
    return raw.trim() as UserType;
  }
  throw new Error(`Unknown usertype: ${raw}`);
}

function ynOrActiveToBool(raw: string): boolean {
  const s = raw.trim().toUpperCase();
  return (
    s === "YES" ||
    s === "Y" ||
    s === "TRUE" ||
    s === "1" ||
    s === "ACTIVE"
  );
}

function csvUserToInsert(u: CsvUser): UserLoginInsert {
  const usertype = normalizeUserType(u.usertype);
  const pwd =
    String(u.passwordHash ?? "").trim() || String(u.password ?? "").trim();
  if (!pwd) {
    throw new Error("First CSV user row has no password or bcrypt hash.");
  }
  const lastUp = (u.lastUpdateDate ?? "").trim();
  return {
    uid: u.uid.trim(),
    usertype,
    username: u.username.trim(),
    password: pwd,
    full_name: u.fullName.trim(),
    is_activated: u.isActivated,
    creation_date: parseDayOrEpoch(u.creationDate),
    club_name: u.clubName.trim(),
    club_photo: (u.clubPhoto ?? "").trim(),
    status: ynOrActiveToBool(u.status || "ACTIVE"),
    lastUpdate_date: lastUp ? parseDay(lastUp) : parseDayOrEpoch(u.creationDate),
    Expiry_date: parseExpiryOptional(u.expiryDate ?? ""),
  };
}

async function main(): Promise<void> {
  const backendRoot = path.join(__dirname, "..");
  loadLocalEnvFile(backendRoot);

  if (!isMongoConfigured()) {
    const envPath = path.join(backendRoot, ".env");
    const nodeEnv = process.env.NODE_ENV ?? "";
    console.error("MongoDB not configured.");
    if (nodeEnv === "production") {
      console.error(
        "NODE_ENV is production: `.env` is not loaded. Export MONGODB_URI (or MONGO_PASSWORD, etc.) in the shell or platform env.",
      );
    } else if (!fs.existsSync(envPath)) {
      console.error(
        `No file at ${envPath}. Copy .env.example to .env in src/backend, then set MONGODB_URI or MONGO_PASSWORD.`,
      );
    } else {
      console.error(
        `Found ${envPath} but MONGODB_URI / MONGO_URI / MONGO_PASSWORD is unset or empty.`,
      );
    }
    process.exit(1);
  }

  const dbName = resolveUserLoginDatabaseName();

  ensureUserlistFileExists();
  const users = loadUsersFromCsv();
  const first = users[0];
  if (!first) {
    console.error(
      "userLogin.csv has no data rows. Add at least one row under backend/data/userLogin.csv (or set USERLOGIN_CSV_PATH).",
    );
    process.exit(1);
  }

  const doc = csvUserToInsert(first);

  await ensureUserLoginCollection(dbName);
  const coll = await getUserLoginCollection(dbName);

  const existing = await coll.findOne({ uid: doc.uid });
  if (existing) {
    console.log(
      `Database "${dbName}" collection userLogin already contains uid=${doc.uid}; no insert.`,
    );
    await closeMongoClient();
    return;
  }

  const r = await coll.insertOne(doc);
  console.log(
    `Inserted user username=${doc.username} uid=${doc.uid} _id=${String(r.insertedId)} into "${dbName}".userLogin`,
  );
  await closeMongoClient();
}

main().catch(async (e) => {
  console.error(e);
  await closeMongoClient().catch(() => undefined);
  process.exit(1);
});
