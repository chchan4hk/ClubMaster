/**
 * Imports `data_club/{clubFolder}/UserList_Student.json` into MongoDB `UserList_Student`
 * (database from `resolveUserListRosterDatabaseName`, default `ClubMaster_DB`).
 *
 * From `src/backend`:
 *   npm run mongo:seed-userlist-student-cm3
 *   npm run mongo:seed-userlist-student-cm3 -- --dry-run
 *   npm run mongo:seed-userlist-student-cm3 -- --database ClubMaster_DB --club-folder CM00000003
 *
 * JSON may use `student_id` / `creation_date` (canonical) or legacy `StudentID` / `created_at`.
 * Optional env: `USERLIST_STUDENT_JSON_PATH` (absolute or relative to `src/backend`).
 */
import fs from "fs";
import path from "path";
import type { Document } from "mongodb";
import { loadLocalEnvFile } from "../src/config/env";
import {
  closeMongoClient,
  getUserListStudentCollection,
  isMongoConfigured,
  resolveUserListRosterDatabaseName,
  USER_LIST_STUDENT_COLLECTION,
} from "../src/db/DBConnection";

function str(row: Record<string, unknown>, ...keys: string[]): string {
  for (const k of keys) {
    const v = row[k];
    if (v !== undefined && v !== null && String(v).trim()) {
      return String(v).trim();
    }
  }
  return "";
}

function clubFolderFromJsonPath(filePath: string): string | null {
  const norm = filePath.replace(/\\/g, "/");
  const m = norm.match(/data_club\/(CM\d+)/i);
  return m?.[1] ?? null;
}

function readUserListStudentJson(filePath: string): {
  version: number;
  students: Record<string, unknown>[];
} {
  const raw = fs.readFileSync(filePath, "utf8");
  const data = JSON.parse(raw) as unknown;
  if (!data || typeof data !== "object") {
    throw new Error("UserList_Student.json must be a JSON object.");
  }
  const o = data as Record<string, unknown>;
  const version = typeof o.version === "number" ? o.version : 0;
  const arr = o.students;
  if (!Array.isArray(arr)) {
    throw new Error("UserList_Student.json must include a `students` array.");
  }
  const students: Record<string, unknown>[] = [];
  for (const row of arr) {
    if (!row || typeof row !== "object" || Array.isArray(row)) {
      continue;
    }
    students.push(row as Record<string, unknown>);
  }
  return { version, students };
}

function mapRow(
  row: Record<string, unknown>,
  clubFolderUid: string,
  meta: { jsonFileVersion: number; lastImportedAt: Date },
): Document {
  const student_id = str(row, "student_id", "StudentID");
  if (!student_id) {
    throw new Error("Each student must have student_id (or legacy StudentID).");
  }
  const creation_date = str(row, "creation_date", "created_at");
  const fromRow = str(row, "club_id");
  const legacyName = str(row, "club_name", "Club_name");
  const club_id =
    fromRow || (/^CM\d+$/i.test(legacyName) ? legacyName : clubFolderUid);
  return {
    club_folder_uid: clubFolderUid,
    student_id,
    club_id,
    full_name: str(row, "full_name"),
    sex: str(row, "sex"),
    email: str(row, "email"),
    contact_number: str(row, "contact_number"),
    guardian: str(row, "guardian"),
    guardian_contact: str(row, "guardian_contact"),
    school: str(row, "school"),
    student_coach: str(row, "student_coach"),
    status: str(row, "status"),
    creation_date,
    remark: str(row, "remark"),
    lastUpdate_date: str(row, "lastUpdate_date"),
    date_of_birth: str(row, "date_of_birth"),
    joined_date: str(row, "joined_date"),
    home_address: str(row, "home_address"),
    country: str(row, "country"),
    json_file_version: meta.jsonFileVersion,
    lastImportedAt: meta.lastImportedAt,
  };
}

async function main(): Promise<void> {
  const backendRoot = path.join(__dirname, "..");
  loadLocalEnvFile(backendRoot);

  if (!isMongoConfigured()) {
    console.error(
      "MongoDB not configured. Set MONGODB_URI / MONGO_URI or MONGO_PASSWORD (see .env.example).",
    );
    process.exit(1);
  }

  const dryRun = process.argv.includes("--dry-run");
  let clubFolder = "CM00000003";
  const cfIdx = process.argv.indexOf("--club-folder");
  if (cfIdx !== -1 && process.argv[cfIdx + 1]) {
    clubFolder = process.argv[cfIdx + 1].trim();
  }

  let targetDb = "";
  const dbIdx = process.argv.indexOf("--database");
  if (dbIdx !== -1 && process.argv[dbIdx + 1]) {
    targetDb = process.argv[dbIdx + 1].trim();
  }

  const envPath = process.env.USERLIST_STUDENT_JSON_PATH?.trim();
  const jsonPath = envPath
    ? path.isAbsolute(envPath)
      ? envPath
      : path.join(backendRoot, envPath)
    : path.join(backendRoot, "data_club", clubFolder, "UserList_Student.json");

  const inferredClub = clubFolderFromJsonPath(jsonPath);
  if (inferredClub) {
    clubFolder = inferredClub;
  }

  if (!fs.existsSync(jsonPath)) {
    console.error(`UserList_Student JSON not found: ${jsonPath}`);
    process.exit(1);
  }

  const { version, students } = readUserListStudentJson(jsonPath);
  const dbName = targetDb || resolveUserListRosterDatabaseName();

  console.log(
    `Import ${jsonPath} → ${dbName}.${USER_LIST_STUDENT_COLLECTION} ` +
      `(${students.length} student row(s), file version=${version}, club_folder_uid=${clubFolder}).`,
  );

  if (students.length === 0) {
    console.error("No students in file; nothing to import.");
    process.exit(1);
  }

  if (dryRun) {
    console.log("Dry run: no writes.");
    return;
  }

  const now = new Date();
  const docs = students.map((r) =>
    mapRow(r, clubFolder, { jsonFileVersion: version, lastImportedAt: now }),
  );

  try {
    const coll = await getUserListStudentCollection(dbName);
    try {
      await coll.createIndex(
        { club_folder_uid: 1, student_id: 1 },
        { unique: true },
      );
    } catch {
      /* index may already exist with same spec */
    }

    const result = await coll.bulkWrite(
      docs.map((doc) => ({
        replaceOne: {
          filter: {
            club_folder_uid: doc.club_folder_uid,
            student_id: doc.student_id,
          },
          replacement: doc,
          upsert: true,
        },
      })),
      { ordered: false },
    );

    console.log(
      JSON.stringify(
        {
          ok: true,
          database: dbName,
          collection: USER_LIST_STUDENT_COLLECTION,
          matchedCount: result.matchedCount,
          modifiedCount: result.modifiedCount,
          upsertedCount: result.upsertedCount,
        },
        null,
        2,
      ),
    );
  } finally {
    await closeMongoClient();
  }
}

main().catch(async (e) => {
  console.error(e instanceof Error ? e.message : String(e));
  await closeMongoClient().catch(() => undefined);
  process.exit(1);
});
