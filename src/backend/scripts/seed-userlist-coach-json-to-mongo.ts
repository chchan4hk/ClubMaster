/**
 * Imports `data_club/{clubFolder}/UserList_Coach.json` into MongoDB `UserList_Coach`
 * on `ClubMaster_DB` by default (`resolveUserListRosterDatabaseName`; override `--database` or env).
 *
 * From `src/backend`:
 *   npm run mongo:seed-userlist-coach
 *   npm run mongo:seed-userlist-coach -- --dry-run
 *   npm run mongo:seed-userlist-coach -- --club-folder CM00000003
 *
 * Optional env: `USERLIST_COACH_JSON_PATH` (absolute or relative to `src/backend`, overrides `--club-folder` default path).
 * Target DB: `MONGO_USERLIST_ROSTER_TARGET_DB` or `ClubMaster_DB` (see `resolveUserListRosterDatabaseName`).
 */
import fs from "fs";
import path from "path";
import type { Document } from "mongodb";
import { loadLocalEnvFile } from "../src/config/env";
import {
  closeMongoClient,
  getUserListCoachCollection,
  isMongoConfigured,
  resolveUserListRosterDatabaseName,
  USER_LIST_COACH_COLLECTION,
} from "../src/db/DBConnection";

function isCoachRow(x: unknown): x is Record<string, unknown> {
  return Boolean(x) && typeof x === "object" && !Array.isArray(x);
}

function coachIdFromRow(row: Record<string, unknown>): string {
  const raw = row.CoachID ?? row.coachID ?? row.coach_id;
  if (typeof raw === "string" && raw.trim()) {
    return raw.trim();
  }
  if (typeof raw === "number" && Number.isFinite(raw)) {
    return String(raw);
  }
  return "";
}

function clubFolderFromJsonPath(filePath: string): string | null {
  const norm = filePath.replace(/\\/g, "/");
  const m = norm.match(/data_club\/(CM\d+)/i);
  return m?.[1] ?? null;
}

function readUserListCoachJson(filePath: string): {
  version: number;
  coaches: Record<string, unknown>[];
} {
  const raw = fs.readFileSync(filePath, "utf8");
  const data = JSON.parse(raw) as unknown;
  if (!data || typeof data !== "object") {
    throw new Error("UserList_Coach.json must be a JSON object.");
  }
  const o = data as Record<string, unknown>;
  const version = typeof o.version === "number" ? o.version : 0;
  const coachesRaw = o.coaches;
  if (!Array.isArray(coachesRaw)) {
    throw new Error("UserList_Coach.json must include a `coaches` array.");
  }
  const coaches: Record<string, unknown>[] = [];
  for (const row of coachesRaw) {
    if (!isCoachRow(row)) {
      continue;
    }
    const coachId = coachIdFromRow(row);
    if (!coachId) {
      throw new Error(
        "Each coach must have CoachID, coachID, or coach_id (non-empty).",
      );
    }
    coaches.push({ ...row, coach_id: coachId });
  }
  return { version, coaches };
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

  const envPath = process.env.USERLIST_COACH_JSON_PATH?.trim();
  const jsonPath = envPath
    ? path.isAbsolute(envPath)
      ? envPath
      : path.join(backendRoot, envPath)
    : path.join(backendRoot, "data_club", clubFolder, "UserList_Coach.json");

  const inferredClub = clubFolderFromJsonPath(jsonPath);
  if (inferredClub) {
    clubFolder = inferredClub;
  }

  if (!fs.existsSync(jsonPath)) {
    console.error(`UserList_Coach JSON not found: ${jsonPath}`);
    process.exit(1);
  }

  const { version, coaches } = readUserListCoachJson(jsonPath);
  const dbName = targetDb || resolveUserListRosterDatabaseName();

  console.log(
    `Import ${jsonPath} → ${dbName}.${USER_LIST_COACH_COLLECTION} ` +
      `(${coaches.length} coach row(s), file version=${version}, club_folder_uid=${clubFolder}).`,
  );

  if (dryRun) {
    console.log("Dry run: no writes.");
    return;
  }

  try {
    const coll = await getUserListCoachCollection(dbName);
    try {
      await coll.createIndex(
        { coach_id: 1, club_folder_uid: 1 },
        { unique: true },
      );
    } catch {
      /* index may already exist */
    }

    const now = new Date();
    const ops = coaches.map((c) => {
      const coach_id = String(
        c.coach_id ?? c.CoachID ?? c.coachID ?? "",
      ).trim();
      const clubIdFromRow = String(c.club_id ?? c.clubId ?? "").trim();
      const body: Document = {
        ...c,
        coach_id,
        /** Keep in sync with `coach_id` for legacy unique indexes on `CoachID` + `club_folder_uid`. */
        CoachID: coach_id,
        club_id: clubIdFromRow || clubFolder,
        club_folder_uid: clubFolder,
        json_file_version: version,
        lastImportedAt: now,
      };
      delete body._id;
      return {
        replaceOne: {
          filter: { coach_id, club_folder_uid: clubFolder },
          replacement: body,
          upsert: true,
        },
      };
    });

    const result = await coll.bulkWrite(ops, { ordered: false });
    console.log(
      JSON.stringify(
        {
          ok: true,
          database: dbName,
          collection: USER_LIST_COACH_COLLECTION,
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

main().catch((e) => {
  console.error(e instanceof Error ? e.message : String(e));
  process.exit(1);
});
