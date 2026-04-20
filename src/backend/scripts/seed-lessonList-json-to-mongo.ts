/**
 * Imports a club `LessonList.json` into MongoDB `ClubMaster_DB.LessonList`
 * as one document per club (`_id` / `club_id` = folder UID, e.g. CM00000003).
 *
 * From `src/backend`:
 *   npm run mongo:seed-lessonlist-json
 *   npm run mongo:seed-lessonlist-json -- --dry-run
 *
 * Optional env:
 *   LESSONLIST_JSON_PATH — relative to `src/backend` or absolute (default below).
 *   MONGO_LESSONLIST_TARGET_DB — DB name (default: ClubMaster_DB).
 */
import fs from "fs";
import path from "path";
import type { Document } from "mongodb";
import { loadLocalEnvFile } from "../src/config/env";
import {
  closeMongoClient,
  ensureLessonListCollection,
  getLessonListCollection,
  isMongoConfigured,
  LESSON_LIST_COLLECTION,
  resolveLessonListDatabaseName,
} from "../src/db/DBConnection";

function clubIdFromJsonPath(filePath: string): string | null {
  const norm = filePath.replace(/\\/g, "/");
  const m = norm.match(/data_club\/(CM\d+)/i);
  return m?.[1] ?? null;
}

function readLessonListJson(filePath: string): {
  club_id: string;
  version: number;
  lessons: Document[];
} {
  const raw = fs.readFileSync(filePath, "utf8");
  const data = JSON.parse(raw) as unknown;
  if (!data || typeof data !== "object") {
    throw new Error("LessonList.json must be a JSON object.");
  }
  const o = data as Record<string, unknown>;
  if (!Array.isArray(o.lessons)) {
    throw new Error("LessonList.json must include a `lessons` array.");
  }
  const lessons = o.lessons as Document[];
  const fromPath = clubIdFromJsonPath(filePath);
  const fromLesson =
    lessons[0] && typeof lessons[0] === "object"
      ? String(
          (lessons[0] as Record<string, unknown>).ClubID ??
            (lessons[0] as Record<string, unknown>).club_id ??
            "",
        ).trim()
      : "";
  const club_id = (fromPath || fromLesson).trim();
  if (!club_id) {
    throw new Error(
      "Could not determine club id: use path like data_club/CM00000003/LessonList.json or set ClubID on lessons[0].",
    );
  }
  const version =
    typeof o.version === "number" && Number.isFinite(o.version)
      ? Math.trunc(o.version)
      : 1;
  return { club_id, version, lessons };
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
  const rel =
    process.env.LESSONLIST_JSON_PATH?.trim() ||
    path.join("data_club", "CM00000003", "LessonList.json");
  const jsonPath = path.isAbsolute(rel)
    ? rel
    : path.join(backendRoot, rel);

  if (!fs.existsSync(jsonPath)) {
    console.error(`LessonList JSON not found: ${jsonPath}`);
    process.exit(1);
  }

  const { club_id, version, lessons } = readLessonListJson(jsonPath);
  const dbName = resolveLessonListDatabaseName();

  console.log(
    `Import ${jsonPath} → ${dbName}.${LESSON_LIST_COLLECTION} (club_id=${club_id}, ${lessons.length} lesson(s), version=${version}).`,
  );

  if (dryRun) {
    console.log("Dry run: no writes.");
    return;
  }

  try {
    await ensureLessonListCollection(dbName);
    const coll = await getLessonListCollection(dbName);
    const doc = {
      _id: club_id,
      club_id,
      version,
      lessons,
      lastImportedAt: new Date(),
    };
    await coll.replaceOne({ _id: club_id }, doc, { upsert: true });

    console.log(
      JSON.stringify(
        {
          ok: true,
          database: dbName,
          collection: LESSON_LIST_COLLECTION,
          club_id,
          lessonCount: lessons.length,
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
