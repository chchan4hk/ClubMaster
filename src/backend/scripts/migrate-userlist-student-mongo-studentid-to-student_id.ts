/**
 * One-time: renames `StudentID` → `student_id` and `created_at` → `creation_date`
 * on `UserList_Student` in MongoDB,
 * drops compound indexes that include `StudentID`, and ensures unique
 * `{ club_folder_uid: 1, student_id: 1 }`.
 *
 * From `src/backend`:
 *   npm run mongo:migrate-userlist-student-field
 *   npm run mongo:migrate-userlist-student-field -- --dry-run
 *   npm run mongo:migrate-userlist-student-field -- --database ClubMaster_DB
 */
import path from "path";
import type { Document } from "mongodb";
import { loadLocalEnvFile } from "../src/config/env";
import {
  closeMongoClient,
  DEFAULT_MONGO_APP_DATABASE,
  getUserListStudentCollection,
  isMongoConfigured,
  resolveUserListRosterDatabaseName,
  USER_LIST_STUDENT_COLLECTION,
} from "../src/db/DBConnection";

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
  let targetDb = "";
  const dbIdx = process.argv.indexOf("--database");
  if (dbIdx !== -1 && process.argv[dbIdx + 1]) {
    targetDb = process.argv[dbIdx + 1].trim();
  }
  const dbName =
    targetDb || resolveUserListRosterDatabaseName() || DEFAULT_MONGO_APP_DATABASE;

  console.log(
    `Migrate ${dbName}.${USER_LIST_STUDENT_COLLECTION}: StudentID → student_id, created_at → creation_date`,
  );

  const coll = await getUserListStudentCollection(dbName);

  const withOld = await coll.countDocuments({ StudentID: { $exists: true } });
  const withNew = await coll.countDocuments({ student_id: { $exists: true } });
  console.log(
    JSON.stringify(
      { documentsWithStudentID: withOld, documentsWithStudent_id: withNew },
      null,
      2,
    ),
  );

  if (dryRun) {
    const indexes = await coll.indexes();
    console.log(
      "Indexes:",
      indexes.map((i) => ({ name: i.name, key: i.key })),
    );
    console.log("Dry run: no writes.");
    await closeMongoClient();
    return;
  }

  try {
    const indexes = await coll.indexes();
    for (const idx of indexes) {
      const key = (idx.key || {}) as Record<string, number>;
      if (
        (key.StudentID != null || key.created_at != null) &&
        idx.name !== "_id_"
      ) {
        console.log(`Dropping index: ${idx.name}`);
        await coll.dropIndex(idx.name);
      }
    }

    const pipeline: Document[] = [
      {
        $set: {
          student_id: {
            $cond: {
              if: {
                $gt: [{ $strLenCP: { $ifNull: ["$student_id", ""] } }, 0],
              },
              then: "$student_id",
              else: { $ifNull: ["$StudentID", ""] },
            },
          },
          creation_date: {
            $cond: {
              if: {
                $gt: [{ $strLenCP: { $ifNull: ["$creation_date", ""] } }, 0],
              },
              then: "$creation_date",
              else: { $ifNull: ["$created_at", ""] },
            },
          },
        },
      },
      { $unset: ["StudentID", "created_at"] },
    ];

    const upd = await coll.updateMany({}, pipeline);
    console.log(`updateMany matched=${upd.matchedCount} modified=${upd.modifiedCount}`);

    await coll.createIndex(
      { club_folder_uid: 1, student_id: 1 },
      { unique: true },
    );
    console.log("Created unique index { club_folder_uid: 1, student_id: 1 }.");

    const remaining = await coll.countDocuments({ StudentID: { $exists: true } });
    if (remaining > 0) {
      console.warn(`Warning: ${remaining} document(s) still have StudentID.`);
    } else {
      console.log("No documents left with field StudentID.");
    }
  } finally {
    await closeMongoClient();
  }
}

main().catch(async (e) => {
  console.error(e instanceof Error ? e.message : String(e));
  await closeMongoClient().catch(() => undefined);
  process.exit(1);
});
