/**
 * Deletes every document from MongoDB collections used for club lessons and coach salary
 * (default database `ClubMaster_DB` per each resolver — override with env vars).
 *
 * Clears:
 * - `LessonList` (see `resolveLessonListDatabaseName`)
 * - `LessonReserveList` (see `resolveLessonReserveListDatabaseName`)
 * - `CoachManager` (coach salary rows; see `resolveCoachSalaryDatabaseName` / `COACH_SALARY_COLLECTION`)
 *
 * If a legacy collection named `CoachSalary` still exists in the salary database, it is cleared too.
 *
 * From `src/backend`:
 *   npx tsx ./scripts/clear-ClubMaster-lesson-and-salary-collections.ts --dry-run
 *   npx tsx ./scripts/clear-ClubMaster-lesson-and-salary-collections.ts --yes
 */
import path from "path";
import { loadLocalEnvFile } from "../src/config/env";
import {
  closeMongoClient,
  COACH_SALARY_COLLECTION,
  getMongoClient,
  isMongoConfigured,
  LESSON_LIST_COLLECTION,
  LESSON_RESERVE_LIST_COLLECTION,
  resolveCoachSalaryDatabaseName,
  resolveLessonListDatabaseName,
  resolveLessonReserveListDatabaseName,
} from "../src/db/DBConnection";

const backendRoot = path.join(__dirname, "..");
loadLocalEnvFile(backendRoot);

const LEGACY_COACH_SALARY_COLLECTION = "CoachSalary";

async function deleteAllIn(
  databaseName: string,
  collectionName: string,
): Promise<number> {
  const client = await getMongoClient();
  const r = await client.db(databaseName).collection(collectionName).deleteMany({});
  return r.deletedCount;
}

async function deleteLegacyCoachSalaryIfPresent(): Promise<{
  databaseName: string;
  deleted: number | null;
}> {
  const databaseName = resolveCoachSalaryDatabaseName();
  const client = await getMongoClient();
  const db = client.db(databaseName);
  const exists =
    (
      await db
        .listCollections({ name: LEGACY_COACH_SALARY_COLLECTION }, { nameOnly: true })
        .toArray()
    ).length > 0;
  if (!exists) {
    return { databaseName, deleted: null };
  }
  const r = await db.collection(LEGACY_COACH_SALARY_COLLECTION).deleteMany({});
  return { databaseName, deleted: r.deletedCount };
}

async function main(): Promise<void> {
  if (!isMongoConfigured()) {
    console.error(
      "MongoDB is not configured (set MONGODB_URI / MONGO_URI or MONGO_PASSWORD).",
    );
    process.exit(1);
  }

  const lessonListDb = resolveLessonListDatabaseName();
  const reserveDb = resolveLessonReserveListDatabaseName();
  const salaryDb = resolveCoachSalaryDatabaseName();

  const client = await getMongoClient();
  const lessonListCount = await client
    .db(lessonListDb)
    .collection(LESSON_LIST_COLLECTION)
    .countDocuments({});
  const reserveCount = await client
    .db(reserveDb)
    .collection(LESSON_RESERVE_LIST_COLLECTION)
    .countDocuments({});
  const coachMgrCount = await client
    .db(salaryDb)
    .collection(COACH_SALARY_COLLECTION)
    .countDocuments({});

  console.log(
    `Planned deletes:\n` +
      `  ${lessonListDb}.${LESSON_LIST_COLLECTION}: ${lessonListCount} doc(s)\n` +
      `  ${reserveDb}.${LESSON_RESERVE_LIST_COLLECTION}: ${reserveCount} doc(s)\n` +
      `  ${salaryDb}.${COACH_SALARY_COLLECTION}: ${coachMgrCount} doc(s)`,
  );

  const salaryDbHandle = client.db(salaryDb);
  const legacyExists =
    (
      await salaryDbHandle
        .listCollections({ name: LEGACY_COACH_SALARY_COLLECTION }, { nameOnly: true })
        .toArray()
    ).length > 0;
  const legacy = legacyExists
    ? await salaryDbHandle.collection(LEGACY_COACH_SALARY_COLLECTION).countDocuments({})
    : null;
  if (legacy !== null) {
    console.log(`  ${salaryDb}.${LEGACY_COACH_SALARY_COLLECTION}: ${legacy} doc(s) (legacy)`);
  }

  if (process.argv.includes("--dry-run")) {
    console.log("Dry run: no documents deleted.");
    return;
  }

  if (!process.argv.includes("--yes")) {
    console.error(
      "Refusing to delete. Re-run with --yes to remove all documents, or --dry-run to only count.",
    );
    process.exit(1);
  }

  const d1 = await deleteAllIn(lessonListDb, LESSON_LIST_COLLECTION);
  const d2 = await deleteAllIn(reserveDb, LESSON_RESERVE_LIST_COLLECTION);
  const d3 = await deleteAllIn(salaryDb, COACH_SALARY_COLLECTION);
  const leg = await deleteLegacyCoachSalaryIfPresent();

  console.log(
    `Deleted: LessonList=${d1}, LessonReserveList=${d2}, ${COACH_SALARY_COLLECTION}=${d3}` +
      (leg.deleted != null
        ? `, ${LEGACY_COACH_SALARY_COLLECTION}=${leg.deleted}`
        : ""),
  );
}

main()
  .catch((e) => {
    console.error(e);
    process.exit(1);
  })
  .finally(() => {
    void closeMongoClient();
  });
