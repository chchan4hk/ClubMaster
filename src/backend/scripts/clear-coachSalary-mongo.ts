/**
 * Removes every document from MongoDB `CoachManager` (database from
 * `MONGO_COACH_SALARY_TARGET_DB` / default `ClubMaster_DB` — see `resolveCoachSalaryDatabaseName`).
 *
 * Usage:
 *   npx tsx ./scripts/clear-coachSalary-mongo.ts --dry-run   # count only
 *   npx tsx ./scripts/clear-coachSalary-mongo.ts --yes       # delete all
 */
import path from "path";
import { loadLocalEnvFile } from "../src/config/env";
import {
  COACH_SALARY_COLLECTION,
  closeMongoClient,
  getCoachSalaryCollection,
  isMongoConfigured,
  resolveCoachSalaryDatabaseName,
} from "../src/db/DBConnection";

const backendRoot = path.join(__dirname, "..");
loadLocalEnvFile(backendRoot);

async function main(): Promise<void> {
  if (!isMongoConfigured()) {
    console.error(
      "MongoDB is not configured (set MONGODB_URI / MONGO_URI or MONGO_PASSWORD).",
    );
    process.exit(1);
  }
  const dbName = resolveCoachSalaryDatabaseName();
  const col = await getCoachSalaryCollection();
  const count = await col.countDocuments({});
  console.log(
    `Target: database "${dbName}", collection "${COACH_SALARY_COLLECTION}", document count: ${count}.`,
  );

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

  const r = await col.deleteMany({});
  console.log(`Deleted ${r.deletedCount} document(s) from CoachManager.`);
}

main()
  .catch((e) => {
    console.error(e);
    process.exit(1);
  })
  .finally(() => {
    void closeMongoClient();
  });
