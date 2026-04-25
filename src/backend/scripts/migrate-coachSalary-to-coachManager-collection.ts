/**
 * One-time migration: copy documents from Mongo collection `CoachSalary` into
 * the new collection `CoachManager` (coach salary rows).
 *
 * From `src/backend`:
 *   node ./node_modules/tsx/dist/cli.mjs ./scripts/migrate-coachSalary-to-coachManager-collection.ts --dry-run
 *   node ./node_modules/tsx/dist/cli.mjs ./scripts/migrate-coachSalary-to-coachManager-collection.ts --yes
 */
import path from "path";
import { loadLocalEnvFile } from "../src/config/env";
import {
  closeMongoClient,
  getMongoClient,
  resolveCoachSalaryDatabaseName,
  ensureCoachSalaryCollection,
  COACH_SALARY_COLLECTION,
} from "../src/db/DBConnection";

async function main(): Promise<void> {
  const backendRoot = path.join(__dirname, "..");
  loadLocalEnvFile(backendRoot);

  const dbName = resolveCoachSalaryDatabaseName();
  const client = await getMongoClient();
  const db = client.db(dbName);

  const srcName = "CoachSalary";
  const dstName = COACH_SALARY_COLLECTION; // "CoachManager"

  const src = db.collection(srcName);
  await ensureCoachSalaryCollection();
  const dst = db.collection(dstName);

  const srcCount = await src.countDocuments({});
  const dstCount = await dst.countDocuments({});

  console.log(`DB ${dbName}`);
  console.log(`Source collection ${srcName}: ${srcCount} doc(s)`);
  console.log(`Target collection ${dstName}: ${dstCount} doc(s)`);

  if (process.argv.includes("--dry-run")) {
    console.log("Dry run: no documents copied.");
    return;
  }
  if (!process.argv.includes("--yes")) {
    console.error("Refusing to write. Re-run with --yes or use --dry-run.");
    process.exit(1);
  }

  let copied = 0;
  const cursor = src.find({});
  for await (const doc of cursor) {
    const id = String((doc as any).CoachSalaryID ?? "").trim();
    if (!id) {
      continue;
    }
    // Upsert by CoachSalaryID; keep existing _id in target.
    const next = { ...doc };
    delete (next as any)._id;
    await dst.replaceOne({ CoachSalaryID: id }, next, { upsert: true });
    copied += 1;
  }

  console.log(`Copied ${copied} doc(s) (upsert by CoachSalaryID).`);
}

main()
  .catch((e) => {
    console.error(e);
    process.exit(1);
  })
  .finally(() => {
    void closeMongoClient();
  });

