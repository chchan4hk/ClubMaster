/**
 * Patch a single coach salary row in Mongo `CoachManager` by CoachSalaryID.
 *
 * From `src/backend`:
 *   node ./node_modules/tsx/dist/cli.mjs ./scripts/patch-coachSalary-row.ts CM00000008-CS000001
 */
import path from "path";
import { loadLocalEnvFile } from "../src/config/env";
import { closeMongoClient, getCoachSalaryCollection } from "../src/db/DBConnection";

const backendRoot = path.join(__dirname, "..");
loadLocalEnvFile(backendRoot);

async function main(): Promise<void> {
  const id = String(process.argv[2] ?? "").trim();
  if (!id) {
    console.error("Usage: <CoachSalaryID> (e.g. CM00000008-CS000001)");
    process.exit(1);
  }
  const col = await getCoachSalaryCollection();
  const now = new Date().toISOString();
  const today = now.slice(0, 10);

  const res = await col.updateOne(
    { CoachSalaryID: id },
    {
      $set: {
        // Expected logical ids for coach and lesson
        lessonId: "LE0000004",
        coach_id: "C000004",
        // Mark as paid today so coach trend uses payment date
        Payment_Status: "Paid",
        Payment_date: today,
        lastUpdatedDate: now,
      },
    },
  );

  console.log(
    `Patch ${id}: matched=${res.matchedCount} modified=${res.modifiedCount} payment_date=${today}`,
  );
  const doc = await col.findOne({ CoachSalaryID: id });
  console.log(JSON.stringify(doc, null, 2));
}

main()
  .catch((e) => {
    console.error(e);
    process.exit(1);
  })
  .finally(() => {
    void closeMongoClient();
  });

