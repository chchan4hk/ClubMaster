/**
 * Upserts rows from `data_club/CM00000003/CoachSalary.json` into MongoDB database
 * `ClubMaster_DB` (or `MONGO_COACH_SALARY_TARGET_DB` / `MONGO_DATABASE`) collection `CoachSalary`.
 */
import path from "path";
import { loadLocalEnvFile } from "../src/config/env";
import { loadCoachSalaryDocument } from "../src/coachSalaryJson";
import {
  closeMongoClient,
  ensureCoachSalaryCollection,
  getCoachSalaryCollection,
  isMongoConfigured,
  type CoachSalaryInsert,
} from "../src/db/DBConnection";

const backendRoot = path.join(__dirname, "..");
loadLocalEnvFile(backendRoot);

const CLUB_FOLDER = "CM00000003";

function rowToInsert(row: {
  CoachSalaryID: string;
  lessonId: string;
  ClubID: string;
  club_name: string;
  coach_id: string;
  salary_amount: number | string;
  Payment_Method: string;
  Payment_Status: string;
  Payment_Confirm: boolean;
  createdAt: string;
  lastUpdatedDate: string;
}): CoachSalaryInsert {
  const amt = row.salary_amount;
  const salaryNum =
    typeof amt === "number" && Number.isFinite(amt)
      ? amt
      : Number.parseFloat(String(amt ?? "").replace(/,/g, ""));
  return {
    CoachSalaryID: String(row.CoachSalaryID ?? "").trim(),
    lessonId: String(row.lessonId ?? "").trim(),
    ClubID: String(row.ClubID ?? "").trim(),
    club_name: String(row.club_name ?? "").trim(),
    coach_id: String(row.coach_id ?? "").trim(),
    salary_amount: Number.isFinite(salaryNum) ? salaryNum : 0,
    Payment_Method: String(row.Payment_Method ?? "").trim(),
    Payment_Status: String(row.Payment_Status ?? "").trim(),
    Payment_Confirm: Boolean(row.Payment_Confirm),
    createdAt: String(row.createdAt ?? "").trim(),
    lastUpdatedDate: String(row.lastUpdatedDate ?? "").trim(),
    lastImportedAt: new Date(),
  };
}

async function main(): Promise<void> {
  if (!isMongoConfigured()) {
    console.error(
      "MongoDB is not configured (set MONGODB_URI / MONGO_URI or MONGO_PASSWORD).",
    );
    process.exit(1);
  }
  const fileDoc = loadCoachSalaryDocument(CLUB_FOLDER);
  await ensureCoachSalaryCollection();
  const col = await getCoachSalaryCollection();
  let upserted = 0;
  for (const row of fileDoc.coachSalaries) {
    const doc = rowToInsert(row);
    if (!doc.CoachSalaryID) {
      console.warn("Skipping row without CoachSalaryID.");
      continue;
    }
    const r = await col.replaceOne(
      { CoachSalaryID: doc.CoachSalaryID },
      doc,
      { upsert: true },
    );
    if (r.upsertedCount || r.modifiedCount || r.matchedCount) {
      upserted += 1;
    }
  }
  console.log(
    `CoachSalary: processed ${upserted} row(s) for club folder ${CLUB_FOLDER} (replaceOne upsert by CoachSalaryID).`,
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
