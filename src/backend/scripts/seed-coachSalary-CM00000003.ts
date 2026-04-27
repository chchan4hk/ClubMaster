/**
 * Upserts sample coach salary rows into MongoDB `ClubMaster_DB` (or
 * `MONGO_COACH_SALARY_TARGET_DB` / `MONGO_DATABASE`) collection `CoachManager`.
 * Edit SAMPLE_ROWS below if you need different seed data.
 */
import path from "path";
import { loadLocalEnvFile } from "../src/config/env";
import type { CoachSalaryRecord } from "../src/coachSalaryJson";
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

const SAMPLE_ROWS: CoachSalaryRecord[] = [
  {
    CoachSalaryID: "CS000001",
    lessonId: "LE000001",
    ClubID: "CM00000003",
    club_name: "流星羽毛球會",
    salary_amount: 4740,
    Payment_Method: "Bank Transfer",
    Payment_Status: "Paid",
    Payment_Confirm: true,
    createdAt: "2026-04-18",
    lastUpdatedDate: "2026-04-18T14:39:07.595Z",
    coach_id: "C000001",
  },
  {
    CoachSalaryID: "CS000002",
    lessonId: "LE000002",
    ClubID: "CM00000003",
    club_name: "流星羽毛球會",
    salary_amount: 7200,
    Payment_Method: "Bank Transfer",
    Payment_Status: "Pending",
    Payment_Confirm: false,
    createdAt: "2026-04-18",
    lastUpdatedDate: "2026-04-18T14:19:09.014Z",
    coach_id: "",
  },
  {
    CoachSalaryID: "CS000003",
    lessonId: "LE000003",
    ClubID: "CM00000003",
    club_name: "流星羽毛球會",
    salary_amount: 1000,
    Payment_Method: "Bank Transfer",
    Payment_Status: "Pending",
    Payment_Confirm: false,
    createdAt: "2026-04-18",
    lastUpdatedDate: "2026-04-18T14:19:11.156Z",
    coach_id: "C000001",
  },
  {
    CoachSalaryID: "CS000004",
    lessonId: "LE000004",
    ClubID: "CM00000003",
    club_name: "流星羽毛球會",
    salary_amount: 15000,
    Payment_Method: "Bank Transfer",
    Payment_Status: "Paid",
    Payment_Confirm: true,
    createdAt: "2026-04-18",
    lastUpdatedDate: "2026-04-18T15:23:30.921Z",
    coach_id: "C000001",
  },
];

function rowToInsert(row: CoachSalaryRecord): CoachSalaryInsert {
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
  await ensureCoachSalaryCollection();
  const col = await getCoachSalaryCollection();
  let upserted = 0;
  for (const row of SAMPLE_ROWS) {
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
    `CoachManager: processed ${upserted} row(s) for club folder ${CLUB_FOLDER} (replaceOne upsert by CoachSalaryID).`,
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
