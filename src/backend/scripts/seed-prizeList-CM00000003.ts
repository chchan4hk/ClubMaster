/**
 * Upserts sample prize rows into MongoDB `ClubMaster_DB` (or
 * `MONGO_PRIZE_LIST_TARGET_DB` / `MONGO_DATABASE`) collection `PrizeList`.
 * Edit SAMPLE_PRIZE_ROWS below if you need different seed data.
 */
import path from "path";
import { loadLocalEnvFile } from "../src/config/env";
import type { PrizeCsvRow } from "../src/prizeListJson";
import { prizeCsvRowToApiFields } from "../src/prizeListJson";
import {
  closeMongoClient,
  ensurePrizeListRowCollection,
  getPrizeListRowCollection,
  isMongoConfigured,
  type PrizeListRowInsert,
} from "../src/db/DBConnection";

const backendRoot = path.join(__dirname, "..");
loadLocalEnvFile(backendRoot);

const CLUB_FOLDER = "CM00000003";

const SAMPLE_PRIZE_ROWS: PrizeCsvRow[] = [
  {
    prizeId: "PR000001",
    clubId: "CM00000003",
    clubName: "流星羽毛球會",
    sportType: "Badminton",
    year: "2025",
    association: "香港羽毛球總會",
    competition: "全港青少年錦標賽",
    ageGroup: "2012-2013",
    prizeType: "Men's Double",
    studentName: "Chan Dai Man",
    ranking: "Gold",
    status: "ACTIVE",
    createdAt: "2026-04-10",
    lastUpdatedDate: "2026-04-11",
    verifiedBy: "Felix Fan",
    remarks: "Good",
  },
];

function apiFieldsToInsert(
  api: ReturnType<typeof prizeCsvRowToApiFields>,
): PrizeListRowInsert {
  return {
    PrizeID: api.PrizeID ?? "",
    ClubID: api.ClubID ?? "",
    Club_name: api.Club_name ?? "",
    SportType: api.SportType ?? "",
    Year: api.Year ?? "",
    Association: api.Association ?? "",
    Competition: api.Competition ?? "",
    Age_group: api.Age_group ?? "",
    Prize_type: api.Prize_type ?? "",
    StudentName: api.StudentName ?? "",
    Ranking: api.Ranking ?? "",
    Status: api.Status ?? "",
    Created_at: api.Created_at ?? "",
    LastUpdated_Date: api.LastUpdated_Date ?? "",
    VerifiedBy: api.VerifiedBy ?? "",
    Remarks: api.Remarks ?? "",
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
  if (!SAMPLE_PRIZE_ROWS.length) {
    console.warn("No sample prize rows defined.");
  }
  await ensurePrizeListRowCollection();
  const col = await getPrizeListRowCollection();
  let n = 0;
  for (const row of SAMPLE_PRIZE_ROWS) {
    const doc = apiFieldsToInsert(prizeCsvRowToApiFields(row));
    if (!doc.PrizeID.trim()) {
      continue;
    }
    await col.replaceOne(
      { ClubID: doc.ClubID, PrizeID: doc.PrizeID },
      doc,
      { upsert: true },
    );
    n += 1;
  }
  console.log(
    `PrizeList: upserted ${n} document(s) for club folder ${CLUB_FOLDER} (key ClubID + PrizeID).`,
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
