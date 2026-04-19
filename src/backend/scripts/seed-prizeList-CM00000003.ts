/**
 * Upserts rows from `data_club/CM00000003/PrizeList.json` into MongoDB database
 * `ClubMaster_DB` (or `MONGO_PRIZE_LIST_TARGET_DB` / `MONGO_DATABASE`) collection `PrizeList`.
 */
import path from "path";
import { loadLocalEnvFile } from "../src/config/env";
import { loadPrizes, prizeCsvRowToApiFields } from "../src/prizeListJson";
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
  const rows = loadPrizes(CLUB_FOLDER);
  if (!rows.length) {
    console.warn(
      `No prize rows loaded for ${CLUB_FOLDER} (check PrizeList.json exists and has prize_id + student_name).`,
    );
  }
  await ensurePrizeListRowCollection();
  const col = await getPrizeListRowCollection();
  let n = 0;
  for (const row of rows) {
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
