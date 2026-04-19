/**
 * Loads `data_club/CM00000003/LessonReserveList.json` and writes each reservation
 * to MongoDB database `ClubMaster_DB` (see `DEFAULT_MONGO_APP_DATABASE`), not `MONGO_DATABASE`,
 * so local `MONGO_DATABASE=test` does not redirect this import. Override:
 *   `MONGO_LESSON_RESERVE_TARGET_DB=ClubMaster_DB`
 * collection `LessonReserveList`.
 *
 * Re-run safe: deletes existing rows with `ClubID: CM00000003` then inserts from file.
 *
 * From `src/backend`: npm run mongo:seed-lesson-reserve-list-cm3
 */
import fs from "fs";
import path from "path";
import { loadLocalEnvFile } from "../src/config/env";
import {
  closeMongoClient,
  DEFAULT_MONGO_APP_DATABASE,
  getMongoClient,
  isMongoConfigured,
} from "../src/db/DBConnection";

const COLLECTION = "LessonReserveList";
const CLUB_ID = "CM00000003";
const REL_JSON = path.join("data_club", CLUB_ID, "LessonReserveList.json");

type ReserveRow = {
  lessonReserveId: string;
  lessonId: string;
  ClubID: string;
  student_id: string;
  Student_Name: string;
  status: string;
  Payment_Status: string;
  Payment_Confirm: boolean;
  createdAt: string;
  lastUpdatedDate: string;
};

function resolveLessonReserveDbName(): string {
  return (
    process.env.MONGO_LESSON_RESERVE_TARGET_DB?.trim() ||
    DEFAULT_MONGO_APP_DATABASE
  );
}

function loadRows(backendRoot: string): ReserveRow[] {
  const fp = path.join(backendRoot, REL_JSON);
  const raw = fs.readFileSync(fp, "utf8");
  const data = JSON.parse(raw) as { version?: number; reservations?: unknown[] };
  if (Number(data.version) !== 1 || !Array.isArray(data.reservations)) {
    throw new Error(`Expected version 1 and reservations[] in ${fp}`);
  }
  const out: ReserveRow[] = [];
  for (const item of data.reservations) {
    if (!item || typeof item !== "object") {
      continue;
    }
    const o = item as Record<string, unknown>;
    const row: ReserveRow = {
      lessonReserveId: String(o.lessonReserveId ?? "").trim(),
      lessonId: String(o.lessonId ?? "").trim(),
      ClubID: String(o.ClubID ?? "").trim(),
      student_id: String(o.student_id ?? o.StudentID ?? "").trim(),
      Student_Name: String(o.Student_Name ?? "").trim(),
      status: String(o.status ?? "ACTIVE").trim() || "ACTIVE",
      Payment_Status: String(o.Payment_Status ?? "UNPAID").trim() || "UNPAID",
      Payment_Confirm:
        typeof o.Payment_Confirm === "boolean"
          ? o.Payment_Confirm
          : String(o.Payment_Confirm ?? "").toLowerCase() === "true",
      createdAt: String(o.createdAt ?? "").trim(),
      lastUpdatedDate: String(o.lastUpdatedDate ?? "").trim(),
    };
    if (!row.lessonReserveId || !row.lessonId || !row.ClubID) {
      throw new Error(`Invalid reservation row in ${fp}: missing id/lessonId/ClubID`);
    }
    out.push(row);
  }
  return out;
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

  const dbName = resolveLessonReserveDbName();
  const rows = loadRows(backendRoot);
  if (rows.length === 0) {
    console.error(`No reservations in ${REL_JSON}`);
    process.exit(1);
  }

  const client = await getMongoClient();
  const coll = client.db(dbName).collection(COLLECTION);

  const del = await coll.deleteMany({ ClubID: CLUB_ID });
  const ins = await coll.insertMany(rows);

  await coll.createIndex(
    { ClubID: 1, lessonReserveId: 1 },
    { unique: true },
  );
  await coll.createIndex({ ClubID: 1, lessonId: 1 });
  await coll.createIndex({ ClubID: 1, student_id: 1 });

  console.log(
    `LessonReserveList → ${dbName}.${COLLECTION}: deleted ${del.deletedCount} (ClubID=${CLUB_ID}), inserted ${ins.insertedCount}`,
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
