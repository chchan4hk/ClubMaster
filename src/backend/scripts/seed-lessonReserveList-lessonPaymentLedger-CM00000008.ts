/**
 * Replaces Mongo rows for club `CM00000008` from local JSON mirrors:
 *   `data_club/CM00000008/LessonReserveList.json` → `LessonReserveList`
 *   `data_club/CM00000008/LessonPaymentLedger.json` → `LessonPaymentLedger`
 *
 * Databases: `MONGO_LESSON_RESERVE_TARGET_DB` / `MONGO_LESSON_PAYMENT_LEDGER_TARGET_DB`
 * or default `ClubMaster_DB` (see `resolveLessonReserveListDatabaseName`,
 * `resolveLessonPaymentLedgerDatabaseName`).
 *
 * From `src/backend`: npm run mongo:seed-lesson-reserve-ledger-cm8
 */
import path from "path";
import { loadLocalEnvFile } from "../src/config/env";
import { loadLessonReservations } from "../src/lessonReserveList";
import { loadLessonPaymentLedger } from "../src/payment_modules/lessonPaymentLedger";
import {
  closeMongoClient,
  ensureLessonPaymentLedgerCollection,
  getLessonPaymentLedgerCollection,
  getLessonReserveListCollection,
  isMongoConfigured,
  resolveLessonPaymentLedgerDatabaseName,
  resolveLessonReserveListDatabaseName,
  LESSON_PAYMENT_LEDGER_COLLECTION,
  LESSON_RESERVE_LIST_COLLECTION,
  type LessonPaymentLedgerInsert,
  type LessonReserveListInsert,
} from "../src/db/DBConnection";

const CLUB_ID = "CM00000008";

async function main(): Promise<void> {
  const backendRoot = path.join(__dirname, "..");
  loadLocalEnvFile(backendRoot);

  if (!isMongoConfigured()) {
    console.error(
      "MongoDB not configured. Set MONGODB_URI / MONGO_URI or MONGO_PASSWORD.",
    );
    process.exit(1);
  }

  const reserves = loadLessonReservations(CLUB_ID);
  const ledgerFile = loadLessonPaymentLedger(CLUB_ID);
  const ledgerRows: LessonPaymentLedgerInsert[] = Object.values(
    ledgerFile.entries,
  ).map((e) => ({
    ClubID: CLUB_ID,
    lessonReserveId: String(e.lessonReserveId ?? "").trim(),
    dueDate: String(e.dueDate ?? "").trim(),
    payments: (e.payments ?? []).map((p) => ({
      paymentId: String(p.paymentId ?? "").trim(),
      amount:
        typeof p.amount === "number" && Number.isFinite(p.amount)
          ? p.amount
          : 0,
      method: String(p.method ?? "").trim(),
      reference: String(p.reference ?? "").trim(),
      paidAt: String(p.paidAt ?? "").trim(),
    })),
  }));

  const reserveDb = resolveLessonReserveListDatabaseName();
  const ledgerDb = resolveLessonPaymentLedgerDatabaseName();

  await ensureLessonPaymentLedgerCollection();
  const reserveCol = await getLessonReserveListCollection();
  const ledgerCol = await getLessonPaymentLedgerCollection();

  const delR = await reserveCol.deleteMany({ ClubID: CLUB_ID });
  const delL = await ledgerCol.deleteMany({ ClubID: CLUB_ID });

  let insR = 0;
  if (reserves.length) {
    const rows: LessonReserveListInsert[] = reserves.map((r) => ({
      lessonReserveId: r.lessonReserveId,
      lessonId: r.lessonId,
      ClubID: r.ClubID,
      student_id: r.student_id,
      Student_Name: r.Student_Name,
      status: r.status,
      Payment_Status: r.Payment_Status,
      Payment_Confirm: r.Payment_Confirm,
      createdAt: r.createdAt,
      lastUpdatedDate: r.lastUpdatedDate,
    }));
    const ir = await reserveCol.insertMany(rows);
    insR = ir.insertedCount;
  }

  let insL = 0;
  const ledgerToInsert = ledgerRows.filter((r) => r.lessonReserveId);
  if (ledgerToInsert.length) {
    const il = await ledgerCol.insertMany(ledgerToInsert);
    insL = il.insertedCount;
  }

  console.log(
    `${LESSON_RESERVE_LIST_COLLECTION} → ${reserveDb}: deleted ${delR.deletedCount}, inserted ${insR} (ClubID=${CLUB_ID}).`,
  );
  console.log(
    `${LESSON_PAYMENT_LEDGER_COLLECTION} → ${ledgerDb}: deleted ${delL.deletedCount}, inserted ${insL} (ClubID=${CLUB_ID}).`,
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
