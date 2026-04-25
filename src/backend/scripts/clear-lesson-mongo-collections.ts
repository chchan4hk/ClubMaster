/**
 * Removes documents from MongoDB lesson-related collections (defaults to `ClubMaster_DB`
 * unless overridden by `MONGO_*_TARGET_DB` env vars — see `DBConnection.ts`).
 *
 * Collections:
 * - `LessonList` (per-club documents; `_id` / `club_id`)
 * - `LessonReserveList` (per reservation row; `ClubID` + …)
 * - `LessonPaymentLedger` (per reservation ledger; `ClubID` + …)
 *
 * From `src/backend`:
 *   npx tsx ./scripts/clear-lesson-mongo-collections.ts
 *   npx tsx ./scripts/clear-lesson-mongo-collections.ts --apply
 *   npx tsx ./scripts/clear-lesson-mongo-collections.ts --apply --club CM00000008
 *
 * Without `--apply`: prints counts only (no deletes).
 */
import path from "path";
import { loadLocalEnvFile } from "../src/config/env";
import { isValidClubFolderId } from "../src/coachListCsv";
import {
  closeMongoClient,
  getLessonListCollection,
  getLessonPaymentLedgerCollection,
  getLessonReserveListCollection,
  isMongoConfigured,
  LESSON_LIST_COLLECTION,
  LESSON_PAYMENT_LEDGER_COLLECTION,
  LESSON_RESERVE_LIST_COLLECTION,
  resolveLessonListDatabaseName,
  resolveLessonPaymentLedgerDatabaseName,
  resolveLessonReserveListDatabaseName,
} from "../src/db/DBConnection";
import type { Filter } from "mongodb";
import type {
  LessonListClubDocument,
  LessonPaymentLedgerDocument,
  LessonReserveListDocument,
} from "../src/db/DBConnection";

function escapeRegExp(s: string): string {
  return s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function argvClub(): string | null {
  const raw = process.argv;
  for (let i = 0; i < raw.length; i++) {
    const a = raw[i]!;
    if (a === "--club" && raw[i + 1]) {
      return String(raw[i + 1]).trim();
    }
    if (a.startsWith("--club=")) {
      return a.slice("--club=".length).trim();
    }
  }
  return null;
}

async function main(): Promise<void> {
  const backendRoot = path.join(__dirname, "..");
  loadLocalEnvFile(backendRoot);

  if (!isMongoConfigured()) {
    console.error(
      "MongoDB is not configured (set MONGODB_URI / MONGO_URI or MONGO_PASSWORD).",
    );
    process.exit(1);
  }

  const apply = process.argv.includes("--apply");
  const clubArg = argvClub();
  if (clubArg && !isValidClubFolderId(clubArg)) {
    console.error(`Invalid --club id: ${clubArg}`);
    process.exit(1);
  }

  const listDb = resolveLessonListDatabaseName();
  const reserveDb = resolveLessonReserveListDatabaseName();
  const ledgerDb = resolveLessonPaymentLedgerDatabaseName();

  const listColl = await getLessonListCollection();
  const resColl = await getLessonReserveListCollection();
  const ledColl = await getLessonPaymentLedgerCollection();

  const clubRe = clubArg
    ? new RegExp(`^${escapeRegExp(clubArg.trim())}$`, "i")
    : null;

  const listFilter: Filter<LessonListClubDocument> = clubRe
    ? {
        $or: [{ _id: clubArg!.trim() }, { club_id: clubRe }],
      }
    : {};

  const resFilter: Filter<LessonReserveListDocument> = clubRe
    ? { ClubID: clubRe }
    : {};

  const ledFilter: Filter<LessonPaymentLedgerDocument> = clubRe
    ? { ClubID: clubRe }
    : {};

  const cList = await listColl.countDocuments(listFilter);
  const cRes = await resColl.countDocuments(resFilter);
  const cLed = await ledColl.countDocuments(ledFilter);

  console.log("Lesson Mongo cleanup");
  console.log(
    `  ${LESSON_LIST_COLLECTION} @ ${listDb}: ${cList} document(s) matching filter`,
  );
  console.log(
    `  ${LESSON_RESERVE_LIST_COLLECTION} @ ${reserveDb}: ${cRes} document(s)`,
  );
  console.log(
    `  ${LESSON_PAYMENT_LEDGER_COLLECTION} @ ${ledgerDb}: ${cLed} document(s)`,
  );
  if (clubArg) {
    console.log(`  Scope: club folder ${clubArg}`);
  } else {
    console.log("  Scope: ALL clubs (entire collections matching empty filter for list)");
  }
  console.log(`  Mode: ${apply ? "APPLY (deleting)" : "DRY-RUN (no deletes)"}`);

  if (!apply) {
    console.log("Re-run with --apply to delete. Add --club CM00000008 to limit to one club.");
    return;
  }

  const rList = await listColl.deleteMany(listFilter);
  const rRes = await resColl.deleteMany(resFilter);
  const rLed = await ledColl.deleteMany(ledFilter);

  console.log(
    `Done. Deleted LessonList: ${rList.deletedCount}, LessonReserveList: ${rRes.deletedCount}, LessonPaymentLedger: ${rLed.deletedCount}.`,
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
