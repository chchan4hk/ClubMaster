/**
 * Copies every document from database `userLogin` collection `clubInfo`
 * into database `ClubMaster_DB` collection `clubInfo` (upsert by `club_id`).
 *
 * Usage (from `src/backend`):
 *   npm run mongo:clone-clubinfo
 *   npm run mongo:clone-clubinfo -- --dry-run
 *
 * Optional env (same as userLogin clone; see `.env.example`):
 *   MONGO_CLONE_SOURCE_DB=userLogin
 *   MONGO_CLONE_TARGET_DB=ClubMaster_DB
 */
import path from "path";
import type { AnyBulkWriteOperation, Document } from "mongodb";
import { loadLocalEnvFile } from "../src/config/env";
import {
  closeMongoClient,
  CLUB_INFO_COLLECTION,
  DEFAULT_MONGO_APP_DATABASE,
  ensureClubInfoCollection,
  getMongoClient,
  isMongoConfigured,
} from "../src/db/DBConnection";

const CHUNK_SIZE = 500;

async function main(): Promise<void> {
  const backendRoot = path.join(__dirname, "..");
  loadLocalEnvFile(backendRoot);

  if (!isMongoConfigured()) {
    console.error(
      "MongoDB not configured. Set MONGODB_URI / MONGO_URI or MONGO_PASSWORD (see .env.example).",
    );
    process.exit(1);
  }

  const dryRun = process.argv.includes("--dry-run");
  const sourceDb =
    process.env.MONGO_CLONE_SOURCE_DB?.trim() || "userLogin";
  const targetDb =
    process.env.MONGO_CLONE_TARGET_DB?.trim() || DEFAULT_MONGO_APP_DATABASE;

  if (sourceDb === targetDb) {
    console.error(
      `Source and target database must differ (both are "${sourceDb}").`,
    );
    process.exit(1);
  }

  const client = await getMongoClient();
  const sourceColl = client.db(sourceDb).collection(CLUB_INFO_COLLECTION);
  const sourceCount = await sourceColl.countDocuments();

  if (sourceCount === 0) {
    const hasColl =
      (
        await client
          .db(sourceDb)
          .listCollections({ name: CLUB_INFO_COLLECTION }, { nameOnly: true })
          .toArray()
      ).length > 0;
    if (!hasColl) {
      console.error(
        `Collection "${CLUB_INFO_COLLECTION}" does not exist in database "${sourceDb}".`,
      );
      process.exit(1);
    }
    console.log(
      `Source "${sourceDb}.${CLUB_INFO_COLLECTION}" has 0 documents; nothing to clone.`,
    );
    await closeMongoClient();
    return;
  }

  console.log(
    `Clone ${sourceDb}.${CLUB_INFO_COLLECTION} → ${targetDb}.${CLUB_INFO_COLLECTION} (${sourceCount} document(s)).`,
  );

  if (dryRun) {
    console.log("Dry run: no writes.");
    await closeMongoClient();
    return;
  }

  await ensureClubInfoCollection(targetDb);
  const targetColl = client.db(targetDb).collection(CLUB_INFO_COLLECTION);

  const cursor = sourceColl.find({});
  let batch: AnyBulkWriteOperation<Document>[] = [];
  let totalUpserted = 0;
  let totalModified = 0;
  let totalMatched = 0;

  const flush = async () => {
    if (batch.length === 0) {
      return;
    }
    const r = await targetColl.bulkWrite(batch, { ordered: false });
    totalUpserted += r.upsertedCount;
    totalModified += r.modifiedCount;
    totalMatched += r.matchedCount;
    batch = [];
  };

  for await (const doc of cursor) {
    const clubId = String((doc as { club_id?: string }).club_id ?? "").trim();
    if (!clubId) {
      console.warn("Skipping document with missing or empty club_id:", doc._id);
      continue;
    }
    batch.push({
      replaceOne: {
        filter: { club_id: clubId },
        replacement: doc as Document,
        upsert: true,
      },
    });
    if (batch.length >= CHUNK_SIZE) {
      await flush();
      process.stdout.write(".");
    }
  }
  await flush();
  if (sourceCount > CHUNK_SIZE) {
    process.stdout.write("\n");
  }

  console.log(
    JSON.stringify(
      {
        ok: true,
        sourceDb,
        targetDb,
        collection: CLUB_INFO_COLLECTION,
        sourceCount,
        bulkWrite: {
          matched: totalMatched,
          modified: totalModified,
          upserted: totalUpserted,
        },
      },
      null,
      2,
    ),
  );

  await closeMongoClient();
}

main().catch((e) => {
  console.error(e instanceof Error ? e.message : String(e));
  process.exit(1);
});
