/**
 * Remove one club folder id and all related documents from MongoDB (default `ClubMaster_DB`).
 * Uses {@link purgeEntireClubFromMongo} — same scope as admin “remove Coach Manager” club data + userLogin.
 *
 * Usage:
 *   npx tsx ./scripts/purge-club-from-mongo.ts CM00000003 --dry-run
 *   npx tsx ./scripts/purge-club-from-mongo.ts CM00000003 --yes
 */
import path from "path";
import { loadLocalEnvFile } from "../src/config/env";
import {
  closeMongoClient,
  isMongoConfigured,
} from "../src/db/DBConnection";
import {
  purgeEntireClubFromMongo,
  totalClubPurgeDeletedCounts,
} from "../src/clubFolderMongoPurge";
import { isValidClubFolderId } from "../src/coachListCsv";

const backendRoot = path.join(__dirname, "..");
loadLocalEnvFile(backendRoot);

async function main(): Promise<void> {
  const args = process.argv.slice(2).filter((a) => a !== "--dry-run" && a !== "--yes");
  const clubId = (args[0] ?? "CM00000003").replace(/^\uFEFF/, "").trim();

  if (!isValidClubFolderId(clubId)) {
    console.error(`Invalid club folder id: ${JSON.stringify(clubId)}`);
    process.exit(1);
  }

  if (!isMongoConfigured()) {
    console.error(
      "MongoDB is not configured (set MONGODB_URI / MONGO_URI or MONGO_PASSWORD).",
    );
    process.exit(1);
  }

  const dry = process.argv.includes("--dry-run");
  const yes = process.argv.includes("--yes");

  if (dry) {
    console.log(
      `Dry run: would purge club ${clubId} (no deletes). Re-run with --yes to execute.`,
    );
    return;
  }

  if (!yes) {
    console.error(
      `Refusing to delete. Run: npx tsx ./scripts/purge-club-from-mongo.ts ${clubId} --yes`,
    );
    process.exit(1);
  }

  console.log(`Purging MongoDB data for club folder id: ${clubId} …`);
  const { collections, userLogin } = await purgeEntireClubFromMongo(clubId);
  const nColl = totalClubPurgeDeletedCounts(collections.deleted);
  console.log(
    `Collections: removed ${nColl} document(s); userLogin removed ${userLogin.deleted}.`,
  );
  if (collections.errors.length > 0) {
    console.error("Collection errors:", collections.errors);
  }
  if (userLogin.error) {
    console.error("userLogin purge error:", userLogin.error);
    process.exit(1);
  }
  console.log("Done.");
}

main()
  .catch((e) => {
    console.error(e);
    process.exit(1);
  })
  .finally(() => {
    void closeMongoClient();
  });
