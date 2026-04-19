import path from "path";
import { loadLocalEnvFile } from "../src/config/env";
import {
  closeMongoClient,
  isMongoConfigured,
  resolveUserLoginDatabaseName,
} from "../src/db/DBConnection";
import { runUserListToUserLoginMigration } from "../src/migrateUserListToUserLogin";

async function main(): Promise<void> {
  const backendRoot = path.join(__dirname, "..");
  loadLocalEnvFile(backendRoot);

  if (!isMongoConfigured()) {
    console.error("MongoDB not configured. Set MONGODB_URI or MONGO_PASSWORD (see .env.example).");
    process.exit(1);
  }

  const removeFromUserList = process.argv.includes("--remove-from-userlist");
  const explicitDb = process.env.MONGO_USERLOGIN_DB?.trim();
  const databaseName = explicitDb || resolveUserLoginDatabaseName();

  console.log(
    `Migrating userList → userLogin (db: "${databaseName}")` +
      (removeFromUserList ? " with --remove-from-userlist" : "") +
      "...",
  );

  const out = await runUserListToUserLoginMigration({
    databaseName: explicitDb || undefined,
    removeFromUserListAfterInsert: removeFromUserList,
  });

  console.log(JSON.stringify(out, null, 2));
  if (out.errors.length > 0) {
    console.warn("Completed with errors (see errors array).");
  }
  await closeMongoClient();
}

main().catch(async (e) => {
  console.error(e);
  await closeMongoClient().catch(() => undefined);
  process.exit(1);
});
