/**
 * Imports `data/BasicInfo.json` into MongoDB `basicInfo` collection as one document
 * with `_id` {@link BASIC_INFO_LISTS_DOC_ID} (countries + sportTypes arrays).
 *
 * From `src/backend`:
 *   npm run mongo:seed-basicinfo-json
 *   npm run mongo:seed-basicinfo-json -- --dry-run
 *
 * Optional: `BASICINFO_JSON_PATH` (absolute or relative to `src/backend`).
 */
import fs from "fs";
import path from "path";
import { loadLocalEnvFile } from "../src/config/env";
import {
  BASIC_INFO_COLLECTION,
  BASIC_INFO_LISTS_DOC_ID,
  closeMongoClient,
  ensureBasicInfoCollection,
  getBasicInfoCollection,
  isMongoConfigured,
  resolveBasicInfoDatabaseName,
} from "../src/db/DBConnection";

function isStringArray(x: unknown): x is string[] {
  return Array.isArray(x) && x.every((v) => typeof v === "string");
}

function readBasicInfoJson(filePath: string): {
  countries: string[];
  sportTypes: string[];
} {
  const raw = fs.readFileSync(filePath, "utf8");
  const data = JSON.parse(raw) as unknown;
  if (!data || typeof data !== "object") {
    throw new Error("BasicInfo.json must be a JSON object.");
  }
  const o = data as Record<string, unknown>;
  if (!isStringArray(o.countries) || !isStringArray(o.sportTypes)) {
    throw new Error(
      "BasicInfo.json must include string arrays `countries` and `sportTypes`.",
    );
  }
  return {
    countries: o.countries.map((s) => s.trim()).filter(Boolean),
    sportTypes: o.sportTypes.map((s) => s.trim()).filter(Boolean),
  };
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

  const dryRun = process.argv.includes("--dry-run");
  const rel =
    process.env.BASICINFO_JSON_PATH?.trim() ||
    path.join("data", "BasicInfo.json");
  const jsonPath = path.isAbsolute(rel)
    ? rel
    : path.join(backendRoot, rel);

  if (!fs.existsSync(jsonPath)) {
    console.error(`BasicInfo JSON not found: ${jsonPath}`);
    process.exit(1);
  }

  const lists = readBasicInfoJson(jsonPath);
  const dbName = resolveBasicInfoDatabaseName();

  console.log(
    `Import ${jsonPath} → ${dbName}.${BASIC_INFO_COLLECTION} (_id="${BASIC_INFO_LISTS_DOC_ID}") ` +
      `(${lists.countries.length} countries, ${lists.sportTypes.length} sport types).`,
  );

  if (dryRun) {
    console.log("Dry run: no writes.");
    return;
  }

  try {
    await ensureBasicInfoCollection(dbName);
    const coll = await getBasicInfoCollection(dbName);
    const doc = {
      _id: BASIC_INFO_LISTS_DOC_ID,
      countries: lists.countries,
      sportTypes: lists.sportTypes,
      lastImportedAt: new Date(),
    };
    await coll.replaceOne({ _id: BASIC_INFO_LISTS_DOC_ID }, doc, {
      upsert: true,
    });

    console.log(
      JSON.stringify(
        {
          ok: true,
          database: dbName,
          collection: BASIC_INFO_COLLECTION,
          upsertedId: doc._id,
        },
        null,
        2,
      ),
    );
  } finally {
    await closeMongoClient();
  }
}

main().catch((e) => {
  console.error(e instanceof Error ? e.message : String(e));
  process.exit(1);
});
