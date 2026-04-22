import type { BasicInfoLists } from "./basicInfoCsv";
import { normalizeBasicInfoKey, parseBasicInfoRow } from "./basicInfoCsv";
import {
  BASIC_INFO_LISTS_DOC_ID,
  getBasicInfoCollection,
  getMongoDb,
  isMongoConfigured,
  resolveBasicInfoDatabaseName,
} from "./db/DBConnection";

/** Legacy / Compass-style collection name (PascalCase) in `ClubMaster_DB`. */
const BASIC_INFO_LEGACY_COLLECTION = "BasicInfo";

function mergeUniqueLists(first?: string[], second?: string[]): string[] {
  const seen = new Set<string>();
  const out: string[] = [];
  for (const src of [first ?? [], second ?? []]) {
    for (const x of src) {
      const t = String(x ?? "").trim();
      if (!t) {
        continue;
      }
      const k = t.toLowerCase();
      if (seen.has(k)) {
        continue;
      }
      seen.add(k);
      out.push(t);
    }
  }
  return out;
}

async function readCanonicalBasicInfoListsDoc(
  databaseName?: string,
): Promise<BasicInfoLists | null> {
  const coll = await getBasicInfoCollection(databaseName);
  const doc = await coll.findOne({ _id: BASIC_INFO_LISTS_DOC_ID });
  if (!doc) {
    return null;
  }
  const countries = Array.isArray(doc.countries)
    ? doc.countries.map((c) => String(c ?? "").trim()).filter(Boolean)
    : [];
  const sportTypes = Array.isArray(doc.sportTypes)
    ? doc.sportTypes.map((c) => String(c ?? "").trim()).filter(Boolean)
    : [];
  return { countries, sportTypes };
}

/**
 * Reads `BasicInfo` (capital B) as either:
 * - the same lists document shape as `basicInfo` (`countries` / `sportTypes` arrays), or
 * - row-style docs (`Key`/`Value`, `Type`/`Value`, etc.) matching `BasicInfo.csv`, or
 * - single-field rows (`Country`, `SportType`, …).
 */
async function readLegacyBasicInfoPascalCollection(
  databaseName?: string,
): Promise<BasicInfoLists | null> {
  const db = await getMongoDb(resolveBasicInfoDatabaseName(databaseName));
  const exists =
    (
      await db
        .listCollections({ name: BASIC_INFO_LEGACY_COLLECTION }, { nameOnly: true })
        .toArray()
    ).length > 0;
  if (!exists) {
    return null;
  }
  const coll = db.collection(BASIC_INFO_LEGACY_COLLECTION);
  const docs = await coll.find({}).toArray();
  if (!docs.length) {
    return null;
  }

  const countries: string[] = [];
  const sportTypes: string[] = [];
  const seenC = new Set<string>();
  const seenS = new Set<string>();
  const addC = (v: string) => {
    const t = v.trim();
    if (!t || seenC.has(t.toLowerCase())) {
      return;
    }
    seenC.add(t.toLowerCase());
    countries.push(t);
  };
  const addS = (v: string) => {
    const t = v.trim();
    if (!t || seenS.has(t.toLowerCase())) {
      return;
    }
    seenS.add(t.toLowerCase());
    sportTypes.push(t);
  };

  for (const raw of docs) {
    const d = raw as Record<string, unknown>;
    if (Array.isArray(d.countries)) {
      for (const c of d.countries) {
        addC(String(c ?? ""));
      }
    }
    if (Array.isArray(d.sportTypes)) {
      for (const s of d.sportTypes) {
        addS(String(s ?? ""));
      }
    }

    const keyStr = (v: unknown) =>
      typeof v === "string" ? v : v != null ? String(v) : "";
    const keyCol = keyStr(
      d.Key ?? d.key ?? d.Type ?? d.type ?? d.Category ?? d.category,
    );
    const valCol = keyStr(
      d.Value ?? d.value ?? d.Name ?? d.name ?? d.Item ?? d.item,
    );
    if (keyCol && valCol) {
      const row = parseBasicInfoRow(keyCol, valCol);
      if (row?.kind === "country") {
        addC(row.value);
      } else if (row?.kind === "sporttype") {
        addS(row.value);
      }
    }

    for (const [k, v] of Object.entries(d)) {
      if (k === "_id" || v == null) {
        continue;
      }
      if (typeof v !== "string") {
        continue;
      }
      const kind = normalizeBasicInfoKey(k);
      if (kind === "country") {
        addC(v);
      } else if (kind === "sporttype") {
        addS(v);
      }
    }
  }

  if (!countries.length && !sportTypes.length) {
    return null;
  }
  return { countries, sportTypes };
}

/**
 * Loads countries / sport types from MongoDB `ClubMaster_DB` (see {@link resolveBasicInfoDatabaseName}):
 * - canonical `basicInfo` document {@link BASIC_INFO_LISTS_DOC_ID}, merged with
 * - legacy `BasicInfo` collection (PascalCase) when present.
 *
 * Returns `null` if Mongo is not configured, both sources are empty/missing, or read fails
 * (caller may fall back to CSV).
 */
export async function readBasicInfoFromMongo(): Promise<BasicInfoLists | null> {
  if (!isMongoConfigured()) {
    return null;
  }
  try {
    const canon = await readCanonicalBasicInfoListsDoc();
    const legacy = await readLegacyBasicInfoPascalCollection();
    const countries = mergeUniqueLists(canon?.countries, legacy?.countries);
    const sportTypes = mergeUniqueLists(canon?.sportTypes, legacy?.sportTypes);
    if (!countries.length && !sportTypes.length) {
      return null;
    }
    return { countries, sportTypes };
  } catch (e) {
    console.warn(
      "[basic-info] Mongo read failed:",
      e instanceof Error ? e.message : String(e),
    );
    return null;
  }
}
