import type { BasicInfoCountryEntry, BasicInfoLists } from "./basicInfoCsv";
import {
  basicInfoCountriesForMongoPayload,
  mergeUniqueCountryLists,
  normalizeBasicInfoCountryEntry,
  normalizeBasicInfoKey,
  parseBasicInfoRow,
} from "./basicInfoCsv";
import {
  BASIC_INFO_LISTS_DOC_ID,
  ensureBasicInfoCollection,
  getBasicInfoCollection,
  getMongoDb,
  isMongoConfigured,
  resolveBasicInfoDatabaseName,
} from "./db/DBConnection";

/** Legacy / Compass-style collection name (PascalCase) in `ClubMaster_DB`. */
const BASIC_INFO_LEGACY_COLLECTION = "BasicInfo";

/** Sync collection validator (object-shaped `countries` rows) before canonical writes. */
async function ensureBasicInfoValidator(): Promise<void> {
  await ensureBasicInfoCollection();
}

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

function countriesFromDocArray(raw: unknown): BasicInfoCountryEntry[] {
  if (!Array.isArray(raw)) {
    return [];
  }
  const out: BasicInfoCountryEntry[] = [];
  for (const c of raw) {
    const e = normalizeBasicInfoCountryEntry(c);
    if (e) {
      out.push(e);
    }
  }
  return mergeUniqueCountryLists(out, []);
}

async function readCanonicalBasicInfoListsDoc(
  databaseName?: string,
): Promise<BasicInfoLists | null> {
  const coll = await getBasicInfoCollection(databaseName);
  const doc = await coll.findOne({ _id: BASIC_INFO_LISTS_DOC_ID });
  if (!doc) {
    return null;
  }
  const countries = countriesFromDocArray(doc.countries);
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

  const countryBuf: BasicInfoCountryEntry[] = [];
  const sportTypes: string[] = [];
  const seenS = new Set<string>();
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
        const e = normalizeBasicInfoCountryEntry(c);
        if (e) {
          countryBuf.push(e);
        }
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
        countryBuf.push({ name: row.value, prefix: "" });
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
        countryBuf.push({ name: v, prefix: "" });
      } else if (kind === "sporttype") {
        addS(v);
      }
    }
  }

  const countries = mergeUniqueCountryLists(countryBuf, []);

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
    const countries = mergeUniqueCountryLists(
      canon?.countries ?? [],
      legacy?.countries ?? [],
    );
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

function countriesForMongoWrite(rows: BasicInfoCountryEntry[]) {
  return basicInfoCountriesForMongoPayload(rows);
}

/**
 * Append a sport type to the canonical `basicInfo` document (`basicInfoLists`) only.
 * Merged reads may still include legacy `BasicInfo` collection entries.
 */
export async function addSportTypeToCanonicalMongo(
  name: string,
): Promise<
  { ok: true; sportTypes: string[] } | { ok: false; error: string }
> {
  if (!isMongoConfigured()) {
    return { ok: false, error: "MongoDB is not configured." };
  }
  const trimmed = String(name ?? "").trim();
  if (!trimmed) {
    return { ok: false, error: "Sport type name is required." };
  }
  await ensureBasicInfoValidator();
  const coll = await getBasicInfoCollection();
  const existing = await coll.findOne({ _id: BASIC_INFO_LISTS_DOC_ID });
  const countries = countriesForMongoWrite(
    countriesFromDocArray(existing?.countries),
  );
  let sportTypes = Array.isArray(existing?.sportTypes)
    ? existing!.sportTypes.map((c) => String(c ?? "").trim()).filter(Boolean)
    : [];
  const lower = trimmed.toLowerCase();
  if (sportTypes.some((s) => s.toLowerCase() === lower)) {
    return { ok: false, error: "That sport type already exists in basicInfo lists." };
  }
  sportTypes = [...sportTypes, trimmed];
  await coll.updateOne(
    { _id: BASIC_INFO_LISTS_DOC_ID },
    {
      $set: {
        countries,
        sportTypes,
        lastImportedAt: new Date(),
      },
      $setOnInsert: { _id: BASIC_INFO_LISTS_DOC_ID },
    },
    { upsert: true },
  );
  return { ok: true, sportTypes };
}

/**
 * Rename one sport type in the canonical `basicInfo` document only (match by `oldName`, trim, case-insensitive).
 */
export async function updateSportTypeInCanonicalMongo(
  oldName: string,
  newName: string,
): Promise<
  { ok: true; sportTypes: string[] } | { ok: false; error: string }
> {
  if (!isMongoConfigured()) {
    return { ok: false, error: "MongoDB is not configured." };
  }
  const o = String(oldName ?? "").trim();
  const n = String(newName ?? "").trim();
  if (!o || !n) {
    return { ok: false, error: "Current and new sport type names are required." };
  }
  await ensureBasicInfoValidator();
  const coll = await getBasicInfoCollection();
  const existing = await coll.findOne({ _id: BASIC_INFO_LISTS_DOC_ID });
  if (!existing) {
    return {
      ok: false,
      error:
        "No basicInfo lists document yet. Add a sport type first (creates the document).",
    };
  }
  let sportTypes = Array.isArray(existing.sportTypes)
    ? existing.sportTypes.map((c) => String(c ?? "").trim()).filter(Boolean)
    : [];
  const idx = sportTypes.findIndex((s) => s.toLowerCase() === o.toLowerCase());
  if (idx < 0) {
    return {
      ok: false,
      error:
        "That sport type is not in the canonical basicInfo document (it may exist only in legacy BasicInfo data).",
    };
  }
  if (sportTypes.some((s, i) => i !== idx && s.toLowerCase() === n.toLowerCase())) {
    return { ok: false, error: "Another sport type already uses that name." };
  }
  const next = [...sportTypes];
  next[idx] = n;
  await coll.updateOne(
    { _id: BASIC_INFO_LISTS_DOC_ID },
    {
      $set: {
        sportTypes: next,
        lastImportedAt: new Date(),
      },
    },
  );
  return { ok: true, sportTypes: next };
}

/**
 * Remove one sport type from the canonical `basicInfo` document (case-insensitive name match).
 */
export async function removeSportTypeFromCanonicalMongo(
  name: string,
): Promise<
  { ok: true; sportTypes: string[] } | { ok: false; error: string }
> {
  if (!isMongoConfigured()) {
    return { ok: false, error: "MongoDB is not configured." };
  }
  const trimmed = String(name ?? "").trim();
  if (!trimmed) {
    return { ok: false, error: "Sport type name is required." };
  }
  await ensureBasicInfoValidator();
  const coll = await getBasicInfoCollection();
  const existing = await coll.findOne({ _id: BASIC_INFO_LISTS_DOC_ID });
  if (!existing) {
    return {
      ok: false,
      error:
        "No basicInfo lists document yet. Add a sport type or country first (creates the document).",
    };
  }
  let sportTypes = Array.isArray(existing.sportTypes)
    ? existing.sportTypes.map((c) => String(c ?? "").trim()).filter(Boolean)
    : [];
  const lower = trimmed.toLowerCase();
  const idx = sportTypes.findIndex((s) => s.toLowerCase() === lower);
  if (idx < 0) {
    return {
      ok: false,
      error:
        "That sport type is not in the canonical basicInfo document (it may exist only in legacy BasicInfo data).",
    };
  }
  const next = sportTypes.filter((_, i) => i !== idx);
  const countries = countriesForMongoWrite(
    countriesFromDocArray(existing.countries),
  );
  await coll.updateOne(
    { _id: BASIC_INFO_LISTS_DOC_ID },
    {
      $set: {
        sportTypes: next,
        countries,
        lastImportedAt: new Date(),
      },
    },
  );
  return { ok: true, sportTypes: next };
}

/**
 * Append a country to the canonical `basicInfo` document (`basicInfoLists`) only.
 */
export async function addCountryToCanonicalMongo(
  name: string,
): Promise<
  | { ok: true; countries: BasicInfoCountryEntry[] }
  | { ok: false; error: string }
> {
  if (!isMongoConfigured()) {
    return { ok: false, error: "MongoDB is not configured." };
  }
  const trimmed = String(name ?? "").trim();
  if (!trimmed) {
    return { ok: false, error: "Country name is required." };
  }
  await ensureBasicInfoValidator();
  const coll = await getBasicInfoCollection();
  const existing = await coll.findOne({ _id: BASIC_INFO_LISTS_DOC_ID });
  const rows = countriesFromDocArray(existing?.countries);
  const lower = trimmed.toLowerCase();
  if (rows.some((r) => r.name.toLowerCase() === lower)) {
    return { ok: false, error: "That country already exists in basicInfo lists." };
  }
  const nextRows = [...rows, { name: trimmed, prefix: "" }];
  const countries = countriesForMongoWrite(nextRows);
  const sportTypes = Array.isArray(existing?.sportTypes)
    ? existing!.sportTypes.map((c) => String(c ?? "").trim()).filter(Boolean)
    : [];
  await coll.updateOne(
    { _id: BASIC_INFO_LISTS_DOC_ID },
    {
      $set: {
        countries,
        sportTypes,
        lastImportedAt: new Date(),
      },
      $setOnInsert: { _id: BASIC_INFO_LISTS_DOC_ID },
    },
    { upsert: true },
  );
  return { ok: true, countries: nextRows };
}

/**
 * Remove one country from the canonical `basicInfo` document (case-insensitive name match).
 */
export async function removeCountryFromCanonicalMongo(
  name: string,
): Promise<
  | { ok: true; countries: BasicInfoCountryEntry[] }
  | { ok: false; error: string }
> {
  if (!isMongoConfigured()) {
    return { ok: false, error: "MongoDB is not configured." };
  }
  const trimmed = String(name ?? "").trim();
  if (!trimmed) {
    return { ok: false, error: "Country name is required." };
  }
  await ensureBasicInfoValidator();
  const coll = await getBasicInfoCollection();
  const existing = await coll.findOne({ _id: BASIC_INFO_LISTS_DOC_ID });
  if (!existing) {
    return {
      ok: false,
      error:
        "No basicInfo lists document yet. Add a sport type or country first (creates the document).",
    };
  }
  const rows = countriesFromDocArray(existing.countries);
  const lower = trimmed.toLowerCase();
  const nextRows = rows.filter((r) => r.name.toLowerCase() !== lower);
  if (nextRows.length === rows.length) {
    return {
      ok: false,
      error:
        "That country is not in the canonical basicInfo document (it may exist only in legacy BasicInfo data).",
    };
  }
  const countries = countriesForMongoWrite(nextRows);
  const sportTypes = Array.isArray(existing.sportTypes)
    ? existing.sportTypes.map((c) => String(c ?? "").trim()).filter(Boolean)
    : [];
  await coll.updateOne(
    { _id: BASIC_INFO_LISTS_DOC_ID },
    {
      $set: {
        countries,
        sportTypes,
        lastImportedAt: new Date(),
      },
    },
  );
  return { ok: true, countries: nextRows };
}
