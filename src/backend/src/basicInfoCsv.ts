import fs from "fs";
import path from "path";
import { readFileCached } from "./dataFileCache";

/** One country row for reference lists (CSV, Mongo `basicInfo`, API). */
export type BasicInfoCountryEntry = {
  name: string;
  /** Dial / country code style prefix (e.g. +852); empty when unset. */
  prefix: string;
  /** ISO-style territory code when known (e.g. HK, US). */
  country_code?: string;
};

/** Display name (trimmed, case-insensitive) → territory code for known countries. */
const COUNTRY_NAME_TO_CODE: Record<string, string> = {
  "hong kong": "HK",
  china: "CN",
  taiwan: "TW",
  singapore: "SG",
  malaysia: "MY",
  thailand: "TH",
  vietnam: "VN",
  philippines: "PH",
  indonesia: "ID",
  japan: "JP",
  "south korea": "KR",
  "united states": "US",
  canada: "CA",
  "united kingdom": "UK",
  australia: "AU",
};

function countryNameLookupKey(name: string): string {
  return name.trim().toLowerCase().replace(/\s+/g, " ");
}

/** Resolves a known `country_code` from the canonical English display name. */
export function lookupCountryCodeForBasicInfoName(name: string): string {
  const code = COUNTRY_NAME_TO_CODE[countryNameLookupKey(name)];
  return code ?? "";
}

/**
 * Shape stored in Mongo `basicInfo` for each country row (legacy strings excluded).
 */
export function basicInfoCountriesForMongoPayload(
  rows: BasicInfoCountryEntry[],
): Array<[string, string]> {
  return rows
    .map((c) => {
      const name = String(c.name ?? "").trim();
      if (!name) {
        return null;
      }
      const code =
        String(c.country_code ?? "").trim() ||
        lookupCountryCodeForBasicInfoName(name) ||
        "";
      return [name, code] as [string, string];
    })
    .filter((x): x is [string, string] => x != null);
}

export type BasicInfoLists = {
  countries: BasicInfoCountryEntry[];
  sportTypes: string[];
};

/**
 * Normalize a legacy string, `{ name, prefix }`, or loose Mongo object into
 * {@link BasicInfoCountryEntry}, or `null` when unusable.
 */
export function normalizeBasicInfoCountryEntry(
  raw: unknown,
): BasicInfoCountryEntry | null {
  if (raw == null) {
    return null;
  }
  // New canonical shape: [country, country_code]
  if (Array.isArray(raw)) {
    const name = String(raw[0] ?? "").trim();
    if (!name) {
      return null;
    }
    const codeRaw = String(raw[1] ?? "").trim();
    const country_code = codeRaw || lookupCountryCodeForBasicInfoName(name);
    return country_code
      ? { name, prefix: "", country_code }
      : { name, prefix: "" };
  }
  if (typeof raw === "string") {
    const name = raw.trim();
    if (!name) {
      return null;
    }
    const country_code = lookupCountryCodeForBasicInfoName(name);
    return country_code
      ? { name, prefix: "", country_code }
      : { name, prefix: "" };
  }
  if (typeof raw === "object") {
    const o = raw as Record<string, unknown>;
    const name = String(o.name ?? o.Name ?? "").trim();
    if (!name) {
      return null;
    }
    const prefix = String(o.prefix ?? o.Prefix ?? "").trim();
    let country_code = String(
      o.country_code ?? o.CountryCode ?? o.countryCode ?? "",
    ).trim();
    if (!country_code) {
      country_code = lookupCountryCodeForBasicInfoName(name);
    }
    return country_code
      ? { name, prefix, country_code }
      : { name, prefix };
  }
  return null;
}

/**
 * Merge two country lists: first list wins display order; later entries with the
 * same name (case-insensitive) only fill an empty `prefix` on the kept row.
 */
export function mergeUniqueCountryLists(
  first: BasicInfoCountryEntry[],
  second: BasicInfoCountryEntry[],
): BasicInfoCountryEntry[] {
  const map = new Map<string, BasicInfoCountryEntry>();
  const order: string[] = [];
  const ingest = (rows: BasicInfoCountryEntry[]) => {
    for (const row of rows) {
      const name = String(row?.name ?? "").trim();
      if (!name) {
        continue;
      }
      const k = name.toLowerCase();
      const prefix = String(row.prefix ?? "").trim();
      const rowCodeRaw = String(row.country_code ?? "").trim();
      const rowCode =
        rowCodeRaw || lookupCountryCodeForBasicInfoName(name) || "";
      const prev = map.get(k);
      if (!prev) {
        map.set(
          k,
          rowCode
            ? { name, prefix, country_code: rowCode }
            : { name, prefix },
        );
        order.push(k);
      } else {
        const nextPrefix = !prev.prefix && prefix ? prefix : prev.prefix;
        const prevCode = String(prev.country_code ?? "").trim();
        const nextCode = prevCode || rowCode || "";
        if (nextPrefix !== prev.prefix || nextCode !== prevCode) {
          map.set(
            k,
            nextCode
              ? {
                  name: prev.name,
                  prefix: nextPrefix,
                  country_code: nextCode,
                }
              : { name: prev.name, prefix: nextPrefix },
          );
        }
      }
    }
  };
  ingest(first);
  ingest(second);
  return order.map((k) => map.get(k)!);
}

/** Normalize CSV key column: SportType, Sport_type, Sport Type → "sporttype"; Country → "country". */
export function normalizeBasicInfoKey(
  rawKey: string,
): "sporttype" | "country" | null {
  const compact = rawKey
    .trim()
    .toLowerCase()
    .replace(/[\s_]+/g, "");
  if (compact === "sporttype") {
    return "sporttype";
  }
  if (compact === "country") {
    return "country";
  }
  return null;
}

/**
 * Parses backend/data/BasicInfo.csv — rows like `SportType, Badminton` or `Country, Hong Kong`.
 * Order matches file order; duplicate values are skipped (first wins).
 */
export function parseBasicInfoContent(content: string): BasicInfoLists {
  const countries: BasicInfoCountryEntry[] = [];
  const sportTypes: string[] = [];
  const seenC = new Set<string>();
  const seenS = new Set<string>();

  const text = content.replace(/^\uFEFF/, "");

  for (const line of text.split(/\r?\n/)) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith("#")) {
      continue;
    }
    const comma = trimmed.indexOf(",");
    if (comma < 0) {
      continue;
    }
    const rawKey = trimmed.slice(0, comma).trim();
    const val = trimmed.slice(comma + 1).trim();
    if (!rawKey || !val) {
      continue;
    }

    const kind = normalizeBasicInfoKey(rawKey);
    if (kind === "sporttype" && !seenS.has(val)) {
      seenS.add(val);
      sportTypes.push(val);
    } else if (kind === "country" && !seenC.has(val)) {
      seenC.add(val);
      countries.push({ name: val, prefix: "" });
    }
  }

  return { countries, sportTypes };
}

/**
 * Classify one CSV-style or Mongo row (`Key` / `Type` + `Value`) into country vs sport type.
 */
export function parseBasicInfoRow(
  rawKey: string,
  val: string,
): { kind: "country" | "sporttype"; value: string } | null {
  const kind = normalizeBasicInfoKey(rawKey);
  const v = val.trim();
  if (!kind || !v) {
    return null;
  }
  return { kind, value: v };
}

export function readBasicInfo(): BasicInfoLists {
  const p = path.join(__dirname, "..", "data", "BasicInfo.csv");
  return readFileCached(
    p,
    (content) => parseBasicInfoContent(content),
    { countries: [], sportTypes: [] },
  );
}
