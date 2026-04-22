import fs from "fs";
import path from "path";
import { readFileCached } from "./dataFileCache";

export type BasicInfoLists = {
  countries: string[];
  sportTypes: string[];
};

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
  const countries: string[] = [];
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
      countries.push(val);
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
