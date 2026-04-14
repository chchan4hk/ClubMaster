import fs from "fs";
import path from "path";
import {
  getDataClubRootPath,
  isValidClubFolderId,
  parseCsvLine,
} from "./coachListCsv";

export const SPORT_CENTER_LIST_FILENAME = "SportCenterList.csv";

function escapeCsvField(s: string): string {
  const v = String(s ?? "");
  if (/[",\n\r]/.test(v)) {
    return `"${v.replace(/"/g, '""')}"`;
  }
  return v;
}

function joinCsvRow(cells: string[]): string {
  return cells.map(escapeCsvField).join(",");
}

function normHeader(h: string): string {
  return h
    .replace(/^\uFEFF/, "")
    .replace(/^"|"$/g, "")
    .replace(/\t/g, " ")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ");
}

function normCompact(h: string): string {
  return normHeader(h).replace(/\s/g, "");
}

function colIndex(headerCells: string[], candidates: string[]): number {
  const norms = headerCells.map(normHeader);
  const compacts = headerCells.map(normCompact);
  for (const cand of candidates) {
    const n = normHeader(cand);
    let i = norms.indexOf(n);
    if (i >= 0) {
      return i;
    }
    const c = normCompact(cand);
    i = compacts.indexOf(c);
    if (i >= 0) {
      return i;
    }
  }
  return -1;
}

export type SportCenterColIdx = {
  status: number;
  country: number;
  sportType: number;
  location: number;
  sportCenter: number;
  address: number;
  link: number;
};

function resolveSportCenterNameColumnIndex(headerCells: string[]): number {
  const direct = colIndex(headerCells, [
    "Sport Center",
    "SportCenter",
    "Sport_Center",
    "sport_center",
    "SPORT_CENTER",
    "sport center",
    "Sport centre",
    "sport centre",
  ]);
  if (direct >= 0) {
    return direct;
  }
  const compacts = headerCells.map(normCompact);
  for (let i = 0; i < compacts.length; i++) {
    const x = compacts[i]!;
    if (
      x === "sportcenter" ||
      x === "sport_center" ||
      x.endsWith("sportcenter") ||
      (x.includes("sport") && x.includes("center"))
    ) {
      return i;
    }
  }
  return -1;
}

export function resolveSportCenterColumnIndices(
  headerCells: string[],
): SportCenterColIdx {
  return {
    status: colIndex(headerCells, ["Status", "status"]),
    country: colIndex(headerCells, ["Country", "country"]),
    sportType: colIndex(headerCells, [
      "SportType",
      "Sport Type",
      "sport_type",
      "sport type",
    ]),
    location: colIndex(headerCells, ["Location", "location"]),
    sportCenter: resolveSportCenterNameColumnIndex(headerCells),
    address: colIndex(headerCells, ["Address", "address"]),
    link: colIndex(headerCells, ["Link", "link", "URL", "url"]),
  };
}

function getCell(cells: string[], i: number): string {
  if (i < 0 || i >= cells.length) {
    return "";
  }
  return cells[i] ?? "";
}

export type SportCenterParsedRow = {
  rowIndex: number;
  Status: string;
  Country: string;
  SportType: string;
  Location: string;
  sportCenter: string;
  Address: string;
  Link: string;
};

export function sportCenterListPath(clubId: string): string {
  return path.join(getDataClubRootPath(), clubId.trim(), SPORT_CENTER_LIST_FILENAME);
}

export function ensureSportCenterListFile(clubId: string): void {
  if (!isValidClubFolderId(clubId)) {
    throw new Error("Invalid club ID.");
  }
  const dir = path.join(getDataClubRootPath(), clubId.trim());
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true });
  }
  const p = sportCenterListPath(clubId);
  if (!fs.existsSync(p)) {
    const src = path.join(getDataClubRootPath(), "Src", SPORT_CENTER_LIST_FILENAME);
    if (fs.existsSync(src)) {
      fs.copyFileSync(src, p);
    } else {
      fs.writeFileSync(
        p,
        "Status,Country,SportType,Location,Sport Center,Address,Link\n",
        "utf8",
      );
    }
  }
}

function rowToParsed(
  rowIndex: number,
  cells: string[],
  idx: SportCenterColIdx,
): SportCenterParsedRow {
  return {
    rowIndex,
    Status: getCell(cells, idx.status),
    Country: getCell(cells, idx.country),
    SportType: getCell(cells, idx.sportType),
    Location: getCell(cells, idx.location),
    sportCenter: getCell(cells, idx.sportCenter),
    Address: getCell(cells, idx.address),
    Link: getCell(cells, idx.link),
  };
}

export function loadSportCenterRows(clubId: string): {
  headerLine: string;
  headers: string[];
  idx: SportCenterColIdx;
  rows: SportCenterParsedRow[];
} {
  ensureSportCenterListFile(clubId);
  const p = sportCenterListPath(clubId);
  const raw = fs.readFileSync(p, "utf8").replace(/^\uFEFF/, "");
  const lines = raw.split(/\r?\n/).filter((l) => l.trim() !== "");
  if (lines.length === 0) {
    const h = "Status,Country,SportType,Location,Sport Center,Address,Link";
    const headers = parseCsvLine(h);
    return {
      headerLine: h,
      headers,
      idx: resolveSportCenterColumnIndices(headers),
      rows: [],
    };
  }
  const headerLine = lines[0]!;
  const headers = parseCsvLine(headerLine);
  const idx = resolveSportCenterColumnIndices(headers);
  const rows: SportCenterParsedRow[] = [];
  for (let i = 1; i < lines.length; i++) {
    const cells = parseCsvLine(lines[i]!);
    rows.push(rowToParsed(i - 1, cells, idx));
  }
  return { headerLine, headers, idx, rows };
}

export function normEqSportType(a: string, b: string): boolean {
  return (
    String(a ?? "").trim().toLowerCase() === String(b ?? "").trim().toLowerCase()
  );
}

function csvStatusIsActive(status: string): boolean {
  const t = String(status ?? "")
    .replace(/^\uFEFF/g, "")
    .replace(/[\u200B-\u200D\uFEFF]/g, "")
    .trim()
    .toUpperCase();
  return t === "ACTIVE";
}

/**
 * Distinct values from the Sport Center column where Status is ACTIVE (case-insensitive).
 * File order; names are exactly as stored in CSV (e.g. 界限街一號體育館).
 */
export function listActiveSportCenterNames(clubId: string): string[] {
  const { rows } = loadSportCenterRows(clubId);
  const seen = new Set<string>();
  const out: string[] = [];
  for (const r of rows) {
    if (!csvStatusIsActive(r.Status)) {
      continue;
    }
    const n = String(r.sportCenter ?? "").trim();
    if (!n) {
      continue;
    }
    const k = n.toLowerCase();
    if (seen.has(k)) {
      continue;
    }
    seen.add(k);
    out.push(n);
  }
  return out;
}

/**
 * Set Status cell for given 0-based data row indices. Other rows unchanged.
 */
const VENUE_LOCATIONS = new Set(["clubhouse", "school", "others"]);

export function isAllowedVenueLocation(location: string): boolean {
  const t = String(location ?? "")
    .trim()
    .toLowerCase();
  return VENUE_LOCATIONS.has(t);
}

/** Append one data row; preserves existing file header column order. */
export function appendSportCenterRow(
  clubId: string,
  input: {
    status: string;
    country: string;
    sportType: string;
    location: string;
    sportCenter: string;
    address: string;
    link: string;
  },
): { ok: true } | { ok: false; error: string } {
  ensureSportCenterListFile(clubId);
  const { headers, idx } = loadSportCenterRows(clubId);
  if (
    idx.status < 0 ||
    idx.country < 0 ||
    idx.sportType < 0 ||
    idx.location < 0 ||
    idx.sportCenter < 0 ||
    idx.address < 0 ||
    idx.link < 0
  ) {
    return {
      ok: false,
      error:
        "SportCenterList.csv must include Status, Country, SportType, Location, sport center name, Address, and Link columns.",
    };
  }
  const cells: string[] = new Array(headers.length).fill("");
  cells[idx.status] = String(input.status ?? "").trim() || "ACTIVE";
  cells[idx.country] = String(input.country ?? "").trim();
  cells[idx.sportType] = String(input.sportType ?? "").trim();
  cells[idx.location] = String(input.location ?? "").trim();
  cells[idx.sportCenter] = String(input.sportCenter ?? "").trim();
  cells[idx.address] = String(input.address ?? "").trim();
  cells[idx.link] = String(input.link ?? "").trim();
  const line = joinCsvRow(cells);
  const p = sportCenterListPath(clubId);
  fs.appendFileSync(p, `${line}\n`, "utf8");
  return { ok: true };
}

export function applySportCenterStatusUpdates(
  clubId: string,
  statusByRowIndex: Map<number, string>,
): void {
  ensureSportCenterListFile(clubId);
  const p = sportCenterListPath(clubId);
  const raw = fs.readFileSync(p, "utf8");
  const allLines = raw.split(/\r?\n/);
  const nonEmpty = allLines.filter((l) => l.trim() !== "");
  if (nonEmpty.length === 0) {
    return;
  }
  const headerLine = nonEmpty[0]!;
  const headers = parseCsvLine(headerLine);
  const idx = resolveSportCenterColumnIndices(headers);
  if (idx.status < 0) {
    throw new Error("SportCenterList.csv: missing Status column.");
  }
  const out: string[] = [headerLine];
  let dataRow = 0;
  for (let i = 1; i < nonEmpty.length; i++) {
    const ln = nonEmpty[i]!;
    let cells = parseCsvLine(ln);
    while (cells.length < headers.length) {
      cells.push("");
    }
    if (statusByRowIndex.has(dataRow)) {
      const st = String(statusByRowIndex.get(dataRow) ?? "").trim();
      cells[idx.status] = st || (cells[idx.status] ?? "");
    }
    out.push(joinCsvRow(cells.slice(0, headers.length)));
    dataRow += 1;
  }
  fs.writeFileSync(p, `${out.join("\n")}\n`, "utf8");
}
