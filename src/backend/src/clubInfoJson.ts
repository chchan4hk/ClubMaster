import fs from "fs";
import path from "path";
import {
  clubDataDir,
  isValidClubFolderId,
  parseCsvLine,
} from "./coachListCsv";

/** Primary club info store under `data_club/{clubId}/`. */
export const CLUB_INFO_FILENAME = "ClubInfo.json";

/** Legacy CSV filename; migrated to JSON on first read (then removed). */
export const CLUB_INFO_CSV_LEGACY = "ClubInfo.csv";

/** Human-readable field order label (API `headerLine` / tabular view). */
export const CLUB_INFO_FIELD_ORDER_LABEL =
  "Sport_type, Club_name,country,setup_date,contact_point,contact_email, club_desc,club_logo,lastUpdate_date";

const CLUB_INFO_FIELD_KEYS: string[] = [
  "Sport_type",
  "Club_name",
  "country",
  "setup_date",
  "contact_point",
  "contact_email",
  "club_desc",
  "club_logo",
  "lastUpdate_date",
];

function emptyCanonicalRow(): Record<string, string> {
  return Object.fromEntries(CLUB_INFO_FIELD_KEYS.map((k) => [k, ""])) as Record<
    string,
    string
  >;
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

type ClubInfoColIdx = {
  sportType: number;
  clubName: number;
  country: number;
  setupDate: number;
  clubDesc: number;
  clubLogo: number;
  lastUpdate: number;
};

function resolveClubInfoColumnIndices(headerCells: string[]): ClubInfoColIdx {
  return {
    sportType: colIndex(headerCells, [
      "SportType",
      "Sport_type",
      "sport_type",
      "Sport type",
      "sport type",
    ]),
    clubName: colIndex(headerCells, [
      "Club_name",
      "club_name",
      "Club Name",
      "club name",
    ]),
    country: colIndex(headerCells, ["country", "Country"]),
    setupDate: colIndex(headerCells, [
      "setup_date",
      "Setup Date",
      "setup date",
    ]),
    clubDesc: colIndex(headerCells, [
      "club_desc",
      "Club Desc",
      "club desc",
      "description",
    ]),
    clubLogo: colIndex(headerCells, [
      "club_logo",
      "Club Logo",
      "club logo",
      "logo",
    ]),
    lastUpdate: colIndex(headerCells, [
      "lastUpdate_date",
      "last_update_date",
      "Last Update Date",
      "last update date",
    ]),
  };
}

export function clubInfoPath(clubId: string): string {
  const dir = clubDataDir(clubId.trim());
  return path.join(dir, CLUB_INFO_FILENAME);
}

function clubInfoLegacyCsvPath(clubId: string): string {
  const dir = clubDataDir(clubId.trim());
  return path.join(dir, CLUB_INFO_CSV_LEGACY);
}

/** Club logo files are stored here (capital I). */
export const CLUB_IMAGE_DIR_NAME = "Image";
export const CLUB_LOGO_FILENAME = "club_logo.jpg";

/** Relative paths under `Image/` for payment QR JPEGs (matches existing club samples). */
export const CLUB_PAYMENT_QR_FILES = {
  payme: "payme_QR.jpg",
  fps: "FPS_QR.jpg",
  wechat: "wechat_QR.jpg",
  alipay: "alipay_QR.jpg",
  /** 支付寶 — separate image from English Alipay (`alipay_QR.jpg`). */
  zhifubao: "zhifubao_QR.jpg",
} as const;

export type ClubPaymentQrChannel = keyof typeof CLUB_PAYMENT_QR_FILES;

/** JSON keys for payment QR image paths in `ClubInfo.json`. */
export const CLUB_PAYMENT_QR_JSON_KEYS: Record<
  ClubPaymentQrChannel,
  string
> = {
  payme: "club_payment_payme",
  fps: "club_payment_FPS",
  wechat: "club_payment_wechat",
  alipay: "club_payment_alipay",
  zhifubao: "club_payment_支付寶",
};

export function clubPaymentQrRelativePath(
  channel: ClubPaymentQrChannel,
): string {
  return `${CLUB_IMAGE_DIR_NAME}/${CLUB_PAYMENT_QR_FILES[channel]}`;
}

export function isClubPaymentQrChannel(s: string): s is ClubPaymentQrChannel {
  return Object.prototype.hasOwnProperty.call(CLUB_PAYMENT_QR_FILES, s);
}

export function clubImageDir(clubId: string): string {
  const base = clubDataDir(clubId.trim());
  if (!base) {
    throw new Error("Invalid club ID.");
  }
  return path.join(base, CLUB_IMAGE_DIR_NAME);
}

/** Relative path stored in `club_logo` inside ClubInfo.json. */
export function clubLogoRelativePath(): string {
  return `${CLUB_IMAGE_DIR_NAME}/${CLUB_LOGO_FILENAME}`;
}

/** Public URL for a club asset path (forward slashes). */
export function clubAssetPublicUrl(clubId: string, relPath: string): string | null {
  const rel = String(relPath || "").trim().replace(/\\/g, "/");
  if (!rel || rel.includes("..")) {
    return null;
  }
  const parts = rel.split("/").filter(Boolean).map((p) => encodeURIComponent(p));
  if (parts.length === 0) {
    return null;
  }
  return `/backend/data_club/${encodeURIComponent(clubId.trim())}/${parts.join("/")}`;
}

export function clubInfoResolvedPath(clubId: string): string {
  const id = clubId.trim();
  if (!isValidClubFolderId(id)) {
    return "";
  }
  return path.normalize(clubInfoPath(id));
}

/**
 * Full `ClubInfo.json` object (includes keys not in the canonical coach-manager row,
 * e.g. `club_payment_payme` QR paths).
 */
export function loadClubInfoExtended(clubId: string): Record<string, unknown> {
  ensureClubInfoFile(clubId);
  const p = clubInfoPath(clubId);
  try {
    const raw = fs.readFileSync(p, "utf8");
    const o = JSON.parse(raw) as Record<string, unknown>;
    return o && typeof o === "object" ? o : {};
  } catch {
    return { version: 1 };
  }
}

function writeClubInfoJsonFile(absPath: string, row: Record<string, string>): void {
  let existing: Record<string, unknown> = {};
  if (fs.existsSync(absPath)) {
    try {
      const raw = JSON.parse(fs.readFileSync(absPath, "utf8")) as unknown;
      if (raw && typeof raw === "object" && !Array.isArray(raw)) {
        existing = raw as Record<string, unknown>;
      }
    } catch {
      existing = {};
    }
  }
  const ver =
    typeof existing.version === "number" && Number.isFinite(existing.version)
      ? existing.version
      : 1;
  const out: Record<string, unknown> = { ...existing, version: ver };
  for (const k of CLUB_INFO_FIELD_KEYS) {
    out[k] = row[k] ?? "";
  }
  fs.writeFileSync(absPath, `${JSON.stringify(out, null, 2)}\n`, "utf8");
}

function readClubInfoJsonFile(absPath: string): Record<string, string> {
  const base = emptyCanonicalRow();
  if (!fs.existsSync(absPath)) {
    return base;
  }
  try {
    const raw = JSON.parse(fs.readFileSync(absPath, "utf8")) as Record<
      string,
      unknown
    >;
    if (!raw || typeof raw !== "object") {
      return base;
    }
    for (const k of CLUB_INFO_FIELD_KEYS) {
      if (raw[k] != null) {
        base[k] = String(raw[k]);
      }
    }
  } catch {
    /* keep empty row */
  }
  return base;
}

function getCell(cells: string[], i: number): string {
  if (i < 0 || i >= cells.length) {
    return "";
  }
  return cells[i] ?? "";
}

function canonicalRowFromLegacyCsv(
  headers: string[],
  cells: string[],
): Record<string, string> {
  const idx = resolveClubInfoColumnIndices(headers);
  const c = cells;
  return {
    Sport_type: idx.sportType >= 0 ? getCell(c, idx.sportType) : "",
    Club_name: idx.clubName >= 0 ? getCell(c, idx.clubName) : "",
    country: idx.country >= 0 ? getCell(c, idx.country) : "",
    setup_date: idx.setupDate >= 0 ? getCell(c, idx.setupDate) : "",
    contact_point: "",
    contact_email: "",
    club_desc: idx.clubDesc >= 0 ? getCell(c, idx.clubDesc) : "",
    club_logo: idx.clubLogo >= 0 ? getCell(c, idx.clubLogo) : "",
    lastUpdate_date: idx.lastUpdate >= 0 ? getCell(c, idx.lastUpdate) : "",
  };
}

function loadLegacyClubInfoCsvRaw(clubId: string): {
  headers: string[];
  headerLine: string;
  rows: string[][];
} {
  const p = clubInfoLegacyCsvPath(clubId);
  const raw = fs.readFileSync(p, "utf8");
  const lines = raw.split(/\r?\n/).filter((l) => l.trim() !== "");
  if (lines.length === 0) {
    const headers = parseCsvLine(CLUB_INFO_FIELD_ORDER_LABEL);
    const empty = headers.map(() => "");
    return {
      headers,
      headerLine: CLUB_INFO_FIELD_ORDER_LABEL,
      rows: [empty],
    };
  }
  const headerLine = lines[0]!;
  const headers = parseCsvLine(headerLine);
  const rows: string[][] = [];
  for (let i = 1; i < lines.length; i++) {
    rows.push(parseCsvLine(lines[i]!));
  }
  if (rows.length === 0) {
    const empty = headers.map(() => "");
    rows.push(empty);
  }
  return { headers, headerLine, rows };
}

function migrateLegacyCsvIfPresent(clubId: string): void {
  const jp = clubInfoPath(clubId);
  const legacy = clubInfoLegacyCsvPath(clubId);
  if (!fs.existsSync(legacy)) {
    return;
  }
  if (fs.existsSync(jp)) {
    try {
      fs.unlinkSync(legacy);
    } catch {
      /* ignore */
    }
    return;
  }
  const { headers, rows } = loadLegacyClubInfoCsvRaw(clubId);
  const cells = rows[0] ?? [];
  const can = canonicalRowFromLegacyCsv(headers, cells);
  writeClubInfoJsonFile(jp, can);
  try {
    fs.unlinkSync(legacy);
  } catch {
    /* ignore */
  }
}

export function ensureClubInfoFile(clubId: string): void {
  if (!isValidClubFolderId(clubId)) {
    throw new Error("Invalid club ID.");
  }
  const dir = clubDataDir(clubId.trim());
  if (!dir) {
    throw new Error("Invalid club ID.");
  }
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true });
  }
  const jp = clubInfoPath(clubId);
  const legacy = clubInfoLegacyCsvPath(clubId);
  if (!fs.existsSync(jp) && !fs.existsSync(legacy)) {
    writeClubInfoJsonFile(jp, emptyCanonicalRow());
  }
}

/** YYYY/MM/DD in server local timezone (matches historical ClubInfo samples). */
export function todaySlashYmd(): string {
  const d = new Date();
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, "0");
  const day = String(d.getDate()).padStart(2, "0");
  return `${y}/${m}/${day}`;
}

/** Tabular view of ClubInfo.json for APIs (single logical row). */
export type ClubInfoRaw = {
  headers: string[];
  headerLine: string;
  rows: string[][];
};

export function loadClubInfoRaw(clubId: string): ClubInfoRaw {
  ensureClubInfoFile(clubId);
  migrateLegacyCsvIfPresent(clubId);
  const jp = clubInfoPath(clubId);
  const can = readClubInfoJsonFile(jp);
  const headers = CLUB_INFO_FIELD_KEYS.map((k) => k);
  const rows = [CLUB_INFO_FIELD_KEYS.map((k) => can[k] ?? "")];
  return {
    headers,
    headerLine: CLUB_INFO_FIELD_ORDER_LABEL,
    rows,
  };
}

/** Flat map field name → value for the club row (UI + API). */
export function clubInfoFirstRowObject(clubId: string): Record<string, string> {
  const { headers, rows } = loadClubInfoRaw(clubId);
  const cells = rows[0] ?? [];
  const o: Record<string, string> = {};
  headers.forEach((h, i) => {
    const key = h.trim();
    if (key) {
      o[key] = cells[i] ?? "";
    }
  });
  return o;
}

function pickField(
  body: Record<string, unknown>,
  keys: string[],
  fallback: string,
): string {
  for (const k of keys) {
    if (Object.prototype.hasOwnProperty.call(body, k)) {
      const v = body[k];
      return v == null ? "" : String(v).trim();
    }
  }
  return fallback;
}

const CLUB_INFO_PAYMENT_PATCH_KEYS = [
  "club_payment_payme",
  "club_payment_FPS",
  "club_payment_fps",
  "club_payment_wechat",
  "club_payment_alipay",
  "club_payment_支付寶",
] as const;

/**
 * Merge editable fields from request body; set lastUpdate field.
 * Persists `ClubInfo.json` (keeps extra keys such as payment QR paths).
 */
export function writeClubInfoFromPatch(
  clubId: string,
  body: Record<string, unknown>,
  lastUpdateDate: string,
): void {
  ensureClubInfoFile(clubId);
  migrateLegacyCsvIfPresent(clubId);
  const jp = clubInfoPath(clubId);
  const can = readClubInfoJsonFile(jp);

  const sport = pickField(
    body,
    ["SportType", "Sport_type", "sport_type"],
    can.Sport_type,
  );
  const name = pickField(body, ["Club_name", "club_name"], can.Club_name);
  const country = pickField(body, ["country", "Country"], can.country);
  const setup = pickField(body, ["setup_date", "setupDate"], can.setup_date);
  const contactPoint = pickField(
    body,
    ["contact_point", "Contact_Point", "Contact Point", "contactPoint"],
    can.contact_point,
  );
  const contactEmail = pickField(
    body,
    ["contact_email", "Contact_Email", "Contact Email", "contactEmail"],
    can.contact_email,
  );
  const desc = pickField(body, ["club_desc", "clubDesc"], can.club_desc);
  const logo = pickField(body, ["club_logo", "clubLogo"], can.club_logo);

  can.Sport_type = sport;
  can.Club_name = name;
  can.country = country;
  can.setup_date = setup;
  can.contact_point = contactPoint;
  can.contact_email = contactEmail;
  can.club_desc = desc;
  can.club_logo = logo;
  can.lastUpdate_date = lastUpdateDate;

  const full = loadClubInfoExtended(clubId);
  for (const k of CLUB_INFO_FIELD_KEYS) {
    full[k] = can[k] ?? "";
  }
  full.lastUpdate_date = lastUpdateDate;
  if (full.version == null || typeof full.version !== "number") {
    full.version = 1;
  }
  for (const k of CLUB_INFO_PAYMENT_PATCH_KEYS) {
    if (Object.prototype.hasOwnProperty.call(body, k)) {
      const v = body[k];
      full[k] = v == null ? "" : String(v).trim();
    }
  }
  fs.writeFileSync(jp, `${JSON.stringify(full, null, 2)}\n`, "utf8");
}
