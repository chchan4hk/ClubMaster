import type { Filter } from "mongodb";
import { MongoServerError } from "mongodb";
import { isValidClubFolderId } from "./coachListCsv";
import {
  ensurePrizeListRowCollection,
  getPrizeListRowCollection,
  isMongoConfigured,
  PRIZE_LIST_ROW_COLLECTION,
  resolvePrizeListRowDatabaseName,
  type PrizeListRowDocument,
  type PrizeListRowInsert,
} from "./db/DBConnection";
import { findUserByUid } from "./userlistCsv";
import { userLoginCsvReadFallbackEnabled } from "./userListMongo";
import {
  appendPrizeRow,
  deletePrizeRow,
  findClubUidForPrizeId,
  loadPrizeListRaw,
  loadPrizes,
  PRIZE_LIST_COLUMNS,
  prizeCsvRowToApiFields,
  type PrizeCsvRow,
  type PrizeListRaw,
  updatePrizeRow,
} from "./prizeListJson";

/** Numeric suffix width after `-P` (e.g. `CM00000008-P0000001`). */
export const PRIZE_CLUB_PREFIX_SUFFIX_PAD = 7;

const LEGACY_PRIZE_ID_RE = /^PR(\d+)$/i;

let prizeListCollectionEnsured = false;

async function ensurePrizeListRowCollectionOnce(): Promise<void> {
  if (prizeListCollectionEnsured) {
    return;
  }
  prizeListCollectionEnsured = true;
  try {
    await ensurePrizeListRowCollection();
  } catch (e) {
    prizeListCollectionEnsured = false;
    throw e;
  }
}

function escapeRegExp(s: string): string {
  return s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function mongoDocToPrizeRow(doc: PrizeListRowDocument): PrizeCsvRow {
  return {
    prizeId: String(doc.PrizeID ?? "").trim(),
    clubId: String(doc.ClubID ?? "").trim(),
    clubName: String(doc.Club_name ?? "").trim(),
    sportType: String(doc.SportType ?? "").trim(),
    year: String(doc.Year ?? "").trim(),
    association: String(doc.Association ?? "").trim(),
    competition: String(doc.Competition ?? "").trim(),
    ageGroup: String(doc.Age_group ?? "").trim(),
    prizeType: String(doc.Prize_type ?? "").trim(),
    studentName: String(doc.StudentName ?? "").trim(),
    ranking: String(doc.Ranking ?? "").trim(),
    status: String(doc.Status ?? "").trim() || "ACTIVE",
    createdAt: String(doc.Created_at ?? "").trim(),
    lastUpdatedDate: String(doc.LastUpdated_Date ?? "").trim(),
    verifiedBy: String(doc.VerifiedBy ?? "").trim(),
    remarks: String(doc.Remarks ?? "").trim(),
  };
}

function prizeRowToInsert(row: PrizeCsvRow): PrizeListRowInsert {
  const api = prizeCsvRowToApiFields(row);
  return {
    PrizeID: api.PrizeID ?? "",
    ClubID: api.ClubID ?? "",
    Club_name: api.Club_name ?? "",
    SportType: api.SportType ?? "",
    Year: api.Year ?? "",
    Association: api.Association ?? "",
    Competition: api.Competition ?? "",
    Age_group: api.Age_group ?? "",
    Prize_type: api.Prize_type ?? "",
    StudentName: api.StudentName ?? "",
    Ranking: api.Ranking ?? "",
    Status: api.Status ?? "",
    Created_at: api.Created_at ?? "",
    LastUpdated_Date: api.LastUpdated_Date ?? "",
    VerifiedBy: api.VerifiedBy ?? "",
    Remarks: api.Remarks ?? "",
  };
}

function clubPrizeScopeFilter(clubFolderUid: string): Filter<PrizeListRowDocument> {
  const club = clubFolderUid.replace(/^\uFEFF/, "").trim();
  return { ClubID: club };
}

function sanitizeCell(s: string): string {
  return String(s ?? "").replace(/,/g, " ").trim();
}

function prizeIdsEqual(a: string, b: string): boolean {
  const x = String(a ?? "")
    .replace(/^\uFEFF/, "")
    .trim();
  const y = String(b ?? "")
    .replace(/^\uFEFF/, "")
    .trim();
  return x.length > 0 && x.toUpperCase() === y.toUpperCase();
}

function applyDefaultClubNameFromLogin(
  clubId: string,
  rows: PrizeCsvRow[],
): PrizeCsvRow[] {
  const sessionUid = clubId.trim();
  const mgr =
    isMongoConfigured() && !userLoginCsvReadFallbackEnabled()
      ? undefined
      : findUserByUid(sessionUid);
  const defaultClubName =
    mgr?.clubName && mgr.clubName.trim() && mgr.clubName.trim() !== "—"
      ? mgr.clubName.trim()
      : "";
  return rows.map((r) => {
    let next =
      r.clubId && r.clubId.trim() ? r : { ...r, clubId: sessionUid };
    if (!next.clubName?.trim() && defaultClubName) {
      next = { ...next, clubName: defaultClubName };
    }
    return next;
  });
}

export async function loadPrizesMongo(clubFolderUid: string): Promise<PrizeCsvRow[]> {
  if (!isValidClubFolderId(clubFolderUid.trim())) {
    return [];
  }
  await ensurePrizeListRowCollectionOnce();
  const col = await getPrizeListRowCollection();
  const docs = await col
    .find(clubPrizeScopeFilter(clubFolderUid))
    .sort({ PrizeID: 1 })
    .toArray();
  return applyDefaultClubNameFromLogin(
    clubFolderUid,
    docs.map((d) => mongoDocToPrizeRow(d)),
  );
}

/**
 * Prize list: MongoDB `PrizeList` in {@link resolvePrizeListRowDatabaseName} when configured,
 * otherwise `data_club/{clubId}/PrizeList.json`.
 */
export async function loadPrizesPreferred(clubId: string): Promise<PrizeCsvRow[]> {
  if (!isMongoConfigured()) {
    return loadPrizes(clubId);
  }
  try {
    return await loadPrizesMongo(clubId);
  } catch (e) {
    console.warn(
      "[PrizeList] Mongo load failed; falling back to JSON files.",
      e instanceof Error ? e.message : e,
    );
    return loadPrizes(clubId);
  }
}

export async function loadPrizeListRawPreferred(clubId: string): Promise<PrizeListRaw> {
  if (!isMongoConfigured()) {
    return loadPrizeListRaw(clubId);
  }
  try {
    const prizes = await loadPrizesMongo(clubId);
    const id = clubId.trim();
    const dbName = resolvePrizeListRowDatabaseName();
    const relativePath = `mongodb/${dbName}/${PRIZE_LIST_ROW_COLLECTION}/${id}`;
    const headers = [...PRIZE_LIST_COLUMNS];
    const rows = prizes.map((p) => {
      const api = prizeCsvRowToApiFields(p);
      return headers.map((h) => api[h] ?? "");
    });
    return { relativePath, headers, rows };
  } catch (e) {
    console.warn(
      "[PrizeList] Mongo raw table failed; falling back to JSON files.",
      e instanceof Error ? e.message : e,
    );
    return loadPrizeListRaw(clubId);
  }
}

/**
 * Next `{Club_ID}-P0000001` style id for this club (Mongo path only).
 * Counts existing rows whose `PrizeID` matches `ClubID-P` + digits (any width), then pads to 7.
 */
export async function allocateNextPrizeIdMongo(clubFolderUid: string): Promise<string> {
  const club = clubFolderUid.replace(/^\uFEFF/, "").trim();
  if (!club) {
    return "";
  }
  const rows = await loadPrizesMongo(club);
  const re = new RegExp(
    `^${escapeRegExp(club)}-P(\\d+)$`,
    "i",
  );
  let max = 0;
  for (const r of rows) {
    const m = r.prizeId.trim().match(re);
    if (m) {
      const n = Number.parseInt(m[1]!, 10);
      if (!Number.isNaN(n)) {
        max = Math.max(max, n);
      }
    }
  }
  return `${club}-P${String(max + 1).padStart(PRIZE_CLUB_PREFIX_SUFFIX_PAD, "0")}`;
}

function normalizeLegacyPrPrizeId(requested: string): string | null {
  const t = requested.trim();
  const m = t.match(LEGACY_PRIZE_ID_RE);
  if (!m) {
    return null;
  }
  const n = m[1]!.replace(/^0+/, "") || "0";
  const num = Number.parseInt(n, 10);
  if (Number.isNaN(num)) {
    return null;
  }
  return `PR${String(num).padStart(6, "0")}`;
}

function normalizeClubPrefixedPrizeId(
  clubId: string,
  requested: string,
): string | null {
  const club = clubId.replace(/^\uFEFF/, "").trim();
  const t = requested.trim();
  const re = new RegExp(`^${escapeRegExp(club)}-P(\\d+)$`, "i");
  const m = t.match(re);
  if (!m) {
    return null;
  }
  const n = m[1]!.replace(/^0+/, "") || "0";
  const num = Number.parseInt(n, 10);
  if (Number.isNaN(num)) {
    return null;
  }
  return `${club}-P${String(num).padStart(PRIZE_CLUB_PREFIX_SUFFIX_PAD, "0")}`;
}

export async function appendPrizeRowMongo(
  clubId: string,
  input: {
    clubName: string;
    sportType: string;
    year: string;
    association: string;
    competition: string;
    ageGroup: string;
    prizeType: string;
    studentName: string;
    ranking: string;
    status?: string;
    verifiedBy: string;
    remarks: string;
    prizeId?: string;
  },
): Promise<{ ok: true; prizeId: string } | { ok: false; error: string }> {
  await ensurePrizeListRowCollectionOnce();
  const studentName = sanitizeCell(input.studentName);
  if (!studentName) {
    return { ok: false, error: "StudentName is required." };
  }
  const competition = sanitizeCell(input.competition);
  if (!competition) {
    return { ok: false, error: "Competition is required." };
  }
  if (!isValidClubFolderId(clubId.trim())) {
    return { ok: false, error: "Invalid club ID." };
  }
  const sessionUid = clubId.trim();
  const rows = await loadPrizesMongo(clubId);
  const requested = input.prizeId?.trim();
  let prizeId: string;
  if (requested) {
    const asClubP = normalizeClubPrefixedPrizeId(sessionUid, requested);
    const asLegacy = normalizeLegacyPrPrizeId(requested);
    if (asClubP) {
      prizeId = asClubP;
    } else if (asLegacy) {
      prizeId = asLegacy;
    } else {
      return {
        ok: false,
        error: `Invalid PrizeID format (expected ${sessionUid}-P + ${PRIZE_CLUB_PREFIX_SUFFIX_PAD} digits, or legacy PR + 6 digits).`,
      };
    }
    if (rows.some((r) => prizeIdsEqual(r.prizeId, prizeId))) {
      return { ok: false, error: "PrizeID already exists in prize list." };
    }
  } else {
    prizeId = await allocateNextPrizeIdMongo(sessionUid);
  }
  const today = new Date().toISOString().slice(0, 10);
  const status = sanitizeCell(input.status || "ACTIVE") || "ACTIVE";
  const clubNameStored = String(input.clubName ?? "").trim();
  const row: PrizeCsvRow = {
    prizeId,
    clubId: sessionUid,
    clubName: clubNameStored,
    sportType: sanitizeCell(input.sportType),
    year: sanitizeCell(input.year),
    association: sanitizeCell(input.association),
    competition,
    ageGroup: sanitizeCell(input.ageGroup),
    prizeType: sanitizeCell(input.prizeType),
    studentName,
    ranking: sanitizeCell(input.ranking),
    status,
    createdAt: today,
    lastUpdatedDate: today,
    verifiedBy: sanitizeCell(input.verifiedBy),
    remarks: sanitizeCell(input.remarks),
  };
  const col = await getPrizeListRowCollection();
  try {
    await col.insertOne(prizeRowToInsert(row));
  } catch (e) {
    if (e instanceof MongoServerError && e.code === 11000) {
      return { ok: false, error: "PrizeID already exists in prize list." };
    }
    throw e;
  }
  return { ok: true, prizeId };
}

export async function updatePrizeRowMongo(
  clubId: string,
  prizeId: string,
  input: {
    clubName: string;
    sportType: string;
    year: string;
    association: string;
    competition: string;
    ageGroup: string;
    prizeType: string;
    studentName: string;
    ranking: string;
    status?: string;
    verifiedBy: string;
    remarks: string;
  },
): Promise<{ ok: true } | { ok: false; error: string }> {
  await ensurePrizeListRowCollectionOnce();
  const id = prizeId.trim();
  if (!id) {
    return { ok: false, error: "PrizeID is required." };
  }
  const studentName = sanitizeCell(input.studentName);
  if (!studentName) {
    return { ok: false, error: "StudentName is required." };
  }
  const competition = sanitizeCell(input.competition);
  if (!competition) {
    return { ok: false, error: "Competition is required." };
  }
  if (!isValidClubFolderId(clubId.trim())) {
    return { ok: false, error: "Invalid club ID." };
  }
  const sessionUid = clubId.trim();
  const col = await getPrizeListRowCollection();
  const existing = await col.findOne({
    ClubID: sessionUid,
    PrizeID: new RegExp(`^${escapeRegExp(id)}$`, "i"),
  });
  if (!existing) {
    return { ok: false, error: "Prize not found." };
  }
  const today = new Date().toISOString().slice(0, 10);
  const status = sanitizeCell(input.status || "ACTIVE") || "ACTIVE";
  const cn = String(input.clubName ?? "").trim();
  const clubIdStored =
    String(existing.ClubID ?? "").trim() || sessionUid;
  const patch: Partial<PrizeListRowInsert> = {
    Club_name: cn || String(existing.Club_name ?? "").trim(),
    SportType: sanitizeCell(input.sportType),
    Year: sanitizeCell(input.year),
    Association: sanitizeCell(input.association),
    Competition: competition,
    Age_group: sanitizeCell(input.ageGroup),
    Prize_type: sanitizeCell(input.prizeType),
    StudentName: studentName,
    Ranking: sanitizeCell(input.ranking),
    Status: status,
    LastUpdated_Date: today,
    VerifiedBy: sanitizeCell(input.verifiedBy),
    Remarks: sanitizeCell(input.remarks),
    ClubID: clubIdStored,
  };
  await col.updateOne(
    { _id: existing._id },
    { $set: patch },
  );
  return { ok: true };
}

export async function deletePrizeRowMongo(
  clubId: string,
  prizeId: string,
): Promise<{ ok: true } | { ok: false; error: string }> {
  await ensurePrizeListRowCollectionOnce();
  const id = prizeId.trim();
  if (!id) {
    return { ok: false, error: "PrizeID is required." };
  }
  if (!isValidClubFolderId(clubId.trim())) {
    return { ok: false, error: "Invalid club ID." };
  }
  const sessionUid = clubId.trim();
  const col = await getPrizeListRowCollection();
  const res = await col.deleteOne({
    ClubID: sessionUid,
    PrizeID: new RegExp(`^${escapeRegExp(id)}$`, "i"),
  });
  if (res.deletedCount === 0) {
    return { ok: false, error: "Prize not found." };
  }
  return { ok: true };
}

type PrizeAppendResult =
  | { ok: true; prizeId: string }
  | { ok: false; error: string };

type PrizeUpdateResult = { ok: true } | { ok: false; error: string };

export async function appendPrizeRowPreferred(
  clubId: string,
  input: Parameters<typeof appendPrizeRow>[1],
): Promise<PrizeAppendResult> {
  if (isMongoConfigured()) {
    try {
      return await appendPrizeRowMongo(clubId, input);
    } catch (e) {
      console.warn(
        "[PrizeList] Mongo append failed; falling back to JSON files.",
        e instanceof Error ? e.message : e,
      );
    }
  }
  return appendPrizeRow(clubId, input);
}

export async function updatePrizeRowPreferred(
  clubId: string,
  prizeId: string,
  input: Parameters<typeof updatePrizeRow>[2],
): Promise<PrizeUpdateResult> {
  if (isMongoConfigured()) {
    try {
      return await updatePrizeRowMongo(clubId, prizeId, input);
    } catch (e) {
      console.warn(
        "[PrizeList] Mongo update failed; falling back to JSON files.",
        e instanceof Error ? e.message : e,
      );
    }
  }
  return updatePrizeRow(clubId, prizeId, input);
}

export async function deletePrizeRowPreferred(
  clubId: string,
  prizeId: string,
): Promise<PrizeUpdateResult> {
  if (isMongoConfigured()) {
    try {
      return await deletePrizeRowMongo(clubId, prizeId);
    } catch (e) {
      console.warn(
        "[PrizeList] Mongo delete failed; falling back to JSON files.",
        e instanceof Error ? e.message : e,
      );
    }
  }
  return deletePrizeRow(clubId, prizeId);
}

export async function findClubUidForPrizeIdPreferred(
  prizeId: string,
): Promise<string | null> {
  const id = prizeId.replace(/^\uFEFF/, "").trim();
  if (!id) {
    return null;
  }
  if (isMongoConfigured()) {
    try {
      await ensurePrizeListRowCollectionOnce();
      const col = await getPrizeListRowCollection();
      const doc = await col.findOne({
        PrizeID: new RegExp(`^${escapeRegExp(id)}$`, "i"),
      });
      const folder = String(doc?.ClubID ?? "").trim();
      if (folder) {
        return folder;
      }
    } catch {
      /* fall through */
    }
  }
  return findClubUidForPrizeId(prizeId);
}
