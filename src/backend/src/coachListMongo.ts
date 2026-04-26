import type { Filter } from "mongodb";
import {
  allocateNextCoachId,
  bumpNumericCoachLoginStyleId,
  bumpPrefixedCoachIdForClub,
  COACH_CLUB_PREFIX_ID_PAD,
  COACH_LIST_COLUMNS,
  coachIdsEqual,
  coachRowToRecord,
  findClubUidForCoachId,
  isValidClubFolderId,
  loadCoachListRaw,
  loadCoaches,
  normalizeCoachIdInput,
  normalizePrefixedCoachIdForClub,
  purgeCoachRowFromAllClubFolders,
  type CoachCsvRow,
  type CoachListRaw,
} from "./coachListCsv";
import {
  getUserListCoachCollection,
  isMongoConfigured,
  USER_LIST_COACH_COLLECTION,
  type UserListCoachDocument,
  type UserListCoachInsert,
} from "./db/DBConnection";

let coachListIndexesEnsured = false;

async function ensureUserListCoachIndexesOnce(): Promise<void> {
  if (coachListIndexesEnsured) {
    return;
  }
  coachListIndexesEnsured = true;
  try {
    const col = await getUserListCoachCollection();
    await col.createIndexes([
      {
        key: { club_folder_uid: 1, coach_id: 1 },
        name: "userlist_coach_club_coach_uid",
        unique: true,
      },
      {
        key: { club_id: 1 },
        name: "userlist_coach_club_id",
      },
      {
        key: { coach_id: 1 },
        name: "userlist_coach_coach_id_lookup",
      },
    ]);
  } catch {
    coachListIndexesEnsured = false;
  }
}

function coachIdRegex(coachId: string): RegExp {
  const id = coachId.replace(/^\uFEFF/, "").trim();
  return new RegExp(`^${id.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}$`, "i");
}

/** Match roster id stored as `coach_id` and/or legacy `CoachID`. */
function coachIdMatchFilter(coachId: string): Filter<UserListCoachDocument> {
  const id = coachId.replace(/^\uFEFF/, "").trim();
  if (!id) {
    return { _id: { $exists: false } };
  }
  const re = coachIdRegex(id);
  return {
    $or: [{ coach_id: re }, { CoachID: re }],
  };
}

/** Club roster scope in Mongo (imports may set only `club_id` or only `club_folder_uid`). */
function clubScopeFilter(clubFolderUid: string): Filter<UserListCoachDocument> {
  const club = clubFolderUid.replace(/^\uFEFF/, "").trim();
  return {
    $or: [{ club_folder_uid: club }, { club_id: club }],
  };
}

function mongoDocToCoachRow(doc: UserListCoachDocument): CoachCsvRow {
  const d = doc as unknown as Record<string, unknown>;
  const hourly =
    String(d["hourly_rate (HKD)"] ?? d.hourly_rate ?? "").trim();
  const cid = String(doc.coach_id ?? doc.CoachID ?? "").trim();
  return {
    coachId: cid,
    clubName: String(doc.club_name ?? "").trim(),
    coachName: String(doc.full_name ?? "").trim(),
    sex: String(doc.sex ?? "").trim(),
    dateOfBirth: String(doc.date_of_birth ?? "").trim(),
    joinedDate: String(doc.joined_date ?? "").trim(),
    homeAddress: String(doc.home_address ?? "").trim(),
    country: String(doc.country ?? "").trim(),
    email: String(doc.email ?? "").trim(),
    phone: String(doc.contact_number ?? "").trim(),
    remark: String(doc.remark ?? "").trim(),
    hourlyRate: hourly,
    status: String(doc.status ?? "").trim() || "ACTIVE",
    createdDate: String(doc.creation_date ?? "").trim(),
    lastUpdateDate: String(doc.lastUpdate_date ?? "").trim(),
  };
}

function coachRowToMongoInsert(
  row: CoachCsvRow,
  clubFolderUid: string,
): UserListCoachInsert {
  const rec = coachRowToRecord(row);
  const coachUid = row.coachId.trim();
  const out: UserListCoachInsert = {
    club_folder_uid: clubFolderUid.trim(),
    club_id: clubFolderUid.trim(),
    coach_id: coachUid,
    CoachID: coachUid,
    club_name: rec.club_name ?? "",
    full_name: rec.full_name ?? "",
    sex: rec.sex ?? "",
    date_of_birth: rec.date_of_birth ?? "",
    joined_date: rec.joined_date ?? "",
    home_address: rec.home_address ?? "",
    country: rec.country ?? "",
    email: rec.email ?? "",
    contact_number: rec.contact_number ?? "",
    status: rec.status ?? "ACTIVE",
    creation_date: rec.creation_date ?? "",
    remark: rec.remark ?? "",
    lastUpdate_date: rec.lastUpdate_date ?? "",
  };
  const hr = rec["hourly_rate (HKD)"] ?? "";
  if (hr) {
    out["hourly_rate (HKD)"] = hr;
  }
  return out;
}

export async function loadCoachesMongo(clubFolderUid: string): Promise<CoachCsvRow[]> {
  if (!isValidClubFolderId(clubFolderUid.trim())) {
    return [];
  }
  const col = await getUserListCoachCollection();
  const docs = await col
    .find(clubScopeFilter(clubFolderUid))
    .sort({ coach_id: 1 })
    .toArray();
  return docs.map((d) => mongoDocToCoachRow(d));
}

/**
 * Coach roster: Mongo `UserList_Coach` when configured, otherwise `UserList_Coach.json` under `data_club`.
 */
export async function loadCoachesPreferred(clubId: string): Promise<CoachCsvRow[]> {
  if (!isMongoConfigured()) {
    return loadCoaches(clubId);
  }
  try {
    return await loadCoachesMongo(clubId);
  } catch (e) {
    console.warn(
      "[UserList_Coach] Mongo load failed; falling back to JSON files.",
      e instanceof Error ? e.message : e,
    );
    return loadCoaches(clubId);
  }
}

export async function loadCoachListRawPreferred(clubId: string): Promise<CoachListRaw> {
  if (!isMongoConfigured()) {
    return loadCoachListRaw(clubId);
  }
  try {
    const coaches = await loadCoachesMongo(clubId);
    const id = clubId.trim();
    const relativePath = `mongodb/${USER_LIST_COACH_COLLECTION}/${id}`;
    const headers = [...COACH_LIST_COLUMNS];
    const rows = coaches.map((c) => {
      const rec = coachRowToRecord(c);
      return COACH_LIST_COLUMNS.map((col) => rec[col] ?? "");
    });
    return { relativePath, headers, rows };
  } catch (e) {
    console.warn(
      "[UserList_Coach] Mongo raw table failed; falling back to JSON files.",
      e instanceof Error ? e.message : e,
    );
    return loadCoachListRaw(clubId);
  }
}

function normEqCoachField(a: string, b: string): boolean {
  return (
    String(a ?? "").trim().toLowerCase() === String(b ?? "").trim().toLowerCase()
  );
}

export async function searchCoachesInClubPreferred(
  clubId: string,
  coachName?: string,
  email?: string,
): Promise<CoachCsvRow[]> {
  const nq = (coachName ?? "").trim();
  const eq = (email ?? "").trim();
  if (!nq && !eq) {
    return [];
  }
  if (!isValidClubFolderId(clubId.trim())) {
    return [];
  }
  const list = await loadCoachesPreferred(clubId);
  return list.filter((row) => {
    if (nq && !normEqCoachField(row.coachName, nq)) {
      return false;
    }
    if (eq && !normEqCoachField(row.email, eq)) {
      return false;
    }
    return true;
  });
}

/**
 * Next `{Club_ID}-C00001` style id for this club folder (Mongo path only).
 */
export async function allocateNextCoachIdMongo(clubFolderUid: string): Promise<string> {
  const club = clubFolderUid.replace(/^\uFEFF/, "").trim();
  if (!club) {
    return "";
  }
  const rows = await loadCoachesMongo(club);
  const re = new RegExp(
    `^${club.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}-C(\\d+)$`,
    "i",
  );
  let max = 0;
  for (const r of rows) {
    const m = r.coachId.trim().match(re);
    if (m) {
      const n = Number.parseInt(m[1]!, 10);
      if (!Number.isNaN(n)) {
        max = Math.max(max, n);
      }
    }
  }
  return `${club}-C${String(max + 1).padStart(COACH_CLUB_PREFIX_ID_PAD, "0")}`;
}

export async function allocateNextCoachIdPreferred(clubFolderUid: string): Promise<string> {
  if (isMongoConfigured()) {
    try {
      return await allocateNextCoachIdMongo(clubFolderUid);
    } catch {
      /* fall through */
    }
  }
  return allocateNextCoachId(clubFolderUid);
}

export function bumpCoachUidForCollisionPreferred(
  clubFolderUid: string,
  current: string,
): string {
  const bumpedPrefixed = bumpPrefixedCoachIdForClub(clubFolderUid, current);
  if (bumpedPrefixed !== current) {
    return bumpedPrefixed;
  }
  return bumpNumericCoachLoginStyleId(current);
}

export async function findClubUidForCoachIdPreferred(
  coachId: string,
): Promise<string | null> {
  const id = coachId.replace(/^\uFEFF/, "").trim();
  if (!id) {
    return null;
  }
  if (isMongoConfigured()) {
    try {
      const col = await getUserListCoachCollection();
      const doc = await col.findOne(coachIdMatchFilter(id));
      const folder = String(
        doc?.club_folder_uid ?? doc?.club_id ?? "",
      ).trim();
      if (folder) {
        return folder;
      }
    } catch {
      /* fall through */
    }
  }
  return findClubUidForCoachId(coachId);
}

export async function appendCoachRowMongo(
  clubId: string,
  clubName: string,
  input: {
    coachName: string;
    email: string;
    phone: string;
    sex?: string;
    dateOfBirth?: string;
    joinedDate?: string;
    homeAddress?: string;
    country?: string;
    remark?: string;
    hourlyRate?: string;
    status?: string;
    coachId?: string;
  },
): Promise<{ ok: true; coachId: string } | { ok: false; error: string }> {
  await ensureUserListCoachIndexesOnce();
  const name = String(input.coachName ?? "").replace(/,/g, " ").trim();
  if (!name) {
    return { ok: false, error: "full_name is required." };
  }
  const email = String(input.email ?? "").replace(/,/g, " ").trim();
  if (!isValidClubFolderId(clubId.trim())) {
    return { ok: false, error: "Invalid club folder id." };
  }
  const clubFolder = clubId.trim();
  const existing = await loadCoachesMongo(clubFolder);
  const requested = input.coachId?.trim();
  let coachId: string;
  if (requested) {
    const prefixed = normalizePrefixedCoachIdForClub(requested, clubFolder);
    const normalized = normalizeCoachIdInput(requested);
    const chosen = prefixed ?? normalized;
    if (!chosen) {
      return {
        ok: false,
        error:
          "Invalid CoachID format (expected CH####, C######, or {Club_ID}-C#####).",
      };
    }
    if (existing.some((r) => coachIdsEqual(r.coachId, chosen))) {
      return { ok: false, error: "CoachID already exists in coach list." };
    }
    coachId = chosen;
  } else {
    coachId = await allocateNextCoachIdMongo(clubFolder);
    if (!coachId) {
      return { ok: false, error: "Could not allocate coach ID." };
    }
  }
  const today = new Date().toISOString().slice(0, 10);
  const status =
    String(input.status ?? "ACTIVE").replace(/,/g, " ").trim() || "ACTIVE";
  const row: CoachCsvRow = {
    coachId,
    clubName: String(clubName ?? "").replace(/,/g, " ").trim(),
    coachName: name,
    sex: String(input.sex ?? "").replace(/,/g, " ").trim(),
    dateOfBirth: String(input.dateOfBirth ?? "").replace(/,/g, " ").trim(),
    joinedDate: String(input.joinedDate ?? "").replace(/,/g, " ").trim(),
    homeAddress: String(input.homeAddress ?? "").replace(/,/g, " ").trim(),
    country: String(input.country ?? "").replace(/,/g, " ").trim(),
    email,
    phone: String(input.phone ?? "").replace(/,/g, " ").trim(),
    remark: String(input.remark ?? "").replace(/,/g, " ").trim(),
    hourlyRate: String(input.hourlyRate ?? "").replace(/,/g, " ").trim(),
    status,
    createdDate: today,
    lastUpdateDate: today,
  };
  const col = await getUserListCoachCollection();
  try {
    await col.insertOne(coachRowToMongoInsert(row, clubFolder));
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    return { ok: false, error: `Mongo insert failed: ${msg}` };
  }
  return { ok: true, coachId };
}

export async function updateCoachRowMongo(
  clubId: string,
  clubName: string,
  coachId: string,
  input: {
    coachName: string;
    email: string;
    phone: string;
    sex?: string;
    dateOfBirth?: string;
    joinedDate?: string;
    homeAddress?: string;
    country?: string;
    remark?: string;
    hourlyRate?: string;
    status?: string;
  },
): Promise<{ ok: true } | { ok: false; error: string }> {
  const id = coachId.trim();
  if (!id) {
    return { ok: false, error: "CoachID is required." };
  }
  const name = String(input.coachName ?? "").replace(/,/g, " ").trim();
  if (!name) {
    return { ok: false, error: "full_name is required." };
  }
  const email = String(input.email ?? "").replace(/,/g, " ").trim();
  const clubFolder = clubId.trim();
  const list = await loadCoachesMongo(clubFolder);
  const prev = list.find((c) => coachIdsEqual(c.coachId, id));
  if (!prev) {
    return { ok: false, error: "Coach not found." };
  }
  const today = new Date().toISOString().slice(0, 10);
  const status =
    String(input.status ?? "ACTIVE").replace(/,/g, " ").trim() || "ACTIVE";
  const next: CoachCsvRow = {
    ...prev,
    clubName: String(clubName ?? "").replace(/,/g, " ").trim(),
    coachName: name,
    sex: String(input.sex ?? "").replace(/,/g, " ").trim(),
    dateOfBirth: String(input.dateOfBirth ?? "").replace(/,/g, " ").trim(),
    joinedDate: String(input.joinedDate ?? "").replace(/,/g, " ").trim(),
    homeAddress: String(input.homeAddress ?? "").replace(/,/g, " ").trim(),
    country: String(input.country ?? "").replace(/,/g, " ").trim(),
    email,
    phone: String(input.phone ?? "").replace(/,/g, " ").trim(),
    remark: String(input.remark ?? "").replace(/,/g, " ").trim(),
    hourlyRate: String(input.hourlyRate ?? "").replace(/,/g, " ").trim(),
    status,
    lastUpdateDate: today,
  };
  const payload = coachRowToMongoInsert(next, clubFolder);
  const col = await getUserListCoachCollection();
  const r = await col.updateOne(
    {
      ...clubScopeFilter(clubFolder),
      ...coachIdMatchFilter(id),
    },
    { $set: payload },
  );
  if (r.matchedCount === 0) {
    return { ok: false, error: "Coach not found." };
  }
  return { ok: true };
}

export async function removeCoachRowMongo(
  clubId: string,
  coachId: string,
): Promise<{ ok: true } | { ok: false; error: string }> {
  const id = coachId.trim();
  if (!id) {
    return { ok: false, error: "CoachID is required." };
  }
  const clubFolder = clubId.trim();
  const today = new Date().toISOString().slice(0, 10);
  const r = await (
    await getUserListCoachCollection()
  ).updateOne(
    { ...clubScopeFilter(clubFolder), ...coachIdMatchFilter(id) },
    { $set: { status: "INACTIVE", lastUpdate_date: today } },
  );
  if (r.matchedCount === 0) {
    return { ok: false, error: "Coach not found." };
  }
  return { ok: true };
}

/** Removes all `UserList_Coach` documents with this coach_id (any club). */
export async function purgeCoachRowsMongoByCoachId(
  coachId: string,
): Promise<{ deletedCount: number }> {
  const id = coachId.replace(/^\uFEFF/, "").trim();
  if (!id) {
    return { deletedCount: 0 };
  }
  const col = await getUserListCoachCollection();
  const r = await col.deleteMany(coachIdMatchFilter(id));
  return { deletedCount: r.deletedCount ?? 0 };
}

/**
 * Deletes coach roster rows for this CoachID in Mongo (when configured) and under every `data_club` folder on disk.
 */
export async function purgeCoachRowFromAllPreferred(
  coachId: string,
): Promise<
  | { ok: true; updatedClubIds: string[]; mongoDeleted: number }
  | { ok: false; error: string }
> {
  let mongoDeleted = 0;
  if (isMongoConfigured()) {
    try {
      const r = await purgeCoachRowsMongoByCoachId(coachId);
      mongoDeleted = r.deletedCount;
    } catch (e) {
      return {
        ok: false,
        error: e instanceof Error ? e.message : String(e),
      };
    }
  }
  const disk = purgeCoachRowFromAllClubFolders(coachId);
  if (!disk.ok) {
    return disk;
  }
  return {
    ok: true,
    updatedClubIds: disk.updatedClubIds,
    mongoDeleted,
  };
}
