import type { Filter } from "mongodb";
import type { CoachSalaryDocument, CoachSalaryInsert } from "./db/DBConnection";
import { getCoachSalaryCollection, isMongoConfigured } from "./db/DBConnection";
import type { CoachCsvRow } from "./coachListCsv";
import {
  applyLessonFeeAllocationsToDocument,
  type CoachSalaryFileV1,
  type CoachSalaryRecord,
  type FeeAllocationApplyItem,
} from "./coachSalaryJson";

export function isCoachSalaryMongoAvailable(): boolean {
  return isMongoConfigured();
}

function escapeRegex(s: string): string {
  return s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

/**
 * Coach folder / Coach Manager JWT `sub` (e.g. `CM…`) may be stored on salary rows as
 * `ClubID`, `club_id`, or `coach_manager_uid`.
 */
function coachSalaryClubScopeFilter(
  managerUid: string,
): Filter<CoachSalaryDocument> {
  const uid = String(managerUid ?? "").replace(/^\uFEFF/, "").trim();
  const uidRe = new RegExp(`^${escapeRegex(uid)}$`, "i");
  return {
    $or: [
      { ClubID: uid },
      { ClubID: uidRe },
      { club_id: uid } as Filter<CoachSalaryDocument>,
      { club_id: uidRe } as Filter<CoachSalaryDocument>,
      { coach_manager_uid: uid } as Filter<CoachSalaryDocument>,
      { coach_manager_uid: uidRe } as Filter<CoachSalaryDocument>,
    ],
  } as Filter<CoachSalaryDocument>;
}

function coerceSalaryNumber(v: number | string): number {
  if (typeof v === "number" && Number.isFinite(v)) {
    return v;
  }
  const n = Number.parseFloat(String(v ?? "").replace(/,/g, ""));
  return Number.isFinite(n) ? n : 0;
}

export function coachSalaryRecordToInsert(row: CoachSalaryRecord): CoachSalaryInsert {
  return {
    CoachSalaryID: String(row.CoachSalaryID ?? "").trim(),
    lessonId: String(row.lessonId ?? "").trim(),
    ClubID: String(row.ClubID ?? "").trim(),
    club_name: String(row.club_name ?? "").trim(),
    coach_id: String(row.coach_id ?? "").trim(),
    salary_amount: coerceSalaryNumber(row.salary_amount),
    Payment_Method: String(row.Payment_Method ?? "").trim(),
    Payment_Status: String(row.Payment_Status ?? "").trim(),
    Payment_Confirm: Boolean(row.Payment_Confirm),
    Payment_date:
      row.Payment_date != null && String(row.Payment_date).trim() !== ""
        ? String(row.Payment_date).trim()
        : undefined,
    createdAt: String(row.createdAt ?? "").trim(),
    lastUpdatedDate: String(row.lastUpdatedDate ?? "").trim(),
  };
}

export function mongoDocToCoachSalaryRecord(d: CoachSalaryDocument): CoachSalaryRecord {
  const raw = d as unknown as Record<string, unknown>;
  const clubId = String(
    d.ClubID ?? raw["club_id"] ?? raw["coach_manager_uid"] ?? "",
  ).trim();
  return {
    CoachSalaryID: String(d.CoachSalaryID ?? "").trim(),
    lessonId: String(d.lessonId ?? "").trim(),
    ClubID: clubId,
    club_name: String(d.club_name ?? "").trim(),
    coach_id: String(
      d.coach_id ?? raw["CoachID"] ?? raw["coachID"] ?? "",
    ).trim(),
    salary_amount: d.salary_amount,
    Payment_Method: String(d.Payment_Method ?? "").trim(),
    Payment_Status: String(d.Payment_Status ?? "").trim(),
    Payment_Confirm: Boolean(d.Payment_Confirm),
    Payment_date: String(
      d.Payment_date ?? raw["payment_date"] ?? raw["PaymentDate"] ?? "",
    ).trim() || undefined,
    createdAt: String(d.createdAt ?? "").trim(),
    lastUpdatedDate: String(d.lastUpdatedDate ?? "").trim(),
  };
}

/**
 * Loads all `CoachManager` documents scoped to a club folder / Coach Manager UID
 * (`ClubID`, `club_id`, or `coach_manager_uid` on each row).
 */
export async function loadCoachSalaryDocumentFromMongo(
  clubFolderId: string,
): Promise<CoachSalaryFileV1> {
  const uid = String(clubFolderId ?? "").replace(/^\uFEFF/, "").trim();
  if (!uid) {
    return { version: 1, coachSalaries: [] };
  }
  const col = await getCoachSalaryCollection();
  const docs = await col
    .find(coachSalaryClubScopeFilter(uid))
    .sort({ CoachSalaryID: 1 })
    .toArray();
  return {
    version: 1,
    coachSalaries: docs.map((d) => mongoDocToCoachSalaryRecord(d)),
  };
}

/**
 * Coach view: load `ClubMaster_DB.CoachManager` rows for one club (`ClubID`) whose coach id
 * matches any of `coachKeys` (JWT `sub`, roster `coach_id`, etc.) on `coach_id` or legacy `CoachID`.
 */
export async function loadCoachSalaryRowsForClubAndCoachKeysFromMongo(
  clubFolderId: string,
  coachKeys: string[],
): Promise<CoachSalaryFileV1> {
  const cid = String(clubFolderId ?? "").replace(/^\uFEFF/, "").trim();
  const keys = [
    ...new Set(
      coachKeys
        .map((k) => String(k ?? "").replace(/^\uFEFF/, "").trim())
        .filter((k) => k.length > 0),
    ),
  ];
  if (!cid || !keys.length) {
    return { version: 1, coachSalaries: [] };
  }
  const col = await getCoachSalaryCollection();
  const coachKeyOr: Filter<CoachSalaryDocument>[] = [];
  for (const k of keys) {
    const re = new RegExp(`^${escapeRegex(k)}$`, "i");
    coachKeyOr.push({ coach_id: re });
    coachKeyOr.push({
      CoachID: re,
    } as unknown as Filter<CoachSalaryDocument>);
  }
  const docs = await col
    .find({
      $and: [coachSalaryClubScopeFilter(cid), { $or: coachKeyOr }],
    } as Filter<CoachSalaryDocument>)
    .sort({ CoachSalaryID: 1 })
    .toArray();
  return {
    version: 1,
    coachSalaries: docs.map((d) => mongoDocToCoachSalaryRecord(d)),
  };
}

export async function upsertCoachSalaryRecordsMongo(
  rows: CoachSalaryRecord[],
): Promise<void> {
  if (!rows.length) {
    return;
  }
  const col = await getCoachSalaryCollection();
  for (const row of rows) {
    const ins = coachSalaryRecordToInsert(row);
    if (!ins.CoachSalaryID) {
      continue;
    }
    await col.replaceOne({ CoachSalaryID: ins.CoachSalaryID }, ins, {
      upsert: true,
    });
  }
}

/** Upserts every salary row in `doc` that belongs to `clubFolderId` (matches `ClubID`). */
export async function saveCoachSalaryDocumentMongo(
  clubFolderId: string,
  doc: CoachSalaryFileV1,
): Promise<void> {
  const cid = String(clubFolderId ?? "").trim();
  const clubRows = doc.coachSalaries.filter(
    (r) => String(r.ClubID ?? "").trim() === cid,
  );
  await upsertCoachSalaryRecordsMongo(clubRows);
}

export async function applyLessonFeeAllocationsMongo(
  fileClub: string,
  clubFolderId: string,
  clubName: string,
  items: FeeAllocationApplyItem[],
  rosterCoaches?: CoachCsvRow[],
): Promise<{ created: number; updated: number }> {
  const doc = await loadCoachSalaryDocumentFromMongo(clubFolderId);
  const summary = await applyLessonFeeAllocationsToDocument(
    doc,
    fileClub,
    clubFolderId,
    clubName,
    items,
    rosterCoaches,
  );
  await saveCoachSalaryDocumentMongo(clubFolderId, doc);
  return summary;
}
