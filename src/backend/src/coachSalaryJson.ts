import type { CoachCsvRow } from "./coachListCsv";
import { loadCoachesPreferred } from "./coachListMongo";
import { loadLessons, type LessonCsvRow } from "./lessonListCsv";
import { loadLessonReservationsPreferred } from "./lessonReserveListMongo";
import type { LessonReserveRecord } from "./lessonReserveList";

export const COACH_SALARY_PAYMENT_METHODS = [
  "Bank Transfer",
  "Cash",
  "PayMe",
  "FPS",
  "Credit Card",
] as const;

export const COACH_SALARY_PAYMENT_STATUSES = [
  "Pending",
  "Paid",
  "Overdue",
  "Partial",
] as const;

export type CoachSalaryRecord = {
  CoachSalaryID: string;
  lessonId: string;
  ClubID: string;
  club_name: string;
  coach_id: string;
  salary_amount: number | string;
  Payment_Method: string;
  Payment_Status: string;
  Payment_Confirm: boolean;
  Payment_date?: string;
  createdAt: string;
  lastUpdatedDate: string;
};

export type CoachSalaryTableRow = {
  CoachSalaryID: string;
  lessonId: string;
  coach_id: string;
  club_name: string;
  coachFullName: string;
  lessonNameDate: string;
  /** YYYY-MM-DD from LessonList when the lesson exists (for month filter). */
  lessonStartDate: string;
  /** YYYY-MM-DD from LessonList when the lesson exists (for month filter). */
  lessonEndDate: string;
  salary_amount: string;
  Payment_Method: string;
  Payment_Status: string;
  Payment_Confirm: boolean;
  Payment_date?: string;
  lastUpdatedDate: string;
  createdAt: string;
};

export type CoachSalaryFileV1 = {
  version: 1;
  coachSalaries: CoachSalaryRecord[];
};

export function normalizeCoachSalaryPaymentMethod(
  value: string,
): (typeof COACH_SALARY_PAYMENT_METHODS)[number] | null {
  const v = value.trim();
  for (const m of COACH_SALARY_PAYMENT_METHODS) {
    if (m.localeCompare(v, undefined, { sensitivity: "base" }) === 0) {
      return m;
    }
  }
  return null;
}

export function normalizeCoachSalaryPaymentStatus(
  value: string,
): (typeof COACH_SALARY_PAYMENT_STATUSES)[number] | null {
  const v = value.trim();
  for (const s of COACH_SALARY_PAYMENT_STATUSES) {
    if (s.localeCompare(v, undefined, { sensitivity: "base" }) === 0) {
      return s;
    }
  }
  return null;
}

function coachNameMap(coaches: CoachCsvRow[]): Map<string, string> {
  const m = new Map<string, string>();
  for (const c of coaches) {
    const id = c.coachId.replace(/^\uFEFF/, "").trim();
    if (!id) {
      continue;
    }
    m.set(id.toUpperCase(), (c.coachName && c.coachName.trim()) || "");
  }
  return m;
}

function lessonDetailMap(lessons: LessonCsvRow[]): Map<string, LessonCsvRow> {
  const m = new Map<string, LessonCsvRow>();
  for (const L of lessons) {
    const id = L.lessonId.replace(/^\uFEFF/, "").trim();
    if (!id) {
      continue;
    }
    m.set(id.toUpperCase(), L);
  }
  return m;
}

function reservationDateByLessonId(
  reservations: LessonReserveRecord[],
): Map<string, string> {
  const m = new Map<string, string>();
  for (const r of reservations) {
    const lid = r.lessonId.trim();
    if (!lid || m.has(lid.toUpperCase())) {
      continue;
    }
    const d = (r.lastUpdatedDate || r.createdAt || "").trim();
    if (d) {
      m.set(lid.toUpperCase(), d);
    }
  }
  return m;
}

function formatSalaryAmount(v: number | string): string {
  if (typeof v === "number" && Number.isFinite(v)) {
    return String(v);
  }
  return String(v ?? "").trim() || "0";
}

function parseClassFeeToNumber(fee: string): number {
  const n = Number.parseFloat(String(fee).replace(/[^0-9.-]/g, ""));
  return Number.isFinite(n) ? Math.round(n * 100) / 100 : 0;
}

export type LessonFeeAllocationRow = {
  lessonId: string;
  /** YYYY-MM-DD from LessonList (for month filter). */
  lessonStartDate: string;
  /** YYYY-MM-DD from LessonList (for month filter). */
  lessonEndDate: string;
  lessonFeeTotal: number;
  receivedLessonFeeTotal: number;
  coachName: string;
  /** Current value from CoachSalary.json (editable). */
  feeAllocation: string;
  coachSalaryId: string | null;
  coach_id: string;
};

function resolveCoachIdFromLessonCoachName(
  coaches: CoachCsvRow[],
  lessonCoachName: string,
): string {
  const t = lessonCoachName.trim().toLowerCase().replace(/\s+/g, " ");
  if (!t) {
    return "";
  }
  for (const c of coaches) {
    const cn = (c.coachName || "").trim().toLowerCase().replace(/\s+/g, " ");
    if (cn === t) {
      return c.coachId.replace(/^\uFEFF/, "").trim();
    }
  }
  return "";
}

/**
 * Next numeric suffix for new salary ids: `{clubId}-CS000001` (and legacy `CS000001` rows
 * in the same club list still advance the counter so we never collide).
 */
export function nextCoachSalarySequenceNumber(
  existing: CoachSalaryRecord[],
  clubFolderId: string,
): number {
  const esc = clubFolderId.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const prefixed = new RegExp("^" + esc + "-CS(\\d+)$", "i");
  const legacy = /^CS(\d+)$/i;
  let max = 0;
  for (const r of existing) {
    const id = r.CoachSalaryID.trim();
    let m = id.match(prefixed);
    if (!m) {
      m = id.match(legacy);
    }
    if (m) {
      const n = Number.parseInt(m[1]!, 10);
      if (!Number.isNaN(n) && n > max) {
        max = n;
      }
    }
  }
  return max + 1;
}

function coachSalaryByLessonId(
  doc: CoachSalaryFileV1,
): Map<string, CoachSalaryRecord> {
  const m = new Map<string, CoachSalaryRecord>();
  for (const r of doc.coachSalaries) {
    const k = r.lessonId.trim().toUpperCase();
    if (k && !m.has(k)) {
      m.set(k, r);
    }
  }
  return m;
}

/**
 * Active lessons with fee totals and received totals (confirmed reservations × class fee).
 */
export async function buildLessonFeeAllocationRows(
  fileClub: string,
  rosterCoaches: CoachCsvRow[] | undefined,
  salaryDoc: CoachSalaryFileV1,
): Promise<LessonFeeAllocationRow[]> {
  const lessonRows = await loadLessons(fileClub);
  const lessons = lessonRows.filter(
    (L) => (L.status || "").trim().toUpperCase() === "ACTIVE",
  );
  lessons.sort((a, b) =>
    a.lessonId
      .trim()
      .toUpperCase()
      .localeCompare(b.lessonId.trim().toUpperCase()),
  );
  const reservations = await loadLessonReservationsPreferred(fileClub);
  const coaches = rosterCoaches ?? (await loadCoachesPreferred(fileClub));
  const doc = salaryDoc;
  const byLesson = coachSalaryByLessonId(doc);

  const rows: LessonFeeAllocationRow[] = [];
  for (const L of lessons) {
    const lid = L.lessonId.trim();
    const fee = parseClassFeeToNumber(L.classFee);
    const maxN = Math.max(0, Math.floor(Number.parseInt(String(L.maxNumber).trim(), 10) || 0));
    const lessonFeeTotal = Math.round(maxN * fee * 100) / 100;
    const nConfirm = reservations.filter(
      (r) =>
        r.lessonId.trim().toUpperCase() === lid.toUpperCase() &&
        r.status.trim().toUpperCase() === "ACTIVE" &&
        r.Payment_Confirm === true,
    ).length;
    const receivedLessonFeeTotal = Math.round(nConfirm * fee * 100) / 100;
    const coachName = (L.coachName || "").trim();
    const existing = byLesson.get(lid.toUpperCase());
    const feeAllocation = existing
      ? formatSalaryAmount(existing.salary_amount)
      : "";
    rows.push({
      lessonId: lid,
      lessonStartDate: (L.lessonStartDate || "").trim().slice(0, 10),
      lessonEndDate: (L.lessonEndDate || "").trim().slice(0, 10),
      lessonFeeTotal,
      receivedLessonFeeTotal,
      coachName,
      feeAllocation,
      coachSalaryId: existing?.CoachSalaryID?.trim()
        ? existing.CoachSalaryID.trim()
        : null,
      coach_id:
        (existing?.coach_id?.trim() || "") ||
        resolveCoachIdFromLessonCoachName(coaches, coachName),
    });
  }
  return rows;
}

export type FeeAllocationApplyItem = {
  lessonId: string;
  feeAllocation: number;
};

/**
 * Mutates `doc` in memory: upserts coach salary rows by lessonId.
 * New rows use `{clubId}-CS000001` style ids (next free sequence; legacy `CS000001` ids
 * in the same list still advance the counter).
 */
export async function applyLessonFeeAllocationsToDocument(
  doc: CoachSalaryFileV1,
  fileClub: string,
  clubId: string,
  clubName: string,
  items: FeeAllocationApplyItem[],
  rosterCoaches?: CoachCsvRow[],
): Promise<{ created: number; updated: number }> {
  if (!items.length) {
    return { created: 0, updated: 0 };
  }
  const lessons = await loadLessons(fileClub);
  const lessonById = new Map(
    lessons.map((l) => [l.lessonId.trim().toUpperCase(), l]),
  );
  const coaches = rosterCoaches ?? (await loadCoachesPreferred(fileClub));
  const now = new Date().toISOString();
  const today = now.slice(0, 10);
  let seq = nextCoachSalarySequenceNumber(doc.coachSalaries, clubId);
  let created = 0;
  let updated = 0;

  for (const item of items) {
    const lid = item.lessonId.trim();
    const L = lessonById.get(lid.toUpperCase());
    if (!L || (L.status || "").trim().toUpperCase() !== "ACTIVE") {
      throw new Error(`Invalid or inactive lesson: ${lid}`);
    }
    const coachName = (L.coachName || "").trim();
    const coachId = resolveCoachIdFromLessonCoachName(coaches, coachName);
    const amt = Number.isFinite(item.feeAllocation)
      ? Math.round(item.feeAllocation * 100) / 100
      : 0;

    const idx = doc.coachSalaries.findIndex(
      (r) => r.lessonId.trim().toUpperCase() === lid.toUpperCase(),
    );
    if (idx >= 0) {
      const row = doc.coachSalaries[idx]!;
      row.salary_amount = amt;
      if (coachId) {
        row.coach_id = coachId;
      }
      row.club_name = clubName;
      row.ClubID = clubId;
      row.lastUpdatedDate = now;
      doc.coachSalaries[idx] = row;
      updated++;
    } else {
      const id = `${clubId}-CS${String(seq).padStart(6, "0")}`;
      seq++;
      doc.coachSalaries.push({
        CoachSalaryID: id,
        lessonId: lid,
        ClubID: clubId,
        club_name: clubName,
        coach_id: coachId,
        salary_amount: amt,
        Payment_Method: "Bank Transfer",
        Payment_Status: "Pending",
        Payment_Confirm: false,
        createdAt: today,
        lastUpdatedDate: now,
      });
      created++;
    }
  }

  return { created, updated };
}

export type BuildCoachSalaryTableRowsOpts = {
  /** When set, only salary rows for this coach_id and only lesson/reservation rows needed for those salaries. */
  onlyCoachId?: string;
  /** When set, avoids an extra coach roster load (Mongo `UserList_Coach`). */
  rosterCoaches?: CoachCsvRow[];
  /** Salary rows from MongoDB `ClubMaster_DB.CoachManager` (or legacy in-memory file shape). */
  salaryDoc: CoachSalaryFileV1;
};

/**
 * Merged rows for Coach Salary UI (coach + lesson lookups).
 * Pass `{ onlyCoachId }` for the logged-in coach API to avoid building rows for every coach in the club.
 */
export async function buildCoachSalaryTableRows(
  fileClub: string,
  opts: BuildCoachSalaryTableRowsOpts,
): Promise<CoachSalaryTableRow[]> {
  const doc = opts.salaryDoc;
  const only = opts?.onlyCoachId?.trim();
  const rawRows = only
    ? doc.coachSalaries.filter(
        (r) =>
          r.coach_id.trim().toUpperCase() === only.toUpperCase(),
      )
    : doc.coachSalaries;

  const lessonIdsNeeded = new Set<string>();
  for (const raw of rawRows) {
    const lid = raw.lessonId.trim().toUpperCase();
    if (lid) {
      lessonIdsNeeded.add(lid);
    }
  }

  const coaches = opts.rosterCoaches ?? (await loadCoachesPreferred(fileClub));
  const coachMap = coachNameMap(coaches);
  const allLessons = await loadLessons(fileClub);
  const lessons =
    only && lessonIdsNeeded.size > 0
      ? allLessons.filter((l) =>
          lessonIdsNeeded.has(l.lessonId.trim().toUpperCase()),
        )
      : allLessons;
  const lessonMap = lessonDetailMap(lessons);
  let reservations = await loadLessonReservationsPreferred(fileClub);
  if (only && lessonIdsNeeded.size > 0) {
    reservations = reservations.filter((r) =>
      lessonIdsNeeded.has(r.lessonId.trim().toUpperCase()),
    );
  }
  const resDates = reservationDateByLessonId(reservations);

  const out: CoachSalaryTableRow[] = [];
  for (const raw of rawRows) {
    const coachId = raw.coach_id.trim();
    const lessonId = raw.lessonId.trim();
    let coachFullName = "N/A";
    if (coachId) {
      const nm = coachMap.get(coachId.toUpperCase());
      if (nm) {
        coachFullName = nm;
      }
    }

    let lessonNameDate = "N/A";
    let lessonStartDate = "";
    let lessonEndDate = "";
    if (lessonId) {
      const L = lessonMap.get(lessonId.toUpperCase());
      if (L) {
        lessonStartDate = (L.lessonStartDate || "").trim().slice(0, 10);
        lessonEndDate = (L.lessonEndDate || "").trim().slice(0, 10);
        const cn = (L.classInfo || "").trim();
        const dt = (L.lessonStartDate || "").trim();
        if (cn && dt) {
          lessonNameDate = `${cn} — ${dt}`;
        } else if (cn) {
          lessonNameDate = cn;
        } else if (dt) {
          lessonNameDate = dt;
        }
      }
      if (lessonNameDate === "N/A") {
        const rd = resDates.get(lessonId.toUpperCase());
        if (rd) {
          lessonNameDate = `Lesson ${lessonId} — ${rd}`;
        }
      }
    }

    const clubName = (raw.club_name || "").trim() || "N/A";

    out.push({
      CoachSalaryID: raw.CoachSalaryID.trim(),
      lessonId,
      coach_id: coachId,
      club_name: clubName,
      coachFullName,
      lessonNameDate,
      lessonStartDate,
      lessonEndDate,
      salary_amount: formatSalaryAmount(raw.salary_amount),
      Payment_Method: raw.Payment_Method,
      Payment_Status: raw.Payment_Status,
      Payment_Confirm: raw.Payment_Confirm,
      Payment_date: raw.Payment_date,
      lastUpdatedDate: raw.lastUpdatedDate,
      createdAt: raw.createdAt,
    });
  }
  return out;
}
