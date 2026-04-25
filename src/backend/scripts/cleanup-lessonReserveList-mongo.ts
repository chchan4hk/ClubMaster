/**
 * Cleans MongoDB `LessonReserveList` (database from `MONGO_LESSON_RESERVE_TARGET_DB` /
 * default `ClubMaster_DB` — see `resolveLessonReserveListDatabaseName`).
 *
 * Actions (when `--apply`):
 * - Deletes documents that cannot be parsed (missing `lessonReserveId` or `lessonId`).
 * - Sets `ClubID` when empty but inferable from `{ClubID}-LR…` style `lessonReserveId`.
 * - Trims / canonicalizes string fields and `Payment_Confirm` to match app parsing.
 * - Removes duplicate `(ClubID, lessonReserveId)` rows (case-insensitive key), keeping the
 *   row with the latest `lastUpdatedDate` (then newest `_id`).
 *
 * Usage (from `src/backend`):
 *   npx tsx ./scripts/cleanup-lessonReserveList-mongo.ts --dry-run
 *   npx tsx ./scripts/cleanup-lessonReserveList-mongo.ts --dry-run --club CM00000008
 *   npx tsx ./scripts/cleanup-lessonReserveList-mongo.ts --apply
 *   npx tsx ./scripts/cleanup-lessonReserveList-mongo.ts --apply --club CM00000008
 */
import type { Filter, ObjectId } from "mongodb";
import path from "path";
import { loadLocalEnvFile } from "../src/config/env";
import { isValidClubFolderId } from "../src/coachListCsv";
import {
  closeMongoClient,
  getMongoClient,
  isMongoConfigured,
  LESSON_RESERVE_LIST_COLLECTION,
  resolveLessonReserveListDatabaseName,
  type LessonReserveListDocument,
} from "../src/db/DBConnection";
import { parseLessonReserveObject } from "../src/lessonReserveList";

const backendRoot = path.join(__dirname, "..");
loadLocalEnvFile(backendRoot);

function escapeRegExp(s: string): string {
  return s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function clubScopeFilter(clubFolderUid: string): Filter<LessonReserveListDocument> {
  const club = clubFolderUid.replace(/^\uFEFF/, "").trim();
  const esc = escapeRegExp(club);
  return {
    $or: [
      { ClubID: new RegExp(`^${esc}$`, "i") },
      { lessonReserveId: new RegExp(`^${esc}-LR`, "i") },
    ],
  };
}

function clubPrefixFromLessonReserveId(rid: string): string | null {
  const id = rid.replace(/^\uFEFF/, "").trim();
  const idx = id.toUpperCase().indexOf("-LR");
  if (idx <= 0) {
    return null;
  }
  const prefix = id.slice(0, idx).trim();
  return isValidClubFolderId(prefix) ? prefix : null;
}

function docToPlain(doc: LessonReserveListDocument & { _id: ObjectId }): Record<string, unknown> {
  const o: Record<string, unknown> = { ...doc };
  delete o._id;
  return o;
}

function parseDateKey(s: string): number {
  const t = Date.parse(s);
  return Number.isNaN(t) ? 0 : t;
}

function argvClub(): string | null {
  const raw = process.argv;
  for (let i = 0; i < raw.length; i++) {
    const a = raw[i]!;
    if (a === "--club" && raw[i + 1]) {
      return String(raw[i + 1]).trim();
    }
    if (a.startsWith("--club=")) {
      return a.slice("--club=".length).trim();
    }
  }
  return null;
}

async function main(): Promise<void> {
  if (!isMongoConfigured()) {
    console.error(
      "MongoDB is not configured (set MONGODB_URI / MONGO_URI or MONGO_PASSWORD).",
    );
    process.exit(1);
  }

  const apply = process.argv.includes("--apply");
  const clubArg = argvClub();
  if (clubArg && !isValidClubFolderId(clubArg)) {
    console.error(`Invalid --club id: ${clubArg}`);
    process.exit(1);
  }

  const dbName = resolveLessonReserveListDatabaseName();
  const client = await getMongoClient();
  const col = client
    .db(dbName)
    .collection<LessonReserveListDocument>(LESSON_RESERVE_LIST_COLLECTION);

  const baseFilter: Filter<LessonReserveListDocument> = clubArg
    ? clubScopeFilter(clubArg)
    : {};

  const cursor = col.find(baseFilter);
  const docs = (await cursor.toArray()) as (LessonReserveListDocument & {
    _id: ObjectId;
  })[];

  console.log(
    `Database "${dbName}", collection LessonReserveList — loaded ${docs.length} document(s)${
      clubArg ? ` (scope: ${clubArg})` : ""
    }. Mode: ${apply ? "APPLY" : "DRY-RUN"}.`,
  );

  const invalidIds: ObjectId[] = [];
  const normalizeOps: {
    _id: ObjectId;
    $set: Record<string, unknown>;
  }[] = [];

  for (const d of docs) {
    const plain = docToPlain(d);
    let parsed = parseLessonReserveObject(plain);
    if (!parsed) {
      invalidIds.push(d._id);
      continue;
    }

    const inferred = clubPrefixFromLessonReserveId(parsed.lessonReserveId);
    if (!parsed.ClubID.trim() && inferred) {
      parsed = { ...parsed, ClubID: inferred };
    }

    const canonicalClub = parsed.ClubID.trim();
    const norm = {
      lessonReserveId: parsed.lessonReserveId,
      lessonId: parsed.lessonId,
      ClubID: /^cm\d+$/i.test(canonicalClub) ? canonicalClub.toUpperCase() : canonicalClub,
      student_id: parsed.student_id,
      Student_Name: parsed.Student_Name,
      status: parsed.status,
      Payment_Status: parsed.Payment_Status,
      Payment_Confirm: parsed.Payment_Confirm,
      createdAt: parsed.createdAt,
      lastUpdatedDate: parsed.lastUpdatedDate,
    };

    const changed =
      norm.lessonReserveId !== String(d.lessonReserveId ?? "").trim() ||
      norm.lessonId !== String(d.lessonId ?? "").trim() ||
      norm.ClubID !== String(d.ClubID ?? "").trim() ||
      norm.student_id !== String(d.student_id ?? "").trim() ||
      norm.Student_Name !== String(d.Student_Name ?? "").trim() ||
      norm.status !== String(d.status ?? "").trim() ||
      norm.Payment_Status !== String(d.Payment_Status ?? "").trim() ||
      norm.Payment_Confirm !== (d.Payment_Confirm === true) ||
      norm.createdAt !== String(d.createdAt ?? "").trim() ||
      norm.lastUpdatedDate !== String(d.lastUpdatedDate ?? "").trim();

    if (changed) {
      normalizeOps.push({
        _id: d._id,
        $set: norm,
      });
    }
  }

  function duplicateIdsFromDocList(
    list: (LessonReserveListDocument & { _id: ObjectId })[],
    skipIds: Set<string>,
  ): ObjectId[] {
    const map = new Map<
      string,
      { _id: ObjectId; lastUpdatedDate: string }[]
    >();
    for (const d of list) {
      if (skipIds.has(String(d._id))) {
        continue;
      }
      const plain = docToPlain(d);
      let parsed = parseLessonReserveObject(plain);
      if (!parsed) {
        continue;
      }
      const inferred = clubPrefixFromLessonReserveId(parsed.lessonReserveId);
      if (!parsed.ClubID.trim() && inferred) {
        parsed = { ...parsed, ClubID: inferred };
      }
      const clubRaw = parsed.ClubID.trim();
      const club = /^cm\d+$/i.test(clubRaw) ? clubRaw.toUpperCase() : clubRaw;
      const rid = parsed.lessonReserveId.trim().toUpperCase();
      const key = `${club.toUpperCase()}|${rid}`;
      const g = map.get(key) ?? [];
      g.push({ _id: d._id, lastUpdatedDate: parsed.lastUpdatedDate });
      map.set(key, g);
    }
    const out: ObjectId[] = [];
    for (const [, group] of map) {
      if (group.length < 2) {
        continue;
      }
      group.sort((a, b) => {
        const dt = parseDateKey(b.lastUpdatedDate) - parseDateKey(a.lastUpdatedDate);
        if (dt !== 0) {
          return dt;
        }
        return String(b._id).localeCompare(String(a._id));
      });
      for (let i = 1; i < group.length; i++) {
        out.push(group[i]!._id);
      }
    }
    return out;
  }

  const skipInvalid = new Set(invalidIds.map((x) => String(x)));
  let duplicateDeleteIds = duplicateIdsFromDocList(docs, skipInvalid);

  console.log(`Invalid (unparseable): ${invalidIds.length}`);
  console.log(`Normalize field updates: ${normalizeOps.length}`);
  console.log(`Duplicate rows to remove (extra copies, pre-apply pass): ${duplicateDeleteIds.length}`);

  if (!apply) {
    if (invalidIds.length) {
      console.log(
        "Sample invalid _id(s):",
        invalidIds
          .slice(0, 8)
          .map((x) => String(x))
          .join(", "),
      );
    }
    if (duplicateDeleteIds.length) {
      console.log(
        "Sample duplicate-removal _id(s):",
        duplicateDeleteIds
          .slice(0, 8)
          .map((x) => String(x))
          .join(", "),
      );
    }
    console.log("Dry run: no writes. Re-run with --apply to execute cleanup.");
    return;
  }

  let deletedInvalid = 0;
  if (invalidIds.length) {
    const r = await col.deleteMany({ _id: { $in: invalidIds } });
    deletedInvalid = r.deletedCount;
  }

  let updated = 0;
  for (const op of normalizeOps) {
    if (invalidIds.some((id) => id.equals(op._id))) {
      continue;
    }
    const res = await col.updateOne({ _id: op._id }, { $set: op.$set });
    if (res.modifiedCount) {
      updated++;
    }
  }

  const afterDocs = (await col.find(baseFilter).toArray()) as (LessonReserveListDocument & {
    _id: ObjectId;
  })[];
  duplicateDeleteIds = duplicateIdsFromDocList(afterDocs, new Set());

  let deletedDup = 0;
  if (duplicateDeleteIds.length) {
    const r2 = await col.deleteMany({ _id: { $in: duplicateDeleteIds } });
    deletedDup = r2.deletedCount;
  }

  console.log(
    `Done. Deleted invalid: ${deletedInvalid}, normalized rows touched: ${updated}, deleted duplicates: ${deletedDup}.`,
  );
}

main()
  .catch((e) => {
    console.error(e);
    process.exit(1);
  })
  .finally(() => {
    void closeMongoClient();
  });
