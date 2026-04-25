/**
 * MongoDB persistence for per-club lesson lists (`ClubMaster_DB.LessonList`, `_id` = club folder UID).
 * JSON under `data_club/{clubId}/LessonList.json` is used only when Mongo is not configured, or once to seed Mongo.
 */
import type { Document } from "mongodb";
import {
  getLessonListCollection,
  isMongoConfigured,
  type LessonListClubDocument,
} from "./db/DBConnection";
import { isValidClubFolderId } from "./coachListCsv";

export function lessonListUsesMongo(): boolean {
  return isMongoConfigured();
}

export async function findLessonListClubDocument(
  clubId: string,
): Promise<LessonListClubDocument | null> {
  if (!lessonListUsesMongo() || !isValidClubFolderId(clubId)) {
    return null;
  }
  const coll = await getLessonListCollection();
  return coll.findOne({ _id: clubId.trim() });
}

export async function replaceLessonListForClub(
  clubId: string,
  lessons: Document[],
): Promise<void> {
  const id = clubId.trim();
  const coll = await getLessonListCollection();
  const prev = await coll.findOne({ _id: id });
  const version =
    prev && typeof prev.version === "number" && Number.isFinite(prev.version)
      ? Math.trunc(prev.version)
      : 1;
  const doc: LessonListClubDocument = {
    _id: id,
    club_id: id,
    version,
    lessons,
  };
  await coll.replaceOne({ _id: id }, doc, { upsert: true });
}

export async function iterateLessonListClubDocuments(): Promise<
  LessonListClubDocument[]
> {
  if (!lessonListUsesMongo()) {
    return [];
  }
  const coll = await getLessonListCollection();
  return coll.find({}).toArray();
}
