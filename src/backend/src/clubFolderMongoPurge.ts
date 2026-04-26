/**
 * When a Coach Manager is permanently removed, delete all MongoDB documents
 * scoped to that club folder id (`ClubID` / `club_id` / `_id` per collection)
 * across app collections (default {@link DEFAULT_MONGO_APP_DATABASE}).
 */
import type { Document, Filter } from "mongodb";
import { isValidClubFolderId } from "./coachListCsv";
import {
  CLUB_INFO_COLLECTION,
  COACH_SALARY_COLLECTION,
  LESSON_LIST_COLLECTION,
  LESSON_PAYMENT_LEDGER_COLLECTION,
  LESSON_RESERVE_LIST_COLLECTION,
  LESSON_SERIES_INFO_COLLECTION,
  PAYMENT_LIST_COLLECTION,
  PRIZE_LIST_ROW_COLLECTION,
  USER_LIST_COACH_COLLECTION,
  USER_LIST_COLLECTION,
  USER_LIST_STUDENT_COLLECTION,
  getClubInfoCollection,
  getCoachSalaryCollection,
  getLessonListCollection,
  getLessonPaymentLedgerCollection,
  getLessonReserveListCollection,
  getLessonSeriesInfoCollection,
  getPaymentListCollection,
  getPrizeListRowCollection,
  getUserListCoachCollection,
  getUserListCollection,
  getUserListStudentCollection,
  getUserLoginCollection,
  isMongoConfigured,
} from "./db/DBConnection";

export type PurgeClubMongoDeletedCounts = {
  clubInfo: number;
  lessonList: number;
  paymentList: number;
  lessonSeriesInfo: number;
  lessonReserveList: number;
  lessonPaymentLedger: number;
  coachManager: number;
  prizeList: number;
  userListStudent: number;
  userListCoach: number;
  userListLegacy: number;
};

export type PurgeClubMongoResult = {
  deleted: PurgeClubMongoDeletedCounts;
  errors: Array<{ collection: string; message: string }>;
};

function emptyDeleted(): PurgeClubMongoDeletedCounts {
  return {
    clubInfo: 0,
    lessonList: 0,
    paymentList: 0,
    lessonSeriesInfo: 0,
    lessonReserveList: 0,
    lessonPaymentLedger: 0,
    coachManager: 0,
    prizeList: 0,
    userListStudent: 0,
    userListCoach: 0,
    userListLegacy: 0,
  };
}

function escapeRegExp(s: string): string {
  return s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function clubIdExactRegex(folderUid: string): RegExp {
  const id = folderUid.replace(/^\uFEFF/, "").trim();
  return new RegExp(`^${escapeRegExp(id)}$`, "i");
}

function lessonReserveOrLedgerPartitionFilter(folderUid: string): Filter<Document> {
  const club = folderUid.replace(/^\uFEFF/, "").trim();
  const esc = escapeRegExp(club);
  return {
    $or: [
      { ClubID: new RegExp(`^${esc}$`, "i") },
      { lessonReserveId: new RegExp(`^${esc}-LR`, "i") },
    ],
  };
}

export function totalClubPurgeDeletedCounts(d: PurgeClubMongoDeletedCounts): number {
  return (
    d.clubInfo +
    d.lessonList +
    d.paymentList +
    d.lessonSeriesInfo +
    d.lessonReserveList +
    d.lessonPaymentLedger +
    d.coachManager +
    d.prizeList +
    d.userListStudent +
    d.userListCoach +
    d.userListLegacy
  );
}

/**
 * Deletes club-scoped rows in all known collections (not `userLogin` — caller handles that).
 */
export async function purgeClubScopedMongoDataForFolderUid(
  folderUid: string,
): Promise<PurgeClubMongoResult> {
  const deleted = emptyDeleted();
  const errors: Array<{ collection: string; message: string }> = [];
  const id = folderUid.replace(/^\uFEFF/, "").trim();

  if (!isMongoConfigured() || !isValidClubFolderId(id)) {
    return { deleted, errors };
  }

  const reClub = clubIdExactRegex(id);
  const reserveLedgerFilter = lessonReserveOrLedgerPartitionFilter(id);

  async function del(
    collectionLabel: string,
    fn: () => Promise<number>,
    assign: (n: number) => void,
  ): Promise<void> {
    try {
      assign(await fn());
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      errors.push({ collection: collectionLabel, message: msg });
    }
  }

  await del(CLUB_INFO_COLLECTION, async () => {
    const c = await getClubInfoCollection();
    const r = await c.deleteMany({ club_id: reClub });
    return r.deletedCount ?? 0;
  }, (n) => {
    deleted.clubInfo = n;
  });

  await del(LESSON_LIST_COLLECTION, async () => {
    const c = await getLessonListCollection();
    const r = await c.deleteMany({
      $or: [{ _id: id }, { club_id: reClub }],
    });
    return r.deletedCount ?? 0;
  }, (n) => {
    deleted.lessonList = n;
  });

  await del(PAYMENT_LIST_COLLECTION, async () => {
    const c = await getPaymentListCollection();
    const r = await c.deleteMany({
      $or: [{ _id: id }, { club_id: reClub }],
    });
    return r.deletedCount ?? 0;
  }, (n) => {
    deleted.paymentList = n;
  });

  await del(LESSON_SERIES_INFO_COLLECTION, async () => {
    const c = await getLessonSeriesInfoCollection();
    const r = await c.deleteMany({ ClubID: reClub });
    return r.deletedCount ?? 0;
  }, (n) => {
    deleted.lessonSeriesInfo = n;
  });

  await del(LESSON_RESERVE_LIST_COLLECTION, async () => {
    const c = await getLessonReserveListCollection();
    const r = await c.deleteMany(reserveLedgerFilter);
    return r.deletedCount ?? 0;
  }, (n) => {
    deleted.lessonReserveList = n;
  });

  await del(LESSON_PAYMENT_LEDGER_COLLECTION, async () => {
    const c = await getLessonPaymentLedgerCollection();
    const r = await c.deleteMany(reserveLedgerFilter);
    return r.deletedCount ?? 0;
  }, (n) => {
    deleted.lessonPaymentLedger = n;
  });

  await del(COACH_SALARY_COLLECTION, async () => {
    const c = await getCoachSalaryCollection();
    const r = await c.deleteMany({ ClubID: reClub });
    return r.deletedCount ?? 0;
  }, (n) => {
    deleted.coachManager = n;
  });

  await del(PRIZE_LIST_ROW_COLLECTION, async () => {
    const c = await getPrizeListRowCollection();
    const r = await c.deleteMany({ ClubID: reClub });
    return r.deletedCount ?? 0;
  }, (n) => {
    deleted.prizeList = n;
  });

  const rosterClubFilter: Filter<Document> = {
    $or: [
      { club_folder_uid: reClub },
      { club_id: reClub },
      { ClubID: reClub },
    ],
  };

  await del(USER_LIST_STUDENT_COLLECTION, async () => {
    const c = await getUserListStudentCollection();
    const r = await c.deleteMany(rosterClubFilter);
    return r.deletedCount ?? 0;
  }, (n) => {
    deleted.userListStudent = n;
  });

  await del(USER_LIST_COACH_COLLECTION, async () => {
    const c = await getUserListCoachCollection();
    const r = await c.deleteMany(rosterClubFilter);
    return r.deletedCount ?? 0;
  }, (n) => {
    deleted.userListCoach = n;
  });

  await del(USER_LIST_COLLECTION, async () => {
    const c = await getUserListCollection();
    const r = await c.deleteMany({
      $or: [{ club_folder_uid: reClub }, { club_id: reClub }],
    });
    return r.deletedCount ?? 0;
  }, (n) => {
    deleted.userListLegacy = n;
  });

  return { deleted, errors };
}

/**
 * Removes all `userLogin` rows for this club folder: Coach Manager (`uid` = folder id),
 * and Coach / Student rows with `club_folder_uid` or `club_id` matching the folder.
 */
export async function purgeUserLoginForClubFolderUid(
  folderUid: string,
): Promise<{ deleted: number; error?: string }> {
  const id = folderUid.replace(/^\uFEFF/, "").trim();
  if (!isMongoConfigured() || !isValidClubFolderId(id)) {
    return { deleted: 0 };
  }
  try {
    const coll = await getUserLoginCollection();
    const reClub = clubIdExactRegex(id);
    const r = await coll.deleteMany({
      $or: [
        { usertype: "Coach Manager", uid: reClub },
        { club_folder_uid: reClub },
        { club_id: reClub },
      ],
    });
    return { deleted: r.deletedCount ?? 0 };
  } catch (e) {
    return {
      deleted: 0,
      error: e instanceof Error ? e.message : String(e),
    };
  }
}

/**
 * Club-scoped collections first, then `userLogin` (so logins are not left pointing at removed club data).
 */
export async function purgeEntireClubFromMongo(folderUid: string): Promise<{
  collections: PurgeClubMongoResult;
  userLogin: { deleted: number; error?: string };
}> {
  const collections = await purgeClubScopedMongoDataForFolderUid(folderUid);
  const userLogin = await purgeUserLoginForClubFolderUid(folderUid);
  return { collections, userLogin };
}
