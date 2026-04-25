/**
 * MongoDB persistence for per-club payment lists (`ClubMaster_DB.PaymentList`, `_id` = club folder UID).
 * JSON under `data_club/{clubId}/PaymentList.json` is used only when Mongo is not configured, or once to seed Mongo.
 */
import type { Document } from "mongodb";
import {
  getPaymentListCollection,
  isMongoConfigured,
  type PaymentListClubDocument,
} from "./db/DBConnection";
import { isValidClubFolderId } from "./coachListCsv";

export function paymentListUsesMongo(): boolean {
  return isMongoConfigured();
}

export async function findPaymentListClubDocument(
  clubId: string,
): Promise<PaymentListClubDocument | null> {
  if (!paymentListUsesMongo() || !isValidClubFolderId(clubId)) {
    return null;
  }
  const coll = await getPaymentListCollection();
  return coll.findOne({ _id: clubId.trim() });
}

export async function replacePaymentListForClub(
  clubId: string,
  payments: Document[],
): Promise<void> {
  const id = clubId.trim();
  const coll = await getPaymentListCollection();
  const prev = await coll.findOne({ _id: id });
  const version =
    prev && typeof prev.version === "number" && Number.isFinite(prev.version)
      ? Math.trunc(prev.version)
      : 1;
  const doc: PaymentListClubDocument = {
    _id: id,
    club_id: id,
    version,
    payments,
  };
  await coll.replaceOne({ _id: id }, doc, { upsert: true });
}
