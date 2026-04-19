import type { BasicInfoLists } from "./basicInfoCsv";
import {
  BASIC_INFO_LISTS_DOC_ID,
  getBasicInfoCollection,
  isMongoConfigured,
} from "./db/DBConnection";

/**
 * Loads countries / sport types from `basicInfo` collection document
 * {@link BASIC_INFO_LISTS_DOC_ID}, or `null` if Mongo is not configured,
 * the document is missing, or read fails (caller may fall back to CSV).
 */
export async function readBasicInfoFromMongo(): Promise<BasicInfoLists | null> {
  if (!isMongoConfigured()) {
    return null;
  }
  try {
    const coll = await getBasicInfoCollection();
    const doc = await coll.findOne({ _id: BASIC_INFO_LISTS_DOC_ID });
    if (!doc) {
      return null;
    }
    const countries = Array.isArray(doc.countries)
      ? doc.countries.map((c) => String(c ?? "").trim()).filter(Boolean)
      : [];
    const sportTypes = Array.isArray(doc.sportTypes)
      ? doc.sportTypes.map((c) => String(c ?? "").trim()).filter(Boolean)
      : [];
    return { countries, sportTypes };
  } catch (e) {
    console.warn(
      "[basic-info] Mongo read failed:",
      e instanceof Error ? e.message : String(e),
    );
    return null;
  }
}
