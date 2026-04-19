import { Router } from "express";
import { BASIC_INFO_COLLECTION } from "../db/DBConnection";
import { readBasicInfoFromMongo } from "../basicInfoMongo";
import { readBasicInfo } from "../basicInfoCsv";

/**
 * Public reference lists (no auth — form dropdowns).
 * Prefers MongoDB `basicInfo` when configured and document `basicInfoLists` exists;
 * otherwise `BasicInfo.csv`.
 */
export function createBasicInfoRouter(): Router {
  const r = Router();
  r.get("/", async (_req, res) => {
    const fromMongo = await readBasicInfoFromMongo();
    if (fromMongo) {
      res.json({
        ok: true,
        countries: fromMongo.countries,
        sportTypes: fromMongo.sportTypes,
        source: "mongodb",
        collection: BASIC_INFO_COLLECTION,
      });
      return;
    }
    const { countries, sportTypes } = readBasicInfo();
    res.json({
      ok: true,
      countries,
      sportTypes,
      source: "csv",
    });
  });
  return r;
}
