import { Router } from "express";
import {
  BASIC_INFO_COLLECTION,
  BASIC_INFO_LISTS_DOC_ID,
  isMongoConfigured,
  resolveBasicInfoDatabaseName,
} from "../db/DBConnection";
import {
  addCountryToCanonicalMongo,
  addSportTypeToCanonicalMongo,
  readBasicInfoFromMongo,
  removeCountryFromCanonicalMongo,
  removeSportTypeFromCanonicalMongo,
} from "../basicInfoMongo";
import { readBasicInfo } from "../basicInfoCsv";
import { requireAuth, requireRole } from "../middleware/requireAuth";

/**
 * Public reference lists (no auth — form dropdowns).
 * Prefers MongoDB: merges **all documents** in the `basicInfo` collection with legacy `BasicInfo`
 * (PascalCase) row/list documents in {@link resolveBasicInfoDatabaseName} (default `ClubMaster_DB`);
 * otherwise `BasicInfo.csv`.
 *
 * Admin Sport Activation uses {@link requireAuth} + {@link requireRole}("Admin") on
 * `/admin/sport-types` and `/admin/countries` for list mutations (Mongo canonical document only);
 * merged reference data includes `sportTypes` and `countries` (name + optional `prefix` + optional `country_code`).
 */
export function createBasicInfoRouter(): Router {
  const r = Router();

  r.get(
    "/admin/sport-types",
    requireAuth,
    requireRole("Admin"),
    async (_req, res) => {
      try {
        const fromMongo = await readBasicInfoFromMongo();
        if (fromMongo) {
          res.json({
            ok: true,
            sportTypes: fromMongo.sportTypes,
            countries: fromMongo.countries,
            source: "mongodb",
            database: resolveBasicInfoDatabaseName(),
            collection: BASIC_INFO_COLLECTION,
            documentId: BASIC_INFO_LISTS_DOC_ID,
          });
          return;
        }
        const { sportTypes, countries } = readBasicInfo();
        res.json({
          ok: true,
          sportTypes,
          countries,
          source: "csv",
          database: resolveBasicInfoDatabaseName(),
          collection: null,
          documentId: null,
        });
      } catch (e) {
        const msg = e instanceof Error ? e.message : String(e);
        res.status(500).json({ ok: false, error: msg });
      }
    },
  );

  r.post(
    "/admin/sport-types",
    requireAuth,
    requireRole("Admin"),
    async (req, res) => {
      try {
        if (!isMongoConfigured()) {
          res.status(400).json({
            ok: false,
            error: "MongoDB is required to add sport types.",
          });
          return;
        }
        const name = String(req.body?.name ?? "").trim();
        const ins = await addSportTypeToCanonicalMongo(name);
        if (!ins.ok) {
          res.status(400).json({ ok: false, error: ins.error });
          return;
        }
        const merged = await readBasicInfoFromMongo();
        res.json({
          ok: true,
          sportTypes: merged?.sportTypes ?? ins.sportTypes,
          countries: merged?.countries ?? [],
          source: "mongodb",
          database: resolveBasicInfoDatabaseName(),
          collection: BASIC_INFO_COLLECTION,
          documentId: BASIC_INFO_LISTS_DOC_ID,
        });
      } catch (e) {
        const msg = e instanceof Error ? e.message : String(e);
        res.status(500).json({ ok: false, error: msg });
      }
    },
  );

  r.delete(
    "/admin/sport-types",
    requireAuth,
    requireRole("Admin"),
    async (req, res) => {
      try {
        if (!isMongoConfigured()) {
          res.status(400).json({
            ok: false,
            error: "MongoDB is required to remove sport types.",
          });
          return;
        }
        const name = String(req.body?.name ?? "").trim();
        const out = await removeSportTypeFromCanonicalMongo(name);
        if (!out.ok) {
          res.status(400).json({ ok: false, error: out.error });
          return;
        }
        const merged = await readBasicInfoFromMongo();
        res.json({
          ok: true,
          sportTypes: merged?.sportTypes ?? out.sportTypes,
          countries: merged?.countries ?? [],
          source: "mongodb",
          database: resolveBasicInfoDatabaseName(),
          collection: BASIC_INFO_COLLECTION,
          documentId: BASIC_INFO_LISTS_DOC_ID,
        });
      } catch (e) {
        const msg = e instanceof Error ? e.message : String(e);
        res.status(500).json({ ok: false, error: msg });
      }
    },
  );

  r.post(
    "/admin/countries",
    requireAuth,
    requireRole("Admin"),
    async (req, res) => {
      try {
        if (!isMongoConfigured()) {
          res.status(400).json({
            ok: false,
            error: "MongoDB is required to add countries.",
          });
          return;
        }
        const name = String(req.body?.name ?? "").trim();
        const country_code = String(
          req.body?.country_code ?? req.body?.countryCode ?? "",
        ).trim();
        const ins = await addCountryToCanonicalMongo(name, { country_code });
        if (!ins.ok) {
          res.status(400).json({ ok: false, error: ins.error });
          return;
        }
        const merged = await readBasicInfoFromMongo();
        res.json({
          ok: true,
          sportTypes: merged?.sportTypes ?? [],
          countries: merged?.countries ?? ins.countries,
          source: "mongodb",
          database: resolveBasicInfoDatabaseName(),
          collection: BASIC_INFO_COLLECTION,
          documentId: BASIC_INFO_LISTS_DOC_ID,
        });
      } catch (e) {
        const msg = e instanceof Error ? e.message : String(e);
        res.status(500).json({ ok: false, error: msg });
      }
    },
  );

  r.delete(
    "/admin/countries",
    requireAuth,
    requireRole("Admin"),
    async (req, res) => {
      try {
        if (!isMongoConfigured()) {
          res.status(400).json({
            ok: false,
            error: "MongoDB is required to remove countries.",
          });
          return;
        }
        const name = String(req.body?.name ?? "").trim();
        const out = await removeCountryFromCanonicalMongo(name);
        if (!out.ok) {
          res.status(400).json({ ok: false, error: out.error });
          return;
        }
        const merged = await readBasicInfoFromMongo();
        res.json({
          ok: true,
          sportTypes: merged?.sportTypes ?? [],
          countries: merged?.countries ?? out.countries,
          source: "mongodb",
          database: resolveBasicInfoDatabaseName(),
          collection: BASIC_INFO_COLLECTION,
          documentId: BASIC_INFO_LISTS_DOC_ID,
        });
      } catch (e) {
        const msg = e instanceof Error ? e.message : String(e);
        res.status(500).json({ ok: false, error: msg });
      }
    },
  );

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
