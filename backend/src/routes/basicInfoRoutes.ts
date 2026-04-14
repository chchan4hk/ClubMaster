import { Router } from "express";
import { readBasicInfo } from "../basicInfoCsv";

/** Public reference lists from BasicInfo.csv (no auth — used to populate form dropdowns). */
export function createBasicInfoRouter(): Router {
  const r = Router();
  r.get("/", (_req, res) => {
    const { countries, sportTypes } = readBasicInfo();
    res.json({ ok: true, countries, sportTypes });
  });
  return r;
}
