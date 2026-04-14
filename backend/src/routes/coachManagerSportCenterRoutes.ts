import { Router, type Request } from "express";
import { requireAuth, requireRole } from "../middleware/requireAuth";
import { findUserByUid } from "../userlistCsv";
import { clubInfoFirstRowObject } from "../clubInfoJson";
import {
  appendSportCenterRow,
  applySportCenterStatusUpdates,
  isAllowedVenueLocation,
  loadSportCenterRows,
  normEqSportType,
  sportCenterListPath,
  SPORT_CENTER_LIST_FILENAME,
} from "../sportCenterListCsv";
import { isValidClubFolderId } from "../coachListCsv";

function coachManagerClubContext(req: Request):
  | { ok: true; clubId: string; clubName: string }
  | { ok: false; status: number; error: string } {
  const clubId = String(req.user?.sub ?? "").trim();
  if (!clubId || !isValidClubFolderId(clubId)) {
    return { ok: false, status: 403, error: "Invalid club session." };
  }
  const row = findUserByUid(clubId);
  if (!row || row.role !== "CoachManager") {
    return { ok: false, status: 403, error: "Coach Manager access only." };
  }
  const clubName = (row.clubName && row.clubName.trim()) || "";
  if (!clubName || clubName === "—") {
    return {
      ok: false,
      status: 400,
      error: "Your account has no club name; contact an administrator.",
    };
  }
  return { ok: true, clubId, clubName };
}

function clubSportTypeFilter(clubId: string): string {
  const fields = clubInfoFirstRowObject(clubId);
  const st =
    fields["SportType"]?.trim() ||
    fields["Sport_type"]?.trim() ||
    fields["Sport Type"]?.trim() ||
    fields["sport_type"]?.trim() ||
    "";
  return st;
}

export function createCoachManagerSportCenterRouter(): Router {
  const r = Router();

  r.use(requireAuth, requireRole("CoachManager"));

  r.post("/", (req, res) => {
    const ctx = coachManagerClubContext(req);
    if (!ctx.ok) {
      res.status(ctx.status).json({ ok: false, error: ctx.error });
      return;
    }
    const b = req.body as Record<string, unknown>;
    const country = String(b?.country ?? b?.Country ?? "").trim();
    const sportType = String(
      b?.sportType ?? b?.SportType ?? b?.sport_type ?? "",
    ).trim();
    const locationRaw = String(b?.location ?? b?.Location ?? "").trim();
    const locationNorm = locationRaw.toLowerCase();
    const locationDisplay =
      locationNorm === "clubhouse"
        ? "Clubhouse"
        : locationNorm === "school"
          ? "School"
          : locationNorm === "others"
            ? "Others"
            : locationRaw;
    const sportCenter = String(
      b?.sport_center ?? b?.sportCenter ?? b?.["Sport Center"] ?? "",
    ).trim();
    const address = String(b?.address ?? b?.Address ?? "").trim();
    const link = String(b?.link ?? b?.Link ?? "").trim();
    if (!country) {
      res.status(400).json({ ok: false, error: "Country is required." });
      return;
    }
    if (!sportType) {
      res.status(400).json({ ok: false, error: "SportType is required." });
      return;
    }
    if (!isAllowedVenueLocation(locationRaw)) {
      res.status(400).json({
        ok: false,
        error: 'Location must be "Clubhouse", "School", or "Others".',
      });
      return;
    }
    if (!sportCenter) {
      res.status(400).json({
        ok: false,
        error: "Sport center (venue name) is required.",
      });
      return;
    }
    const out = appendSportCenterRow(ctx.clubId, {
      status: "ACTIVE",
      country,
      sportType,
      location: locationDisplay,
      sportCenter,
      address,
      link,
    });
    if (!out.ok) {
      res.status(400).json({ ok: false, error: out.error });
      return;
    }
    res.json({
      ok: true,
      message: "Venue added to SportCenterList.csv with status ACTIVE.",
    });
  });

  r.get("/", (req, res) => {
    const ctx = coachManagerClubContext(req);
    if (!ctx.ok) {
      res.status(ctx.status).json({ ok: false, error: ctx.error });
      return;
    }
    try {
      const sportTypeFilter = clubSportTypeFilter(ctx.clubId);
      const { rows } = loadSportCenterRows(ctx.clubId);
      const filtered = sportTypeFilter
        ? rows.filter((row) => normEqSportType(row.SportType, sportTypeFilter))
        : rows.slice();

      const orderLoc = new Map<string, number>();
      const byLocation = new Map<string, typeof filtered>();
      for (const row of filtered) {
        const loc = row.Location.trim() || "—";
        if (!byLocation.has(loc)) {
          byLocation.set(loc, []);
          orderLoc.set(loc, orderLoc.size);
        }
        byLocation.get(loc)!.push(row);
      }
      const locations = [...byLocation.keys()].sort((a, b) =>
        a.localeCompare(b, undefined, { sensitivity: "base" }),
      );
      const sections = locations.map((location) => ({
        location,
        centers: (byLocation.get(location) ?? []).map((row) => ({
          rowIndex: row.rowIndex,
          status: row.Status.trim(),
          sportCenter: row.sportCenter,
          address: row.Address,
          link: row.Link,
          sportType: row.SportType,
        })),
      }));

      const idEnc = encodeURIComponent(ctx.clubId);
      const fileEnc = encodeURIComponent(SPORT_CENTER_LIST_FILENAME);
      res.json({
        ok: true,
        clubId: ctx.clubId,
        sportTypeFilter: sportTypeFilter || null,
        sportCentersFileUrl: `/backend/data_club/${idEnc}/${fileEnc}`,
        sportCentersResolvedPath: sportCenterListPath(ctx.clubId),
        sections,
        totalRowsInFile: rows.length,
      });
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      res.status(500).json({ ok: false, error: msg });
    }
  });

  r.put("/", (req, res) => {
    const ctx = coachManagerClubContext(req);
    if (!ctx.ok) {
      res.status(ctx.status).json({ ok: false, error: ctx.error });
      return;
    }
    try {
      const body = req.body as { activeRowIndices?: unknown };
      const raw = body?.activeRowIndices;
      const activeSet = new Set<number>();
      if (Array.isArray(raw)) {
        for (const x of raw) {
          const n = Number(x);
          if (Number.isInteger(n) && n >= 0) {
            activeSet.add(n);
          }
        }
      }
      const sportTypeFilter = clubSportTypeFilter(ctx.clubId);
      const { rows } = loadSportCenterRows(ctx.clubId);
      const updates = new Map<number, string>();
      for (const row of rows) {
        if (
          sportTypeFilter &&
          !normEqSportType(row.SportType, sportTypeFilter)
        ) {
          continue;
        }
        updates.set(
          row.rowIndex,
          activeSet.has(row.rowIndex) ? "ACTIVE" : "INACTIVE",
        );
      }
      applySportCenterStatusUpdates(ctx.clubId, updates);
      res.json({ ok: true, updatedCount: updates.size });
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      res.status(500).json({ ok: false, error: msg });
    }
  });

  return r;
}
