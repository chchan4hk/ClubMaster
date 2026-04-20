import { Router, type Request } from "express";
import { requireAuth, requireRole } from "../middleware/requireAuth";
import {
  coachManagerClubContextAsync,
  findCoachManagerUserRowForClubUid,
} from "../coachManagerSession";
import {
  csvCoachFieldMatchesLoggedCoach,
  filterRawRowsByIdColumn,
  findCoachRosterRow,
} from "../coachSelfFilter";
import {
  findClubUidForCoachId,
  isValidClubFolderId,
  loadCoaches,
} from "../coachListCsv";
import {
  appendPrizeRow,
  ensurePrizeListFile,
  loadPrizeListRaw,
  loadPrizes,
  PRIZE_LIST_FILENAME,
  prizeCsvRowToApiFields,
  prizeListResolvedPath,
  prizeListStorageClubId,
  updatePrizeRow,
} from "../prizeListJson";

function readPrizeWriteBody(body: unknown): {
  sportType: string;
  year: string;
  association: string;
  competition: string;
  ageGroup: string;
  prizeType: string;
  studentName: string;
  ranking: string;
  status: string;
  verifiedBy: string;
  remarks: string;
} {
  const b = body as Record<string, unknown>;
  return {
    sportType: String(b?.SportType ?? b?.sportType ?? "").trim(),
    year: String(b?.Year ?? b?.year ?? "").trim(),
    association: String(b?.Association ?? b?.association ?? "").trim(),
    competition: String(b?.Competition ?? b?.competition ?? "").trim(),
    ageGroup: String(
      b?.Age_group ?? b?.age_group ?? b?.ageGroup ?? "",
    ).trim(),
    prizeType: String(
      b?.Prize_type ?? b?.prize_type ?? b?.prizeType ?? "",
    ).trim(),
    studentName: String(
      b?.StudentName ?? b?.studentName ?? "",
    ).trim(),
    ranking: String(b?.Ranking ?? b?.ranking ?? "").trim(),
    status: String(b?.Status ?? b?.status ?? "ACTIVE").trim(),
    verifiedBy: String(b?.VerifiedBy ?? b?.verifiedBy ?? "").trim(),
    remarks: String(b?.Remarks ?? b?.remarks ?? "").trim(),
  };
}

/** Coach Manager: JWT sub is club folder id. Coach: JWT sub is CoachID → resolve club via roster. */
async function resolvePrizeClubContextAsync(
  req: Request,
):
  Promise<
    | { ok: true; clubId: string; clubName: string }
    | { ok: false; status: number; error: string }
  > {
  const role = String(req.user?.role ?? "");
  if (role === "CoachManager") {
    return coachManagerClubContextAsync(req);
  }
  if (role !== "Coach") {
    return { ok: false, status: 403, error: "Forbidden" };
  }
  const coachId = String(req.user?.sub ?? "").trim();
  if (!coachId) {
    return { ok: false, status: 403, error: "Invalid session." };
  }
  const clubId = findClubUidForCoachId(coachId);
  if (!clubId) {
    return {
      ok: false,
      status: 403,
      error: "No club roster found for this coach account.",
    };
  }
  const managerRow = await findCoachManagerUserRowForClubUid(clubId);
  if (!managerRow || managerRow.role !== "CoachManager") {
    return { ok: false, status: 403, error: "Invalid club for prize access." };
  }
  const clubName = (managerRow.clubName && managerRow.clubName.trim()) || "";
  if (!clubName || clubName === "—") {
    return {
      ok: false,
      status: 400,
      error: "Your club has no name configured; contact an administrator.",
    };
  }
  const inRoster = loadCoaches(clubId).some(
    (c) => c.coachId.trim().toUpperCase() === coachId.toUpperCase()
  );
  if (!inRoster) {
    return { ok: false, status: 403, error: "Coach not in club roster." };
  }
  return { ok: true, clubId, clubName };
}

export function createCoachManagerPrizeRouter(): Router {
  const r = Router();

  r.use(requireAuth, requireRole("CoachManager", "Coach"));

  r.get("/", async (_req, res) => {
    const ctx = await resolvePrizeClubContextAsync(_req);
    if (!ctx.ok) {
      res.status(ctx.status).json({ ok: false, error: ctx.error });
      return;
    }
    try {
      ensurePrizeListFile(ctx.clubId);
      const prizeListRaw = loadPrizeListRaw(ctx.clubId);
      let prizes: ReturnType<typeof loadPrizes> = [];
      let prizesParseWarning: string | null = null;
      try {
        prizes = loadPrizes(ctx.clubId);
      } catch (parseErr) {
        prizesParseWarning =
          parseErr instanceof Error ? parseErr.message : String(parseErr);
      }
      let prizeListRawFiltered = prizeListRaw;
      if (_req.user?.role === "Coach") {
        const crow = findCoachRosterRow(ctx.clubId, String(_req.user.sub));
        const uname = String(_req.user.username ?? "");
        if (crow) {
          prizes = prizes.filter((p) =>
            csvCoachFieldMatchesLoggedCoach(p.verifiedBy, crow, uname),
          );
        } else {
          prizes = [];
        }
        const keep = new Set(
          prizes.map((p) => p.prizeId.trim().toUpperCase()),
        );
        prizeListRawFiltered = {
          ...prizeListRaw,
          rows: filterRawRowsByIdColumn(
            prizeListRaw,
            ["PrizeID", "prizeid", "PRIZEID"],
            keep,
          ).rows,
        };
      }
      const fileEnc = encodeURIComponent(PRIZE_LIST_FILENAME);
      const storageId = prizeListStorageClubId(ctx.clubId);
      const storageEnc = encodeURIComponent(storageId);
      const listFileUrl = `/backend/data_club/${storageEnc}/${fileEnc}`;
      res.json({
        ok: true,
        clubId: ctx.clubId,
        clubName: ctx.clubName,
        prizeListStorageClubId: storageId,
        prizeListFileUrl: listFileUrl,
        prizeListResolvedPath: prizeListResolvedPath(ctx.clubId),
        prizes: prizes.map((p) => prizeCsvRowToApiFields(p)),
        /** Tabular view derived from PrizeList.json (not CSV). */
        prizeListRaw: prizeListRawFiltered,
        ...(prizesParseWarning ? { prizesParseWarning } : {}),
      });
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      res.status(500).json({ ok: false, error: msg });
    }
  });

  r.post("/", async (req, res) => {
    const ctx = await resolvePrizeClubContextAsync(req);
    if (!ctx.ok) {
      res.status(ctx.status).json({ ok: false, error: ctx.error });
      return;
    }
    let w = readPrizeWriteBody(req.body);
    if (req.user?.role === "Coach") {
      const crow = findCoachRosterRow(ctx.clubId, String(req.user.sub));
      if (!crow) {
        res.status(403).json({ ok: false, error: "Coach roster row not found." });
        return;
      }
      const vn = (crow.coachName && crow.coachName.trim()) || w.verifiedBy;
      w = { ...w, verifiedBy: vn };
    }
    /** Appends to backend/data_club/{UID}/PrizeList.json with ClubID and Club_name from session. */
    const result = appendPrizeRow(ctx.clubId, { ...w, clubName: ctx.clubName });
    if (!result.ok) {
      res.status(400).json({ ok: false, error: result.error });
      return;
    }
    res.json({ ok: true, prizeId: result.prizeId, message: "Prize created." });
  });

  r.put("/", async (req, res) => {
    const ctx = await resolvePrizeClubContextAsync(req);
    if (!ctx.ok) {
      res.status(ctx.status).json({ ok: false, error: ctx.error });
      return;
    }
    const pid = String(req.body?.PrizeID ?? req.body?.prizeId ?? "").trim();
    const w = readPrizeWriteBody(req.body);
    if (req.user?.role === "Coach") {
      const crow = findCoachRosterRow(ctx.clubId, String(req.user.sub));
      if (!crow) {
        res.status(403).json({ ok: false, error: "Coach roster row not found." });
        return;
      }
      const existing = loadPrizes(ctx.clubId).find(
        (p) => p.prizeId.trim().toUpperCase() === pid.toUpperCase(),
      );
      if (
        !existing ||
        !csvCoachFieldMatchesLoggedCoach(
          existing.verifiedBy,
          crow,
          String(req.user.username ?? ""),
        )
      ) {
        res.status(403).json({
          ok: false,
          error: "You can only update prizes you verified (VerifiedBy).",
        });
        return;
      }
    }
    const result = updatePrizeRow(ctx.clubId, pid, {
      ...w,
      clubName: ctx.clubName,
    });
    if (!result.ok) {
      res.status(400).json({ ok: false, error: result.error });
      return;
    }
    res.json({ ok: true, message: "Prize updated." });
  });

  return r;
}
