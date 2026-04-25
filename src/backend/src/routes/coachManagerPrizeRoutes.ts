import { Router, type Request } from "express";
import { requireAuth, requireRole } from "../middleware/requireAuth";
import {
  coachManagerClubContextAsync,
  resolveClubFolderRoleContextAsync,
  resolveClubFolderUidForCoachRequest,
} from "../coachManagerSession";
import {
  csvCoachFieldMatchesLoggedCoach,
  findCoachRosterRow,
} from "../coachSelfFilter";
import { isValidClubFolderId, type CoachCsvRow } from "../coachListCsv";
import {
  PRIZE_LIST_ROW_COLLECTION,
  isMongoConfigured,
  resolvePrizeListRowDatabaseName,
} from "../db/DBConnection";
import {
  appendPrizeRowPreferred,
  deletePrizeRowPreferred,
  loadPrizesPreferred,
  updatePrizeRowPreferred,
} from "../prizeListMongo";
import {
  PRIZE_LIST_FILENAME,
  prizeCsvRowToApiFields,
  prizeCsvRowsToPrizeListRaw,
  prizeListResolvedPath,
  prizeListStorageClubId,
} from "../prizeListJson";
import { enrichPrizeStudentNamesFromStudentRoster } from "../prizeStudentNameEnrich";

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

function coachJwtClubFolderMatches(
  req: Request,
  clubId: string,
): boolean {
  const jwtCfu = String(req.user?.club_folder_uid ?? "").trim();
  return (
    Boolean(jwtCfu) &&
    isValidClubFolderId(jwtCfu) &&
    jwtCfu.toUpperCase() === clubId.trim().toUpperCase()
  );
}

/**
 * Coach roster row, or a synthetic row when the signed-in coach JWT is bound to this
 * `clubId` (`club_folder_uid` from userLogin `club_id`) but roster `coach_id` ≠ login `uid`.
 * `VerifiedBy` filtering then uses login `username` + `sub` like {@link csvCoachFieldMatchesLoggedCoach}.
 */
async function coachRosterOrJwtSynthetic(
  req: Request,
  clubId: string,
  clubName: string,
): Promise<CoachCsvRow | null> {
  const sub = String(req.user?.sub ?? "").trim();
  if (!sub) {
    return null;
  }
  const fromRoster = await findCoachRosterRow(clubId, sub);
  if (fromRoster) {
    return fromRoster;
  }
  if (coachJwtClubFolderMatches(req, clubId)) {
    const uname = String(req.user?.username ?? "").trim();
    return {
      coachId: sub,
      coachName: uname || sub,
      clubName: clubName || "",
      sex: "",
      dateOfBirth: "",
      joinedDate: "",
      homeAddress: "",
      country: "",
      email: "",
      phone: "",
      remark: "",
      hourlyRate: "",
      status: "ACTIVE",
      createdDate: "",
      lastUpdateDate: "",
    };
  }
  return null;
}

/** Coach Manager: JWT sub is club folder id. Coach: club from `club_folder_uid` / roster; Mongo `PrizeList` by `ClubID`. */
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
  const clubId = await resolveClubFolderUidForCoachRequest(req);
  if (!clubId) {
    return {
      ok: false,
      status: 403,
      error: "No club roster found for this coach account.",
    };
  }
  const folderCtx = await resolveClubFolderRoleContextAsync(clubId, "prize");
  if (!folderCtx.ok) {
    return folderCtx;
  }
  const clubName = folderCtx.clubName;
  if (!clubName || clubName === "—") {
    return {
      ok: false,
      status: 400,
      error: "Your club has no name configured; contact an administrator.",
    };
  }
  const coachAccess = await coachRosterOrJwtSynthetic(req, clubId, clubName);
  if (!coachAccess) {
    return { ok: false, status: 403, error: "Coach not in club roster." };
  }
  return { ok: true, clubId, clubName };
}

const PRIZE_LIST_PAGE_MAX = 10;

/** Optional `page` / `limit` (or `pageSize`): return at most 10 prizes per page. */
function parsePrizeListQuery(req: Request): { page: number; limit: number } | null {
  const q = req.query;
  const hasPage = q.page != null && String(q.page).trim() !== "";
  const hasLimit =
    (q.limit != null && String(q.limit).trim() !== "") ||
    (q.pageSize != null && String(q.pageSize).trim() !== "");
  if (!hasPage && !hasLimit) {
    return null;
  }
  const page = Math.max(1, parseInt(String(q.page ?? "1"), 10) || 1);
  const limRaw =
    q.limit != null && String(q.limit).trim() !== ""
      ? String(q.limit)
      : String(q.pageSize ?? String(PRIZE_LIST_PAGE_MAX));
  const limit = Math.min(
    PRIZE_LIST_PAGE_MAX,
    Math.max(1, parseInt(limRaw, 10) || PRIZE_LIST_PAGE_MAX),
  );
  return { page, limit };
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
      let prizes: Awaited<ReturnType<typeof loadPrizesPreferred>> = [];
      let prizesParseWarning: string | null = null;
      try {
        prizes = await loadPrizesPreferred(ctx.clubId);
      } catch (parseErr) {
        prizesParseWarning =
          parseErr instanceof Error ? parseErr.message : String(parseErr);
      }
      try {
        prizes = await enrichPrizeStudentNamesFromStudentRoster(
          ctx.clubId,
          prizes,
        );
      } catch {
        /* keep prize-store names if roster load fails */
      }
      if (_req.user?.role === "Coach") {
        const crow = await coachRosterOrJwtSynthetic(
          _req,
          ctx.clubId,
          ctx.clubName,
        );
        const uname = String(_req.user.username ?? "");
        if (crow) {
          prizes = prizes.filter((p) =>
            csvCoachFieldMatchesLoggedCoach(p.verifiedBy, crow, uname),
          );
        } else {
          prizes = [];
        }
      }
      let prizeListRawFiltered = prizeCsvRowsToPrizeListRaw(ctx.clubId, prizes);
      if (isMongoConfigured()) {
        const dbName = resolvePrizeListRowDatabaseName();
        prizeListRawFiltered = {
          ...prizeListRawFiltered,
          relativePath: `mongodb/${dbName}/${PRIZE_LIST_ROW_COLLECTION}/${ctx.clubId.trim()}`,
        };
      }
      const fileEnc = encodeURIComponent(PRIZE_LIST_FILENAME);
      const storageId = prizeListStorageClubId(ctx.clubId);
      const storageEnc = encodeURIComponent(storageId);
      const listFileUrl = isMongoConfigured()
        ? null
        : `/backend/data_club/${storageEnc}/${fileEnc}`;
      const listQ = parsePrizeListQuery(_req);
      const totalPrizes = prizes.length;
      let pagePrizes = prizes;
      if (listQ) {
        const start = (listQ.page - 1) * listQ.limit;
        pagePrizes = prizes.slice(start, start + listQ.limit);
      }
      const includeFullPrizeRaw =
        listQ == null || listQ.page <= 1;
      const prizeListRawOut = includeFullPrizeRaw
        ? prizeListRawFiltered
        : {
            ...prizeListRawFiltered,
            rows: [] as string[][],
            truncated: true,
          };
      const payload: Record<string, unknown> = {
        ok: true,
        clubId: ctx.clubId,
        clubName: ctx.clubName,
        prizeListStorage: isMongoConfigured() ? "mongodb" : "json_file",
        prizeListStorageClubId: storageId,
        prizeListFileUrl: listFileUrl,
        prizeListResolvedPath: isMongoConfigured()
          ? `mongodb:${resolvePrizeListRowDatabaseName()}/${PRIZE_LIST_ROW_COLLECTION}`
          : prizeListResolvedPath(ctx.clubId),
        prizes: pagePrizes.map((p) => prizeCsvRowToApiFields(p)),
        /** Tabular view: Mongo `PrizeList` collection or `PrizeList.json`. */
        prizeListRaw: prizeListRawOut,
        ...(prizesParseWarning ? { prizesParseWarning } : {}),
      };
      if (listQ) {
        payload.prizeTotal = totalPrizes;
        payload.prizePage = listQ.page;
        payload.prizePageSize = listQ.limit;
      }
      res.json(payload);
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
      const crow = await coachRosterOrJwtSynthetic(
        req,
        ctx.clubId,
        ctx.clubName,
      );
      if (!crow) {
        res.status(403).json({ ok: false, error: "Coach roster row not found." });
        return;
      }
      const vn = (crow.coachName && crow.coachName.trim()) || w.verifiedBy;
      w = { ...w, verifiedBy: vn };
    }
    const result = await appendPrizeRowPreferred(ctx.clubId, {
      ...w,
      clubName: ctx.clubName,
    });
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
      const crow = await coachRosterOrJwtSynthetic(
        req,
        ctx.clubId,
        ctx.clubName,
      );
      if (!crow) {
        res.status(403).json({ ok: false, error: "Coach roster row not found." });
        return;
      }
      const existing = (await loadPrizesPreferred(ctx.clubId)).find(
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
    const result = await updatePrizeRowPreferred(ctx.clubId, pid, {
      ...w,
      clubName: ctx.clubName,
    });
    if (!result.ok) {
      res.status(400).json({ ok: false, error: result.error });
      return;
    }
    res.json({ ok: true, message: "Prize updated." });
  });

  r.delete("/", async (req, res) => {
    const ctx = await resolvePrizeClubContextAsync(req);
    if (!ctx.ok) {
      res.status(ctx.status).json({ ok: false, error: ctx.error });
      return;
    }
    const pid = String(req.body?.PrizeID ?? req.body?.prizeId ?? "").trim();
    if (!pid) {
      res.status(400).json({ ok: false, error: "PrizeID is required." });
      return;
    }
    if (req.user?.role === "Coach") {
      const crow = await coachRosterOrJwtSynthetic(
        req,
        ctx.clubId,
        ctx.clubName,
      );
      if (!crow) {
        res.status(403).json({ ok: false, error: "Coach roster row not found." });
        return;
      }
      const existing = (await loadPrizesPreferred(ctx.clubId)).find(
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
          error: "You can only remove prizes you verified (VerifiedBy).",
        });
        return;
      }
    }
    const result = await deletePrizeRowPreferred(ctx.clubId, pid);
    if (!result.ok) {
      res.status(400).json({ ok: false, error: result.error });
      return;
    }
    res.json({ ok: true, message: "Prize removed." });
  });

  return r;
}
