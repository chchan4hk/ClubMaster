import fs from "fs";
import path from "path";
import multer from "multer";
import { Router, type Request } from "express";
import { requireAuth, requireRole } from "../middleware/requireAuth";
import { findUserByUid } from "../userlistCsv";
import {
  clubAssetPublicUrl,
  clubImageDir,
  clubInfoFirstRowObject,
  clubInfoResolvedPath,
  CLUB_INFO_FILENAME,
  CLUB_LOGO_FILENAME,
  clubLogoRelativePath,
  clubPaymentQrRelativePath,
  CLUB_PAYMENT_QR_JSON_KEYS,
  isClubPaymentQrChannel,
  loadClubInfoExtended,
  loadClubInfoRaw,
  todaySlashYmd,
  writeClubInfoFromPatch,
  type ClubPaymentQrChannel,
} from "../clubInfoJson";
import { clubDataDir, isValidClubFolderId } from "../coachListCsv";

const logoUpload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 12 * 1024 * 1024 },
  fileFilter: (_req, file, cb) => {
    if (/^image\/(jpeg|png|webp|gif)$/i.test(file.mimetype)) {
      cb(null, true);
    } else {
      cb(null, false);
    }
  },
});

const paymentQrUpload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 12 * 1024 * 1024 },
  fileFilter: (_req, file, cb) => {
    if (/^image\/(jpeg|png|webp|gif)$/i.test(file.mimetype)) {
      cb(null, true);
    } else {
      cb(null, false);
    }
  },
});

function paymentQrDiskPath(clubId: string, rel: string): string {
  const clubRoot = clubDataDir(clubId);
  if (!clubRoot || rel.includes("..")) {
    return "";
  }
  return path.normalize(
    path.join(clubRoot, ...rel.split("/").filter(Boolean)),
  );
}

function mergePaymentFieldsIntoCoachFields(
  fields: Record<string, string>,
  clubId: string,
): void {
  const ext = loadClubInfoExtended(clubId);
  for (const ch of Object.keys(CLUB_PAYMENT_QR_JSON_KEYS) as ClubPaymentQrChannel[]) {
    const key = CLUB_PAYMENT_QR_JSON_KEYS[ch];
    const v = ext[key];
    fields[key] = v == null ? "" : String(v).trim();
  }
}

function paymentQrPublicUrls(
  clubId: string,
): Record<ClubPaymentQrChannel, string | null> {
  const ext = loadClubInfoExtended(clubId);
  const out = {} as Record<ClubPaymentQrChannel, string | null>;
  for (const ch of Object.keys(CLUB_PAYMENT_QR_JSON_KEYS) as ClubPaymentQrChannel[]) {
    const key = CLUB_PAYMENT_QR_JSON_KEYS[ch];
    const rel = String(ext[key] ?? "").trim();
    const disk = rel ? paymentQrDiskPath(clubId, rel) : "";
    const ok = Boolean(disk && fs.existsSync(disk));
    out[ch] = rel && ok ? clubAssetPublicUrl(clubId, rel) : null;
  }
  return out;
}

function clubLogoRelFromFields(fields: Record<string, string>): string {
  const a = fields["club_logo"]?.trim() ?? "";
  if (a) {
    return a;
  }
  const b = fields["clubLogo"]?.trim() ?? "";
  return b;
}

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

export function createCoachManagerClubInfoRouter(): Router {
  const r = Router();

  r.use(requireAuth, requireRole("CoachManager"));

  r.get("/", (_req, res) => {
    const ctx = coachManagerClubContext(_req);
    if (!ctx.ok) {
      res.status(ctx.status).json({ ok: false, error: ctx.error });
      return;
    }
    try {
      const raw = loadClubInfoRaw(ctx.clubId);
      const fields = clubInfoFirstRowObject(ctx.clubId);
      mergePaymentFieldsIntoCoachFields(fields, ctx.clubId);
      const logoRel = clubLogoRelFromFields(fields);
      const clubRoot = clubDataDir(ctx.clubId);
      const logoDisk =
        logoRel && clubRoot && !logoRel.includes("..")
          ? path.normalize(
              path.join(clubRoot, ...logoRel.split("/").filter(Boolean)),
            )
          : "";
      const logoExists = Boolean(logoDisk && fs.existsSync(logoDisk));
      const club_logo_url =
        logoRel && logoExists ? clubAssetPublicUrl(ctx.clubId, logoRel) : null;
      const idEnc = encodeURIComponent(ctx.clubId);
      const fileEnc = encodeURIComponent(CLUB_INFO_FILENAME);
      res.json({
        ok: true,
        clubId: ctx.clubId,
        clubName: ctx.clubName,
        clubInfoFileUrl: `/backend/data_club/${idEnc}/${fileEnc}`,
        clubInfoResolvedPath: clubInfoResolvedPath(ctx.clubId),
        club_logo_url,
        paymentQrUrls: paymentQrPublicUrls(ctx.clubId),
        fields,
        clubInfoJson: {
          headers: raw.headers,
          rows: raw.rows,
          headerLine: raw.headerLine,
        },
      });
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      res.status(500).json({ ok: false, error: msg });
    }
  });

  r.post("/logo", logoUpload.single("logo"), (req, res) => {
    const ctx = coachManagerClubContext(req);
    if (!ctx.ok) {
      res.status(ctx.status).json({ ok: false, error: ctx.error });
      return;
    }
    const file = req.file;
    if (!file?.buffer?.length) {
      res.status(400).json({
        ok: false,
        error: "No image uploaded. Use JPEG, PNG, WebP, or GIF.",
      });
      return;
    }
    try {
      const dir = clubImageDir(ctx.clubId);
      fs.mkdirSync(dir, { recursive: true });
      const outPath = path.join(dir, CLUB_LOGO_FILENAME);
      fs.writeFileSync(outPath, file.buffer);
      const rel = clubLogoRelativePath();
      const lastUpdate = todaySlashYmd();
      writeClubInfoFromPatch(
        ctx.clubId,
        { club_logo: rel, clubLogo: rel },
        lastUpdate,
      );
      const fields = clubInfoFirstRowObject(ctx.clubId);
      mergePaymentFieldsIntoCoachFields(fields, ctx.clubId);
      res.json({
        ok: true,
        clubId: ctx.clubId,
        lastUpdate_date: lastUpdate,
        club_logo: rel,
        club_logo_url: clubAssetPublicUrl(ctx.clubId, rel),
        paymentQrUrls: paymentQrPublicUrls(ctx.clubId),
        fields,
      });
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      res.status(500).json({ ok: false, error: msg });
    }
  });

  r.post("/payment-qr", paymentQrUpload.single("qr"), (req, res) => {
    const ctx = coachManagerClubContext(req);
    if (!ctx.ok) {
      res.status(ctx.status).json({ ok: false, error: ctx.error });
      return;
    }
    const channelRaw = String(req.body?.channel ?? req.body?.type ?? "").trim();
    if (!isClubPaymentQrChannel(channelRaw)) {
      res.status(400).json({
        ok: false,
        error:
          "Invalid channel. Use payme, fps, wechat, alipay, or zhifubao.",
      });
      return;
    }
    const channel = channelRaw;
    const file = req.file;
    if (!file?.buffer?.length) {
      res.status(400).json({
        ok: false,
        error: "No image uploaded. Use JPEG, PNG, WebP, or GIF.",
      });
      return;
    }
    try {
      const dir = clubImageDir(ctx.clubId);
      fs.mkdirSync(dir, { recursive: true });
      const rel = clubPaymentQrRelativePath(channel);
      const fileName = path.basename(rel);
      const outPath = path.join(dir, fileName);
      fs.writeFileSync(outPath, file.buffer);
      const jsonKey = CLUB_PAYMENT_QR_JSON_KEYS[channel];
      const lastUpdate = todaySlashYmd();
      const patch: Record<string, unknown> = { [jsonKey]: rel };
      writeClubInfoFromPatch(ctx.clubId, patch, lastUpdate);
      const fields = clubInfoFirstRowObject(ctx.clubId);
      mergePaymentFieldsIntoCoachFields(fields, ctx.clubId);
      res.json({
        ok: true,
        clubId: ctx.clubId,
        lastUpdate_date: lastUpdate,
        channel,
        relPath: rel,
        jsonKey,
        url: clubAssetPublicUrl(ctx.clubId, rel),
        paymentQrUrls: paymentQrPublicUrls(ctx.clubId),
        fields,
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
      const body =
        req.body && typeof req.body === "object"
          ? (req.body as Record<string, unknown>)
          : {};
      const lastUpdate = todaySlashYmd();
      writeClubInfoFromPatch(ctx.clubId, body, lastUpdate);
      const fields = clubInfoFirstRowObject(ctx.clubId);
      mergePaymentFieldsIntoCoachFields(fields, ctx.clubId);
      const logoRel = clubLogoRelFromFields(fields);
      const club_logo_url = logoRel ? clubAssetPublicUrl(ctx.clubId, logoRel) : null;
      res.json({
        ok: true,
        clubId: ctx.clubId,
        lastUpdate_date: lastUpdate,
        club_logo_url,
        paymentQrUrls: paymentQrPublicUrls(ctx.clubId),
        fields,
      });
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      res.status(500).json({ ok: false, error: msg });
    }
  });

  return r;
}
