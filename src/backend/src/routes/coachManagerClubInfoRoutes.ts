import fs from "fs";
import path from "path";
import multer from "multer";
import { Router, type Request } from "express";
import { requireAuth, requireRole } from "../middleware/requireAuth";
import { coachManagerClubContextAsync } from "../coachManagerSession";
import {
  clubAssetPublicUrl,
  clubImageDir,
  CLUB_LOGO_FILENAME,
  clubLogoRelativePath,
  clubPaymentQrRelativePath,
  CLUB_PAYMENT_QR_JSON_KEYS,
  isClubPaymentQrChannel,
  todaySlashYmd,
  type ClubPaymentQrChannel,
} from "../clubInfoJson";
import {
  clubInfoDocumentToCoachFields,
  clubInfoDocumentToRaw,
  getOrCreateClubInfoDocument,
  patchClubInfoFields,
  updateClubInfoFromBodyPatch,
} from "../clubInfoMongo";
import { isMongoConfigured } from "../db/DBConnection";
import { clubDataDir } from "../coachListCsv";
import type { ClubInfoDocument } from "../db/DBConnection";

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
  const cleaned = String(rel ?? "").trim().replace(/\\/g, "/");
  if (!clubRoot || cleaned.includes("..")) {
    return "";
  }
  return path.normalize(
    path.join(clubRoot, ...cleaned.split("/").filter(Boolean)),
  );
}

function paymentQrPublicUrls(
  clubId: string,
  doc: ClubInfoDocument,
): Record<ClubPaymentQrChannel, string | null> {
  const ext = doc as unknown as Record<string, unknown>;
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

function mongoRequiredResponse(res: import("express").Response): void {
  res.status(503).json({
    ok: false,
    error:
      "MongoDB is not configured. Club Master stores club profile in the `clubInfo` collection; set MONGODB_URI / MONGO_URI or MONGO_PASSWORD.",
  });
}

export function createCoachManagerClubInfoRouter(): Router {
  const r = Router();

  r.use(requireAuth, requireRole("CoachManager"));

  r.get("/", async (req, res) => {
    const ctx = await coachManagerClubContextAsync(req);
    if (!ctx.ok) {
      res.status(ctx.status).json({ ok: false, error: ctx.error });
      return;
    }
    if (!isMongoConfigured()) {
      mongoRequiredResponse(res);
      return;
    }
    try {
      const doc = await getOrCreateClubInfoDocument(ctx.clubId, ctx.clubName);
      const fields = clubInfoDocumentToCoachFields(doc);
      const logoRel = clubLogoRelFromFields(fields);
      const clubRoot = clubDataDir(ctx.clubId);
      const logoRelClean = String(logoRel ?? "").trim().replace(/\\/g, "/");
      const logoDisk =
        logoRelClean && clubRoot && !logoRelClean.includes("..")
          ? path.normalize(
              path.join(clubRoot, ...logoRelClean.split("/").filter(Boolean)),
            )
          : "";
      const logoExists = Boolean(logoDisk && fs.existsSync(logoDisk));
      const club_logo_url =
        logoRelClean && logoExists
          ? clubAssetPublicUrl(ctx.clubId, logoRelClean)
          : null;
      const raw = clubInfoDocumentToRaw(doc);
      res.json({
        ok: true,
        clubId: ctx.clubId,
        clubName: ctx.clubName,
        storage: "mongodb",
        clubInfoCollection: "clubInfo",
        club_logo_url,
        paymentQrUrls: paymentQrPublicUrls(ctx.clubId, doc),
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

  r.post("/logo", logoUpload.single("logo"), async (req, res) => {
    const ctx = await coachManagerClubContextAsync(req);
    if (!ctx.ok) {
      res.status(ctx.status).json({ ok: false, error: ctx.error });
      return;
    }
    if (!isMongoConfigured()) {
      mongoRequiredResponse(res);
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
      const doc = await patchClubInfoFields(
        ctx.clubId,
        ctx.clubName,
        { club_logo: rel },
        lastUpdate,
      );
      const fields = clubInfoDocumentToCoachFields(doc);
      res.json({
        ok: true,
        clubId: ctx.clubId,
        lastUpdate_date: lastUpdate,
        club_logo: rel,
        club_logo_url: clubAssetPublicUrl(ctx.clubId, rel),
        paymentQrUrls: paymentQrPublicUrls(ctx.clubId, doc),
        fields,
      });
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      res.status(500).json({ ok: false, error: msg });
    }
  });

  r.post("/payment-qr", paymentQrUpload.single("qr"), async (req, res) => {
    const ctx = await coachManagerClubContextAsync(req);
    if (!ctx.ok) {
      res.status(ctx.status).json({ ok: false, error: ctx.error });
      return;
    }
    if (!isMongoConfigured()) {
      mongoRequiredResponse(res);
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
      const doc = await patchClubInfoFields(
        ctx.clubId,
        ctx.clubName,
        { [jsonKey]: rel },
        lastUpdate,
      );
      const fields = clubInfoDocumentToCoachFields(doc);
      res.json({
        ok: true,
        clubId: ctx.clubId,
        lastUpdate_date: lastUpdate,
        channel,
        relPath: rel,
        jsonKey,
        url: clubAssetPublicUrl(ctx.clubId, rel),
        paymentQrUrls: paymentQrPublicUrls(ctx.clubId, doc),
        fields,
      });
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      res.status(500).json({ ok: false, error: msg });
    }
  });

  r.put("/", async (req, res) => {
    const ctx = await coachManagerClubContextAsync(req);
    if (!ctx.ok) {
      res.status(ctx.status).json({ ok: false, error: ctx.error });
      return;
    }
    if (!isMongoConfigured()) {
      mongoRequiredResponse(res);
      return;
    }
    try {
      const body =
        req.body && typeof req.body === "object"
          ? (req.body as Record<string, unknown>)
          : {};
      const lastUpdate = todaySlashYmd();
      const doc = await updateClubInfoFromBodyPatch(
        ctx.clubId,
        ctx.clubName,
        body,
        lastUpdate,
      );
      const fields = clubInfoDocumentToCoachFields(doc);
      const logoRel = clubLogoRelFromFields(fields);
      const club_logo_url = logoRel
        ? clubAssetPublicUrl(ctx.clubId, logoRel)
        : null;
      res.json({
        ok: true,
        clubId: ctx.clubId,
        lastUpdate_date: lastUpdate,
        club_logo_url,
        paymentQrUrls: paymentQrPublicUrls(ctx.clubId, doc),
        fields,
      });
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      res.status(500).json({ ok: false, error: msg });
    }
  });

  return r;
}
