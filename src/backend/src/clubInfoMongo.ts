import type {
  ClubInfoDocument,
  ClubInfoInsert,
} from "./db/DBConnection";
import { ensureClubInfoCollection, getClubInfoCollection } from "./db/DBConnection";
import {
  CLUB_INFO_FIELD_ORDER_LABEL,
  CLUB_PAYMENT_QR_JSON_KEYS,
  type ClubInfoRaw,
  type ClubPaymentQrChannel,
  todaySlashYmd,
} from "./clubInfoJson";

function pickField(
  body: Record<string, unknown>,
  keys: string[],
  fallback: string,
): string {
  for (const k of keys) {
    if (Object.prototype.hasOwnProperty.call(body, k)) {
      const v = body[k];
      return v == null ? "" : String(v).trim();
    }
  }
  return fallback;
}

const CLUB_INFO_PAYMENT_PATCH_KEYS = [
  "club_payment_payme",
  "club_payment_FPS",
  "club_payment_fps",
  "club_payment_wechat",
  "club_payment_alipay",
  "club_payment_支付寶",
] as const;

function defaultClubInfoInsert(
  clubId: string,
  clubDisplayName: string,
): ClubInfoInsert {
  const today = todaySlashYmd();
  return {
    club_id: clubId.trim(),
    Currency: "HKD",
    Sport_type: "",
    Club_name: clubDisplayName.trim(),
    country: "",
    setup_date: "",
    club_desc: "",
    club_logo: "",
    club_payment_payme: "",
    club_payment_FPS: "",
    club_payment_wechat: "",
    club_payment_alipay: "",
    club_payment_支付寶: "",
    lastUpdate_date: today,
  };
}

/** Ensure a `clubInfo` row exists for this club folder id (Coach Manager UID). */
export async function getOrCreateClubInfoDocument(
  clubId: string,
  defaultClubDisplayName: string,
): Promise<ClubInfoDocument> {
  await ensureClubInfoCollection();
  const col = await getClubInfoCollection();
  const id = clubId.trim();
  const onInsert = defaultClubInfoInsert(id, defaultClubDisplayName);
  await col.updateOne({ club_id: id }, { $setOnInsert: onInsert }, { upsert: true });
  const doc = await col.findOne({ club_id: id });
  if (!doc) {
    throw new Error("clubInfo: could not read document after upsert.");
  }
  return doc as ClubInfoDocument;
}

/** Coach Master form + API `fields` (tabular keys + payment path keys). */
export function clubInfoDocumentToCoachFields(
  doc: ClubInfoDocument,
): Record<string, string> {
  const o: Record<string, string> = {
    Sport_type: String(doc.Sport_type ?? ""),
    Club_name: String(doc.Club_name ?? ""),
    country: String(doc.country ?? ""),
    setup_date: String(doc.setup_date ?? ""),
    club_desc: String(doc.club_desc ?? ""),
    club_logo: String(doc.club_logo ?? ""),
    lastUpdate_date: String(doc.lastUpdate_date ?? ""),
    Currency: String(doc.Currency ?? ""),
  };
  const d = doc as unknown as Record<string, unknown>;
  for (const ch of Object.keys(CLUB_PAYMENT_QR_JSON_KEYS) as ClubPaymentQrChannel[]) {
    const key = CLUB_PAYMENT_QR_JSON_KEYS[ch];
    o[key] = String(d[key] ?? "").trim();
  }
  return o;
}

/** Same shape as legacy `loadClubInfoRaw` (single logical row). */
export function clubInfoDocumentToRaw(doc: ClubInfoDocument): ClubInfoRaw {
  const headers = [
    "Sport_type",
    "Club_name",
    "country",
    "setup_date",
    "club_desc",
    "club_logo",
    "lastUpdate_date",
  ];
  const d = doc as unknown as Record<string, string>;
  const row = headers.map((h) => String(d[h] ?? ""));
  return {
    headers,
    headerLine: CLUB_INFO_FIELD_ORDER_LABEL,
    rows: [row],
  };
}

/**
 * Merge editable fields from the request body into Mongo `clubInfo`
 * (same field rules as `writeClubInfoFromPatch` for JSON).
 */
export async function updateClubInfoFromBodyPatch(
  clubId: string,
  defaultClubDisplayName: string,
  body: Record<string, unknown>,
  lastUpdateDate: string,
): Promise<ClubInfoDocument> {
  const cur = await getOrCreateClubInfoDocument(
    clubId,
    defaultClubDisplayName,
  );
  const can = clubInfoDocumentToCoachFields(cur);

  const sport = pickField(
    body,
    ["SportType", "Sport_type", "sport_type"],
    can.Sport_type,
  );
  const name = pickField(body, ["Club_name", "club_name"], can.Club_name);
  const country = pickField(body, ["country", "Country"], can.country);
  const setup = pickField(body, ["setup_date", "setupDate"], can.setup_date);
  const desc = pickField(body, ["club_desc", "clubDesc"], can.club_desc);
  const logo = pickField(body, ["club_logo", "clubLogo"], can.club_logo);
  const currency = pickField(body, ["Currency", "currency"], can.Currency);

  const $set: Record<string, string> = {
    Sport_type: sport,
    Club_name: name,
    country,
    setup_date: setup,
    club_desc: desc,
    club_logo: logo,
    Currency: (currency || cur.Currency || "HKD").trim() || "HKD",
    lastUpdate_date: lastUpdateDate,
  };

  for (const k of CLUB_INFO_PAYMENT_PATCH_KEYS) {
    if (Object.prototype.hasOwnProperty.call(body, k)) {
      const v = body[k];
      const key =
        k === "club_payment_fps" ? "club_payment_FPS" : k;
      $set[key] = v == null ? "" : String(v).trim();
    }
  }

  const col = await getClubInfoCollection();
  const id = clubId.trim();
  await col.updateOne({ club_id: id }, { $set });
  const next = await col.findOne({ club_id: id });
  if (!next) {
    throw new Error("clubInfo: document missing after update.");
  }
  return next as ClubInfoDocument;
}

/** Partial update (e.g. logo or one payment path) without touching other columns unnecessarily. */
export async function patchClubInfoFields(
  clubId: string,
  defaultClubDisplayName: string,
  patch: Record<string, string>,
  lastUpdateDate: string,
): Promise<ClubInfoDocument> {
  await getOrCreateClubInfoDocument(clubId, defaultClubDisplayName);
  const col = await getClubInfoCollection();
  const id = clubId.trim();
  const $set: Record<string, string> = { ...patch, lastUpdate_date: lastUpdateDate };
  await col.updateOne({ club_id: id }, { $set });
  const next = await col.findOne({ club_id: id });
  if (!next) {
    throw new Error("clubInfo: document missing after patch.");
  }
  return next as ClubInfoDocument;
}
