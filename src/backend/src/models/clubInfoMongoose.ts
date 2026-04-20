/**
 * Mongoose model for MongoDB collection `clubInfo`.
 *
 * Legacy `ClubInfo.json` may include `club_payment_支付寶` alongside Alipay; this schema
 * keeps a single canonical field `club_payment_alipay` (migrate 支付寶 → alipay on import).
 */
import mongoose, { Schema, type Model } from "mongoose";

/** ISO 4217-style codes from `country` when `Currency` is omitted. */
export function defaultCurrencyForCountry(country: string | undefined | null): string {
  const n = String(country ?? "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ");
  if (!n) {
    return "HKD";
  }
  if (n === "hong kong" || n.includes("hong kong")) {
    return "HKD";
  }
  if (n === "china" || n === "people's republic of china" || n === "prc") {
    return "CNY";
  }
  if (n === "japan") {
    return "JPY";
  }
  if (n === "uk" || n === "united kingdom" || n === "great britain" || n === "england") {
    return "GBP";
  }
  if (n === "taiwan" || n === "republic of china") {
    return "TWD";
  }
  if (n === "singapore") {
    return "SGD";
  }
  return "HKD";
}

export interface IClubInfo {
  club_id: string;
  Currency: string;
  Sport_type: string;
  Club_name: string;
  country: string;
  setup_date: Date;
  club_desc: string;
  club_logo: string;
  club_payment_payme: string;
  club_payment_FPS: string;
  club_payment_wechat: string;
  club_payment_alipay: string;
  lastUpdate_date: Date;
}

const iso4217 = /^[A-Z]{3}$/;

const clubInfoSchema = new Schema<IClubInfo>(
  {
    club_id: {
      type: String,
      required: [true, "club_id is required"],
      trim: true,
      minlength: [4, "club_id is too short"],
      maxlength: [64, "club_id is too long"],
    },
    Currency: {
      type: String,
      trim: true,
      uppercase: true,
      match: [iso4217, "Currency must be a 3-letter ISO 4217 code (e.g. HKD)"],
    },
    Sport_type: {
      type: String,
      required: [true, "Sport_type is required"],
      trim: true,
      maxlength: [120, "Sport_type is too long"],
    },
    Club_name: {
      type: String,
      required: [true, "Club_name is required"],
      trim: true,
      maxlength: [200, "Club_name is too long"],
    },
    country: {
      type: String,
      required: [true, "country is required"],
      trim: true,
      maxlength: [120, "country is too long"],
    },
    setup_date: {
      type: Date,
      required: [true, "setup_date is required"],
    },
    club_desc: {
      type: String,
      required: [true, "club_desc is required"],
      trim: true,
      maxlength: [4000, "club_desc is too long"],
    },
    club_logo: {
      type: String,
      required: [true, "club_logo is required"],
      trim: true,
      maxlength: [512, "club_logo path is too long"],
    },
    club_payment_payme: {
      type: String,
      default: "",
      trim: true,
      maxlength: [512, "club_payment_payme path is too long"],
    },
    club_payment_FPS: {
      type: String,
      default: "",
      trim: true,
      maxlength: [512, "club_payment_FPS path is too long"],
    },
    club_payment_wechat: {
      type: String,
      default: "",
      trim: true,
      maxlength: [512, "club_payment_wechat path is too long"],
    },
    club_payment_alipay: {
      type: String,
      default: "",
      trim: true,
      maxlength: [512, "club_payment_alipay path is too long"],
    },
  },
  {
    collection: "clubInfo",
    timestamps: { updatedAt: "lastUpdate_date", createdAt: false },
    strict: true,
  },
);

clubInfoSchema.pre("validate", function (next) {
  const doc = this as mongoose.Document & Partial<IClubInfo>;
  const cur = doc.Currency;
  if (cur == null || String(cur).trim() === "") {
    doc.Currency = defaultCurrencyForCountry(doc.country);
  }
  if (doc.Currency) {
    doc.Currency = String(doc.Currency).trim().toUpperCase();
  }
  next();
});

clubInfoSchema.index({ club_id: 1 }, { unique: true });
clubInfoSchema.index({ country: 1, Club_name: 1 });
clubInfoSchema.index({ lastUpdate_date: -1 });

export type ClubInfoMongooseModel = Model<IClubInfo>;

/** Reuses compiled model in dev (e.g. tsx watch) to avoid OverwriteModelError. */
export const ClubInfo: ClubInfoMongooseModel =
  (mongoose.models.ClubInfo as ClubInfoMongooseModel | undefined) ??
  mongoose.model<IClubInfo>("ClubInfo", clubInfoSchema);
