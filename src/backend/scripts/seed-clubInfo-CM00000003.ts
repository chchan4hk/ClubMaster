/**
 * Upserts `data_club/CM00000003/ClubInfo.json` into MongoDB collection `clubInfo`
 * (same shape as `clubInfoJsonSchema` in `src/db/DBConnection.ts`).
 */
import path from "path";
import { loadLocalEnvFile } from "../src/config/env";
import {
  closeMongoClient,
  ensureClubInfoCollection,
  getClubInfoCollection,
  isMongoConfigured,
  type ClubInfoInsert,
} from "../src/db/DBConnection";

const backendRoot = path.join(__dirname, "..");
loadLocalEnvFile(backendRoot);

const CLUB_ID = "CM00000003";

const doc: ClubInfoInsert = {
  club_id: CLUB_ID,
  Currency: "HKD",
  Sport_type: "Badminton",
  Club_name: "流星羽毛球會",
  country: "Hong Kong",
  setup_date: "2023/7/26",
  club_desc:
    "建立羽毛球聯誼交流平台, 增加波友之間的友誼, 將他們的羽毛球經驗和心得",
  club_logo: "Image/club_logo.jpg",
  club_payment_payme: "Image/payme_QR.jpg",
  club_payment_FPS: "Image/FPS_QR.jpg",
  club_payment_wechat: "Image/wechat_QR.jpg",
  club_payment_alipay: "Image/alipay_QR.jpg",
  lastUpdate_date: "2026/04/12",
  club_payment_支付寶: "Image/alipay_QR.jpg",
};

async function main(): Promise<void> {
  if (!isMongoConfigured()) {
    console.error(
      "MongoDB is not configured (set MONGODB_URI / MONGO_URI or MONGO_PASSWORD).",
    );
    process.exit(1);
  }
  await ensureClubInfoCollection();
  const col = await getClubInfoCollection();
  const r = await col.updateOne(
    { club_id: CLUB_ID },
    { $set: doc },
    { upsert: true },
  );
  console.log(
    `clubInfo upsert for ${CLUB_ID}: matched=${r.matchedCount} modified=${r.modifiedCount} upserted=${r.upsertedCount ?? 0}`,
  );
}

main()
  .catch((e) => {
    console.error(e);
    process.exit(1);
  })
  .finally(() => {
    void closeMongoClient();
  });
