/**
 * One-shot: ensure every club folder has UserList_Coach.json (migrates from CSV if present).
 * Run: npx tsx scripts/migrate-userlist-coach-csv-to-json.ts
 */
import fs from "fs";
import path from "path";
import {
  COACH_LIST_CSV_LEGACY,
  COACH_LIST_FILENAME,
  ensureCoachListFile,
  getDataClubRootPath,
  isValidClubFolderId,
  parseAllCoachesFromCsvFilePath,
  coachListPath,
  writeCoachListJsonAtPath,
} from "../src/coachListCsv.ts";

const root = getDataClubRootPath();
for (const name of fs.readdirSync(root)) {
  const full = path.join(root, name);
  if (!fs.statSync(full).isDirectory()) {
    continue;
  }
  if (isValidClubFolderId(name)) {
    ensureCoachListFile(name);
    console.log("OK club", name, "->", coachListPath(name));
    continue;
  }
  if (name === "Src") {
    const pJson = path.join(full, COACH_LIST_FILENAME);
    const pCsv = path.join(full, COACH_LIST_CSV_LEGACY);
    if (!fs.existsSync(pJson) && fs.existsSync(pCsv)) {
      const coaches = parseAllCoachesFromCsvFilePath(pCsv);
      writeCoachListJsonAtPath(pJson, coaches);
      fs.unlinkSync(pCsv);
      console.log("OK Src template ->", pJson);
    } else if (!fs.existsSync(pJson)) {
      writeCoachListJsonAtPath(pJson, []);
      console.log("OK Src empty ->", pJson);
    }
  }
}
