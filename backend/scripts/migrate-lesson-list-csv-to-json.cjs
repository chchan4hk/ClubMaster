/* One-time helper: each club folder LessonList.csv becomes LessonList.json (camelCase).
   Renames CSV to LessonList.csv.bak. */
const fs = require("fs");
const path = require("path");

const clubsRoot = path.join(__dirname, "..", "data_club");

function parseCsvLine(line) {
  const out = [];
  let cur = "";
  let inQ = false;
  for (let i = 0; i < line.length; i++) {
    const c = line[i];
    if (inQ) {
      if (c === '"') {
        if (line[i + 1] === '"') {
          cur += '"';
          i++;
        } else {
          inQ = false;
        }
      } else {
        cur += c;
      }
    } else if (c === '"') {
      inQ = true;
    } else if (c === ",") {
      out.push(cur);
      cur = "";
    } else {
      cur += c;
    }
  }
  out.push(cur);
  return out;
}

function rowFromCsvLine(headers, cells) {
  const m = {};
  headers.forEach((h, j) => {
    m[String(h).replace(/^\uFEFF/, "").trim()] = String(cells[j] ?? "").trim();
  });
  const id = m.LessonID || "";
  if (!id) {
    return null;
  }
  return {
    lessonId: id,
    sportType: m.SportType || "",
    year: m.Year || "",
    classId: m.class_id || "",
    classInfo: m.class_info || "",
    classTime: m.class_time || "",
    classFee: m.class_fee || "",
    classSun: m.class_sun || "N",
    classMon: m.class_mon || "N",
    classTue: m.class_tue || "N",
    classWed: m.class_wed || "N",
    classThur: m.class_thur || "N",
    classFri: m.class_fri || "N",
    classSat: m.class_sat || "N",
    ageGroup: m.Age_group || "",
    maxNumber: m.max_number || "",
    frequency: m.Frequency || "",
    lessonStartDate: m.lesson_start_date || "",
    lessonEndDate: m.lesson_end_date || "",
    sportCenter: m.Sport_center || "",
    courtNo: m.court_no || "",
    coachName: m["Coach Name"] || "",
    status: m.status || "ACTIVE",
    createdAt: m.Created_at || "",
    lastUpdatedDate: m.LastUpdated_Date || "",
    remarks: m.Remarks || "",
  };
}

function lessonsFromCsvText(text) {
  const lines = text.split(/\r?\n/).filter((l) => l.trim());
  if (lines.length < 2) {
    return [];
  }
  const headers = parseCsvLine(lines[0].replace(/^\uFEFF/, ""));
  const out = [];
  for (let i = 1; i < lines.length; i++) {
    const cells = parseCsvLine(lines[i]);
    const row = rowFromCsvLine(headers, cells);
    if (row) {
      out.push(row);
    }
  }
  return out;
}

function main() {
  if (!fs.existsSync(clubsRoot)) {
    console.error("Missing", clubsRoot);
    process.exit(1);
  }
  for (const name of fs.readdirSync(clubsRoot)) {
    const dir = path.join(clubsRoot, name);
    if (!fs.statSync(dir).isDirectory()) {
      continue;
    }
    const pCsv = path.join(dir, "LessonList.csv");
    const pJson = path.join(dir, "LessonList.json");
    if (fs.existsSync(pJson)) {
      continue;
    }
    if (fs.existsSync(pCsv)) {
      const lessons = lessonsFromCsvText(fs.readFileSync(pCsv, "utf8"));
      fs.writeFileSync(
        pJson,
        `${JSON.stringify({ version: 1, lessons }, null, 2)}\n`,
        "utf8",
      );
      fs.renameSync(pCsv, `${pCsv}.bak`);
      console.log("Migrated", name, lessons.length, "lessons");
    } else {
      fs.writeFileSync(
        pJson,
        `${JSON.stringify({ version: 1, lessons: [] }, null, 2)}\n`,
        "utf8",
      );
      console.log("Created empty", name);
    }
  }
}

main();
