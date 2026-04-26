import fs from "node:fs";
import path from "path";
import express from "express";
import cors from "cors";
import compression from "compression";
import cookieParser from "cookie-parser";
import { createAuthRouter } from "./routes/authRoutes";
import { createSubscriptionRenewalRouter } from "./routes/subscriptionRenewalRoutes";
import { createMockPaymentRouter } from "./routes/mockPaymentRoutes";
import {
  createUserAccountRouter,
  accountPayloadForUid,
  coachProfileFromUserLoginCoachCsv,
  studentProfileFromUserLoginStudentCsv,
} from "./routes/userAccountRoutes";
import {
  ensureBasicInfoCollection,
  ensureClubInfoCollection,
  ensureCoachSalaryCollection,
  ensureLessonListCollection,
  ensurePaymentListCollection,
  ensureLessonSeriesInfoCollection,
  ensureLessonReserveListCollection,
  ensureLessonPaymentLedgerCollection,
  ensurePrizeListRowCollection,
  ensureUserLoginCollection,
  getClubInfoCollection,
  isMongoConfigured,
} from "./db/DBConnection";
import { loadMeProfileFromUserLoginMongo } from "./userLoginCollectionMongo";
import {
  readCoachManagerProfileContactMongo,
  userLoginCsvReadFallbackEnabled,
} from "./userListMongo";
import { loadClubInfoContactFieldsMongo } from "./clubInfoMongo";
import { requireAuth } from "./middleware/requireAuth";
import { ensureCoachStudentLoginFilesExist } from "./coachStudentLoginCsv";
import {
  ensureUserlistFileExists,
  ensureUserlistSchema,
  findCoachManagerClubUidByClubName,
} from "./userlistCsv";
import { createAdminRouter } from "./routes/adminRoutes";
import { createCoachManagerCoachRouter } from "./routes/coachManagerCoachRoutes";
import { createCoachManagerPrizeRouter } from "./routes/coachManagerPrizeRoutes";
import { createCoachManagerStudentRouter } from "./routes/coachManagerStudentRoutes";
import { createCoachManagerClubInfoRouter } from "./routes/coachManagerClubInfoRoutes";
import { createCoachManagerSportCenterRouter } from "./routes/coachManagerSportCenterRoutes";
import { createCoachManagerLessonRouter } from "./routes/coachManagerLessonRoutes";
import { createCoachSalaryPaymentRouter } from "./routes/coachSalaryPaymentRoutes";
import { Lesson_payment_status } from "./payment_modules/Lesson_payment_status";
import { createUserLoginPaymentStatusRouter } from "./payment_modules/UserLogin_payment_status";
import { Student_payment } from "./payment_modules/Student_payment";
import { createBasicInfoRouter } from "./routes/basicInfoRoutes";
import {
  coachLoginUidMatchesRosterCoachId,
  isValidClubFolderId,
} from "./coachListCsv";
import {
  findClubUidForCoachIdPreferred,
  loadCoachesPreferred,
} from "./coachListMongo";
import {
  rebuildStudentIdClubIndex,
  resolveStudentClubSession,
  type StudentClubSessionResult,
} from "./studentListCsv";
import { ensureUserListStudentIndexes } from "./studentListMongo";
import { getDataFileCacheStats } from "./dataFileCache";
import { getRdsPoolStats } from "./db/rdsPostgres";
import {
  accessRequestLogger,
  shouldEnableAccessLog,
  slowRequestLogger,
  startProductionMemoryLogging,
} from "./middleware/performanceLogging";
import {
  assertRequiredProductionEnv,
  loadLocalEnvFile,
  resolveListenPort,
} from "./config/env";

/** Backend package root (`backend/`: contains `dist/`, `data/`, optional `.env`). */
const backendRoot = path.join(__dirname, "..");
loadLocalEnvFile(backendRoot);
assertRequiredProductionEnv();

const app = express();
const PORT = resolveListenPort();

/**
 * Static files (`main.html`, `js/`, …): defaults to the parent of `backend/` (monorepo layout).
 * On Zeabur / Docker when only `backend/` is deployed, set `SPORT_COACH_STATIC_ROOT` to that
 * folder (and place or copy the web root there), or to a path that contains `main.html`.
 */
function resolveStaticRoot(): string {
  const fromEnv = process.env.SPORT_COACH_STATIC_ROOT?.trim();
  if (fromEnv) {
    return path.isAbsolute(fromEnv)
      ? fromEnv
      : path.resolve(process.cwd(), fromEnv);
  }
  return path.join(backendRoot, "..");
}

const staticRoot = resolveStaticRoot();
const sourceImageStatic = path.join(staticRoot, "source", "image");
const dataClubStatic = path.join(backendRoot, "data_club");
const adminDataStatic = path.join(backendRoot, "data", "admin");
const adminUiStatic = path.join(backendRoot, "admin");

ensureUserlistFileExists();
ensureUserlistSchema();
ensureCoachStudentLoginFilesExist();

console.log(
  "[startup] StudentID / CoachID / LessonID / PrizeID indexes are built lazily on first use (avoids scanning every data_club folder at boot).",
);

startProductionMemoryLogging();

app.use(
  cors({
    origin: (_origin, callback) => callback(null, true),
    credentials: true,
  })
);
app.use(
  compression({
    threshold: 1024,
    filter: (req, res) => {
      if (req.headers["x-no-compression"]) {
        return false;
      }
      return compression.filter(req, res);
    },
  }),
);
app.use(express.json());
app.use(cookieParser());
if (shouldEnableAccessLog()) {
  app.use(accessRequestLogger);
}
app.use(slowRequestLogger);

const isProd = process.env.NODE_ENV === "production";
const STATIC_MAX_AGE_MS = 86400000;
/** WebP/JPEG overview art: long cache + ETag (mounted before generic static). */
const SOURCE_IMAGE_CACHE_MS = isProd
  ? 7 * 24 * 60 * 60 * 1000
  : 60 * 60 * 1000;

/** Homepage: serve login UI (avoid redirect — some proxies/CDNs mishandle `/` → `/main.html`). */
app.get("/", (_req, res) => {
  const mainHtml = path.join(staticRoot, "main.html");
  if (fs.existsSync(mainHtml)) {
    res.sendFile(mainHtml);
    return;
  }
  res.type("text/plain").send("Welcome to ClubMaster");
});

app.get("/student/payment", (_req, res) => {
  res.sendFile(
    path.join(
      backendRoot,
      "payment_modules",
      "student_payment",
      "student-payment.html",
    ),
  );
});

/** Club assets & JSON — short cache in production to avoid stale API-backed files. */
app.use(
  "/backend/data_club",
  express.static(dataClubStatic, isProd ? { maxAge: 60_000, etag: true } : {}),
);
app.use(
  "/backend/data/admin",
  express.static(adminDataStatic, isProd ? { maxAge: 60_000, etag: true } : {}),
);
/** Admin HTML modules (e.g. Sport Activation). */
app.use(
  "/backend/admin",
  express.static(adminUiStatic, isProd ? { maxAge: STATIC_MAX_AGE_MS, etag: true } : {}),
);

app.get("/api/health", (_req, res) => {
  const mem = process.memoryUsage();
  res.json({
    ok: true,
    uptimeSec: Math.floor(process.uptime()),
    env: process.env.NODE_ENV ?? "development",
    memory: {
      rss: mem.rss,
      heapUsed: mem.heapUsed,
      heapTotal: mem.heapTotal,
      external: mem.external,
    },
    dataFileCache: getDataFileCacheStats(),
    rdsPool: getRdsPoolStats(),
  });
});

app.use("/api/basic-info", createBasicInfoRouter());

app.use("/api/auth", createAuthRouter());
app.use("/api/subscription", createSubscriptionRenewalRouter());
app.use("/api/payments/mock", createMockPaymentRouter());

app.get("/api/me", requireAuth, async (req, res) => {
  const uid = req.user?.sub;
  let fromCsv: ReturnType<typeof accountPayloadForUid> | null = null;
  let studentLogin: ReturnType<
    typeof studentProfileFromUserLoginStudentCsv
  > | null = null;
  let coachLogin: ReturnType<typeof coachProfileFromUserLoginCoachCsv> | null =
    null;

  if (isMongoConfigured() && uid != null) {
    try {
      const mongoMe = await loadMeProfileFromUserLoginMongo(
        String(uid),
        req.user?.role,
        {
          username: String(req.user?.username ?? "").trim(),
          clubFolderUid: String(req.user?.club_folder_uid ?? "").trim(),
        },
      );
      if (mongoMe) {
        if (mongoMe.fromCsv) {
          fromCsv = mongoMe.fromCsv;
        }
        if (mongoMe.coachLogin) {
          coachLogin = mongoMe.coachLogin;
        }
        if (mongoMe.studentLogin) {
          studentLogin = mongoMe.studentLogin;
        }
      }
    } catch {
      /* fall through to file-backed profile */
    }
  }
  if (
    !fromCsv &&
    uid != null &&
    (!isMongoConfigured() || userLoginCsvReadFallbackEnabled())
  ) {
    fromCsv = accountPayloadForUid(uid);
  }
  const allowFileRoleProfiles =
    !isMongoConfigured() || userLoginCsvReadFallbackEnabled();
  if (
    !studentLogin &&
    req.user?.role === "Student" &&
    uid != null &&
    allowFileRoleProfiles
  ) {
    studentLogin = studentProfileFromUserLoginStudentCsv(uid);
  }
  if (
    !coachLogin &&
    req.user?.role === "Coach" &&
    uid != null &&
    allowFileRoleProfiles
  ) {
    coachLogin = coachProfileFromUserLoginCoachCsv(uid);
  }
  const clubNameForFolder =
    coachLogin?.club_name ??
    studentLogin?.club_name ??
    fromCsv?.club_name ??
    "—";
  const clubNameTrimmed =
    clubNameForFolder &&
    String(clubNameForFolder).trim() !== "" &&
    String(clubNameForFolder).trim() !== "—"
      ? String(clubNameForFolder).trim()
      : "";

  let club_folder_uid: string | null = null;
  if (req.user?.role === "CoachManager" && uid != null) {
    const s = String(uid).trim();
    club_folder_uid = s || null;
  } else if (req.user?.role === "Coach" && uid != null) {
    const fromLoginCoach = (coachLogin?.club_folder_uid ?? "").trim();
    if (fromLoginCoach && isValidClubFolderId(fromLoginCoach)) {
      club_folder_uid = fromLoginCoach;
    } else {
      const fromJwt = String(req.user?.club_folder_uid ?? "").trim();
      if (fromJwt && isValidClubFolderId(fromJwt)) {
        club_folder_uid = fromJwt;
      } else if (clubNameTrimmed) {
        club_folder_uid = findCoachManagerClubUidByClubName(clubNameTrimmed);
      }
    }
    if (!club_folder_uid) {
      const fromRoster = await findClubUidForCoachIdPreferred(
        String(uid).trim(),
      );
      if (fromRoster && isValidClubFolderId(fromRoster)) {
        club_folder_uid = fromRoster;
      }
    }
  } else if (req.user?.role === "Student" && uid != null) {
    const fromLoginStu = (studentLogin?.club_folder_uid ?? "").trim();
    if (fromLoginStu && isValidClubFolderId(fromLoginStu)) {
      club_folder_uid = fromLoginStu;
    } else {
      const fromJwt = String(req.user?.club_folder_uid ?? "").trim();
      if (fromJwt && isValidClubFolderId(fromJwt)) {
        club_folder_uid = fromJwt;
      } else if (clubNameTrimmed) {
        club_folder_uid = findCoachManagerClubUidByClubName(clubNameTrimmed);
      } else {
        const sess = await resolveStudentClubSession(String(uid).trim());
        club_folder_uid = sess.ok ? sess.clubId : null;
      }
    }
  }

  const folderUidForClubAssets = (() => {
    const fromFolder = club_folder_uid && String(club_folder_uid).trim();
    if (fromFolder && isValidClubFolderId(fromFolder)) {
      return fromFolder;
    }
    if (req.user?.role === "CoachManager" && uid != null) {
      const s = String(uid).trim();
      return isValidClubFolderId(s) ? s : "";
    }
    return "";
  })();

  const club_logo_jpg_url_path =
    folderUidForClubAssets &&
    isValidClubFolderId(folderUidForClubAssets)
      ? `/backend/data_club/${encodeURIComponent(folderUidForClubAssets)}/Image/club_logo.jpg`
      : null;

  const fullNameForMe = (() => {
    const good = (v: string | undefined) => {
      if (v == null) {
        return "";
      }
      const s = String(v).trim();
      return s !== "" && s !== "—" ? s : "";
    };
    if (req.user?.role === "Student") {
      return good(studentLogin?.full_name) || good(fromCsv?.full_name) || "—";
    }
    if (req.user?.role === "Coach") {
      return good(coachLogin?.full_name) || good(fromCsv?.full_name) || "—";
    }
    return good(fromCsv?.full_name) || "—";
  })();

  let studentCoachFromRoster = "";
  let studentClubSession: StudentClubSessionResult | null = null;
  if (req.user?.role === "Student" && uid != null) {
    const sid = String(uid).trim();
    studentClubSession = await resolveStudentClubSession(sid);
    if (
      studentClubSession.ok &&
      studentClubSession.rosterRow?.studentCoach?.trim()
    ) {
      studentCoachFromRoster = studentClubSession.rosterRow.studentCoach.trim();
    }
  }

  let profileContactNumber = "—";
  let profileEmailAddress = "—";
  let studentDateOfBirth = "—";
  let studentSchool = "—";
  let studentHomeAddress = "—";
  let coachDateOfBirth = "—";
  let coachHomeAddress = "—";
  if (
    req.user?.role === "Coach" &&
    uid != null &&
    club_folder_uid &&
    isValidClubFolderId(club_folder_uid)
  ) {
    try {
      const coaches = await loadCoachesPreferred(club_folder_uid);
      const crow = coaches.find((c) =>
        coachLoginUidMatchesRosterCoachId(
          club_folder_uid,
          c.coachId,
          String(uid),
        ),
      );
      if (crow) {
        const em = String(crow.email ?? "").trim();
        const ph = String(crow.phone ?? "").trim();
        profileEmailAddress = em || "—";
        profileContactNumber = ph || "—";
        const dob = String(crow.dateOfBirth ?? "").trim();
        const home = String(crow.homeAddress ?? "").trim();
        coachDateOfBirth = dob || "—";
        coachHomeAddress = home || "—";
      }
    } catch {
      /* ignore roster read errors for profile extras */
    }
  }
  if (
    req.user?.role === "Student" &&
    studentClubSession?.ok &&
    studentClubSession.rosterRow
  ) {
    const em = String(studentClubSession.rosterRow.email ?? "").trim();
    const ph = String(studentClubSession.rosterRow.phone ?? "").trim();
    profileEmailAddress = em || "—";
    profileContactNumber = ph || "—";
    const dob = String(studentClubSession.rosterRow.dateOfBirth ?? "").trim();
    const school = String(studentClubSession.rosterRow.school ?? "").trim();
    const home = String(studentClubSession.rosterRow.homeAddress ?? "").trim();
    studentDateOfBirth = dob || "—";
    studentSchool = school || "—";
    studentHomeAddress = home || "—";
  }

  if (req.user?.role === "CoachManager" && isMongoConfigured() && uid != null) {
    try {
      /**
       * Coach Manager profile contact fields come from `clubInfo` (club settings),
       * keyed by club_ID / club folder uid.
       */
      const clubId = String(club_folder_uid ?? "").trim();
      const clubInfoContact = clubId
        ? await loadClubInfoContactFieldsMongo(clubId)
        : null;
      if (clubInfoContact) {
        profileEmailAddress = clubInfoContact.contact_email || "—";
        profileContactNumber = clubInfoContact.contact_point || "—";
      } else {
        /** Fallback for older deployments: read from `userLogin` if present. */
        const c = await readCoachManagerProfileContactMongo(
          String(uid),
          String(req.user?.username ?? "").trim(),
        );
        if (c) {
          profileEmailAddress = c.email_address ? c.email_address : "—";
          profileContactNumber = c.contact_number ? c.contact_number : "—";
        }
      }
    } catch {
      /* ignore Coach Manager profile extras read errors */
    }
  }

  let club_theme: string | null = null;
  if (isMongoConfigured() && folderUidForClubAssets) {
    try {
      const clubInfoCol = await getClubInfoCollection();
      const doc = await clubInfoCol.findOne({ club_id: folderUidForClubAssets });
      const t = doc?.club_theme != null ? String(doc.club_theme).trim() : "";
      club_theme = t || null;
    } catch {
      /* ignore clubInfo read errors */
    }
  }

  res.json({
    ok: true,
    user: {
      uid: req.user?.sub,
      username: req.user?.username,
      full_name: fullNameForMe,
      role: req.user?.role,
      club_folder_uid,
      /** Site-relative URL to `backend/data_club/{ClubUID}/Image/club_logo.jpg` for left-panel logo. */
      club_logo_jpg_url_path,
      usertype:
        req.user?.role === "Coach"
          ? "Coach"
          : req.user?.role === "Student"
            ? "Student"
            : (() => {
                const u = fromCsv?.usertype ?? req.user?.usertype;
                if (
                  u != null &&
                  String(u).trim().toLowerCase() === "student"
                ) {
                  return "Student";
                }
                return u;
              })(),
      status: fromCsv?.status ?? "ACTIVE",
      club_name: clubNameForFolder,
      contact_number: profileContactNumber,
      email_address: profileEmailAddress,
      club_photo: fromCsv?.club_photo ?? "",
      club_photo_url: fromCsv?.club_photo_url ?? null,
      club_theme,
      creation_date:
        coachLogin?.creation_date ??
        studentLogin?.creation_date ??
        fromCsv?.creation_date ??
        "—",
      expiry_date:
        coachLogin?.expiry_date ??
        studentLogin?.expiry_date ??
        fromCsv?.expiry_date ??
        "—",
      ...(req.user?.role === "Student"
        ? {
            student_coach: studentCoachFromRoster,
            date_of_birth: studentDateOfBirth,
            school: studentSchool,
            home_address: studentHomeAddress,
          }
        : {}),
      ...(req.user?.role === "Coach"
        ? {
            date_of_birth: coachDateOfBirth,
            home_address: coachHomeAddress,
          }
        : {}),
    },
  });
});

app.use("/api/me", createUserAccountRouter());
app.use("/api/admin", createAdminRouter());
app.use("/api/coach-manager/coaches", createCoachManagerCoachRouter());
app.use("/api/coach-manager/prizes", createCoachManagerPrizeRouter());
app.use("/api/coach-manager/students", createCoachManagerStudentRouter());
const coachManagerClubInfoRouter = createCoachManagerClubInfoRouter();
app.use("/api/coach-manager/club-info", coachManagerClubInfoRouter);
/** Alias (no hyphen) — avoids proxies or clients that mishandle hyphenated paths. */
app.use("/api/coach-manager/clubinfo", coachManagerClubInfoRouter);
app.use("/api/coach-manager/sport-centers", createCoachManagerSportCenterRouter());
app.use("/api/coach-manager/lessons", createCoachManagerLessonRouter());
const lessonPaymentStatusRouter = Lesson_payment_status();
app.use("/api/coach-manager/lesson-payment-status", lessonPaymentStatusRouter);
/** No-hyphen alias — same idea as /clubinfo; some proxies or hosts mishandle hyphenated segments. */
app.use("/api/coach-manager/lessonpaymentstatus", lessonPaymentStatusRouter);
const userLoginPaymentStatusRouter = createUserLoginPaymentStatusRouter();
app.use("/api/admin/userlogin-payment-status", userLoginPaymentStatusRouter);
app.use("/api/admin/userloginpaymentstatus", userLoginPaymentStatusRouter);
app.use("/api/student/payment", Student_payment());
app.use("/api/coach/salary-payment", createCoachSalaryPaymentRouter());

/**
 * Web UI (main.html, js/, …). Mounted after /api routes so paths like `/api/coach-manager/coachsalary`
 * are never shadowed by a static file under the web root.
 */
app.use(
  "/source/image",
  express.static(sourceImageStatic, {
    maxAge: SOURCE_IMAGE_CACHE_MS,
    etag: true,
  }),
);
app.use(
  express.static(staticRoot, isProd ? { maxAge: STATIC_MAX_AGE_MS, etag: true } : {}),
);

app.use("/api", (req, res) => {
  res.status(404).json({
    ok: false,
    error: "API route not found.",
    method: req.method,
    path: req.originalUrl,
  });
});

const server = app.listen(PORT, "0.0.0.0", () => {
  const envLabel = process.env.NODE_ENV === "production" ? "production" : "development";
  console.log(`Server listening on ${PORT} (${envLabel}).`);
  console.log(`  Login: /main.html (same origin as this server)`);
  console.log(`Static root: ${staticRoot}`);
  console.log(`Backend root: ${backendRoot}`);
  if (!shouldEnableAccessLog()) {
    console.log(
      "  Logs: slow requests >= " +
        (process.env.SLOW_REQUEST_LOG_MS || "200") +
        "ms · Set ACCESS_LOG=1 for every request line · PERF_MEMORY_LOG=0 to disable 60s RSS logs.",
    );
  }
  if (isMongoConfigured()) {
    void Promise.all([
      ensureUserLoginCollection(),
      ensureBasicInfoCollection(),
      ensureClubInfoCollection(),
      ensureLessonListCollection(),
      ensurePaymentListCollection(),
      ensureLessonSeriesInfoCollection(),
      ensureLessonReserveListCollection(),
      ensureLessonPaymentLedgerCollection(),
      ensurePrizeListRowCollection(),
      ensureCoachSalaryCollection(),
      ensureUserListStudentIndexes(),
      rebuildStudentIdClubIndex(),
    ])
      .then(() => {
        console.log(
          "MongoDB: `userLogin`, `basicInfo`, `clubInfo`, `LessonList`, `PaymentList`, `LessonSeriesInfo`, `LessonReserveList`, `LessonPaymentLedger`, `PrizeList`, `CoachManager`, and `UserList_Student` indexes/roster index warmed (see src/backend/src/db/DBConnection.ts).",
        );
      })
      .catch((e) => {
        const msg = e instanceof Error ? e.message : String(e);
        console.warn(
          "MongoDB collection init (userLogin / basicInfo / clubInfo / LessonList / PaymentList / LessonSeriesInfo / LessonReserveList / LessonPaymentLedger / PrizeList / CoachManager / UserList_Student):",
          msg,
        );
      });
  }
});

server.on("error", (err: NodeJS.ErrnoException) => {
  if (err.code === "EADDRINUSE") {
    console.error(
      `\nPort ${PORT} is already in use — another process is listening.\n` +
        `  • Stop the other server, or\n` +
        `  • Set the PORT environment variable to a free port (local dev: optional backend/.env when not in production).\n`
    );
    process.exit(1);
  }
  throw err;
});
