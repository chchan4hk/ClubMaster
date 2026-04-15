import path from "path";
import express from "express";
import cors from "cors";
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
import { requireAuth } from "./middleware/requireAuth";
import { ensureCoachStudentLoginFilesExist } from "./coachStudentLoginCsv";
import {
  ensureUserlistFileExists,
  ensureUserlistSchema,
  findCoachManagerClubUidByClubName,
  userlistPath,
} from "./userlistCsv";
import { createAdminRouter } from "./routes/adminRoutes";
import { createCoachManagerCoachRouter } from "./routes/coachManagerCoachRoutes";
import { createCoachManagerPrizeRouter } from "./routes/coachManagerPrizeRoutes";
import { createCoachManagerStudentRouter } from "./routes/coachManagerStudentRoutes";
import { createCoachManagerClubInfoRouter } from "./routes/coachManagerClubInfoRoutes";
import { createCoachManagerSportCenterRouter } from "./routes/coachManagerSportCenterRoutes";
import { createCoachManagerLessonRouter } from "./routes/coachManagerLessonRoutes";
import { Lesson_payment_status } from "./payment_modules/Lesson_payment_status";
import { createUserLoginPaymentStatusRouter } from "./payment_modules/UserLogin_payment_status";
import { Student_payment } from "./payment_modules/Student_payment";
import { createBasicInfoRouter } from "./routes/basicInfoRoutes";
import { isValidClubFolderId } from "./coachListCsv";
import {
  rebuildStudentIdClubIndex,
  resolveStudentClubSession,
} from "./studentListCsv";
import { getDataFileCacheStats } from "./dataFileCache";
import { getRdsPoolStats } from "./db/rdsPostgres";
import {
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
const dataClubStatic = path.join(backendRoot, "data_club");
const adminDataStatic = path.join(backendRoot, "data", "admin");

ensureUserlistFileExists();
ensureUserlistSchema();
ensureCoachStudentLoginFilesExist();
rebuildStudentIdClubIndex();

startProductionMemoryLogging();

app.use(
  cors({
    origin: (_origin, callback) => callback(null, true),
    credentials: true,
  })
);
app.use(express.json());
app.use(cookieParser());
app.use(slowRequestLogger);

/** Homepage: / → login page */
app.get("/", (_req, res) => {
  res.redirect(302, "/main.html");
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

const isProd = process.env.NODE_ENV === "production";
const STATIC_MAX_AGE_MS = 86400000;
/** Long cache for HTML/JS/CSS; club JSON can change often — no long browser cache there. */
app.use(
  express.static(staticRoot, isProd ? { maxAge: STATIC_MAX_AGE_MS, etag: true } : {}),
);
/** Club assets & JSON — short cache in production to avoid stale API-backed files. */
app.use(
  "/backend/data_club",
  express.static(dataClubStatic, isProd ? { maxAge: 60_000, etag: true } : {}),
);
app.use(
  "/backend/data/admin",
  express.static(adminDataStatic, isProd ? { maxAge: 60_000, etag: true } : {}),
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

app.get("/api/me", requireAuth, (req, res) => {
  const uid = req.user?.sub;
  const fromCsv = uid != null ? accountPayloadForUid(uid) : null;
  const studentLogin =
    req.user?.role === "Student" && uid != null
      ? studentProfileFromUserLoginStudentCsv(uid)
      : null;
  const coachLogin =
    req.user?.role === "Coach" && uid != null
      ? coachProfileFromUserLoginCoachCsv(uid)
      : null;
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
    } else if (clubNameTrimmed) {
      club_folder_uid = findCoachManagerClubUidByClubName(clubNameTrimmed);
    }
  } else if (req.user?.role === "Student" && uid != null) {
    const fromLoginStu = (studentLogin?.club_folder_uid ?? "").trim();
    if (fromLoginStu && isValidClubFolderId(fromLoginStu)) {
      club_folder_uid = fromLoginStu;
    } else if (clubNameTrimmed) {
      club_folder_uid = findCoachManagerClubUidByClubName(clubNameTrimmed);
    } else {
      const sess = resolveStudentClubSession(String(uid).trim());
      club_folder_uid = sess.ok ? sess.clubId : null;
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
  if (req.user?.role === "Student" && uid != null) {
    const sid = String(uid).trim();
    const stuSession = resolveStudentClubSession(sid);
    if (stuSession.ok && stuSession.rosterRow?.studentCoach?.trim()) {
      studentCoachFromRoster = stuSession.rosterRow.studentCoach.trim();
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
      club_photo: fromCsv?.club_photo ?? "",
      club_photo_url: fromCsv?.club_photo_url ?? null,
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
        ? { student_coach: studentCoachFromRoster }
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

app.use("/api", (req, res) => {
  res.status(404).json({
    ok: false,
    error: "API route not found.",
    method: req.method,
    path: req.originalUrl,
  });
});

const server = app.listen(PORT, () => {
  const mode = process.env.NODE_ENV ?? "development";
  console.log(`Server running on port ${PORT} in ${mode} mode.`);
  console.log(`  Login: /main.html (same origin as this server)`);
  console.log(`Static root: ${staticRoot}`);
  console.log(`Backend root: ${backendRoot}`);
  try {
    console.log(`User login store: ${userlistPath()}`);
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    console.warn(`User login store: (could not resolve path) ${msg}`);
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
