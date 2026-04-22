import fs from "fs";
import path from "path";
import dotenv from "dotenv";

/**
 * Zeabur / low-memory hosts (see `dataFileCache.ts`, `server.ts`):
 * `StudentID`/`CoachID`/`LessonID`/`PrizeID` indexes over `data_club` are built lazily on first use (not at boot).
 *
 * - `NODE_ENV=production` — enables 60s `[perf:memory]` logs (disable with `PERF_MEMORY_LOG=0`).
 * - `ACCESS_LOG=1` — log every request (`[access] GET /path 200 12ms`) if the platform hides slow-only logs.
 * - `DATA_FILE_CACHE_MAX_ENTRIES` — default 250 parsed files in RAM; try `80` on small instances.
 * - `NODE_OPTIONS=--max-old-space-size=...` — only if Node OOMs; prefer a larger plan if RSS stays >85%.
 *
 * Production: **`npm run build` then `npm start`** (`dist/server.js`). Avoid `start:tsx` — extra memory for TS execution.
 *
 * Load `backend/.env` only for local development. In production, the platform
 * (Zeabur, Docker, etc.) injects env vars — we never read a file that could override them.
 * dotenv never overrides variables already set in the environment.
 */
export function loadLocalEnvFile(backendPackageRoot: string): void {
  if (process.env.NODE_ENV === "production") {
    return;
  }
  const envPath = path.join(backendPackageRoot, ".env");
  if (!fs.existsSync(envPath)) {
    return;
  }
  dotenv.config({ path: envPath, override: false });
}

/**
 * HTTP listen port: `process.env.PORT` at runtime (Zeabur, Railway, etc.).
 * Fallback `3000` only when PORT is unset — typical for local `npm run dev`.
 */
export function resolveListenPort(): number {
  const raw = process.env.PORT;
  if (raw != null && String(raw).trim() !== "") {
    const n = Number.parseInt(String(raw).trim(), 10);
    if (Number.isFinite(n) && n >= 1 && n <= 65535) {
      return n;
    }
    console.warn(
      `[config] Invalid PORT="${raw}" — using development fallback 3000`,
    );
  }
  return 3000;
}

/**
 * In production, refuse to start without a real JWT signing secret.
 * (Development may use the fallback in `requireAuth`.)
 */
export function assertRequiredProductionEnv(): void {
  if (process.env.NODE_ENV !== "production") {
    return;
  }
  if (!process.env.JWT_SECRET?.trim()) {
    console.error(
      "[config] FATAL: JWT_SECRET must be set in production (platform environment variables).",
    );
    process.exit(1);
  }
}
