import fs from "fs";
import path from "path";
import dotenv from "dotenv";

/**
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
