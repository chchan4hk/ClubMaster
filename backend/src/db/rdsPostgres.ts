import fs from "fs";
import path from "path";
import pg from "pg";

const { Pool } = pg;

/** Default cluster endpoint (ap-southeast-2). Override with `RDS_HOST`. */
export const RDS_DEFAULT_HOST =
  "sport-master-db.cluster-cjsyqwwa4dr6.ap-southeast-2.rds.amazonaws.com";

/** Backend root: …/backend (works from `src/` via dist/ at runtime). */
function backendRootDir(): string {
  return path.join(__dirname, "..", "..");
}

/**
 * Path to AWS RDS global CA PEM (`sslrootcert`).
 * Default: `backend/certs/global-bundle.pem` — run `npm run rds:download-ca` once.
 */
export function resolveRdsSslCaPath(): string {
  const fromEnv = process.env.RDS_SSL_CA_PATH?.trim();
  if (fromEnv) {
    return path.isAbsolute(fromEnv)
      ? fromEnv
      : path.join(process.cwd(), fromEnv);
  }
  return path.join(backendRootDir(), "certs", "global-bundle.pem");
}

export function isRdsPostgresConfigured(): boolean {
  return Boolean(process.env.RDS_PASSWORD?.trim());
}

function readSslCa(): string {
  const caPath = resolveRdsSslCaPath();
  if (!fs.existsSync(caPath)) {
    throw new Error(
      `RDS SSL CA file not found: ${caPath}\n` +
        "Run: cd backend && npm run rds:download-ca",
    );
  }
  return fs.readFileSync(caPath, "utf8");
}

/**
 * `sslmode=verify-full` equivalent: verify server cert + hostname using AWS global bundle.
 */
function buildPoolConfig(): pg.PoolConfig {
  const host = process.env.RDS_HOST?.trim() || RDS_DEFAULT_HOST;
  const port = Number.parseInt(process.env.RDS_PORT || "5432", 10);
  const database = process.env.RDS_DATABASE?.trim() || "postgres";
  const user = process.env.RDS_USER?.trim() || "metaservices";
  const password = process.env.RDS_PASSWORD?.trim();
  if (!password) {
    throw new Error(
      "RDS_PASSWORD is not set. Add it to backend/.env (never commit secrets).",
    );
  }

  return {
    host,
    port: Number.isFinite(port) && port > 0 ? port : 5432,
    database,
    user,
    password,
    ssl: {
      rejectUnauthorized: true,
      ca: readSslCa(),
    },
    max: Number.parseInt(process.env.RDS_POOL_MAX || "10", 10) || 10,
    idleTimeoutMillis: 30_000,
    connectionTimeoutMillis: 15_000,
  };
}

let poolSingleton: pg.Pool | null = null;

/**
 * Shared pool for AWS RDS PostgreSQL. Lazily created; throws if unconfigured or CA missing.
 */
export function getRdsPool(): pg.Pool {
  if (!isRdsPostgresConfigured()) {
    throw new Error(
      "RDS PostgreSQL not configured: set RDS_PASSWORD in backend/.env",
    );
  }
  if (!poolSingleton) {
    poolSingleton = new Pool(buildPoolConfig());
  }
  return poolSingleton;
}

/** Returns a pool only when `RDS_PASSWORD` is set; otherwise `null`. */
export function getRdsPoolOrNull(): pg.Pool | null {
  if (!isRdsPostgresConfigured()) {
    return null;
  }
  return getRdsPool();
}

export async function closeRdsPool(): Promise<void> {
  if (poolSingleton) {
    await poolSingleton.end();
    poolSingleton = null;
  }
}

/**
 * Quick connectivity check (`SELECT 1`).
 * If `RDS_PASSWORD` is unset, returns `{ ok: false }` without throwing.
 */
export async function testRdsConnection(): Promise<
  { ok: true; latencyMs: number } | { ok: false; error: string }
> {
  if (!isRdsPostgresConfigured()) {
    return { ok: false, error: "RDS not configured (missing RDS_PASSWORD)." };
  }
  const started = Date.now();
  try {
    const pool = getRdsPool();
    const r = await pool.query("SELECT 1 AS ok");
    const row = r.rows[0] as { ok?: number } | undefined;
    if (row?.ok !== 1) {
      return { ok: false, error: "Unexpected SELECT 1 result." };
    }
    return { ok: true, latencyMs: Date.now() - started };
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    return { ok: false, error: msg };
  }
}
