/**
 * Mtime-based LRU cache for parsed file contents. Avoids re-reading and re-parsing
 * hot JSON/CSV on every request when the file has not changed.
 *
 * Writes to disk update mtime, so the next read automatically misses cache and reloads.
 */
import fs from "fs";
import path from "path";
import { LRUCache } from "lru-cache";

type CacheEntry = { mtimeMs: number; data: unknown };

const maxEntries =
  Number.parseInt(process.env.DATA_FILE_CACHE_MAX_ENTRIES || "250", 10) || 250;

const cache = new LRUCache<string, CacheEntry>({
  max: maxEntries,
});

function normKey(absPath: string): string {
  return path.normalize(absPath);
}

/**
 * Safe parse: on missing file, missing parse result, or parse throw — returns `ifMissing`.
 */
export function readFileCached<T>(
  absPath: string,
  parse: (raw: string) => T,
  ifMissing: T,
): T {
  const key = normKey(absPath);
  if (!fs.existsSync(key)) {
    cache.delete(key);
    return ifMissing;
  }
  const st = fs.statSync(key);
  const hit = cache.get(key);
  if (hit && hit.mtimeMs === st.mtimeMs) {
    return hit.data as T;
  }
  let raw: string;
  try {
    raw = fs.readFileSync(key, "utf8");
  } catch {
    return ifMissing;
  }
  let parsed: T;
  try {
    parsed = parse(raw);
  } catch {
    return ifMissing;
  }
  cache.set(key, { mtimeMs: st.mtimeMs, data: parsed });
  return parsed;
}

/**
 * Like {@link readFileCached} but propagates parse errors (caller expects strict JSON, etc.).
 */
export function readFileCachedStrict<T>(
  absPath: string,
  parse: (raw: string) => T,
): T {
  const key = normKey(absPath);
  if (!fs.existsSync(key)) {
    cache.delete(key);
    throw new Error(`File not found: ${key}`);
  }
  const st = fs.statSync(key);
  const hit = cache.get(key);
  if (hit && hit.mtimeMs === st.mtimeMs) {
    return hit.data as T;
  }
  const raw = fs.readFileSync(key, "utf8");
  const data = parse(raw);
  cache.set(key, { mtimeMs: st.mtimeMs, data });
  return data;
}

export function invalidateDataFileCache(absPath: string): void {
  cache.delete(normKey(absPath));
}

/** Invalidate cached paths under a directory (e.g. after deleting `data_club/CM…`). */
export function invalidateDataFileCacheUnderDir(dirAbs: string): void {
  const prefix = normKey(dirAbs);
  const sep = path.sep;
  for (const k of cache.keys()) {
    if (k === prefix || k.startsWith(prefix + sep)) {
      cache.delete(k);
    }
  }
}

export function clearDataFileCache(): void {
  cache.clear();
}

export function getDataFileCacheStats(): {
  entries: number;
  maxEntries: number;
} {
  return { entries: cache.size, maxEntries: maxEntries };
}
