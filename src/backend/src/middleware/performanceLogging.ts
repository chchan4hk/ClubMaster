import type { Request, Response, NextFunction } from "express";

const SLOW_MS = Number.parseInt(process.env.SLOW_REQUEST_LOG_MS || "200", 10) || 200;

let memoryLogInterval: ReturnType<typeof setInterval> | null = null;

/**
 * Logs `process.memoryUsage()` every 60s when `NODE_ENV=production` (opt-out with PERF_MEMORY_LOG=0).
 */
export function startProductionMemoryLogging(): void {
  if (process.env.NODE_ENV !== "production") {
    return;
  }
  if (process.env.PERF_MEMORY_LOG === "0" || process.env.PERF_MEMORY_LOG === "false") {
    return;
  }
  if (memoryLogInterval) {
    return;
  }
  const tick = () => {
    const m = process.memoryUsage();
    console.log(
      "[perf:memory]",
      JSON.stringify({
        rss: m.rss,
        heapUsed: m.heapUsed,
        heapTotal: m.heapTotal,
        external: m.external,
      }),
    );
  };
  tick();
  memoryLogInterval = setInterval(tick, 60_000);
  memoryLogInterval.unref?.();
}

/** For tests / graceful shutdown */
export function stopProductionMemoryLogging(): void {
  if (memoryLogInterval) {
    clearInterval(memoryLogInterval);
    memoryLogInterval = null;
  }
}

/**
 * Logs slow requests after response finishes (status >= 400 still logged if slow).
 */
export function slowRequestLogger(
  req: Request,
  res: Response,
  next: NextFunction,
): void {
  const start = process.hrtime.bigint();
  res.on("finish", () => {
    const elapsedMs = Number(process.hrtime.bigint() - start) / 1e6;
    if (elapsedMs >= SLOW_MS) {
      console.warn(
        `[perf:slow] ${req.method} ${req.originalUrl} ${elapsedMs.toFixed(1)}ms`,
      );
    }
  });
  next();
}

/**
 * One line per request (method, path, status, ms). Enable on Zeabur/Railway with `ACCESS_LOG=1`.
 */
export function accessRequestLogger(
  req: Request,
  res: Response,
  next: NextFunction,
): void {
  const start = process.hrtime.bigint();
  res.on("finish", () => {
    const elapsedMs = Number(process.hrtime.bigint() - start) / 1e6;
    console.log(
      `[access] ${req.method} ${req.originalUrl} ${res.statusCode} ${elapsedMs.toFixed(0)}ms`,
    );
  });
  next();
}

export function shouldEnableAccessLog(): boolean {
  const v = process.env.ACCESS_LOG?.trim().toLowerCase();
  return v === "1" || v === "true" || v === "yes";
}
