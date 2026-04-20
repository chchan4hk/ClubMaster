/** When true, coach-manager routes and auth may attach a `debug` object or log extra detail. */
export function sportCoachDebugOn(): boolean {
  const v = process.env.SPORT_COACH_DEBUG?.trim().toLowerCase();
  return v === "1" || v === "true" || v === "yes";
}
