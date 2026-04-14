/**
 * True if `Expiry_date` value means the account is past its subscription end (date-only, local calendar).
 * Empty / invalid / "—" → not expired (no block).
 */
export function isLoginExpiryDatePast(
  expiryDateStr: string | undefined | null,
): boolean {
  const t = String(expiryDateStr ?? "").trim();
  if (!t || t === "—") {
    return false;
  }
  if (!/^\d{4}-\d{2}-\d{2}$/.test(t)) {
    return false;
  }
  const today = new Date().toISOString().slice(0, 10);
  return today > t;
}
