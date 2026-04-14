import bcrypt from "bcryptjs";

/** Cost factor for new password hashes (bcrypt). */
export const BCRYPT_ROUNDS = 10;

export function hashPassword(plain: string): string {
  return bcrypt.hashSync(plain.trim(), BCRYPT_ROUNDS);
}

/** True if the stored value looks like a bcrypt hash. */
export function looksLikeBcrypt(stored: string): boolean {
  return /^\$2[aby]\$\d{2}\$/.test(String(stored).trim());
}

/**
 * Check a login attempt against a stored credential.
 * Supports bcrypt hashes and legacy plaintext (for CSV-only mode).
 */
export function verifyPassword(plain: string, stored: string): boolean {
  const s = String(stored ?? "").trim();
  if (!s) {
    return false;
  }
  if (looksLikeBcrypt(s)) {
    try {
      return bcrypt.compareSync(String(plain), s);
    } catch {
      return false;
    }
  }
  return plain === stored;
}
