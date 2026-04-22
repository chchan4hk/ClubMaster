import type { Request, Response, NextFunction } from "express";
import jwt from "jsonwebtoken";
import { sportCoachDebugOn } from "../sportCoachDebug";

export type JwtPayload = {
  sub: string;
  username: string;
  role: string;
  usertype?: string;
  /** Set at Coach/Student sign-in: `data_club/{club_folder_uid}/` (disambiguates duplicate roster IDs). */
  club_folder_uid?: string;
};

declare global {
  namespace Express {
    interface Request {
      user?: JwtPayload;
    }
  }
}

const secret = () => process.env.JWT_SECRET || "dev-secret";

export function requireAuth(req: Request, res: Response, next: NextFunction): void {
  const header = req.headers.authorization;
  const bearer = header?.startsWith("Bearer ") ? header.slice(7) : null;
  const cookieTok = req.cookies?.token as string | undefined;
  const token = bearer || cookieTok;
  if (!token) {
    const body: Record<string, unknown> = { ok: false, error: "Unauthorized" };
    if (sportCoachDebugOn()) {
      body.debug = {
        hint: "No Bearer token or cookie",
        hasAuthorizationHeader: Boolean(header),
        hasCookieToken: Boolean(req.cookies && (req.cookies as { token?: string }).token),
      };
    }
    res.status(401).json(body);
    return;
  }
  try {
    const p = jwt.verify(token, secret()) as jwt.JwtPayload;
    const cfu =
      p.club_folder_uid != null ? String(p.club_folder_uid).trim() : "";
    req.user = {
      sub: String(p.sub ?? ""),
      username: String(p.username || ""),
      role: String(p.role || ""),
      usertype: p.usertype != null ? String(p.usertype) : undefined,
      ...(cfu ? { club_folder_uid: cfu } : {}),
    };
    next();
  } catch {
    const body: Record<string, unknown> = { ok: false, error: "Invalid token" };
    if (sportCoachDebugOn()) {
      body.debug = { hint: "JWT verify failed (wrong secret or expired token)" };
    }
    res.status(401).json(body);
  }
}

export function requireRole(...roles: string[]) {
  return (req: Request, res: Response, next: NextFunction): void => {
    if (!req.user || !roles.includes(req.user.role)) {
      const body: Record<string, unknown> = { ok: false, error: "Forbidden" };
      if (sportCoachDebugOn()) {
        body.debug = {
          hint: `Need role: ${roles.join(" or ")}`,
          jwtRole: req.user?.role ?? null,
          jwtSub: req.user?.sub ?? null,
        };
      }
      res.status(403).json(body);
      return;
    }
    next();
  };
}

export { secret as jwtSecret };
