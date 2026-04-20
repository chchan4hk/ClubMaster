import { Router } from "express";
import { requireAuth } from "../middleware/requireAuth";

/** Mock payment — no real charge; extend when Finance module is implemented. */
export function createMockPaymentRouter(): Router {
  const r = Router();
  r.post("/", requireAuth, (req, res) => {
    const amount = Number(req.body?.amount);
    const reference = String(req.body?.reference || "").trim();
    if (!Number.isFinite(amount) || amount <= 0) {
      res.status(400).json({ ok: false, error: "Invalid amount" });
      return;
    }
    res.json({
      ok: true,
      message: "Mock payment recorded (no funds moved)",
      transactionId: `MOCK-${Date.now()}`,
      amount,
      reference: reference || null,
      uid: req.user?.sub,
    });
  });
  return r;
}
