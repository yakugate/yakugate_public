-- Accept-time cost/balance disclosure (PO 2026-07-27): P1 now optionally
-- tells the client, per accepted recipient, what this request cost (real
-- route/rate-card sell price, priced synchronously via billing.Rater — the
-- same pricing logic P9 uses, just called inline instead of async) and the
-- account's remaining balance afterward (EffectiveBalance() minus that cost —
-- a best-effort cached-balance estimate, since P9 still settles the real
-- ledger asynchronously; same accepted drift as the existing synchronous-
-- reservation feature). Both OFF the same table send_sms_cost already lives
-- on (sms_service_config, migration 0031) rather than a new one.
--
-- send_sms_cost already exists (0031); this migration adds its balance
-- counterpart. Both are read by a follow-up code pass (internal/refdata's
-- loader now also LEFT JOINs sms_service_config by username for just these
-- two flags — the full loader cutover to sms_service_config as the sole
-- source is still pending, per 0031's expand/contract note).
ALTER TABLE sms_service_config
  ADD COLUMN send_remaining_balance TINYINT(1) NOT NULL DEFAULT 1 AFTER send_sms_cost;
