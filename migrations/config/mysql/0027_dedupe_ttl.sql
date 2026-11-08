-- PO 2026-07-27: dedupe/idempotency TTL moves from a single global
-- YAKU_DEDUPE_TTL_SEC to a per-account override. NULL = use the process-wide
-- default (still YAKU_DEDUPE_TTL_SEC, 24h) — purely additive, no existing
-- account's behavior changes until an operator sets this.
ALTER TABLE accounts
  ADD COLUMN dedupe_ttl_sec INT UNSIGNED NULL AFTER quota;
