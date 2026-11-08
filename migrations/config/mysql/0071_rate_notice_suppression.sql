-- GW-30 follow-up (PO 2026-08-28): "don't send rate change update", at the two
-- levels the PO chose.
--
-- 1. PER CARD — rate_cards.notify_customers = 0 means changes on this card
--    never generate a customer notice at all (internal/test cards, or pricing
--    handled by contract outside the portal). Default 1: silence has to be
--    chosen deliberately, never inherited by a card someone forgot to check.
--
-- 2. PER PENDING BATCH — suppressed_at marks changes the operator decided not
--    to announce (a typo fix, a correction that would only alarm the
--    customer). Suppressed rows are excluded from the banner, from a manual
--    send, and from the auto-send net; suppressed_by_user_id records WHO
--    silenced it, because "the customer was never told" is exactly the kind of
--    decision an audit needs a name against.
ALTER TABLE rate_cards
  ADD COLUMN notify_customers TINYINT(1) NOT NULL DEFAULT 1 AFTER is_disabled;

ALTER TABLE rate_change_notices
  ADD COLUMN suppressed_at DATETIME NULL AFTER notified_at,
  ADD COLUMN suppressed_by_user_id VARCHAR(64) NULL AFTER suppressed_at;
