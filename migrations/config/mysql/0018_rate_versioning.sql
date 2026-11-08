-- ============================================================================
-- 0018_rate_versioning.sql — immutable rate rows (2026-07-22)
--
-- Problem (saleem-hadal-db-review.md §7.4): rate_card_rates rows were mutable,
-- and records store only a POINTER to the row (rate_cards audit JSON =
-- {buy:{CardID,RateID,PPM}, sell:{...}}). Editing a rate in place therefore
-- rewrote history: every past charge pointing at that row became unexplainable.
-- The reference gateway avoided this by snapshotting the whole plan onto every
-- message row (~3 KB/row) — correct but ruinously heavy.
--
-- Fix: keep the cheap pointer, make the target immutable. A rate change is an
-- INSERT of a new row plus stamping valid_to on the old one; nothing is ever
-- UPDATEd in place. valid_to IS NULL == currently active.
--
--   supersede: UPDATE rate_card_rates SET valid_to=NOW() WHERE id=<old>;
--              INSERT INTO rate_card_rates (..., version=<old.version+1>);
--
-- Readers (refdata loader, P9 rater) filter valid_to IS NULL. Historical
-- explains resolve by id and always find the exact row that priced the message.
-- ============================================================================

ALTER TABLE rate_card_rates
  ADD COLUMN version    INT UNSIGNED NOT NULL DEFAULT 1 AFTER rate_card_id,
  ADD COLUMN valid_from DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP AFTER price_per_segment,
  ADD COLUMN valid_to   DATETIME     NULL AFTER valid_from,
  ADD COLUMN superseded_by INT UNSIGNED NULL AFTER valid_to,
  ADD KEY idx_current (rate_card_id, valid_to);

-- Existing rows are the current version; effective_from (legacy, DATE) seeds
-- valid_from where it was set.
UPDATE rate_card_rates
   SET valid_from = COALESCE(effective_from, created_at, CURRENT_TIMESTAMP),
       valid_to   = NULL,
       version    = 1
 WHERE valid_to IS NULL;
