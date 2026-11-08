-- ============================================================================
-- 0042_client_dlr_done_at.sql — a client receipt carries WHEN its state was
-- reached (Saleem 2026-10-15: "no delivery datetime in dlr webhook call").
--
-- The receipt text has had "done date:YYMMDDhhmm" since 2026-09-11, to the
-- minute, inside one string; the webhook body had no time at all. The time is
-- the vendor's delivery time when it gave one, else when the receipt reached
-- the gateway (or, for a receipt held back by the account's receipt policy,
-- when it is let go) — the same instant the text's done date is written from.
-- Unix milliseconds, so no session time zone can move it. NULL on rows queued
-- before this column; the webhook then says when the row was queued.
-- A Redis queue carries it in the job's JSON and needs nothing.
-- One column per ALTER (the bootstrap rule).
-- ============================================================================
ALTER TABLE client_dlr ADD COLUMN done_at_ms BIGINT NULL
  COMMENT 'when the receipt''s state was reached, unix ms (vendor done date, else arrival)';
