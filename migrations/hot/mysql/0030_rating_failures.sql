-- A record that can never be priced stops being retried forever (P9 gap 2).
--
-- Until now a Charge() or ApplyRating() error meant `continue`: the row stayed
-- unrated and came back on the next sweep, and the next, with nothing counting
-- the attempts and nothing telling anyone. A record whose account_id no longer
-- resolves — a deleted tenant, a bad backfill — is retried every ten seconds
-- for the life of the deployment, occupying a slot in every batch, and the
-- only symptom is a log line that scrolls away.
--
-- rating_attempts counts the FAILURES, not the sweeps: a row is only touched
-- when something actually went wrong, so a healthy record never carries a
-- number at all.
--
-- rated_at deliberately stays NULL on a parked row. rated_at means PRICED, and
-- a row nobody could price is not priced — saying otherwise would hide it from
-- every unrated count and quietly turn a billing failure into a free message.
-- The query excludes it by attempts instead, so it stays visible and
-- searchable, and clearing the cause lets someone reset the counter and let it
-- through.

ALTER TABLE mtlog_template
  ADD COLUMN rating_attempts SMALLINT UNSIGNED NOT NULL DEFAULT 0
    COMMENT 'consecutive rating/charging FAILURES; at the cap the row is parked and no longer swept',
  ADD COLUMN rating_error VARCHAR(255) NULL
    COMMENT 'why the last attempt failed — the reason an operator needs, kept on the record rather than only in a log line';

ALTER TABLE dlrlog_template
  ADD COLUMN rating_attempts SMALLINT UNSIGNED NOT NULL DEFAULT 0
    COMMENT 'consecutive rating/charging FAILURES; at the cap the row is parked and no longer swept',
  ADD COLUMN rating_error VARCHAR(255) NULL
    COMMENT 'why the last attempt failed';
