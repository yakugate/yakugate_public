-- 0035_mo_log_rating_failures.sql — the rating-failure columns MO never got.
--
-- hot/0030 added rating_attempts/rating_error to mtlog_template and
-- dlrlog_template and missed mologtemplate. P9's unrated sweep reads the same
-- query for every family, so since 0030 the sweep of every mo_log_YYYY_MM
-- partition failed with "Unknown column 'rating_attempts'" and no MO message
-- was rated or charged — the same silent loss hot/0020 fixed once before
-- (found by the 2026-09-10 load run: 11,996 MO, none billed).
--
-- Same column shapes as 0030. Existing mo_log_YYYY_MM clones follow the
-- template at start-up (bootstrap syncCloneColumns), so they need no
-- statement here.
ALTER TABLE mologtemplate
  ADD COLUMN rating_attempts SMALLINT UNSIGNED NOT NULL DEFAULT 0
    COMMENT 'consecutive rating/charging FAILURES; at the cap the row is parked and no longer swept',
  ADD COLUMN rating_error VARCHAR(255) NULL
    COMMENT 'why the last attempt failed';
