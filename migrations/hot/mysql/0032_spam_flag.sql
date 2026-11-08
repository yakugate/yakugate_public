-- The soft-flag on the delivered record (config/0098, queue/0027).
--
-- This is the column that makes observe mode worth having: it is what an
-- operator groups by to answer "what would this rule have refused last week,
-- and whose traffic was it?" before switching the rule to reject.
--
-- Deliberately on mtlog and not on rejects: a flagged message was ACCEPTED and
-- SENT. It is ordinary traffic that happens to carry a note. A refused one is
-- already covered by rejects.reason_audit (hot/0031).
ALTER TABLE mtlog_template
  ADD COLUMN spam_flag VARCHAR(190) NULL
    COMMENT 'a spam rule with action=flag matched this message at accept - it was sent anyway';
