-- Move send_sms_cost/send_remaining_balance to sms_service_dialects (PO
-- 2026-07-28: "send_sms_cost, send_remaining_balance those can go to
-- dialects"). Companion to 0044/0045.
--
-- Real tradeoff, flagged rather than hidden: a dialect_id NULL account
-- (native, no shared dialect row -- the common case) now has NO row to hold
-- this per-account preference. internal/refdata defaults ShowRequestCost/
-- ShowRemainingBalance to true when dialect_id is NULL (matching these
-- columns' original DEFAULT 1 here), so native accounts keep today's
-- behavior; a native account cannot individually opt OUT of cost/balance
-- disclosure without being given its own dialect row. Also: accounts sharing
-- one dialect_id now share this preference too, same as they already share
-- url/format/mapping -- a natural consequence of "one dialect, many
-- accounts," not a bug.
ALTER TABLE sms_service_dialects
  ADD COLUMN send_sms_cost TINYINT(1) NOT NULL DEFAULT 1,
  ADD COLUMN send_remaining_balance TINYINT(1) NOT NULL DEFAULT 1;

-- Backfill from one representative account per dialect_id (same
-- one-policy-per-shared-row tradeoff as 0044's company consolidation).
UPDATE sms_service_dialects d
JOIN (
  SELECT s1.dialect_id, s1.send_sms_cost, s1.send_remaining_balance
  FROM sms_service_config s1
  JOIN (SELECT dialect_id, MIN(id) AS min_id FROM sms_service_config
        WHERE dialect_id IS NOT NULL GROUP BY dialect_id) first
    ON first.dialect_id = s1.dialect_id AND first.min_id = s1.id
) rep ON rep.dialect_id = d.id
SET d.send_sms_cost = rep.send_sms_cost,
    d.send_remaining_balance = rep.send_remaining_balance;

ALTER TABLE sms_service_config
  DROP COLUMN send_sms_cost,
  DROP COLUMN send_remaining_balance;
