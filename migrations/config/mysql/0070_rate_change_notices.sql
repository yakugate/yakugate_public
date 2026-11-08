-- GW-30 (PO 2026-08-28): rate-change notification to the companies billed by
-- a SELLING rate card.
--
-- Every change to a sell card's rates (create / edit / deactivate / activate-
-- now) lands here as a PENDING notice. They are not emailed one-by-one — an
-- editing session touches several destinations and the customer should get
-- ONE message covering the lot, the way the reference (Hadal) alert did.
--
-- Flow (PO ruling): the portal prompts "notify N companies?" when the operator
-- closes the rate card editor; a persistent banner shows anything still
-- pending; and whatever is still unsent after RATE_NOTICE_AUTOSEND_MIN
-- (default 10 minutes) is sent automatically by the Admin API's poller, so a
-- forgotten notice can never leave customers on stale prices unannounced.
--
-- before_json/after_json snapshot the row on both sides so the email can show
-- the old -> new diff even after further edits, and a notice already sent
-- keeps its historical truth (notified_at stamped, never re-sent).
CREATE TABLE IF NOT EXISTS rate_change_notices (
  id                 BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  rate_card_id       INT UNSIGNED    NOT NULL,
  rate_id            INT UNSIGNED    NULL,
  change_type        ENUM('create','update','deactivate','activate') NOT NULL,
  before_json        JSON            NULL,
  after_json         JSON            NULL,
  -- Set when the change is scheduled for a future date: the second,
  -- "it is live now" email fires from this (PO: notify on edit AND when it
  -- takes effect). NULL = immediate change, one email only.
  effective_from     DATETIME        NULL,
  effective_notified_at DATETIME     NULL,
  changed_by_user_id VARCHAR(64)     NULL,
  notified_at        DATETIME        NULL,
  created_at         DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY idx_pending (rate_card_id, notified_at),
  KEY idx_effective (effective_from, effective_notified_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
