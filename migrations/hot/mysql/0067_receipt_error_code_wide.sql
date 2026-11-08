-- ============================================================================
-- 0067_receipt_error_code_wide.sql — a receipt row's error code holds 32
-- characters, as the message's own does (hot/0064) (issues 22 and 23,
-- 2026-10-27)
--
-- The gateway's own reason codes have their full names now: VALIDITY_EXPIRED
-- (16) and MAX_ATTEMPTS_REACHED (20; it was MAX_ATTEMPTS). A receipt row may
-- carry one (a receipt policy's row, an older build's "internal" verdict
-- row), and tpl_dlr took the old dlrlog width, 16, so such a write was
-- refused. The receipts summary counts by the same code.
--
-- tpl_day_dlr is the receipts' daily template of the storage redesign
-- (hot/0066_daily_logs, 2026-10-27), also 16 wide; on a store without it the
-- statement is skipped (the bootstrap skips an ALTER on an absent table).
-- Numbered 0067 to come after it. Existing clones (dlr_<company>_<month>, and
-- the daily ones) are widened from their template at start (bootstrap
-- widenCloneColumns; VARCHAR 16 -> 32 in utf8mb4 keeps its one-byte length,
-- so MySQL does it in place). One column per statement.
-- ============================================================================
ALTER TABLE tpl_dlr MODIFY error_code VARCHAR(32) DEFAULT NULL;
ALTER TABLE summary_dlr MODIFY error_code VARCHAR(32) NOT NULL DEFAULT '';
ALTER TABLE tpl_day_dlr MODIFY error_code VARCHAR(32) DEFAULT NULL;
