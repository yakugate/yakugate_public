-- ============================================================================
-- 0009_smpp_config.sql — SMPP bind credentials & policy (Process 2, 2026-07-17)
-- P1 is JWT-only; SMPP binds with system_id + password, so accounts need a
-- dedicated SMPP identity. smpp_config JSON (per process-2 scope §5, PO decision):
--   { "system_id": "<=16 chars, unique>",
--     "password_hash": "<bcrypt>",
--     "allowed_modes": ["TX","RX","TRX"],
--     "max_binds": <int> }
-- The existing accounts.smpp tinyint stays the channel on/off flag; this column
-- holds the credentials/policy used only when smpp=1.
-- ============================================================================

ALTER TABLE accounts
  ADD COLUMN smpp_config JSON NULL AFTER smpp;
