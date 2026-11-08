-- ============================================================================
-- 0035_send_round.sql — where a message is in its rule's plan (2026-09-30)
--
--   send_round   rounds already failed (0 = still in its first round). A
--                round is Main then every backup of the rule (config/0131);
--                when no vendor of a round takes the message it goes back to
--                wait, and after the rule's last retry it fails.
--   route_level  the level it is queued for: 0 = Main, 1 = Backup 1, …
--
-- tried_vendors (queue/0018) now lists the vendors tried IN THIS ROUND, so a
-- vendor is never offered the same message twice in one round.
-- One column per statement.
-- ============================================================================
ALTER TABLE queue_template ADD COLUMN send_round INT NOT NULL DEFAULT 0;
ALTER TABLE queue_template ADD COLUMN route_level INT NOT NULL DEFAULT 0;
