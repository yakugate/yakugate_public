-- P6-B7 (PO 2026-09-17): record_index says which way a message went, so a
-- reader no longer guesses from the partition name. MT = outgoing (mtlog_ /
-- records_), MO = inbound (mo_log_). Existing rows are backfilled from
-- rec_table.
ALTER TABLE record_index
  ADD COLUMN direction ENUM('MT','MO') NOT NULL DEFAULT 'MT' AFTER status,
  ADD KEY idx_ri_direction (account_id, direction);
UPDATE record_index SET direction = 'MO' WHERE rec_table LIKE 'mo_log_%';
