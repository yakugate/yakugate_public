-- 0056_blocklist_expiry.sql — optional expiry on blocklist (DND) entries
-- (P1 scope finalize 2026-08-09). NULL = permanent (blocked until removed); a
-- timestamp = auto-unblocks once past. Checked at query time, so an entry
-- passes the instant it expires, not only at the next cache refresh.
ALTER TABLE blocklist
  ADD COLUMN expires_at DATETIME NULL;
