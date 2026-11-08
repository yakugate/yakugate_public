-- BUG fix (2026-08-09): add 'latin1' to the queue encoding enum.
--
-- The encoding column was enum('gsm7','ucs2') — but the pipeline legitimately
-- emits encoding='latin1' (HTTP encoding="latin1", and SMPP data_coding=3 since
-- the dc3->latin1 mapping fix the same day). Enqueuing a Latin-1 message failed
-- the INSERT (RMSGQFUL) on the MySQL queue backend, while the Redis backend
-- (JSON, no enum) silently stored it. Surfaced by the both-scenarios P2 run.
-- Mirrors 0020: fix the template (future vendor tables) + the existing ones.
ALTER TABLE queue_template MODIFY encoding ENUM('gsm7','ucs2','latin1') NOT NULL DEFAULT 'gsm7';
ALTER TABLE queue_testvendor MODIFY encoding ENUM('gsm7','ucs2','latin1') NOT NULL DEFAULT 'gsm7';
ALTER TABLE mo_template MODIFY encoding ENUM('gsm7','ucs2','latin1') NOT NULL DEFAULT 'gsm7';
ALTER TABLE mo_queue MODIFY encoding ENUM('gsm7','ucs2','latin1') NOT NULL DEFAULT 'gsm7';
