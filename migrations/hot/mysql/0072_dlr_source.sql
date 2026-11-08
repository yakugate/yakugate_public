-- Every receipt the gateway generates is saved as system generated (Saleem,
-- 27 Oct 2026): receipt-policy answers, the final receipt of a failure, out
-- of tries or validity over, the gateway's ACCEPTD / ENROUTE notices, a
-- write-off, the blackhole simulator's receipts. Only a receipt that came
-- from a vendor is `vendor`. `attempt` is the try whose submission a vendor's
-- receipt answers (sub_.attempt). Rows written before read as vendor, but
-- those whose raw starts "policy:" (the readers say so).
ALTER TABLE tpl_day_dlr ADD COLUMN source ENUM('vendor','system') NOT NULL DEFAULT 'vendor' AFTER account_id;
ALTER TABLE tpl_day_dlr ADD COLUMN attempt SMALLINT UNSIGNED DEFAULT NULL AFTER part;
