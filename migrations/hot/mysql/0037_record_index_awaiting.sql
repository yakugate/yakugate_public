-- 0037_record_index_awaiting.sql — the HTTP-vendor status poll's index
-- (PO sign-off 2026-09-14).
--
-- http-vendor's poll mode asks record_index for one vendor's messages that are
-- 'sent' and still waiting for a receipt (mysql.Store.SentAwaitingDLR). No
-- index started with vendor_id, so every poll read the table: on the dev
-- database 413,000 of 827,000 rows, and the first poll came about 10 seconds
-- after sending (found by the 2026-09-14 end-to-end run, p4h TC07/TC22).
--
-- (vendor_id, status) takes the poll straight to that vendor's waiting rows.
-- InnoDB builds it online; inserts and updates carry on meanwhile.
ALTER TABLE record_index
  ADD INDEX idx_vendor_status (vendor_id, status),
  ALGORITHM=INPLACE, LOCK=NONE;
