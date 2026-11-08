-- 0105_blocklist_vendor_source.sql — a number blocked because a vendor said so
-- (PO 2026-09-15, status-code action block_destination).
--
-- An SMPP vendor answering a submit with a code mapped to block_destination
-- adds the number to the SENDING ACCOUNT's blocklist, with no expiry (PO), and
-- the message fails. The row's source says where it came from, so an operator
-- can tell these from manual, opt-out, abuse and trap entries, and lift them.
ALTER TABLE blocklist
  MODIFY COLUMN source ENUM('manual','unsub','abuse','honeypot','vendor') NOT NULL DEFAULT 'manual'
    COMMENT 'manual/unsub/abuse/vendor = an ordinary block (vendor = a vendor code asked for it); honeypot = a trap number, refuse AND alarm';
