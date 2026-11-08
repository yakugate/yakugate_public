-- ============================================================================
-- 0043_mtlog_sent_parts.sql — how many parts the vendor was actually sent
--
-- PO 2026-09-21. A message is billed to the client by the gateway's own part
-- count (mtlog.segments, from the encoding it was planned in). The vendor
-- bills us for the PDUs it RECEIVES, which can differ:
--
--   * SAR/TLV concatenation carries 160 GSM-7 characters a part, not 153, so
--     310 characters is 3 parts to the client and 2 to the vendor;
--   * a vendor whose encoding rules send Latin-1 or GSM-7 as UCS-2
--     (protocol_config.encodings) receives MORE parts than were billed.
--
-- The SMPP vendor role writes the count here; billing prices the BUY side by
-- it and the SELL side by segments. NULL = not known (an HTTP vendor makes one
-- call for the whole message, and every record written before this), and the
-- buy side then uses segments, as it always did.
--
-- Daily tables made from now on have the column. Existing ones get it from
-- the store on first write (the same self-repair as concat_parts, hot/0042),
-- and the billing reader treats a table without it as "not known".
-- ============================================================================
ALTER TABLE mtlog_template
  ADD COLUMN sent_parts SMALLINT UNSIGNED NULL
    COMMENT 'PDUs the vendor was sent (SMPP); prices the vendor side. NULL = use segments'
    AFTER segments;
