-- The seeded Pakistani number ranges in international form (QA BUG-YK-OPS-001,
-- 2026-10-05).
--
-- scripts/seed/config.sql wrote five ranges as 0300, 0301, 0303, 0304, 0306 —
-- the national form. A number the gateway reads is international (92300…),
-- so these ranges matched nothing: only numbers already in the HLR data got a
-- network, and the rest were priced by the any-destination row. The seed now
-- writes 92300…; this corrects a box seeded before.
--
-- Only the seed's own rows are touched: calling code 92, MCC 410, one of the
-- seed's five networks by name, a four-digit 03xx range whose two ends are
-- the same. A real operator's data is never in that shape (the console and
-- the Admin API refuse a range with a leading 0 now). IGNORE: a box that
-- already has the international row keeps it.
-- MySQL applies the SET list left to right with the NEW values, so the
-- columns that read from_series come before it.
UPDATE IGNORE operators
   SET hlr_shard_key = CONCAT('92', SUBSTRING(from_series, 2)),
       to_series     = CONCAT('92', SUBSTRING(from_series, 2)),
       from_series   = CONCAT('92', SUBSTRING(from_series, 2))
 WHERE ccc = '92' AND mcc = '410'
   AND operator_name IN ('Op A', 'Op B', 'Op C', 'Op D')
   AND from_series REGEXP '^03[0-9]{2}$' AND to_series = from_series;
