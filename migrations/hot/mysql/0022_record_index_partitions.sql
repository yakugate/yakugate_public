-- GW-24 (PO approved 2026-08-27): record_index gains a LIFECYCLE. The locator
-- was the one global hot-store table nothing ever deleted from — data
-- partitions get dropped by retention, their index rows stayed forever.
-- Range-partitioning by ym makes index cleanup the same O(1) DROP the data
-- families already enjoy; the retention role now reorganises the catch-all
-- into named months ahead of time and drops index partitions alongside the
-- data partitions they point to.
--
-- The PK must contain the partition column (MySQL rule), so it widens to
-- (msg_id, ym) — msg_id stays globally unique in practice (ULID), the wider
-- key only satisfies the partitioning constraint. One-time rebuild; approved
-- to run NOW while the table is small.
ALTER TABLE record_index DROP PRIMARY KEY, ADD PRIMARY KEY (msg_id, ym);
ALTER TABLE record_index PARTITION BY RANGE COLUMNS(ym) (
  PARTITION p202607 VALUES LESS THAN ('202608'),
  PARTITION p202608 VALUES LESS THAN ('202609'),
  PARTITION p202609 VALUES LESS THAN ('202610'),
  PARTITION pmax VALUES LESS THAN (MAXVALUE)
);
