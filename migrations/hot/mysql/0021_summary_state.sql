-- GW-19.3 (PO approved 2026-08-27): persisted watermark for the summary
-- reconcile. The billing role's sweep no longer rescans every mtlog partition
-- every interval — it recomputes only the buckets whose record_index rows
-- changed since the stored watermark (hot/0016 built updated_at + its index
-- for exactly this). Persisting the watermark here means a restart resumes
-- incrementally instead of forcing a full rescan; an ABSENT row is the
-- explicit "do one full heal first" signal (first boot, or an operator
-- deleting the row / setting YAKU_SUMMARY_FULL_RESYNC=1 to force a rebuild).
-- The watermark is written from and compared against the SQL session clock
-- (NOW()), never Go time — the vendor_sessions clock rule.
CREATE TABLE IF NOT EXISTS summary_state (
  name       VARCHAR(32) NOT NULL PRIMARY KEY,  -- 'mt' today; 'mo', 'dlr_flow'... as GW-19.2 writers land
  watermark  DATETIME    NOT NULL,
  updated_at DATETIME    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
