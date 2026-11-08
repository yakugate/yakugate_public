-- GW-23 lifecycle (PO ruled 2026-08-28: "partition + post-invoice rollup").
-- Step 1 (config/0062) indexed the dedup guard; this is step 2:
--
--   * balance_history is range-partitioned by month so future cleanup has
--     data locality (the PK widens to (id, created_at) — MySQL requires the
--     partition column in every unique key; id stays first, AUTO_INCREMENT
--     unaffected).
--   * balance_history_rollup receives the DAILY per-company/account totals
--     of per-message commit rows once the period's client invoice is SENT
--     and PAID — after that, the invoice PDF/CSV are the per-message
--     evidence and the raw rows delete. topup/adjust/refund rows are NEVER
--     rolled up: they are few and they ARE the balance audit trail.
--
-- The retention role owns the calendar (partitions ahead, rollup behind) —
-- its first config-store duty, mirroring its record_index upkeep.
CREATE TABLE IF NOT EXISTS balance_history_rollup (
  day          DATE            NOT NULL,
  company_id   BIGINT UNSIGNED NOT NULL,
  account_id   BIGINT UNSIGNED NOT NULL,
  commits      INT UNSIGNED    NOT NULL,
  amount_total DECIMAL(16,4)   NOT NULL,
  rolled_at    DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (day, company_id, account_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

ALTER TABLE balance_history DROP PRIMARY KEY, ADD PRIMARY KEY (id, created_at);

ALTER TABLE balance_history PARTITION BY RANGE COLUMNS(created_at) (
  PARTITION p202607 VALUES LESS THAN ('2026-08-01'),
  PARTITION p202608 VALUES LESS THAN ('2026-09-01'),
  PARTITION p202609 VALUES LESS THAN ('2026-10-01'),
  PARTITION pmax VALUES LESS THAN (MAXVALUE)
);
