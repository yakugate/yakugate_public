-- ============================================================================
-- 0004  Config schema v2 — reconcile older config DBs with the current 0001.
-- MySQL has no "ADD/DROP COLUMN IF [NOT] EXISTS", so every change is guarded via
-- information_schema + PREPARE (a no-op `DO 0` when already in the target state).
-- This makes the whole migration idempotent and safe on fresh DBs too.
--
-- Changes:
--   * accounts:      + balance; throughput -> INT (per-second TPS cap);
--                    drop throughput_limit, campaign_tps_limit,
--                    local_capping, local_capping_limit
--   * country_rules: + daily_quota/daily_used/monthly_quota/monthly_used
--                    (quotas moved from account_balance, now per destination country)
--   * routes:        + match_mcc/match_mnc (MCC/MNC routing)
--   * operators:     + numbering_plan/country_code/country_name; drop name, country
--   * drop tables:   account_balance, destination_caps
-- ============================================================================

-- ---- accounts.balance (add if missing) -------------------------------------
SET @s := (SELECT IF(COUNT(*)=0,
  'ALTER TABLE accounts ADD COLUMN balance DECIMAL(14,4) NOT NULL DEFAULT 0',
  'DO 0') FROM information_schema.columns
  WHERE table_schema=DATABASE() AND table_name='accounts' AND column_name='balance');
PREPARE s FROM @s; EXECUTE s; DEALLOCATE PREPARE s;

-- ---- accounts.throughput -> INT (was a TINYINT flag) -----------------------
ALTER TABLE accounts MODIFY COLUMN throughput INT NOT NULL DEFAULT 0;

-- ---- drop deprecated accounts columns (if present) -------------------------
SET @s := (SELECT IF(COUNT(*)>0,'ALTER TABLE accounts DROP COLUMN throughput_limit','DO 0')
  FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='accounts' AND column_name='throughput_limit');
PREPARE s FROM @s; EXECUTE s; DEALLOCATE PREPARE s;

SET @s := (SELECT IF(COUNT(*)>0,'ALTER TABLE accounts DROP COLUMN campaign_tps_limit','DO 0')
  FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='accounts' AND column_name='campaign_tps_limit');
PREPARE s FROM @s; EXECUTE s; DEALLOCATE PREPARE s;

SET @s := (SELECT IF(COUNT(*)>0,'ALTER TABLE accounts DROP COLUMN local_capping','DO 0')
  FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='accounts' AND column_name='local_capping');
PREPARE s FROM @s; EXECUTE s; DEALLOCATE PREPARE s;

SET @s := (SELECT IF(COUNT(*)>0,'ALTER TABLE accounts DROP COLUMN local_capping_limit','DO 0')
  FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='accounts' AND column_name='local_capping_limit');
PREPARE s FROM @s; EXECUTE s; DEALLOCATE PREPARE s;

-- ---- country_rules: per-country quotas (add if missing) --------------------
SET @s := (SELECT IF(COUNT(*)=0,'ALTER TABLE country_rules ADD COLUMN daily_quota INT NULL','DO 0')
  FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='country_rules' AND column_name='daily_quota');
PREPARE s FROM @s; EXECUTE s; DEALLOCATE PREPARE s;

SET @s := (SELECT IF(COUNT(*)=0,'ALTER TABLE country_rules ADD COLUMN daily_used INT NOT NULL DEFAULT 0','DO 0')
  FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='country_rules' AND column_name='daily_used');
PREPARE s FROM @s; EXECUTE s; DEALLOCATE PREPARE s;

SET @s := (SELECT IF(COUNT(*)=0,'ALTER TABLE country_rules ADD COLUMN monthly_quota INT NULL','DO 0')
  FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='country_rules' AND column_name='monthly_quota');
PREPARE s FROM @s; EXECUTE s; DEALLOCATE PREPARE s;

SET @s := (SELECT IF(COUNT(*)=0,'ALTER TABLE country_rules ADD COLUMN monthly_used INT NOT NULL DEFAULT 0','DO 0')
  FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='country_rules' AND column_name='monthly_used');
PREPARE s FROM @s; EXECUTE s; DEALLOCATE PREPARE s;

-- ---- routes: MCC/MNC match columns (add if missing) ------------------------
SET @s := (SELECT IF(COUNT(*)=0,'ALTER TABLE routes ADD COLUMN match_mcc VARCHAR(3) NULL','DO 0')
  FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='routes' AND column_name='match_mcc');
PREPARE s FROM @s; EXECUTE s; DEALLOCATE PREPARE s;

SET @s := (SELECT IF(COUNT(*)=0,'ALTER TABLE routes ADD COLUMN match_mnc VARCHAR(3) NULL','DO 0')
  FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='routes' AND column_name='match_mnc');
PREPARE s FROM @s; EXECUTE s; DEALLOCATE PREPARE s;

-- ---- operators: reshape to numbering_plan/country_code/country_name --------
SET @s := (SELECT IF(COUNT(*)=0,'ALTER TABLE operators ADD COLUMN numbering_plan VARCHAR(16) NULL','DO 0')
  FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='operators' AND column_name='numbering_plan');
PREPARE s FROM @s; EXECUTE s; DEALLOCATE PREPARE s;

SET @s := (SELECT IF(COUNT(*)=0,'ALTER TABLE operators ADD COLUMN country_code VARCHAR(6) NULL','DO 0')
  FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='operators' AND column_name='country_code');
PREPARE s FROM @s; EXECUTE s; DEALLOCATE PREPARE s;

SET @s := (SELECT IF(COUNT(*)=0,'ALTER TABLE operators ADD COLUMN country_name VARCHAR(64) NULL','DO 0')
  FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='operators' AND column_name='country_name');
PREPARE s FROM @s; EXECUTE s; DEALLOCATE PREPARE s;

SET @s := (SELECT IF(COUNT(*)>0,'ALTER TABLE operators DROP COLUMN name','DO 0')
  FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='operators' AND column_name='name');
PREPARE s FROM @s; EXECUTE s; DEALLOCATE PREPARE s;

SET @s := (SELECT IF(COUNT(*)>0,'ALTER TABLE operators DROP COLUMN country','DO 0')
  FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='operators' AND column_name='country');
PREPARE s FROM @s; EXECUTE s; DEALLOCATE PREPARE s;

-- ---- drop removed tables ---------------------------------------------------
DROP TABLE IF EXISTS account_balance;
DROP TABLE IF EXISTS destination_caps;
