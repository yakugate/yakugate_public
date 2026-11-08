-- ============================================================================
-- 0005  Operators v2 — CONFIG (MySQL)
-- Replaces the legacy telco-digit operators table (id 1-6, code, country) with
-- the MCC/MNC catalogue the HLR/routing architecture actually uses (and the
-- seed expects). No engine code read the legacy shape (the /sms/v3/operator
-- endpoint is a stub), so the legacy table is dropped, not migrated.
-- ============================================================================

DROP TABLE IF EXISTS operators;

CREATE TABLE operators (
  mcc            VARCHAR(3)  NOT NULL,                 -- mobile country code
  mnc            VARCHAR(3)  NOT NULL,                 -- mobile network code
  numbering_plan VARCHAR(8)  NOT NULL DEFAULT 'E164',
  country_code   VARCHAR(4)  NOT NULL,                 -- dialing prefix, e.g. '92'
  country_name   VARCHAR(64) NOT NULL,
  name           VARCHAR(64) NULL,                     -- operator display name
  PRIMARY KEY (mcc, mnc)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
