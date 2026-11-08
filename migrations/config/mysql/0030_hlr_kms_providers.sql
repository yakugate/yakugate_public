-- Per-account HLR/KMS provider selection (PO 2026-07-27, decision #4): rather
-- than a free-form per-account provider blob (Hadal's cached_hlr/
-- third_party_hlr/kms JSON), Yakugate curates its OWN catalog of supported
-- providers here; an account then selects one or more from this list
-- (sms_service_config.hlr_provider_ids/kms_provider_ids, migration 0031).
-- Salman: "we will offer them to select one or more from our provided lists."
CREATE TABLE hlr_providers (
  id            BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  name          VARCHAR(128) NOT NULL,
  provider_type VARCHAR(64) NOT NULL,   -- e.g. "db", "dip_url", "third_party_api"
  config        JSON NULL,             -- provider-specific connection config
  is_active     TINYINT(1) NOT NULL DEFAULT 1,
  created_at    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uq_hlr_providers_name (name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE kms_providers (
  id            BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  name          VARCHAR(128) NOT NULL,
  provider_type VARCHAR(64) NOT NULL,   -- e.g. "file", "env", "vault"
  config        JSON NULL,
  is_active     TINYINT(1) NOT NULL DEFAULT 1,
  created_at    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uq_kms_providers_name (name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Seed with today's actual (single) providers so existing accounts have a
-- real row to reference once the engine is wired to read these lists.
INSERT INTO hlr_providers (name, provider_type, config) VALUES
  ('default-db-hlr', 'db', JSON_OBJECT());
INSERT INTO kms_providers (name, provider_type, config) VALUES
  ('default-file-kms', 'file', JSON_OBJECT());
