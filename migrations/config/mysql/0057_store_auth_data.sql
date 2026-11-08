-- 0057_store_auth_data.sql — per-account flag: capture the presented
-- credentials on a credential-related auth failure, for debugging (P1 scope
-- finalize 2026-08-09, PO: "add a flag into the config, store auth data or
-- not, decided per that flag for that user"). Default 0 (off) — the reject
-- log's allow-list sanitizer never stores credentials unless a specific
-- account opts in. Only genuine credential failures (bad password/token) are
-- captured; a valid credential that failed for another reason is never stored.
ALTER TABLE sms_service_config
  ADD COLUMN store_auth_data TINYINT(1) NOT NULL DEFAULT 0;
