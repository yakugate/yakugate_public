-- ============================================================================
-- 0012_vendor_adapter.sql — config-driven HTTP vendor adapter (2026-07-19)
-- adapter_config holds the JSON Profile that the http-vendor process (P4) uses to
-- talk to ANY HTTP/REST/XML/SOAP vendor by config only (no code): templated
-- request ({placeholders}) + regex extraction, optional session auth, and
-- delivery status by webhook or polling. See internal/vendors/httpadapter.
-- ============================================================================

ALTER TABLE vendors
  ADD COLUMN adapter_config JSON NULL AFTER queue_config;
