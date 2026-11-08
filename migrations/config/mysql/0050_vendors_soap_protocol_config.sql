-- Vendors review (PO 2026-07-28): "soap should also be added into
-- vendors.transport. smpp_config and http_config, will be only one field
-- that is protocol_config"
--
-- 'soap' is a genuinely new egress path, not just a label: P4 (http-vendor)
-- and P5 (dlr webhook loader) both filter `WHERE transport='http'` when
-- deciding which vendors they own — a vendor created with transport='soap'
-- would otherwise never get its queue drained or its webhook registered.
-- Fixed in the same change (internal/process/httpvendor/workings.go,
-- internal/process/dlr/workings.go: `transport IN ('http','soap')`) — SOAP
-- vendors are still served by the existing config-driven HTTP adapter
-- (internal/vendors/httpadapter already handles XML/SOAP bodies via
-- adapter_config, e.g. the ZONG .NET/SOAP sample vendor), this only changes
-- which vendors that worker picks up.
--
-- smpp_config/http_config -> protocol_config: confirmed dead columns before
-- merging — neither is read anywhere in the Go engine (P3/smpp-vendor reads
-- connection details from queue_config; P4/http-vendor reads adapter_config;
-- these two were landed by the Hadal reconciliation and never wired to
-- anything). Merged into one JSON column since nothing distinguishes them
-- structurally today and nothing reads either, consistent with "vendors
-- gains ... deliberately additive, inert until a follow-up pass" elsewhere
-- in this reconciliation.
ALTER TABLE vendors
  MODIFY COLUMN transport ENUM('smpp','http','soap','blackhole') NOT NULL;

ALTER TABLE vendors
  ADD COLUMN protocol_config JSON DEFAULT NULL AFTER encodings;

UPDATE vendors
   SET protocol_config = COALESCE(smpp_config, http_config)
 WHERE smpp_config IS NOT NULL OR http_config IS NOT NULL;

ALTER TABLE vendors
  DROP COLUMN smpp_config,
  DROP COLUMN http_config;
