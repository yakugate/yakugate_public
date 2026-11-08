-- 0100_kms_http_providers.sql — HTTP key services (PO 2026-09-13).
--
-- A company's master key may now live at an HTTP key service (Vault Transit,
-- an in-house key service, an HSM gateway): an http_endpoints row of type
-- 'kms' whose config describes an encrypt call and a decrypt call. A stored
-- data key names its master key as http-kms://<endpoint id>/<key name>.
--
-- data_keys.kek_ref was VARCHAR(128) while tenant_kms_config.key_ref allows
-- 500: a long key name made every submit for that company fail on insert.
-- wrapped_dek grows too — a provider's locked key is text (Vault's
-- "vault:v1:…"), and RSA-wrapping providers return 256+ bytes before
-- encoding.
ALTER TABLE data_keys
  MODIFY kek_ref     VARCHAR(600)    NOT NULL,
  MODIFY wrapped_dek VARBINARY(2048) NOT NULL;
