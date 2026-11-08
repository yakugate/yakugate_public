-- I-13 (PO 2026-09-17): the seeded KMS provider `default-file-kms` (config/0030,
-- carried into http_endpoints by config/0052) points at no encrypt/decrypt
-- call, yet showed as active and could be assigned to a company — which
-- would then have every message refused 503 (fail closed). Nothing can ever
-- have locked a key under it (the engine needs both calls), so it is removed
-- wherever it is still the unconfigured seed: no calls, no assignment, no
-- data key. A row an operator has since completed is left alone.
DELETE FROM http_endpoints
 WHERE type = 'kms'
   AND name = 'default-file-kms'
   AND JSON_EXTRACT(config, '$.encrypt_endpoint_id') IS NULL
   AND JSON_EXTRACT(config, '$.decrypt_endpoint_id') IS NULL
   AND NOT EXISTS (SELECT 1 FROM tenant_kms_config t WHERE t.kms_provider_id = http_endpoints.id)
   AND NOT EXISTS (SELECT 1 FROM data_keys k WHERE k.kek_ref LIKE CONCAT('http-kms://', http_endpoints.id, '/%'));
