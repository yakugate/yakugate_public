-- The HLR provider's call kept with its lookup (Saleem, 6 Nov 2026: "in hlr
-- logs cant see request and response of hlr hit"). The twin of
-- migrations/hot/mssql/0080_hlr_log_call.sql.
--
-- A live lookup (source dip or dip_miss) now keeps the call it made to the
-- HLR provider, as a vendor's call is kept on its sub_ row (hot/0077):
-- method, URL, request headers and body, the answer's HTTP status, headers
-- and body, how long it took, and the error when the call failed. Secrets
-- are masked before the row is made; bodies are cut at 16 KB (request) and
-- 8 KB (answer), *_cut says so. The request carries the number only, never the
-- text, so nothing is sealed or masked. NULL
-- on a lookup that made no call (our HLR data, the ranges, the client's
-- code). YAKU_HLR_CALL_LOG (all | failed | off) says which lookups keep
-- their call. The day tables made before get the columns from the template
-- at start (schema_clones). One column per ALTER (gateway-run rule).
ALTER TABLE tpl_day_hlr ADD COLUMN method VARCHAR(8) DEFAULT NULL;
ALTER TABLE tpl_day_hlr ADD COLUMN url VARCHAR(2048) DEFAULT NULL;
ALTER TABLE tpl_day_hlr ADD COLUMN req_headers TEXT;
ALTER TABLE tpl_day_hlr ADD COLUMN req_body MEDIUMTEXT;
ALTER TABLE tpl_day_hlr ADD COLUMN req_cut TINYINT(1) NOT NULL DEFAULT 0;
ALTER TABLE tpl_day_hlr ADD COLUMN http_status SMALLINT DEFAULT NULL;
ALTER TABLE tpl_day_hlr ADD COLUMN resp_headers TEXT;
ALTER TABLE tpl_day_hlr ADD COLUMN resp_body MEDIUMTEXT;
ALTER TABLE tpl_day_hlr ADD COLUMN resp_cut TINYINT(1) NOT NULL DEFAULT 0;
ALTER TABLE tpl_day_hlr ADD COLUMN took_ms INT UNSIGNED DEFAULT NULL;
ALTER TABLE tpl_day_hlr ADD COLUMN call_error VARCHAR(500) DEFAULT NULL;
