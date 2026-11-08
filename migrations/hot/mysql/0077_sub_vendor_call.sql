-- An HTTP vendor's call kept raw with its answer (Saleem, 5 Nov 2026: "check
-- if we save vendor responses and requests in raw for mt, if not then save
-- them too, as they are also https"). The twin of
-- migrations/hot/mssql/0077_sub_vendor_call.sql.
--
-- A sub_ row is one answer of a vendor to one try of a message; for an HTTP
-- or SOAP vendor it now also keeps the call as it went: method, URL, request
-- headers and body, the answer's headers and body (its status is http_status),
-- and how long it took. Secrets are masked before the row is made (an
-- Authorization or cookie header, any header, query value, JSON, form or XML
-- field named like a key, token, secret, password or signature; a URL's
-- password). Bodies are cut at 16 KB (request) and 8 KB (answer); *_cut says
-- so. sealed = 1: both bodies are encrypted under the message's company key
-- (an all_encrypted company: the request carries the text), or were not kept
-- when the role had no encryption. A bulk call is kept on each message it
-- carried. NULL on an SMPP answer. YAKU_VENDOR_CALL_LOG (all | failed | off)
-- says which answers keep their call; the row itself is always written.
-- One column per ALTER (gateway-run rule).
ALTER TABLE tpl_day_sub ADD COLUMN method VARCHAR(8) DEFAULT NULL;
ALTER TABLE tpl_day_sub ADD COLUMN url VARCHAR(2048) DEFAULT NULL;
ALTER TABLE tpl_day_sub ADD COLUMN req_headers TEXT;
ALTER TABLE tpl_day_sub ADD COLUMN req_body MEDIUMTEXT;
ALTER TABLE tpl_day_sub ADD COLUMN req_cut TINYINT(1) NOT NULL DEFAULT 0;
ALTER TABLE tpl_day_sub ADD COLUMN resp_headers TEXT;
ALTER TABLE tpl_day_sub ADD COLUMN resp_body MEDIUMTEXT;
ALTER TABLE tpl_day_sub ADD COLUMN resp_cut TINYINT(1) NOT NULL DEFAULT 0;
ALTER TABLE tpl_day_sub ADD COLUMN sealed TINYINT(1) NOT NULL DEFAULT 0;
ALTER TABLE tpl_day_sub ADD COLUMN took_ms INT UNSIGNED DEFAULT NULL;
