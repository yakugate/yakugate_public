-- The sample vendors leave a new install (Saleem, 2026-10-02).
--
-- config/0052 and 0053 put six example HTTP vendors (cid sample_ufone,
-- sample_telenor, sample_zong, sample_cmt, sample_jazz, sample_v3) and their
-- ten HTTP calls into every config store, paused, with made-up URLs. A fresh
-- install therefore opened with six vendors and ten HTTP calls nobody made,
-- and the console's and portal's lists started full of them (CLI end-to-end
-- run, 2026-10-02, finding 4).
--
-- Only the UNUSED ones go. A sample vendor stays where anyone has used it:
--   * a live routing rule names it (routes.vendor_id, or anywhere in a
--     send_plan, exceptions included);
--   * it has status codes of its own (vendor_status_codes);
--   * someone set up one of its inbound doors (a credential, a dialect or an
--     address list) — config/0121 gave every vendor two empty doors, and an
--     empty door is not use;
--   * it was switched on, or renamed away from "Sample: …".
-- (Traffic is not checked here: the summaries that would show it belong to
-- the Admin API, and a sample vendor has made-up URLs, so it never sent.)
--
-- Retired, not destroyed: each row is copied to a *_retired_samples table
-- first (vendors, http_endpoints, inbound_endpoints). Nothing reads the
-- copies. A door's stored secret (Admin API inbound_secrets) goes with the
-- door by its own ON DELETE CASCADE; a retired door has no credential, so
-- there is nothing in it to lose.
CREATE TABLE IF NOT EXISTS vendors_retired_samples LIKE vendors;

CREATE TABLE IF NOT EXISTS http_endpoints_retired_samples LIKE http_endpoints;

CREATE TABLE IF NOT EXISTS inbound_endpoints_retired_samples LIKE inbound_endpoints;

INSERT IGNORE INTO vendors_retired_samples
SELECT v.* FROM vendors v
WHERE v.cid IN ('sample_ufone','sample_telenor','sample_zong','sample_cmt','sample_jazz','sample_v3')
  AND v.service_status = 'paused'
  AND v.name LIKE 'Sample:%'
  AND NOT EXISTS (SELECT 1 FROM routes r
                  WHERE r.vendor_id = v.id
                     OR CAST(r.send_plan AS CHAR) REGEXP CONCAT('"vendor_id": *"?', v.id, '[^0-9]'))
  AND NOT EXISTS (SELECT 1 FROM vendor_status_codes c WHERE c.vendor_id = v.id)
  AND NOT EXISTS (SELECT 1 FROM inbound_endpoints i
                  WHERE i.vendor_id = v.id
                    AND (i.jwt IS NOT NULL OR i.token_hash IS NOT NULL OR i.username IS NOT NULL
                         OR i.password IS NOT NULL OR i.dialect_id IS NOT NULL
                         OR (i.allowed_ip_list IS NOT NULL AND JSON_LENGTH(i.allowed_ip_list) > 0)));

INSERT IGNORE INTO http_endpoints_retired_samples
SELECT e.* FROM http_endpoints e
WHERE e.vendor_id IN (SELECT id FROM vendors_retired_samples);

INSERT IGNORE INTO inbound_endpoints_retired_samples
SELECT i.* FROM inbound_endpoints i
WHERE i.vendor_id IN (SELECT id FROM vendors_retired_samples);

DELETE FROM inbound_endpoints WHERE vendor_id IN (SELECT id FROM vendors_retired_samples);

DELETE FROM http_endpoints WHERE vendor_id IN (SELECT id FROM vendors_retired_samples);

DELETE FROM vendors WHERE id IN (SELECT id FROM vendors_retired_samples);
