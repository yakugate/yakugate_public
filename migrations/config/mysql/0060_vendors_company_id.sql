-- 0060_vendors_company_id.sql — vendors.company_id (PO 2026-08-10): a vendor
-- can be owned by a specific company (a private carrier relationship one
-- tenant buys/sells through) rather than always being operator-wide/shared.
--
-- Deliberately a NEW column, not a repurposing of `cid` — `cid` is the
-- vendor's own load-bearing identity (queue table names `queue_<cid>`,
-- Redis keys `q:<cid>`/`qmsg:<cid>:<id>`, every vendor-egress process claims
-- work via `queue.Claim(ctx, vendor.cid, ...)`); overloading it to also mean
-- "owning company" would break queue routing engine-wide.
--
-- NULL (default) = operator-wide/shared, the existing behavior for every
-- vendor today — this is additive, nothing currently routing is affected
-- until an operator deliberately assigns a vendor to a company. No FK
-- constraint, matching sms_service_config.company_id's existing convention
-- in this schema.
ALTER TABLE vendors
  ADD COLUMN company_id BIGINT UNSIGNED NULL AFTER cid,
  ADD KEY idx_vendors_company (company_id);
