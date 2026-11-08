-- Per-vendor delivery-receipt code overrides (P3 register 3.9.2 / 3.9.4).
--
-- vendor_status_codes has had a `dlr` scope since it was created, and P3 loads
-- those rows — but nothing has ever consulted them, so a per-vendor receipt
-- rule silently did nothing. Two things were missing.
--
-- 1. target_status.
--
-- A receipt carries two useful parts: `stat:` (the vendor's word for what
-- happened) and `err:` (their numeric reason). We read stat: only, so one
-- UNDELIV covers three different situations:
--
--     err 000  the number does not exist       -> give up, it will never work
--     err 0AB  handset off / out of coverage   -> would likely work later
--     err 058  the vendor was throttling us    -> our fault, send it again
--
-- All three become `failed` today, so a dead number and a phone in a lift are
-- reported to the customer identically.
--
-- The existing `action` column answers "what do we DO" (give up, retry, slow
-- down). It cannot answer "what does the message BECOME on the record", and
-- the two are different questions: a rule saying `retry` with no target status
-- leaves the record marked FAILED while we are still retrying it — telling the
-- customer it failed and then delivering it. target_status is what stops that.
--
-- 2. A per-vendor opt-in.
--
-- PO ruling 2026-09-09: overrides apply only to a vendor that has asked for
-- them. Default off means every vendor keeps exactly today's behaviour, and a
-- half-finished code table cannot start changing outcomes the moment a row is
-- added. Turning it on is then a deliberate act per vendor.
--
-- Deliberately NOT added: is_default (a per-vendor catch-all). Ruled 2026-09-09
-- that an unlisted code keeps using our generic mapping — DELIVRD delivered,
-- UNDELIV failed — so a vendor does not need a complete code list before it can
-- go live.

ALTER TABLE vendor_status_codes
  ADD COLUMN target_status VARCHAR(12) NULL
    COMMENT 'what the MESSAGE becomes: delivered|failed|expired|sent. NULL = leave the status to the generic mapping and let `action` decide only what we do next';

ALTER TABLE vendors
  ADD COLUMN dlr_code_overrides TINYINT(1) NOT NULL DEFAULT 0
    COMMENT 'opt-in (PO 2026-09-09): 0 = ignore this vendor rows in vendor_status_codes scope=dlr, exactly as before. 1 = consult them when a receipt arrives';
