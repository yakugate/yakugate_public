-- Per-destination frequency cap (register row 1.2.8) — how many messages one
-- account may send to the SAME number inside a fixed window.
--
-- This is NOT duplicate detection. Dedupe's fingerprint is
-- sender + body + destination, so it only fires on the identical message sent
-- twice and replays the original id. Ten DIFFERENT messages to one number in
-- an hour have ten different fingerprints and sail straight through it. That
-- is the case this cap exists for: bombing and harassment of one subscriber,
-- where every message is unique but the volume to one person is the abuse.
--
--   dest_cap_hourly -> most messages to one number in a UTC clock hour
--   dest_cap_daily  -> most messages to one number in a UTC day
--
-- Both NULL by DEFAULT, which means OFF, and off is free: the stage returns
-- before touching a counter, so an account that does not use this pays
-- nothing on the hot path.
--
-- Windows are FIXED periods stamped into the counter key, exactly like the
-- company and country quotas (PO 2026-09-09, chosen over a rolling window).
-- Be aware of what fixed means at the edge: with dest_cap_hourly=5, five at
-- 10:59 and five more at 11:01 are both allowed -- ten in two minutes. A
-- rolling window would close that, at the cost of keeping a timestamp list
-- per destination. The daily cap is the backstop for exactly that gap, which
-- is why there are two windows rather than one.
--
-- Scope is per ACCOUNT (PO 2026-09-09), matching blocklist, whitelist and
-- dedupe_ttl_sec. Consequence to know: a tenant holding three accounts can
-- send 3x the cap to the same subscriber, because each account counts on its
-- own. Company-wide was the alternative and was not chosen.
--
-- Counting happens at ENQUEUE, never at check time (the chunk-6 ruling 6.1
-- shape quota already uses): a message refused further down the pipeline --
-- no balance, no route, spam -- does not consume the recipient's allowance.
-- The same ruling's known race applies here too: two submits arriving
-- together can both read 4-of-5 and both enqueue, so the cap is a close
-- ceiling rather than a hard one.

ALTER TABLE sms_service_config
  ADD COLUMN dest_cap_hourly INT UNSIGNED NULL
    COMMENT 'Max messages to one destination per UTC hour (NULL = no cap)',
  ADD COLUMN dest_cap_daily INT UNSIGNED NULL
    COMMENT 'Max messages to one destination per UTC day (NULL = no cap)';
