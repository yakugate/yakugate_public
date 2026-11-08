-- ============================================================================
-- 0009_ingress.sql — record client ingress protocol (2026-07-19)
-- `transport` = how the message LEAVES toward the vendor (egress; set by the
-- route stage from the matched vendor). `ingress` = how the CLIENT submitted it
-- (http via P1 / smpp via P2). Distinct facts: an SMPP-submitted message may
-- still egress to an HTTP vendor. Defaults to 'http' for legacy rows.
-- Template only: live queue_<vendor> tables have dynamic names — upgrade manually:
--   ALTER TABLE queue_<vendor>
--     ADD COLUMN ingress ENUM('http','smpp') NOT NULL DEFAULT 'http' AFTER transport;
-- New vendor queues clone the template and are born correct.
-- ============================================================================

ALTER TABLE queue_template
  ADD COLUMN ingress ENUM('http','smpp') NOT NULL DEFAULT 'http' AFTER transport;
