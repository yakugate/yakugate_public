-- Monitoring history: throughput by direction, vendor latency, queue age.
--
-- 0033 lifted ONE throughput column, `tps`, filled only by the two ingress
-- roles. Every other role wrote 0, and the overview summed the column across
-- roles — which was right only as long as nothing else reported. The moment
-- vendor, receipt and MO roles publish their own rates, one summed column
-- counts each message three or four times.
--
-- So the rate is split by DIRECTION (PO 2026-09-11, "separate lines"). `tps`
-- keeps its meaning — messages accepted from clients — and three siblings sit
-- beside it. A gap between tps and tps_out is a queue building up.
--
-- vendor_p95_ms is kept apart from latency_p95_ms for the same reason: the
-- pipeline's p95 is milliseconds, a saturated SMPP window is seconds, and one
-- column holding both would flatten the pipeline line into noise.
--
-- queue_age_sec: depth alone cannot tell busy from stalled. Depth AND age
-- rising together is a vendor that has stopped taking messages (PO
-- 2026-07-27); depth rising with age flat is a burst being worked off.
--
-- RETENTION, superseding 0033's three-tier plan (PO 2026-09-11): raw 5-second
-- samples are kept for 7 days and then dropped. No rollups. 0033's `tier`
-- column stays (applied migrations are immutable) and is simply always 0.
-- 0033 also said "partitioned by day" but created only p_start and p_max, so
-- every row sat in one partition; the engine now splits p_max into daily
-- partitions ahead of time and drops expired ones (monitor.SQLSink.Maintain).

ALTER TABLE monitor_samples
  ADD COLUMN tps_out       DECIMAL(10,2) NOT NULL DEFAULT 0 COMMENT 'messages a vendor accepted, per second' AFTER tps,
  ADD COLUMN tps_receipts  DECIMAL(10,2) NOT NULL DEFAULT 0 COMMENT 'delivery receipts received from vendors, per second' AFTER tps_out,
  ADD COLUMN tps_mo        DECIMAL(10,2) NOT NULL DEFAULT 0 COMMENT 'inbound MO received from vendors, per second' AFTER tps_receipts,
  ADD COLUMN vendor_p95_ms DECIMAL(10,2) NOT NULL DEFAULT 0 COMMENT 'vendor round trip p95; 0 = not measured' AFTER latency_p95_ms,
  ADD COLUMN queue_age_sec BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'oldest pending message across vendor queues' AFTER queue_depth;
