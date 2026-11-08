-- ============================================================================
-- 0014_abuse_rules.sql — Abuse Monitor signal rules (2026-07-20)
-- P10/D-034: thresholds are DATA, not code (ADR-013). One row per detection
-- signal; params JSON carries the signal's tuning + its warn/throttle/suspend
-- thresholds. The abuse-monitor worker reloads this each sweep, so operators
-- tune (or disable) a signal without recompiling; P12 gets a UI surface later.
--
-- Common params: window_min (sliding window), cooldown_min (min gap between
-- repeated actions on one account), throttle_pct (throughput % kept on throttle).
-- Per signal:
--   ait      min_msgs + uniq_ratio_* (unique-destination ratio) + prefix_len /
--            prefix_share (single-prefix concentration; AIT pumping targets one range)
--   spend    baseline_hours trailing average vs current window; mult_* multipliers;
--            min_baseline floor so brand-new accounts aren't divided by ~zero
--   spam     normalized+leet-folded body matches vs spam_keywords; hits_* counts
--   template drift ratio of non-template-matching bodies (accounts with templates
--            that are NOT accept-time enforced); min_msgs gate + ratio_* levels
-- ============================================================================

CREATE TABLE abuse_rules (
  id          INT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  signal_name VARCHAR(32) NOT NULL UNIQUE,
  is_disabled TINYINT(1) NOT NULL DEFAULT 0,
  params      JSON NOT NULL,
  created_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO abuse_rules (signal_name, params) VALUES
('ait', JSON_OBJECT(
  'window_min', 10, 'cooldown_min', 60, 'throttle_pct', 10,
  'min_msgs', 50, 'prefix_len', 5,
  'uniq_ratio_warn', 0.85, 'uniq_ratio_throttle', 0.92, 'uniq_ratio_suspend', 0.97,
  'prefix_share', 0.60)),
('spend', JSON_OBJECT(
  'window_min', 60, 'cooldown_min', 60, 'throttle_pct', 10,
  'baseline_hours', 24, 'min_baseline', 1.0,
  'mult_warn', 3, 'mult_throttle', 5, 'mult_suspend', 10)),
('spam', JSON_OBJECT(
  'window_min', 60, 'cooldown_min', 60, 'throttle_pct', 10,
  'scan_limit', 2000,
  'hits_warn', 5, 'hits_throttle', 20, 'hits_suspend', 50)),
('template', JSON_OBJECT(
  'window_min', 60, 'cooldown_min', 60, 'throttle_pct', 10,
  'scan_limit', 2000, 'min_msgs', 10,
  'ratio_warn', 0.30, 'ratio_throttle', 0.60, 'ratio_suspend', 0.90));
