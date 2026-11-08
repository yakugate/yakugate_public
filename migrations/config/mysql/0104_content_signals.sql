-- 0104_content_signals.sql — the P10 content signals and the weighted score
-- (register rulings of 2026-09-09, wired 2026-09-14).
--
-- 1. Brand impersonation reuses sender_ids (PO 2026-09-09) instead of a new
--    table: it already records which account may use which name, with an
--    operator approval step and a screen. But "may SEND AS" is not "may
--    MENTION": if every approved sender id were protected, a generic one like
--    INFO or ALERT would flag everybody's ordinary text. So only a row an
--    operator marks here protects its name in message bodies. The accounts
--    entitled to write it are those holding it as an approved sender id.
ALTER TABLE sender_ids
  ADD COLUMN body_protected TINYINT(1) NOT NULL DEFAULT 0
    COMMENT 'operator: this name may appear in message text only from accounts entitled to it (P10 brand signal)';

-- 2. The rules. formatting, optout and brand are score inputs only (ruled):
--    they compute a strength and never act alone. score sums the weighted
--    inputs — the four ladder signals the ruling named plus the three content
--    signals — and may warn or throttle, never suspend (ruled), and only when
--    at least two different signals agree. A signal missing from "weights"
--    counts for nothing. Live on apply, like the other signals (0093, 0094).
INSERT IGNORE INTO abuse_rules (signal_name, is_disabled, params) VALUES
  ('formatting', 0, JSON_OBJECT(
     'window_min',        60,
     'baseline_hours',    24,
     'min_window_msgs',   20,
     'min_baseline_msgs', 50,
     'delta_warn',        0.30,
     'delta_full',        0.60,
     'padding_full',      0.05,
     'sample_msgs',       200,
     'scan_limit',        5000
  )),
  ('optout', 0, JSON_OBJECT(
     'window_min',        1440,
     'min_messages',      500,
     'per_thousand_warn', 5,
     'per_thousand_full', 30
  )),
  ('brand', 0, JSON_OBJECT(
     'window_min',     60,
     'min_messages',   3,
     'disguised_full', 0.9,
     'plain_full',     0.5,
     'scan_limit',     2000
  )),
  ('score', 0, JSON_OBJECT(
     'window_min',      60,
     'weights',         JSON_OBJECT('ait', 30, 'spend', 30, 'spam', 30, 'template', 20,
                                    'formatting', 10, 'optout', 20, 'brand', 40),
     'points_warn',     25,
     'points_throttle', 50,
     'min_signals',     2,
     'throttle_pct',    10,
     'cooldown_min',    60
  ));
