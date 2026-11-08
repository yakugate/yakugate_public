-- Two more abuse signals (P1 register 1.2.4, ruled to P10 2026-07-17, built
-- 2026-09-09).
--
-- badnum — the share of an account's HLR-consulted destinations that came back
-- unknown. Real customer databases do not contain many numbers no network
-- claims; a high share is the signature of a scraped or purchased list. It is
-- a fraud signal and a route-quality problem at once, because the carrier sees
-- the same undeliverable traffic and eventually says so.
--
-- The divisor is CONSULTED, not total: an account supplying its own operator
-- codes skips HLR, and counting those messages would flatter exactly the
-- account best placed to hide a bad list. min_msgs 100 keeps the ratio
-- meaningful — two misses out of three is 67% and means nothing.
--
-- charset — an account sending in an encoding it has essentially never used.
-- A GSM-7-only sender suddenly emitting UCS-2 is what a compromised account
-- pushing somebody else's content looks like.
--
-- WHY THIS ONE ONLY EVER WARNS. The record stores the ENCODING, not the
-- script, so UCS-2 covers Urdu, Arabic, Chinese and emoji alike. This cannot
-- separate a hijacked account from a customer launching a legitimate Urdu
-- campaign — a real and likely event. share_throttle and share_suspend are
-- therefore left unset, and the evaluator treats an unset threshold as "never
-- fires". A warning asks a human to look, which is the most this evidence
-- honestly supports. Scoring it properly needs the dominant script recorded
-- per message; until then, do not raise these without deciding who handles
-- the false positives.

INSERT IGNORE INTO abuse_rules (signal_name, is_disabled, params) VALUES
  ('badnum', 0, JSON_OBJECT(
     'window_min',     60,
     'min_msgs',       100,
     'ratio_warn',     0.20,
     'ratio_throttle', 0.40,
     'ratio_suspend',  0.70,
     'throttle_pct',   10,
     'cooldown_min',   60
  )),
  ('charset', 0, JSON_OBJECT(
     'window_min',     60,
     'baseline_hours', 24,
     'min_msgs',       100,
     'min_baseline',   200,
     'share_warn',     0.50,
     'cooldown_min',   180
  ));
