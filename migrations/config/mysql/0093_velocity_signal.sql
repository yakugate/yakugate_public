-- Burst velocity: a fifth abuse signal (P1 register 1.2.4, ruled to P10 on
-- 2026-07-17, built 2026-09-09).
--
-- The gap it fills, precisely. EvalAIT needs the traffic CONCENTRATED in one
-- number range and returns None the moment it is spread — that concentration
-- is what makes AIT pumping recognisable, so it is right to ignore everything
-- else. EvalSpend notices a spike only through COST, so cheap traffic, or an
-- account whose baseline is already generous, can climb a long way unnoticed.
--
-- Between them sits the case nothing watched: an account suddenly sending far
-- more than it normally does, at ordinary prices, spread across networks.
-- That is the shape of a leaked API key or a compromised account pushing
-- somebody else's traffic.
--
-- Thresholds are deliberately looser than spend's (5/10/25 against spend's
-- 3/5/10). Message count is a noisier measure than money — a legitimate
-- campaign is a real burst — so the multiples have to be high enough that a
-- planned send does not suspend a customer on a Monday morning.
--
-- min_msgs 200 is the quiet-window floor: an account that normally sends three
-- messages an hour and sends thirty is not bursting, and firing on that would
-- teach an operator to ignore this signal.
--
-- min_baseline 20 is the least history the signal will judge on, NOT a floor
-- applied to the divisor. The difference matters and flooring was tried first:
-- set high enough to protect a brand-new account, a floor also swallows real
-- baselines, so an established customer normally sending 100 an hour ends up
-- measured against the floor rather than against itself and its genuine
-- fivefold burst scores as two.
--
-- "Is this account sending far more than usual" has no answer when there is no
-- usual. So an account with less than 20 messages per window of history is not
-- scored by this signal at all until it has some — which also means a new
-- customer's first campaign, however large, cannot be throttled by it.
--
-- INSERT IGNORE, and is_disabled defaults 0 so it is live on apply. The ladder
-- is warn -> throttle -> suspend, identical to the other four (PO ruling).
INSERT IGNORE INTO abuse_rules (signal_name, is_disabled, params) VALUES
  ('velocity', 0, JSON_OBJECT(
     'window_min',     60,
     'baseline_hours', 24,
     'min_msgs',       200,
     'min_baseline',   20.0,
     'mult_warn',      5,
     'mult_throttle',  10,
     'mult_suspend',   25,
     'throttle_pct',   10,
     'cooldown_min',   60
  ));
