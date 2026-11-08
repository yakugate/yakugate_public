-- ============================================================================
-- 0015_spam_strict.sql — per-account strict spam matching (2026-07-20)
-- D-035: opt-in variant rejection at accept time. Accounts with
-- enforce_spam_strict=1 get their bodies normalized (lowercase, separators and
-- zero-width stripped, leet/homoglyph folded) and matched against the same
-- normalized spam keywords — so "l.o.a.n.o.f.f.e.r" / "l0an0ffer" reject 400
-- inline. Opt-in ONLY: variant matching inherently false-positives on legit
-- bodies ("personal loan. Offer ends Friday" folds to a hit), so the operator
-- chooses per account which side of that trade to take. Default stays exact
-- (enforce_spam_check); the async P10 monitor scores everyone regardless.
-- ============================================================================

ALTER TABLE accounts
  ADD COLUMN enforce_spam_strict TINYINT(1) NOT NULL DEFAULT 0 AFTER enforce_spam_check;
