-- An operator's reset of an account's reputation (Saleem 2026-10-07:
-- "also need reputation reset button").
--
-- The score (config/0107) is rewritten once a day from the last 90 / 30 days
-- of evidence, so a number put back to 100 by hand would be 48 again the next
-- morning. A reset is therefore a MOMENT: the abuse-monitor role counts only
-- what happened after `reset_at` (abuse actions, delivery, number lookups,
-- opt-outs, refusals). The account's age still counts — it is a fact, not
-- evidence. Nothing acts on the score, before or after.
--
-- Written by the Admin API (POST /api/accounts/:id/reputation/reset); the
-- role's own daily rewrite leaves both columns alone. A reset newer than the
-- row's `computed_at` makes the role score again at its next sweep.
--
-- One column per ALTER (the bootstrap skips a whole statement when its first
-- column is already there).
ALTER TABLE account_reputation
  ADD COLUMN reset_at DATETIME NULL
      COMMENT 'UTC. An operator reset the reputation here: evidence before it is not counted';

ALTER TABLE account_reputation
  ADD COLUMN reset_by VARCHAR(128) NULL
      COMMENT 'Who reset it (a name for people to read)';
