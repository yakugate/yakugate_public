-- What a spam rule DOES when it matches (register row 1.2.4).
--
-- Until now a rule had exactly one outcome: refuse the message. That makes
-- adding a rule a gamble. Write one for "loan", switch it on, and you find out
-- on Monday that a bank customer sending "your loan application is approved"
-- was refused all weekend - real messages, real money, an angry call. There
-- was no way to ask what a rule WOULD do before it did it.
--
--   reject           - refuse the message. Exactly today's behaviour, and the
--                      default, so this migration changes nothing by itself.
--   flag             - SEND the message normally and mark the record. The
--                      customer notices nothing. This is observe mode: switch
--                      a new rule on as flag, read a week of records, then
--                      move it to reject once you trust it.
--   require_template - do not refuse outright, but demand the text match an
--                      approved template. Matches one, it goes; matches none,
--                      422 TEMPLATE_MISMATCH. For vocabulary that is fine in
--                      wording the operator approved and not fine freestyle.
--
-- PO 2026-09-09 ruled this PER RULE rather than per account, because that is
-- how a rule is actually rolled out: add it in flag mode, watch, then enforce.
-- A per-account switch would soften a bad rule for one customer while it kept
-- refusing everyone else.
--
-- Note on evaluation cost: require_template needs the template stage to have
-- formed a verdict, and that stage returns immediately for the many accounts
-- with enforce_template_check off. The engine therefore only asks it to
-- evaluate when some rule in scope actually uses require_template, so an
-- operator who never touches this pays nothing on the hot path.

ALTER TABLE spam_keywords
  ADD COLUMN action ENUM('reject','flag','require_template') NOT NULL DEFAULT 'reject'
    COMMENT 'reject = refuse (default), flag = send but mark the record, require_template = allow only in approved wording'
    AFTER list_type;
