-- Trap ("honeypot") numbers — register row 1.2.8.
--
-- A trap number belongs to NOBODY. Anti-spam groups, regulators and operators
-- create them and plant them where list-scrapers will find them: web pages,
-- old databases, lists that get sold around. Nobody ever opts in on one. So a
-- send to a trap has exactly one explanation -- the sender's recipient list
-- was scraped, bought or stolen.
--
-- Why this is not just another blocklist row:
--
--   a DND row  = a REAL person asked not to be texted. Thousands a day. Routine.
--   a trap row = nobody is there. One hit says the sender's WHOLE list is dirty,
--                and the operator or regulator saw it happen too. That is the
--                road to filtered sender IDs, blocked routes and fines -- on
--                us, the aggregator, not only on the customer.
--
-- Mechanically the two are the same instruction ("do not send here"), which is
-- why this reuses the blocklist rather than adding a table. What differs is
-- what a hit MEANS, so the engine needs to be able to tell them apart -- until
-- now source could not say "trap", and a trap hit was invisible.
--
-- PO 2026-09-09, two rulings baked in downstream:
--
--   1. A trap hit REFUSES and RAISES AN ALARM, and stops there. No automatic
--      suspension: one mistyped number in this list would otherwise take a
--      paying customer offline with no human in the loop.
--   2. The sender is NEVER told it was a trap. The client sees the same
--      403 DESTINATION_BLOCKED as any other blocked number. A distinct code
--      would let anyone with an account probe the gateway to find and strip
--      every trap from their list -- destroying the traps AND confirming the
--      rest of the list is safe to hit. For the same reason the alarm goes to
--      the operator's system log only, never to the customer's own log file.
--
-- Trap rows should be GLOBAL (account_id and company_id NULL): no account may
-- ever send to one. Nothing here enforces that -- scope stays the blocklist's
-- own three-tier rule -- but an account-scoped trap row would only be reached
-- by that one account, which is almost certainly not what the operator meant.
--
-- Note you cannot buy a complete trap list; a published trap is a dead trap.
-- These are learned one at a time, usually from an operator complaint naming
-- the number. That is why loading reuses the existing blocklist import rather
-- than expecting a feed.

ALTER TABLE blocklist
  MODIFY COLUMN source ENUM('manual','unsub','abuse','honeypot')
    NOT NULL DEFAULT 'manual'
    COMMENT 'manual/unsub/abuse = an ordinary block; honeypot = a trap number, refuse AND alarm';
