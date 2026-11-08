-- Reclassify the postpaid rows already in the ledger (follows config/0091).
--
-- 0091 gave postpaid charges their own type, but only for rows written after
-- it. The ones already there stayed 'commit' — misclassified in exactly the
-- way 0091 exists to prevent, so a historical invoice or a ledger filter would
-- still mix them with real debits.
--
-- Separate migration because 0091 is already applied and applied migrations
-- are immutable here: additions only, never edits.
--
-- THE PREDICATE IS EXACT, not a guess. A postpaid charge moves no money, so
-- previous_balance = new_balance; a prepaid commit always moves it, because
-- Charge() only writes a row when the price is greater than zero. There is no
-- third case: verified on this deployment before writing — all 17 commit rows
-- had an unchanged balance, none had a changed one.
--
-- Deliberately NOT touching amount, reference, reason or the balances. This
-- corrects what a row is CALLED, nothing about what it recorded.
UPDATE balance_history
   SET type = 'invoice_item'
 WHERE type = 'commit'
   AND previous_balance = new_balance;
