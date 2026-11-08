-- Every HLR lookup is kept, whether its message was accepted or refused
-- (Saleem, 6 Nov 2026: "save all hlr calls, whether sms accepted or not"). A
-- live lookup (an HLR provider asked, found or not) of a message refused
-- after it — no route, a duplicate, the queue — now writes its log row too,
-- with the refusal's code here and no charge (sell_rate_id NULL). NULL = the
-- message was accepted, or a lookup with no message. The day tables made
-- before get the column from the template at start (schema_clones). The twin
-- of the mssql file of the same name.
ALTER TABLE tpl_day_hlr ADD COLUMN refused_reason VARCHAR(40) DEFAULT NULL;
