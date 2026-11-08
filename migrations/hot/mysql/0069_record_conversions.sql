-- The rest of a message's conversions on its record (Saleem, 27 Oct 2026:
-- card -> company -> base, every rate kept). fx_rate (the selling card's
-- currency -> the customer company's) was there; these complete the chain,
-- each fixed when the message was priced. NULL = not known (a record written
-- before, or no rate).
--
--   base_rate      the customer company's currency -> the base currency
--   buy_fx_rate    the buying card's currency -> the vendor's own company's
--   buy_base_rate  the vendor's company's currency -> the base currency
--
-- On the templates only; the day tables that exist get them when the gateway
-- starts (bootstrap syncCloneColumns). A receipt row is read at its message's
-- rates, as fx_rate is. Additions only, one column per ALTER.
ALTER TABLE tpl_day_mt ADD COLUMN base_rate DECIMAL(24,12) DEFAULT NULL COMMENT 'customer company currency -> base' AFTER fx_rate;
ALTER TABLE tpl_day_mt ADD COLUMN buy_fx_rate DECIMAL(24,12) DEFAULT NULL COMMENT 'buying card currency -> the vendor''s company currency' AFTER base_rate;
ALTER TABLE tpl_day_mt ADD COLUMN buy_base_rate DECIMAL(24,12) DEFAULT NULL COMMENT 'the vendor''s company currency -> base' AFTER buy_fx_rate;
ALTER TABLE tpl_day_rejects ADD COLUMN base_rate DECIMAL(24,12) DEFAULT NULL AFTER fx_rate;
ALTER TABLE tpl_day_rejects ADD COLUMN buy_fx_rate DECIMAL(24,12) DEFAULT NULL AFTER base_rate;
ALTER TABLE tpl_day_rejects ADD COLUMN buy_base_rate DECIMAL(24,12) DEFAULT NULL AFTER buy_fx_rate;
ALTER TABLE tpl_day_mo ADD COLUMN base_rate DECIMAL(24,12) DEFAULT NULL AFTER fx_rate;
ALTER TABLE tpl_day_mo ADD COLUMN buy_fx_rate DECIMAL(24,12) DEFAULT NULL AFTER base_rate;
ALTER TABLE tpl_day_mo ADD COLUMN buy_base_rate DECIMAL(24,12) DEFAULT NULL AFTER buy_fx_rate;
ALTER TABLE tpl_day_hlr ADD COLUMN base_rate DECIMAL(24,12) DEFAULT NULL AFTER fx_rate;
