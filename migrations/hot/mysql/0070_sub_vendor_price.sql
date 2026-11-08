-- The three-table design (Saleem and the PO, 27 Oct 2026): mt_ is written at
-- ACCEPTANCE and holds nothing about the vendor; every vendor answer is a
-- sub_ row; every receipt is a dlr_ row. What belonged to a submission and
-- sat on mt_ moves to sub_: the vendor's price of that try (its rate row and
-- the rates it was converted at, fixed when the vendor answered) and the
-- routing round it was sent in. One column per ALTER (the bootstrap rule).
ALTER TABLE tpl_day_sub ADD COLUMN buy_rate_id BIGINT UNSIGNED DEFAULT NULL COMMENT 'rate_card_rates.id of the vendor''s price of this try' AFTER session;
ALTER TABLE tpl_day_sub ADD COLUMN buy_fx_rate DECIMAL(24,12) DEFAULT NULL COMMENT 'buying card currency -> the vendor''s company currency' AFTER buy_rate_id;
ALTER TABLE tpl_day_sub ADD COLUMN buy_base_rate DECIMAL(24,12) DEFAULT NULL COMMENT 'the vendor''s company currency -> base' AFTER buy_fx_rate;
ALTER TABLE tpl_day_sub ADD COLUMN route_id BIGINT UNSIGNED DEFAULT NULL COMMENT 'the routing rule of this try' AFTER buy_base_rate;
ALTER TABLE tpl_day_sub ADD COLUMN route_level INT DEFAULT NULL COMMENT 'main / backup level of the plan' AFTER route_id;
ALTER TABLE tpl_day_sub ADD COLUMN send_round INT DEFAULT NULL COMMENT 'the round of retries' AFTER route_level;
