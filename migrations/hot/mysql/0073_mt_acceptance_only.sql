-- The three-table design (Saleem and the PO, 27 Oct 2026): mt_ is written at
-- ACCEPTANCE and holds NOTHING about the vendor. Every vendor fact is in
-- sub_ (each try, each part). Saleem: dropped everywhere — here from the
-- template; the bootstrap drops them from every mt_ day table that exists
-- (dropCloneColumns). One change per ALTER (the bootstrap rule: a drop that
-- is done already is satisfied, 1091).
ALTER TABLE tpl_day_mt DROP INDEX idx_vendor_msg;
ALTER TABLE tpl_day_mt DROP INDEX idx_drill;
ALTER TABLE tpl_day_mt DROP COLUMN vendor_id;
ALTER TABLE tpl_day_mt DROP COLUMN vendor_msg_id;
ALTER TABLE tpl_day_mt DROP COLUMN vendor_protocol;
ALTER TABLE tpl_day_mt DROP COLUMN sent_parts;
ALTER TABLE tpl_day_mt DROP COLUMN attempts;
ALTER TABLE tpl_day_mt DROP COLUMN vendor_ack_at;
ALTER TABLE tpl_day_mt DROP COLUMN sent_deployment_id;
ALTER TABLE tpl_day_mt DROP COLUMN route_level;
ALTER TABLE tpl_day_mt DROP COLUMN send_round;
ALTER TABLE tpl_day_mt DROP COLUMN buy_rate_id;
ALTER TABLE tpl_day_mt DROP COLUMN buy_fx_rate;
ALTER TABLE tpl_day_mt DROP COLUMN buy_base_rate;
ALTER TABLE tpl_day_mt ADD KEY idx_drill (accepted_at);
