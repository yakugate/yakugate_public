-- S-11 / PO 2026-07-27: a template may force its own priority when the client
-- omits `priority` from the request. 0 = no override, the generic default (9)
-- applies as before. Purely additive — every existing row keeps today's
-- behavior.
ALTER TABLE templates
  ADD COLUMN default_priority INT NOT NULL DEFAULT 0 AFTER regex;
