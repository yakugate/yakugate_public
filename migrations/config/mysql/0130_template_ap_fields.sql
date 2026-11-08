-- ============================================================================
-- 0130_template_ap_fields.sql — a template's own ap1/ap2/apg1/apg2 (Saleem,
-- 2026-09-29: "add template wise apgs replacement to channel")
--
-- templates.channel was a free tag nothing read (retired the same day). In its
-- place a template carries the four client pass-through fields: when a message
-- matches it on an account with the template check on, each field the CLIENT
-- left empty is filled from the template — the client's own value always wins,
-- the same rule as default_priority. The values then travel like a client's
-- (queue row → record, and an HTTP vendor call may pass them on).
--
-- One column per statement: the bootstrap skips a whole statement whose first
-- column already exists.
-- ============================================================================
ALTER TABLE templates ADD COLUMN ap1 VARCHAR(255) NULL;
ALTER TABLE templates ADD COLUMN ap2 VARCHAR(255) NULL;
ALTER TABLE templates ADD COLUMN apg1 VARCHAR(255) NULL;
ALTER TABLE templates ADD COLUMN apg2 VARCHAR(255) NULL;
