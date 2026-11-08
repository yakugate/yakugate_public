-- Hadal reconciliation, routing/vendor group (PO 2026-07-27): per-vendor
-- custom encoding/charset rules (Hadal's connectors.encodings — "worked well"
-- per the original review, config read whole, rarely written). Purely
-- additive; inert until a follow-up code pass reads it (today's encoding
-- plan is still the fixed global GSM7/UCS2/Latin1 logic in internal/encoding).
ALTER TABLE vendors
  ADD COLUMN encodings JSON DEFAULT NULL AFTER adapter_config;
