-- GW-19.1 (PO approved 2026-08-27, "fields on the MT record" option): the HLR
-- lookup outcome is stamped on the record instead of a new hlrlog_* family —
-- no extra hot-path write, and summary_lookup becomes RECOMPUTE-able from the
-- log like every other summary. hlr_source: '' = not consulted; 'operator' =
-- client-supplied operator, HLR skipped; 'db' = cache/shard hit; 'dip' = live
-- external lookup; 'miss' = consulted, nothing found.
-- syncCloneColumns propagates these to existing mtlog_* clones at bootstrap.
ALTER TABLE mtlog_template
  ADD COLUMN hlr_source VARCHAR(8) NOT NULL DEFAULT '' AFTER mnc,
  ADD COLUMN hlr_ported TINYINT(1) NOT NULL DEFAULT 0 AFTER hlr_source;
