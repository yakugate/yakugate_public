-- Signature pass, queue-store half (D-084 loop guards): the expanded
-- vendor-error actions need per-message state on the queue row —
-- hlr_refreshed caps hlr_retry at ONCE per message (a re-dip returning the
-- same mccmnc would otherwise loop forever); tried_vendors is failover's
-- skip-list so a message never revisits a vendor that already refused it.
ALTER TABLE queue_template
  ADD COLUMN hlr_refreshed TINYINT(1) NOT NULL DEFAULT 0,
  ADD COLUMN tried_vendors JSON DEFAULT NULL;

ALTER TABLE queue_testvendor
  ADD COLUMN hlr_refreshed TINYINT(1) NOT NULL DEFAULT 0,
  ADD COLUMN tried_vendors JSON DEFAULT NULL;
