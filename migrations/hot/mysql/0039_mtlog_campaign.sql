-- 0039_mtlog_campaign.sql — the client's campaignId on the record, and both
-- campaign identifiers findable (P10 register "Content-hash campaign
-- signature", ruled 2026-09-09).
--
-- campaign_hash has been on mtlog since hot/0002 and was never written; it is
-- filled from 2026-09-14 (queue/0030 carries it from accept). campaign_id is
-- new. Both are indexed on the template so "what else went out under this
-- campaign" does not read a whole day: daily tables made from now on have the
-- indexes; existing ones get the column at start-up (bootstrap
-- syncCloneColumns) but not the index, and are searched by scan.
ALTER TABLE mtlog_template
  ADD COLUMN campaign_id VARCHAR(100) NULL
    COMMENT 'the client''s campaignId, as sent' AFTER campaign_hash,
  ADD INDEX idx_campaign_hash (campaign_hash),
  ADD INDEX idx_campaign_id (campaign_id);
