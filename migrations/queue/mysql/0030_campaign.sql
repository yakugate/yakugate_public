-- 0030_campaign.sql — carry the campaign from accept to the record (P10
-- register "Content-hash campaign signature", ruled 2026-09-09).
--
-- campaign_id: the client's own campaignId. The API accepted it, validated it
-- and carried it on the message — then dropped it: neither the queue INSERT
-- nor the record INSERT named it, so a customer could tag campaigns and the
-- tag was thrown away.
--
-- campaign_hash: the gateway's own signature of the text (abuse.CampaignHash:
-- disguises folded, digits and short words dropped, words sorted), taken at
-- accept while the text is still readable — an encrypted company's body is
-- sealed before it is queued. The id says what the sender CALLED it; the hash
-- says what it LOOKS like, across accounts that never coordinated. For
-- investigation only: nothing gates on either.
--
-- Same "carry it on the queue row" path as spam_flag (0027). Existing
-- queue_<vendor> clones follow the template at start-up.
ALTER TABLE queue_template
  ADD COLUMN campaign_id VARCHAR(100) NULL
    COMMENT 'the client''s campaignId, as sent',
  ADD COLUMN campaign_hash CHAR(64) NULL
    COMMENT 'signature of the text (abuse.CampaignHash), for "what else went out like this"';
