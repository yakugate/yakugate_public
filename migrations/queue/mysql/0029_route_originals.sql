-- 0029_route_originals.sql — keep what a route rewrite replaced (PO
-- 2026-09-14, routing rules review).
--
-- The winning route may rewrite a message's sender, number or text. When its
-- vendor then goes down, the message moves to the next route — and that route
-- must start from what the client sent, not from the first route's rewrite.
-- These columns hold the originals, and only when a rewrite changed them;
-- NULL means the value in sender/destination/body is the client's own.
-- orig_body is sealed exactly like body when the company's messages are
-- encrypted. Existing queue_<vendor> tables follow the template at start-up
-- (bootstrap syncCloneColumns).
ALTER TABLE queue_template
  ADD COLUMN orig_sender VARCHAR(32) NULL
    COMMENT 'sender before the route rewrote it; NULL = not rewritten',
  ADD COLUMN orig_destination VARCHAR(15) NULL
    COMMENT 'number before the route rewrote it; NULL = not rewritten',
  ADD COLUMN orig_body TEXT NULL
    COMMENT 'text before the route rewrote it (sealed like body); NULL = not rewritten';
