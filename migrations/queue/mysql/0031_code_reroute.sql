-- 0031_code_reroute.sql — a vendor's code asked for this message to go
-- another way (PO 2026-09-15: "build all four" status-code actions).
--
-- When an SMPP vendor answers a submit with a code mapped to failover,
-- hlr_retry or pause_vendor, smpp-vendor hands the message to the re-route
-- inbox (queue__reroute) and the mover in http-server routes it again, skipping
-- that vendor. This column says what was asked, by which code, of which
-- vendor: "<action>|<code>|<vendor cid>".
--
-- It stays on the message after the move, and that is what makes the ruling
-- "at most once per message" hold: a second failover or hlr_retry code for a
-- message that already carries one fails it instead of bouncing it between
-- vendors. NULL for every other message. Existing queue_<vendor> clones follow
-- the template at start-up.
ALTER TABLE queue_template
  ADD COLUMN code_reroute VARCHAR(96) NULL
    COMMENT '<action>|<code>|<vendor cid> — a vendor code sent this message another way (once only)';
