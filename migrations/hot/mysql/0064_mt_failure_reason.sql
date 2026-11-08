-- Why a message failed, on the message's own row (Saleem 2026-10-15: "i can
-- see the failed messages in logs, but i cant see the reason of failure? i.e
-- any response or error code etc").
--
-- error_code: a short code — the vendor's refusal (HTTP_400, SMPP_45), a
-- receipt's error code or word (UNDELIV), or the gateway's own
-- (MAX_ATTEMPTS, NO_ROUTE, EXPIRED). error_text: the words behind it — the
-- vendor's answer, the receipt's raw text, or the gateway's sentence.
-- Written for a failed or expired message only. Existing mt_ tables get the
-- two columns from the template at start (bootstrap syncCloneColumns).
-- One column per ALTER (the bootstrap rule).
ALTER TABLE tpl_mt ADD COLUMN error_code VARCHAR(32) DEFAULT NULL;
ALTER TABLE tpl_mt ADD COLUMN error_text VARCHAR(255) DEFAULT NULL;
