-- Room for the whole of why a message failed (Saleem 2026-10-15: "reason is
-- not clear … if body failed then body details, if session error then session
-- details"): the re-route mover's sentence now carries the last vendor's own
-- answer (its HTTP status and body, or its login failure), which does not fit
-- in 255. 512, as a receipt's raw text. Existing mt_ tables are widened from
-- the template at start (bootstrap widenCloneColumns; VARCHAR 255 -> 512 in
-- utf8mb4 keeps its two-byte length, so MySQL does it in place).
ALTER TABLE tpl_mt MODIFY error_text VARCHAR(512) DEFAULT NULL;
