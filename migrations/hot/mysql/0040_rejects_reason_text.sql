-- The sentence behind a refusal, in its own column (PO 2026-09-17, I-18).
--
-- reason_code is the field every report groups and counts by. Some stages
-- built their reason as "CODE: sentence" and the writer stored it whole, so
-- one kind of refusal landed in as many buckets as it had wordings
-- ("DESTINATION_CAP: hourly limit to this number reached" beside
-- "DESTINATION_CAP: daily limit to this number reached"), and a wording change
-- split a code's history in two. The client response always separated the
-- two (status = code, error = sentence); the records now do the same:
-- reason_code holds the bare code, reason_text the sentence.
--
-- reason_detail keeps what it has always held — the client's IP address —
-- and is labelled as such on the screen from now on.
--
-- Only the template is altered here. A daily/monthly partition that already
-- exists gets the column from the writer itself the first time it needs it
-- (mysql.WriteRejected adds it and retries), the same way it creates a new
-- partition from the template — a migration cannot list partitions it has
-- never seen.
ALTER TABLE rejects_template
  ADD COLUMN reason_text VARCHAR(190) NULL
    COMMENT 'the sentence that came with reason_code, as the sender saw it'
    AFTER reason_code;
