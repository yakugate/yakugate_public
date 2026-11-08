-- The client's own id survives the whole journey (PO 2026-09-06: "message id
-- can be provided by client and it remains same throughout the sms journey in
-- reply as well ... however reference is also saved in db").
--
-- What happened before: a client-supplied `messageId` was written to the queue
-- row's `idempotency_key` column and then never read back — it is not in the
-- column list the vendor processes claim with. So the value existed for the
-- seconds a message sat in the queue, was used to compute the duplicate lock,
-- and then vanished. It never reached the record, the reply, the receipt or
-- the logs. That is why "stays the same throughout the journey" was not true:
-- not because anything overwrote it, but because nothing kept it.
--
-- The engine's own ULID stays the message id and the key (PO ruling, after
-- weighing the alternative): a client id is unique only per customer and may
-- be REUSED once the account's dedupe window passes, so making it the primary
-- key would put two different messages on one `record_index` row — the second
-- overwriting the first, and the first's delivery outcome lost when its
-- receipt arrived. The client's id travels ALONGSIDE ours instead, which
-- costs nothing and makes reuse harmless: two records, one reference, both
-- outcomes intact.
--
-- Indexed on record_index because looking a message up BY the client's own
-- reference is the point of storing it (GET /messages/{id} accepts either id).
-- Not unique: reuse after the window is expected, and a multi-recipient send
-- legitimately puts one reference on several messages.
ALTER TABLE mtlog_template
  ADD COLUMN client_ref VARCHAR(64) NULL
    COMMENT 'the id the CLIENT supplied at submit (HTTP messageId / SMPP user_message_reference); NULL when they sent none';

ALTER TABLE record_index
  ADD COLUMN client_ref VARCHAR(64) NULL
    COMMENT 'the id the CLIENT supplied at submit; not unique — reuse after the dedupe window is allowed, and one reference may cover several recipients',
  ADD KEY idx_record_index_client_ref (client_ref);
