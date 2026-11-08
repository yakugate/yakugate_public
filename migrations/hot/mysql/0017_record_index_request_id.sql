-- Portal lookup by requestId (chunk-1 ruling 1.4, PO 2026-07-31: "request id
-- will stay there in post or in get. we will use it to find messages from
-- portal in the end"). The client-supplied (or minted) requestId already
-- lives on every mtlog row, but the portal's search screen reads
-- record_index — the O(1) locator — which didn't carry it. One column + one
-- index makes "find my batch by the id I submitted" a single indexed query.
ALTER TABLE record_index
  ADD COLUMN request_id VARCHAR(64) DEFAULT NULL AFTER msg_id,
  ADD KEY idx_record_index_request (request_id);
