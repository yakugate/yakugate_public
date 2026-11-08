-- A client's receipt carries the message it is about (D-140, 5 Nov 2026).
-- client_msg_id is the id the client is told — a part's id for a long message
-- an SMPP client sent in parts — so each try to give the receipt to the
-- client (hot dlr_sub_) is filed with its message, not under a part's id.
-- NULL = the same as client_msg_id (every receipt queued before this).
ALTER TABLE client_dlr ADD COLUMN msg_id CHAR(26) NULL;
