-- ============================================================================
-- 0032_client_dlr_stat.sql — the word a client receipt carries (2026-09-23)
--
-- A receipt on its way to a client knows its message status (delivered,
-- failed, …), and the SMPP push turned that back into a `stat:` word and a
-- message_state TLV. With SMPP's eight states now kept (hot/0045), that
-- round trip loses four of them: a DELETED and a REJECTD would both reach the
-- client as UNDELIV, and an ENROUTE as ACCEPTD.
--
-- `stat` carries the exact word from the vendor's receipt to the client's
-- deliver_sm. Empty (every row written before this, and every receipt the
-- gateway decides itself) falls back to the word for the status, which is
-- what the push has always used.
-- ============================================================================
ALTER TABLE client_dlr
  ADD COLUMN stat VARCHAR(8) NULL
    COMMENT 'the SMPP 3.4 stat: word for this receipt; NULL = derive it from status, as before'
    AFTER status;
