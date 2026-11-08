-- ============================================================================
-- 0045_dlrlog_stat.sql — the receipt's own word (2026-09-23)
--
-- `dlrlog.status` holds the MESSAGE status a receipt implied — one of the
-- gateway's five — so four of SMPP's eight delivery states were lost on the
-- way in: DELETED and REJECTD were stored as `failed` like any other
-- undeliverable, and ENROUTE and ACCEPTD were not stored at all, because a
-- receipt that moved nothing was dropped before it reached this table.
--
-- `stat` keeps the vendor's own word, all eight. status stays exactly as it
-- was, so billing, the rollups and every report are unchanged, and an
-- intermediate receipt lands as status='sent' — the message it belongs to is
-- still on its way.
--
-- Daily tables made from now on have the column; existing ones get it from
-- the store on first write, the same self-repair as mtlog.sent_parts
-- (hot/0043) and concat_parts (hot/0042).
-- ============================================================================
ALTER TABLE dlrlog_template
  ADD COLUMN stat VARCHAR(8) NULL
    COMMENT 'the SMPP 3.4 word this receipt carried: ENROUTE|DELIVRD|EXPIRED|DELETED|UNDELIV|ACCEPTD|UNKNOWN|REJECTD. NULL = a receipt from before the eight were kept'
    AFTER status;

-- And what the CLIENT asked to hear about in between (registered_delivery
-- bit 4 for an SMPP submit; an HTTP submit has no such bit and has always
-- been sent the gateway's own submit notice, so it counts as asked).
--
-- It lives here for the same reason no_receipt and bind_slot do: by the time
-- a vendor's ENROUTE arrives, the message it belongs to is long gone and this
-- locator row is the only thing left that remembers what the client wanted.
ALTER TABLE record_index
  ADD COLUMN want_intermediate TINYINT(1) NOT NULL DEFAULT 0
    COMMENT 'the client asked for intermediate receipts (registered_delivery bit 4, or an HTTP submit)';
