-- ============================================================================
-- 0120_dlr_levels.sql — every level of a delivery receipt (2026-09-23)
--
-- PO: "accepted itself is a dlr". SMPP 3.4 has EIGHT delivery states —
-- ENROUTE, DELIVRD, EXPIRED, DELETED, UNDELIV, ACCEPTD, UNKNOWN, REJECTD —
-- and until now the gateway understood four of them. ENROUTE and ACCEPTD were
-- not in the vocabulary at all: a vendor sending one got a log line and the
-- receipt was thrown away, unrecorded. DELETED and REJECTD arrived as a plain
-- `failed`, so a customer could not tell a cancelled message from a rejected
-- one.
--
-- The eight now live on the RECEIPT (hot/0045 `dlrlog.stat`). The message
-- keeps the five states everything else runs on, so retry, expiry, routing,
-- quality, billing and every report are untouched.
--
-- Three settings make the rest of it an operator's choice.
--
-- 1. accounts.dlr_levels — which INTERMEDIATE receipts reach this account's
--    clients. Final receipts always go, subject to the client's own
--    registered_delivery exactly as before.
--
--      registered   what the client asked for: an SMPP client gets them when
--                   it set registered_delivery bit 4 — which is what that bit
--                   means — and an HTTP client, which has no such bit and has
--                   always been sent the gateway's submit notice, gets them.
--                   DEFAULT.
--      all          every level to every client of this account, whatever
--                   their registered_delivery says.
--      finals_only  nothing intermediate at all, not even the gateway's own
--                   submit notice.
--
--    Note on upgrading: a vendor's own ENROUTE/ACCEPTD used to be thrown
--    away, so an HTTP-side client of a `registered` account will now start
--    seeing them. An account whose customer cannot read a non-final receipt
--    goes to finals_only.
--
-- 2. accounts.dlr_charge_mode gains `all_levels`.
--
--    The two existing values keep their exact meaning — every FINAL receipt,
--    or delivered ones only — so no account's bill changes the day this
--    ships. An operator who wants intermediates charged asks for it.
--
-- 3. vendors.own_intermediate_dlr — this vendor sends its own ACCEPTD.
--
--    The gateway sends a client ACCEPTD the moment a vendor accepts a
--    message. A vendor that sends its own would make that two. With this set,
--    the gateway's is suppressed for that vendor and the network's own notice
--    is the one the client gets (PO 2026-09-23: the vendor's wins).
--
-- Also: an SMPP vendor may now carry a status map of its own, in
-- vendors.dlr_config.status_map ({"DLVD":"DELIVRD"} …) — the same thing an
-- HTTP vendor has had in adapter_config.status.map. No column needed;
-- dlr_config is already JSON.
-- ============================================================================

ALTER TABLE sms_service_config
  ADD COLUMN dlr_levels ENUM('registered','all','finals_only') NOT NULL DEFAULT 'registered'
    COMMENT 'which intermediate receipts reach this account clients: registered (what the client asked for), all, finals_only',
  MODIFY COLUMN dlr_charge_mode ENUM('all_receipts','delivered_only','all_levels') NOT NULL DEFAULT 'all_receipts'
    COMMENT 'all_receipts = every FINAL receipt (unchanged), delivered_only, all_levels = intermediates too';

ALTER TABLE vendors
  ADD COLUMN own_intermediate_dlr TINYINT(1) NOT NULL DEFAULT 0
    COMMENT '1 = this vendor sends its own ACCEPTD/ENROUTE, so the gateway does not send its own submit notice for it';
