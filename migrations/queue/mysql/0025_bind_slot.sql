-- A receipt goes back to the bind that sent the message (PO 2026-09-05:
-- "submit it to the bind from where the message was originated, i.e if message
-- came from bind/session 1 dlr should also go to 1 if 9 then 9").
--
-- Until now every receipt and every MO for an account went down ONE bind —
-- bindRegistry.sessionFor returned the most-recent RX/TRX session — so a client
-- holding ten binds had nine of them idle on the receive side.
--
-- SMPP gives a bind no name of its own, so we assign one: a SLOT, the lowest
-- free number for that account, handed out at bind time and released when the
-- connection closes. A client that drops bind 3 and reconnects takes the free
-- slot 3 back, which is what makes the number worth storing at all (a
-- per-connection random id would never match again after a reconnect).
--
--   queue_*.bind_slot     rides the message to the vendor, so P3/P4 can hand
--                          the slot to PushSubmitDLR/PushTerminalDLR while the
--                          full model.Message is still in scope — the same
--                          reason queue/0024 had to carry the receipt bits.
--   client_dlr.bind_slot  the receipt job itself: what the delivery loop reads
--                          when it picks a session to push down.
--
-- 0 means "no originating bind" — an HTTP submit, a row written before this
-- migration, or a slot we could not resolve. The delivery loop treats 0, an
-- unknown slot, and a send-only (TX) bind identically: fall back to the
-- account's live receive-capable binds, taking turns (PO ruling, same session).
--
-- SMALLINT not TINYINT: max_binds = 0 means unlimited (D-065), so slot numbers
-- are not bounded by the default cap of 10.
--
-- Bootstrap's missingCloneColumns sync propagates ADD COLUMN from
-- queue_template to every existing queue_<vendor> table at next startup, so
-- this migration touches the template plus the seeded dev vendor only —
-- the convention queue/0018 and queue/0024 both follow.
ALTER TABLE queue_template
  ADD COLUMN bind_slot SMALLINT UNSIGNED NOT NULL DEFAULT 0
    COMMENT 'per-account SMPP bind slot the submit arrived on; 0 = none (HTTP submit or unknown)';

ALTER TABLE queue_testvendor
  ADD COLUMN bind_slot SMALLINT UNSIGNED NOT NULL DEFAULT 0
    COMMENT 'per-account SMPP bind slot the submit arrived on; 0 = none (HTTP submit or unknown)';

ALTER TABLE client_dlr
  ADD COLUMN bind_slot SMALLINT UNSIGNED NOT NULL DEFAULT 0
    COMMENT 'bind slot to deliver this receipt on; 0 = any live receive-capable bind';
