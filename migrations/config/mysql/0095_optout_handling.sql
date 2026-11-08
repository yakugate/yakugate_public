-- Honour STOP: turn an inbound opt-out into a blocklist entry (P1 register
-- 1.2.4 prerequisite, PO ruling 2026-09-09).
--
-- The gateway already had the ENFORCEMENT half and not the CAPTURE half, which
-- is the more dangerous way round because it looks handled: blocklist.source
-- has carried an 'unsub' value since the table was created, and the accept
-- pipeline refuses blocklisted numbers at stage 4 — but nothing anywhere wrote
-- 'unsub'. All three live rows are 'manual'. A subscriber replying STOP
-- produced nothing at all: P6 routed the message to the customer and no
-- keyword was ever read.
--
-- OPT-IN PER CUSTOMER (PO ruling). Some customers run their own opt-out list
-- and their own compliance, and a gateway silently blocking numbers underneath
-- them would be worse than useless — they would keep sending, we would keep
-- refusing, and neither side would understand why. So the default is
-- 'customer': unchanged behaviour, the STOP is delivered and nothing else
-- happens. 'gateway' opts in to us recording it.
--
-- The MO is delivered to the customer EITHER WAY. Even when we act on it, the
-- customer needs to see the opt-out to keep their own records straight —
-- swallowing it would leave their database disagreeing with ours.

ALTER TABLE sms_service_config
  ADD COLUMN optout_mode ENUM('customer','gateway') NOT NULL DEFAULT 'customer'
    COMMENT 'who acts on an inbound STOP: customer = we only deliver it (unchanged); gateway = we also add the sender to this accounts blocklist as source=unsub',
  ADD COLUMN optout_keywords VARCHAR(255) NULL
    COMMENT 'comma-separated words that mean opt out; NULL = the built-in set (stop, stopall, unsubscribe, end, quit, cancel, optout, opt-out)',
  ADD COLUMN optin_keywords VARCHAR(255) NULL
    COMMENT 'comma-separated words that undo an opt-out; NULL = the built-in set (start, unstop, subscribe, optin, opt-in). A subscriber who asks to resume must be able to';
