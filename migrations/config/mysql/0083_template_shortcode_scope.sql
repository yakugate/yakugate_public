-- Template ↔ shortcode scope (S-11 second half, PO 2026-09-04:
-- "add short code scope, if it is provided then only make that template
-- available to that shortcode").
--
-- Today a template is scoped to an account or a company, and ANY sender that
-- account is allowed may carry it. This narrows that when — and only when —
-- an operator asks for it: a template with a shortcode scope stops matching
-- messages sent from any other sender.
--
-- NULL or an empty list means "every sender", which is what every existing
-- row gets, so nothing already approved changes behaviour.
--
-- A JSON array rather than one VARCHAR, matching sms_service_config
-- .allowed_sender_ids, which holds exactly this kind of list. One entry gives
-- the PO's case ("only that shortcode"); two avoid duplicating an identical
-- template for a brand that sends under MYBANK and MYBANK-OTP.
--
-- This is deliberately NOT the same thing as `default_sender` (P1 scope
-- 2026-08-09), which REWRITES a matched message's sender. That one forces;
-- this one restricts. A template may carry both — the scope decides whether
-- the template matches at all, and default_sender then applies as it always
-- has.
ALTER TABLE templates
  ADD COLUMN shortcodes JSON DEFAULT NULL
    COMMENT 'senders allowed to use this template; NULL/empty = any sender the account may use'
    AFTER default_sender;
