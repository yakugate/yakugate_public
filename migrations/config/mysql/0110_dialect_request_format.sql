-- How to read a client's request body when its Content-Type cannot be
-- trusted (PO 2026-09-16), companion to config/0109.
--
-- The engine reads the header: application/json, x-www-form-urlencoded,
-- application/xml or text/xml, and anything else is refused 400. That is right
-- for a client whose header matches its body, which is almost every client —
-- and useless for the ones migrating off an older platform, which send JSON as
-- text/plain, XML with no Content-Type at all, or a form declared as JSON.
-- Refusing those taught the customer nothing except that we would not take
-- their traffic.
--
-- 'json' | 'form' | 'xml' pins the reading and REPLACES the header, because
-- the header is the part that is wrong. NULL keeps reading it.
--
-- Read before authentication, like request_wrapper, so it comes from the URL
-- and applies to an account with its own submit path.
ALTER TABLE sms_service_dialects
  ADD COLUMN request_format ENUM('json','form','xml') NULL DEFAULT NULL
    COMMENT 'read the body as this regardless of Content-Type; NULL = trust the header'
    AFTER request_wrapper;
