-- Every try to give a client an incoming message or a receipt, and what the
-- client answered (Saleem 4-5 Nov 2026, D-140): "same design as we did for
-- mt — introduce sub_ for mo and dlr; whatever mo and dlr submission results
-- from client are should be saved there and brought when records are fetched
-- and displayed on portal, no updates at all, and all mo, dlr and their subs
-- should support bulk/batch inserts".
--
-- As an MT has mt_ (taken in) and sub_ (each vendor answer), an MO has mo_
-- (taken in) and mo_sub_ (each try to its client), and a receipt has dlr_ (the
-- vendor's or the gateway's receipt) and dlr_sub_ (each try to give it to the
-- client). One row per try, written once, never changed (issue 14). The day
-- is the day of the MESSAGE's id (billingstore.TableOf), so every try about
-- one message sits beside its mo_ / mt_ row however many days the tries take.
--
-- channel  smpp = a deliver_sm on one of the client's binds (session says
--          which), http = a call to the client's webhook (endpoint_id); NULL
--          on a row that is no try: the gateway gave up (result expired or
--          failed).
-- result   accepted  = the client took it (ESME_ROK, or the webhook's
--                      success rule)
--          refused   = the client answered no (command_status / http_status)
--          no_answer = no answer in time, or the call could not be made
--          expired   = the gateway gave up: out of time or out of tries
--          failed    = the gateway could not hand it over and will not try
-- try_id   a ULID made when the try was made: a write that is repeated (the
--          DB worker after a crash) leaves out the rows that are there.
--
-- A webhook try carries the call as it went: method, URL, headers and body,
-- and the answer's headers and body (this replaces the hook_ table of 4 Nov,
-- never shipped). Secrets are masked before the row is made; bodies are cut
-- at 16 KB (request) and 8 KB (answer), *_cut says so; sealed = 1: the two
-- bodies are encrypted under the company's key (an all_encrypted company's
-- MO; the Admin API opens them as it opens a message's text), or not kept when
-- the gateway has no encryption. YAKU_WEBHOOK_CALL_LOG (all | failed | off)
-- says which tries keep the call; the try row is always written.
--
-- mo_sub_.final_of: the MO this row ended — the try its client took, or the
-- gateway's giving up — unique, so an MO is delivered (and counted in
-- summary_mo) once whoever writes second. Its status is worked out when read:
-- accepted = delivered, else the row's result. mo_out_ (hot/0066) is no
-- longer written; the days that have one are still read.
--
-- dlr_sub_: msg_id is our message the receipt is about; client_msg_id the id
-- the client was told when it differs (a part's id, for a long message an
-- SMPP client sent in parts); status / stat the receipt's words.
CREATE TABLE IF NOT EXISTS tpl_day_mo_sub (
  id             BIGINT UNSIGNED   NOT NULL AUTO_INCREMENT,
  try_id         CHAR(26)          NOT NULL,
  msg_id         CHAR(26)          NOT NULL,
  account_id     BIGINT UNSIGNED   DEFAULT NULL,
  attempt        SMALLINT UNSIGNED DEFAULT NULL,
  channel        ENUM('smpp','http') DEFAULT NULL,
  result         ENUM('accepted','refused','no_answer','expired','failed') NOT NULL,
  final_of       CHAR(26)          DEFAULT NULL COMMENT 'the MO this row ended; NULL on every other row',
  command_status INT UNSIGNED      DEFAULT NULL,
  http_status    SMALLINT UNSIGNED DEFAULT NULL,
  error_code     VARCHAR(32)       DEFAULT NULL,
  error          VARCHAR(512)      DEFAULT NULL,
  endpoint_id    BIGINT UNSIGNED   DEFAULT NULL,
  session        VARCHAR(64)       DEFAULT NULL,
  method         VARCHAR(8)        DEFAULT NULL,
  url            VARCHAR(2048)     DEFAULT NULL,
  req_headers    TEXT,
  req_body       MEDIUMTEXT,
  req_cut        TINYINT(1)        NOT NULL DEFAULT 0,
  resp_headers   TEXT,
  resp_body      MEDIUMTEXT,
  resp_cut       TINYINT(1)        NOT NULL DEFAULT 0,
  sealed         TINYINT(1)        NOT NULL DEFAULT 0,
  sent_at        DATETIME(3)       NOT NULL,
  took_ms        INT UNSIGNED      DEFAULT NULL,
  deployment_id  VARCHAR(64)       DEFAULT NULL,
  created_at     DATETIME          NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY uq_try (try_id),
  UNIQUE KEY uq_final (final_of),
  KEY idx_msg (msg_id),
  KEY idx_at (sent_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS tpl_day_dlr_sub (
  id             BIGINT UNSIGNED   NOT NULL AUTO_INCREMENT,
  try_id         CHAR(26)          NOT NULL,
  msg_id         CHAR(26)          NOT NULL,
  client_msg_id  VARCHAR(64)       DEFAULT NULL,
  account_id     BIGINT UNSIGNED   DEFAULT NULL,
  status         VARCHAR(12)       DEFAULT NULL COMMENT 'the status the receipt told',
  stat           VARCHAR(8)        DEFAULT NULL COMMENT 'the receipt''s SMPP word',
  attempt        SMALLINT UNSIGNED DEFAULT NULL,
  channel        ENUM('smpp','http') DEFAULT NULL,
  result         ENUM('accepted','refused','no_answer','expired','failed') NOT NULL,
  command_status INT UNSIGNED      DEFAULT NULL,
  http_status    SMALLINT UNSIGNED DEFAULT NULL,
  error_code     VARCHAR(32)       DEFAULT NULL,
  error          VARCHAR(512)      DEFAULT NULL,
  endpoint_id    BIGINT UNSIGNED   DEFAULT NULL,
  session        VARCHAR(64)       DEFAULT NULL,
  method         VARCHAR(8)        DEFAULT NULL,
  url            VARCHAR(2048)     DEFAULT NULL,
  req_headers    TEXT,
  req_body       MEDIUMTEXT,
  req_cut        TINYINT(1)        NOT NULL DEFAULT 0,
  resp_headers   TEXT,
  resp_body      MEDIUMTEXT,
  resp_cut       TINYINT(1)        NOT NULL DEFAULT 0,
  sealed         TINYINT(1)        NOT NULL DEFAULT 0,
  sent_at        DATETIME(3)       NOT NULL,
  took_ms        INT UNSIGNED      DEFAULT NULL,
  deployment_id  VARCHAR(64)       DEFAULT NULL,
  created_at     DATETIME          NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY uq_try (try_id),
  KEY idx_msg (msg_id),
  KEY idx_at (sent_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
