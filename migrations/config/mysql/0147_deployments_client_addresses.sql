-- Where a CLIENT connects to a deployment (Saleem 2026-10-15: "no guidance to
-- integrate, mo, mt and dlr, where clients will send sms, no ip no port
-- defined in portal, we need one, so that operator may know").
--
-- The register had `base_url` (what a VENDOR is told to call for /dlr/<cid>
-- and /mo/<cid>) and `admin_url` (where the Admin API calls), but nothing a
-- customer is told: the HTTP address to send to and the SMPP host and ports
-- to bind to. The engine never reads these (it listens where its settings
-- say: YAKU_API_ADDR, YAKU_SMPP_ADDR, YAKU_SMPP_TLS_ADDR); they are what the
-- public side of a load balancer or a firewall's NAT makes of those, and only
-- an operator knows them. The Admin API builds each account's "How to
-- connect" sheet from them, for the deployments the account is placed on.
--
--   client_api_url        base URL for HTTP clients, e.g. https://sms.example.com
--   client_smpp_host      host name or IP for SMPP binds
--   client_smpp_port      plain SMPP port, e.g. 2775
--   client_smpp_tls_port  SMPP over TLS port; NULL = none offered
--
-- All NULL = not said yet; the sheet then says so. One column per ALTER
-- (bootstrap rule).

ALTER TABLE deployments
  ADD COLUMN client_api_url VARCHAR(255) NULL
    COMMENT 'Base URL HTTP clients send to on this deployment (public side)';

ALTER TABLE deployments
  ADD COLUMN client_smpp_host VARCHAR(255) NULL
    COMMENT 'Host name or IP SMPP clients bind to on this deployment (public side)';

ALTER TABLE deployments
  ADD COLUMN client_smpp_port INT NULL
    COMMENT 'Plain SMPP port clients bind to';

ALTER TABLE deployments
  ADD COLUMN client_smpp_tls_port INT NULL
    COMMENT 'SMPP over TLS port clients bind to; NULL = none';
