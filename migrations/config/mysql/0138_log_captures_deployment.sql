-- Which deployment a capture runs on (Saleem 2026-10-02, D-123: "i should
-- choose deployment for logs capture").
--
-- Packet capture (type 'traffic'): tcpdump on ONE gateway's host, the sealed
-- file on its disk. The Admin API used to call one fixed gateway; since it
-- calls every deployment in the register (config/0137) the operator chooses
-- the deployment, and download and deletion go back to that same one. NULL =
-- the Admin API's .env gateway (no register address, and every row before).
--
-- Flow capture (type 'flow'): NULL = every deployment's roles record it, as
-- before; a deployment id = only that deployment's roles do
-- (internal/logcfg RefreshCaptures).
ALTER TABLE log_captures
  ADD COLUMN deployment_id VARCHAR(64) NULL
      COMMENT 'Deployment the capture runs on: packet capture = the gateway that took it (NULL = the Admin API .env gateway); flow capture = only that deployment records it (NULL = every deployment)'
      AFTER host;
