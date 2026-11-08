-- Process 13 Type 4 — which gateway host took a packet capture.
--
-- tcpdump runs on the host of the http-server that was asked, and the sealed
-- file stays on that host. The row names it so that:
--   * the operator can see where the capture was taken;
--   * an http-server that restarts marks only ITS OWN interrupted captures as
--     failed, never another host's.
ALTER TABLE log_captures
  ADD COLUMN host VARCHAR(190) DEFAULT NULL COMMENT 'traffic: the gateway host that holds the file';
