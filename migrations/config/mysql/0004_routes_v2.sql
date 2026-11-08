-- ============================================================================
-- 0004  Routes v2 + rate-card catalog — CONFIG (MySQL)
-- Enriches routes with route_type/weight, a name, per-route HLR policy, rating
-- refs, QoS, and audit timestamps; adds a rate-card catalog (rate_cards +
-- rate_card_rates) so a route can drive buy (vendor cost) and sell (account
-- price) pricing. The matcher stays match_mcc/match_mnc/match_prefix (aligned
-- with the HLR component and internal/refload.LoadRoutes).
-- ============================================================================

-- Rate-card catalog ----------------------------------------------------------
CREATE TABLE rate_cards (
  id         INT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  name       VARCHAR(128) NOT NULL,
  kind       ENUM('buy','sell') NOT NULL,            -- buy = vendor cost, sell = account price
  currency   CHAR(3) NOT NULL DEFAULT 'USD',
  status     TINYINT(1) NOT NULL DEFAULT 1,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Per-destination price rows for a card. Most specific match (mcc/mnc then
-- prefix) wins; price is per message part (segment).
CREATE TABLE rate_card_rates (
  id             INT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  rate_card_id   INT UNSIGNED NOT NULL,
  match_mcc      VARCHAR(3),                          -- NULL = any
  match_mnc      VARCHAR(3),                          -- NULL = any
  match_prefix   VARCHAR(16),                         -- NULL = any
  price_per_msg  DECIMAL(12,6) NOT NULL,              -- price per message part
  effective_from DATE,                                -- NULL = always in effect
  KEY idx_card (rate_card_id, match_mcc, match_mnc, match_prefix),
  CONSTRAINT fk_rate_card FOREIGN KEY (rate_card_id) REFERENCES rate_cards(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Routes v2 ------------------------------------------------------------------
ALTER TABLE routes
  ADD COLUMN name                 VARCHAR(128) NULL AFTER id,
  ADD COLUMN route_type           ENUM('direct','failover','loadbalance','blackhole') NOT NULL DEFAULT 'direct' AFTER priority,
  ADD COLUMN weight               INT NOT NULL DEFAULT 100 AFTER route_type,   -- loadbalance split within equal priority
  ADD COLUMN match_sender         VARCHAR(32) NULL AFTER match_prefix,          -- optional sender pattern; NULL = any
  ADD COLUMN require_hlr          TINYINT(1) NOT NULL DEFAULT 0,
  ADD COLUMN accept_skipped_hlr   TINYINT(1) NOT NULL DEFAULT 0,
  ADD COLUMN buying_rate_card_id  INT UNSIGNED NULL,                            -- vendor cost
  ADD COLUMN selling_rate_card_id INT UNSIGNED NULL,                            -- account price
  ADD COLUMN loss_detection       TINYINT(1) NOT NULL DEFAULT 0,               -- per-route no-DLR monitoring
  ADD COLUMN created_at           TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  ADD COLUMN updated_at           TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  ADD CONSTRAINT fk_route_buy_card  FOREIGN KEY (buying_rate_card_id)  REFERENCES rate_cards(id),
  ADD CONSTRAINT fk_route_sell_card FOREIGN KEY (selling_rate_card_id) REFERENCES rate_cards(id);

-- A blackhole route has no vendor target.
ALTER TABLE routes MODIFY COLUMN vendor_id BIGINT UNSIGNED NULL;
