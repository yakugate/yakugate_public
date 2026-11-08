-- ============================================================================
-- audit_log belongs to the ENGINE (PO 2026-09-18).
--
-- WHY THIS FILE EXISTS
--
-- The engine creates `audit_log` in config/0001 and writes to it from the abuse
-- monitor; the Admin API reads it, writes to it, and until today also RESHAPED
-- it — `admin/0007_audit_log_readable.sql` added the readable columns and
-- `admin/0008_audit_log_hadal_shape.sql` the request-context ones. That split
-- ownership made a fresh database impossible to build:
--
--     admin/0003   creates `users`            <- engine config/0045 needs it
--     config/0001  creates `audit_log`        <- admin/0007 needs it
--     admin/0007   adds audit_log.actor_name  <- config/0088's trigger needs it
--
-- Neither side could finish alone, so a cold start had to alternate between the
-- two runners — undocumented, and nobody's contract. The PO's ruling: the
-- engine owns the table outright. This file brings the Admin API's two shapes
-- into the engine's own set, so the order is simply "Admin API first (for
-- users), then the engine".
--
-- WHY THE LETTER IN THE NAME
--
-- Migrations apply in filename order and `0088_audit_chain.sql` builds a
-- trigger that reads `actor_name`, so the shape must land BEFORE it. `0087a`
-- sorts after `0087_capture_system_scope.sql` and before `0088`. Applied
-- migrations stay immutable: this adds a file, it edits none.
--
-- ON A DATABASE THAT ALREADY HAS THESE COLUMNS
--
-- Every existing deployment applied admin/0007 and admin/0008 long ago, so each
-- ALTER below is already true there. The bootstrap treats "column already
-- exists" and "key already exists" on an ALTER the way it already treats an
-- ALTER against a table absent on this deployment: the end state holds, so it
-- logs the statement as satisfied and carries on. Nothing is re-added and no
-- data is touched.
--
-- The statements are deliberately one column per ALTER rather than one big
-- ALTER with nine clauses, so a deployment that has SOME of them (a hand-patched
-- box) settles the rest instead of failing on the first clause.
-- ============================================================================

-- The readable trail (was admin/0007, PO 2026-08-20: "user FIO and company id
-- be there ... make it understandable schema for db").
ALTER TABLE audit_log ADD COLUMN actor_name   VARCHAR(128) NULL AFTER actor;
ALTER TABLE audit_log ADD COLUMN actor_email  VARCHAR(190) NULL AFTER actor_name;
ALTER TABLE audit_log ADD COLUMN company_name VARCHAR(190) NULL AFTER company_id;
ALTER TABLE audit_log ADD COLUMN target_label VARCHAR(190) NULL AFTER entity_id;
ALTER TABLE audit_log ADD COLUMN summary      VARCHAR(255) NULL AFTER action;
ALTER TABLE audit_log ADD COLUMN outcome      ENUM('success','failure') NOT NULL DEFAULT 'success' AFTER summary;
ALTER TABLE audit_log ADD COLUMN reason       VARCHAR(255) NULL AFTER outcome;
ALTER TABLE audit_log ADD KEY idx_audit_actor_time   (actor, created_at);
ALTER TABLE audit_log ADD KEY idx_audit_company_time (company_id, created_at);

-- The request context (was admin/0008): what was called, with what, and what
-- came back. The Admin API fills these; the engine only creates them.
ALTER TABLE audit_log ADD COLUMN method        VARCHAR(10)   NULL AFTER action;
ALTER TABLE audit_log ADD COLUMN route         VARCHAR(1024) NULL AFTER method;
ALTER TABLE audit_log ADD COLUMN params        JSON          NULL AFTER route;
ALTER TABLE audit_log ADD COLUMN query         JSON          NULL AFTER params;
ALTER TABLE audit_log ADD COLUMN response_code INT           NULL AFTER query;
ALTER TABLE audit_log ADD COLUMN feature       VARCHAR(256)  NULL AFTER entity;
ALTER TABLE audit_log ADD COLUMN module        VARCHAR(256)  NULL AFTER feature;
ALTER TABLE audit_log ADD COLUMN user_agent    VARCHAR(1024) NULL AFTER ip;
ALTER TABLE audit_log ADD COLUMN content_type  VARCHAR(255)  NULL AFTER user_agent;
ALTER TABLE audit_log ADD KEY idx_audit_method        (method);
ALTER TABLE audit_log ADD KEY idx_audit_response_code (response_code);
ALTER TABLE audit_log ADD KEY idx_audit_feature       (feature);
ALTER TABLE audit_log ADD KEY idx_audit_module        (module);
