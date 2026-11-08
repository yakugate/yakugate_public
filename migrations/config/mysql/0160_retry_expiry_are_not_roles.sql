-- The retry and expiry roles are retired (issue 22 of the design audit,
-- Saleem and the PO, 23–26 Oct 2026): a vendor role refuses a message whose
-- validity ended (VALIDITY_EXPIRED) or whose tries ran out
-- (MAX_ATTEMPTS_REACHED) when it takes it from its queue. The roles are no
-- longer in the engine, the Admin API or the portal; listed as expected roles
-- they could never be seen running, so the rows already saved go.
-- Numbered 0160 to leave room for the migrations of work still being merged.
DELETE FROM deployment_roles WHERE role IN ('retry', 'expiry');
