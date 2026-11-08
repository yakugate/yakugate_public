-- The console is not a deployment role (Saleem, 2026-10-01).
--
-- `yakugate console` is the operator's menu tool, run by hand in a terminal:
-- it is not a service, the gateway supervisor never starts it, and it pushes
-- no monitoring snapshot (#26). Listed as an expected role it could never be
-- seen running, so every deployment that named it showed as one role short.
-- The Admin API and the portal no longer offer it; the rows already saved go.
DELETE FROM deployment_roles WHERE role = 'console';
