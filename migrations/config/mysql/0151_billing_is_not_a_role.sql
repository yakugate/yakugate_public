-- The billing role is retired (Saleem, 26 Oct 2026: "we need to retire the
-- billing process, no sweeping at all"). Money is booked from the billing
-- queue by each deployment's db-worker; the role is no longer in the engine,
-- the Admin API or the portal. Listed as an expected role it could never be
-- seen running, so the rows already saved go, with its claim on a home and
-- the note of where it last ran.
DELETE FROM deployment_roles WHERE role = 'billing';
DELETE FROM singleton_claims WHERE role = 'billing';
DELETE FROM singleton_holders WHERE role = 'billing';
