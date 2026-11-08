-- ============================================================================
-- 0023_native_path_v4.sql — native submit path moves to /sms/v4/send (2026-07-26)
--
-- PO decision 2026-07-26: the gateway's native submit endpoint is
-- POST /sms/v4/send. The old /sms/v3/send stops being the shared default.
--
-- Everything about account_dialects that ENCODES the native path must move
-- together, atomically with the binary that serves /sms/v4/send:
--   1. every existing row holding the old shared default is rewritten — if
--      this step were skipped, the Go loader (refdata.buildDialect, which
--      compares url against its native-path constant) would suddenly see
--      '/sms/v3/send' as a CUSTOM path on every plain account, colliding all
--      of them onto one URL and 404ing the real native endpoint;
--   2. the column DEFAULT moves to '/sms/v4/send';
--   3. url_exclusive's generated expression compares against the new default
--      (a STORED generated column's expression cannot be altered in place —
--      drop + re-add, key included);
--   4. the auto-provisioning trigger (0021) inserts the new default.
--
-- Genuinely custom paths (url_exclusive NOT NULL) are untouched — a customer's
-- own URL is their contract, not ours to version. /sms/v3/ also stops being a
-- reserved prefix in internal/dialect, so a migrating account that wants to
-- keep POSTing to /sms/v3/send can now claim it explicitly as a custom path.
-- ============================================================================

ALTER TABLE account_dialects DROP KEY uq_url_exclusive;
ALTER TABLE account_dialects DROP COLUMN url_exclusive;

UPDATE account_dialects SET url = '/sms/v4/send' WHERE url = '/sms/v3/send';

ALTER TABLE account_dialects
  MODIFY COLUMN url VARCHAR(190) NOT NULL DEFAULT '/sms/v4/send';

ALTER TABLE account_dialects
  ADD COLUMN url_exclusive VARCHAR(190)
    GENERATED ALWAYS AS (NULLIF(url, '/sms/v4/send')) STORED AFTER url,
  ADD UNIQUE KEY uq_url_exclusive (url_exclusive);

DROP TRIGGER IF EXISTS trg_accounts_after_insert;

CREATE TRIGGER trg_accounts_after_insert
AFTER INSERT ON accounts
FOR EACH ROW
BEGIN
  INSERT INTO account_dialects (account_id, url) VALUES (NEW.id, '/sms/v4/send');
END;
