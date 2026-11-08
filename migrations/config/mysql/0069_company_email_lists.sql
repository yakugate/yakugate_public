-- PO 2026-08-28: every company email field is a LIST (the portal's tag-style
-- MultiInput — one chip per address), stored comma-joined. VARCHAR(190) only
-- comfortably held two addresses; widen so a real distribution list fits.
-- All mailers (invoice, low-balance alert) already pass the joined form to
-- nodemailer as-is.
ALTER TABLE companies
  MODIFY company_email     VARCHAR(512) NULL,
  MODIFY support_email     VARCHAR(512) NULL,
  MODIFY billing_email     VARCHAR(512) NULL,
  MODIFY rates_email       VARCHAR(512) NULL,
  MODIFY low_balance_email VARCHAR(512) NULL;
