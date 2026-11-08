-- How many days a company has to pay (Saleem, 27 Oct 2026: receivables and
-- dues on Sales and the Dashboard). An invoice to the company, or a bill of
-- it, falls due the day after its period plus these days; NULL = 7, the
-- constant every invoice used until now (p13-backend services/invoices.js
-- DUE_DAYS). Read and written by the Admin API only; the gateway does not
-- use it. Additions only.
ALTER TABLE companies ADD COLUMN payment_terms_days INT NULL COMMENT 'days to pay after an invoice or bill period ends; NULL = 7';
