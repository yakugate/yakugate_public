-- Who approves a company's templates is a setting of the COMPANY (Arsalan
-- 2026-10-13), not something chosen on each template.
--
-- It was templates.required_approval_level, picked in the template form —
-- three values (NONE, COMPANY, ADMIN) — so two templates of one customer
-- could need different sign-offs for no reason anybody could give, and the
-- person writing a template chose who had to check it.
--
-- Five values now, set by an operator on the company:
--
--   NONE               no sign-off: a template is approved when it is saved
--   COMPANY            someone of the company approves — never an admin
--   ADMIN              an admin approves — never the company
--   ADMIN_OR_COMPANY   either one is enough
--   ADMIN_AND_COMPANY  both must: one sign-off from each side
--
-- ADMIN is the default, and so what every existing company gets: it is what
-- the template form preselected until today, and the careful reading for a
-- gateway on which only APPROVED templates are matched — a template is a live
-- traffic control, not paperwork.
--
-- The engine reads none of this. It loads templates WHERE status = 'approved'
-- and nothing else; who may set that status is the Admin API's rule
-- (p13-backend/src/services/templateApproval.js). templates
-- .required_approval_level stays: each template is stamped there with the
-- level it was judged under, and templates.users_involved (config/0036, unused
-- until now) keeps who signed, on which side, when and why.
ALTER TABLE companies
  ADD COLUMN template_approval VARCHAR(32) NOT NULL DEFAULT 'ADMIN'
    COMMENT 'who approves this company''s templates: NONE | COMPANY | ADMIN | ADMIN_OR_COMPANY | ADMIN_AND_COMPANY';
