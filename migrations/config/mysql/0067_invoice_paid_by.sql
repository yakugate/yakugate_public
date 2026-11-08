-- Invoice payment provenance (PO 2026-08-28): the operator who marked the
-- invoice paid, auto-picked from the portal session. Rendered on the PDF's
-- green PAID stamp together with payment_reference and payment_notes
-- (obtained from the user in the mark-paid dialog). Paid invoices are
-- terminal and never regenerated.
ALTER TABLE invoices ADD COLUMN paid_by VARCHAR(64) NULL AFTER paid_at;
