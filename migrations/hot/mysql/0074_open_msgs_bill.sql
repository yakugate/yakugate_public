-- What a waiting message's final receipt books (BUG-RR-02, 2 Nov 2026): the
-- message's charge mark (model.Priced.Charge, which carries billing's note
-- of the receipt fee, a delivery charge point and the vendor's receipt
-- price). With Redis the note rides the receipt facts (dlr.Facts.Bill);
-- open_msgs is where a message waits without Redis, and it had no place for
-- it, so no final booked anything on a MySQL-only install. Read by the
-- record store's BillNote; gone with the row when the message closes.
ALTER TABLE open_msgs ADD COLUMN bill VARCHAR(2000) DEFAULT NULL;
