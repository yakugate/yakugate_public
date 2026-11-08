-- What a waiting message's final receipt books (BUG-RR-02, 2 Nov 2026): the
-- twin of migrations/hot/mysql/0074_open_msgs_bill.sql. The message's charge
-- mark, read by the record store's BillNote; without Redis open_msgs is where
-- a message waits, and the receipt fee was never booked.
ALTER TABLE open_msgs ADD bill NVARCHAR(2000) NULL;
