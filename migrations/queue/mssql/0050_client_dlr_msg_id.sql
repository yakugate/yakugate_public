-- A client's receipt carries the message it is about (D-140, 5 Nov 2026):
-- the twin of migrations/queue/mysql/0050_client_dlr_msg_id.sql.
IF COL_LENGTH(N'client_dlr', N'msg_id') IS NULL
  ALTER TABLE [client_dlr] ADD [msg_id] CHAR(26) COLLATE Latin1_General_100_BIN2 NULL;
