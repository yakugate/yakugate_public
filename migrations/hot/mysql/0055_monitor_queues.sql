-- ============================================================================
-- 0055_monitor_queues.sql — every queue's numbers with each sample
-- (Saleem 2026-10-05, D-127: "we need queues as well and their entire
-- visibility")
--
-- One small JSON document per sample, lifted out of the gzip'd body like the
-- other columns, so a queue's history and its events are read without opening
-- a single snapshot:
--
--   {"d":  ["mt"],                       kinds whose depths this sample reports
--    "mt": {"<vendor>": {"w":12,"r":10,"h":2,"t":1,"a":40,"i":3.5,"o":3.1,
--                        "it":9001,"ot":8990}},
--    "dlr": {"<account id>": {...}}, "mo": {"<account id>": {...}}}
--
--   w waiting, r ready now, h held (being sent, waiting to retry, scheduled),
--   t taken before and still there, a seconds the oldest ready one has
--   waited, i/o messages a second in/out over the last minute, it/ot this
--   process's totals in/out since it started. Zero values are left out; a
--   queue missing under a kind listed in "d" is empty. NULL: the sample says
--   nothing about queues.
-- ============================================================================
ALTER TABLE monitor_samples
  ADD COLUMN queues JSON NULL
    COMMENT 'per-queue figures of this sample (D-127): waiting, ready, held, age, rates in/out';
