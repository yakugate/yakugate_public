// The settings of SMPP, long messages and the test vendor.
package config

var _ = settings("SMPP", []setting{
	{"YAKU_SMPP_MAX_CONNS", "Most client SMPP connections at once; 0 = no limit (2000)"},
	{"YAKU_SMPP_DELIVER_WINDOW", "Receipts and MO one client bind may have unanswered (64)"},
	{"YAKU_SMPP_WINDOW", "Messages one client bind may have in flight (default 16)"},
	{"YAKU_SMPP_WINDOW_GRACE_MS", "Wait for a free window slot before refusing, ms (200)"},
	{"YAKU_SMPP_SERVER_MAX_IN_FLIGHT", "Submits smpp-server works on at once; off = no limit (25 a connection)"},
	{"YAKU_SMPP_SERVER_OVER_LIMIT", "Over that limit: hold (wait their turn) or throttle (0x58) (hold)"},
	{"YAKU_SMPP_DELIVER_BATCH", "Receipts/incoming taken per pass for SMPP clients (200)"},
	{"YAKU_SMPP_DELIVER_MS", "Retired: read for receipts and MO while YAKU_QUEUE_DLR/MO_* are unset"},
	{"YAKU_SMPP_DRAIN_SEC", "Seconds a vendor bind waits for answers when stopping (5)"},
	{"YAKU_SMPP_THROTTLE_PAUSE_SEC", "Pause after a vendor says throttled, when its own is unset (1)"},
	{"YAKU_SMPP_VENDOR_INBOUND_WORKERS", "Workers per vendor bind for receipts and incoming (4)"},
	{"YAKU_RECEIPTS_IN_REDIS", "Receipts use Redis, MySQL written after in batches (on; 0 = off)"},
	{"YAKU_VENDOR_RELOAD_SEC", "Seconds between re-reading vendor settings (default 30)"},
	{"YAKU_VENDOR_CALL_LOG", "HTTP vendor answers keeping their raw call: all, failed, off (all)"},
	{"YAKU_CONCAT_ASSEMBLY_TIMEOUT_SEC", "Seconds to wait for all parts of a long message (60)"},
	{"YAKU_CONCAT_MO_STORE", "Where incoming (MO) long-message parts wait: mysql, redis, memory"},
	{"YAKU_CONCAT_MT_STORE", "Where a client's long-message parts wait: mysql, redis, memory"},
	{"YAKU_CONCAT_STALE_HOURS", "Hours to keep part receipts that never all came (48)"},
	{"YAKU_BLACKHOLE_BATCH", "Messages the test vendor takes per pass (default 500)"},
})
