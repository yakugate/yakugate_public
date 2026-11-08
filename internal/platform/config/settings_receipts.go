// The settings of receipts and incoming messages.
package config

var _ = settings("Receipts and incoming", []setting{
	{"YAKU_DLR_CORRELATE_GRACE_MS", "Wait before a receipt is called unmatched, ms (250)"},
	{"YAKU_ORPHAN_REMATCH_SEC", "Seconds between retries of unmatched receipts; 0 = off (5)"},
	{"YAKU_ORPHAN_REMATCH_MAX_SEC", "Longest wait between tries of a receipt that matches nothing (300)"},
	{"YAKU_ORPHAN_REMATCH_MINUTES", "How far back unmatched receipts are retried, minutes (60)"},
	{"YAKU_ORPHAN_REMATCH_LIMIT", "Unmatched receipts retried per pass (default 200)"},
	// Inbound security (config/0121): how many failures lock one vendor door,
	// and how many from one address throttle it.
	{"YAKU_INBOUND_FAIL_THRESHOLD", "Failed vendor logins before that vendor door locks (5)"},
	{"YAKU_INBOUND_IP_THRESHOLD", "Failed vendor logins from one IP before it is slowed (20)"},
	{"YAKU_PUSH_MAX_ATTEMPTS", "Tries to deliver a receipt/incoming to a client URL (30)"},
	{"YAKU_PUSH_BASE_DELAY_SEC", "First wait before trying a client URL again, s (60)"},
	{"YAKU_PUSH_MULTIPLIER", "Each wait is this many times the last (default 2)"},
	{"YAKU_PUSH_MAX_DELAY_SEC", "Longest wait between tries, seconds (default 3600)"},
	{"YAKU_PUSH_GIVE_UP_AFTER_SEC", "Give up on a client URL after this many seconds (86400)"},
	{"YAKU_WEBHOOK_CALL_LOG", "Webhook tries keeping their request and answer: all, failed, off (all)"},
})
