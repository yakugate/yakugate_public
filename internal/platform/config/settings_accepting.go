// The settings of accepting messages.
package config

var _ = settings("Accepting messages", []setting{
	{"YAKU_MAX_RECIPIENTS", "Most recipients in one submit request (default 500)"},
	{"YAKU_MAX_BATCH", "Most items in a personalised messages[] list (default 500)"},
	{"YAKU_MAX_SEGMENTS", "Most parts in one long message; longer is refused (default 10)"},
	{"YAKU_SENDER_MAX_ALPHA", "Longest letter sender ID (default 11)"},
	{"YAKU_SENDER_MAX_NUMERIC", "Longest number sender ID (default 15)"},
	{"YAKU_GLOBAL_TPS", "Gateway-wide limit, message parts per second; 0 = none"},
	{"YAKU_DEDUPE_TTL_SEC", "Seconds a message is remembered to catch duplicates (1800)"},
	{"YAKU_DEFAULT_TTL_SEC", "How long a message may wait to be sent, seconds (48 h)"},
	{"YAKU_SPRAY_DISTINCT_THRESHOLD", "Different logins one IP may try before it is blocked (3)"},
	{"YAKU_LOG_REJECTS", "off = do not record refused submits"},
	{"YAKU_REROUTE", "off = do not move messages off a vendor that is down"},
	{"YAKU_MASK_MIN_DIGITS", "Shortest digit run hidden when text is masked (default 4)"},
})
