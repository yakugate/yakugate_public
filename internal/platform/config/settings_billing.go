// The settings of billing and the abuse monitor.
package config

var _ = settings("Billing and abuse", []setting{
	{"YAKU_BILLING_INTERVAL_SEC", "Retired 26 Oct 2026: nothing reads it"},
	{"YAKU_BILLING_BATCH", "Records priced per billing pass (default 1000)"},
	{"YAKU_BILLING_DRAIN_SEC", "Retired 26 Oct 2026: nothing reads it"},
	{"YAKU_BILLING_MAX_RATING_ATTEMPTS", "Failed pricing tries before a record is set aside (5)"},
	{"YAKU_BILLING_HOT_DAYS", "Days of record tables the billing role reads on every sweep (3)"},
	{"YAKU_BILLING_FULL_PASS_SEC", "Retired 26 Oct 2026: nothing reads it"},
	{"YAKU_ABUSE_INTERVAL_SEC", "Seconds between abuse-monitor checks (default 60)"},
	{"YAKU_ABUSE_SOURCE", "Abuse monitor reads: counters (default with Redis), records or compare"},
})
