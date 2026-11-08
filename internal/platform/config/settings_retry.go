// The settings of send tries and vendor quality.
package config

var _ = settings("Retry and quality", []setting{
	{"YAKU_MAX_ATTEMPTS", "Send tries per message before it fails (default 5)"},
	{"YAKU_QUALITY_WINDOW_MIN", "Minutes of results used to judge a vendor (default 60)"},
	{"YAKU_QUALITY_MIN_SAMPLE", "Results a vendor needs before it is judged (default 20)"},
})
