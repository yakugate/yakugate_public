// The settings of logs, money settings, monitoring and captures.
package config

var _ = settings("Logs, monitoring, captures", []setting{
	{"YAKU_LOG_DIR", "Folder for log files (default /var/log/yakugate)"},
	// Currency providers (2026-10-07): the longest one call to a rate
	// provider may take, and what the loss check does with no rate.
	{"YAKU_FX_TIMEOUT_MS", "Longest one call to a currency rate provider may take, ms (3000)"},
	// No ledger row for each message (2026-10-13). 1 keeps the old row at
	// acceptance while a deployment of an earlier build may still run billing.
	{"YAKU_LEDGER_PER_MESSAGE", "1 = still write a ledger row per message (only during an upgrade)"},
	// Billing prices a message by the rates of its accept time (2026-10-12):
	// how long replaced rate versions are held for that.
	{"YAKU_RATE_HISTORY_HOURS", "Hours back a message is priced by its accept-time rates (72; 0=off)"},
	{"YAKU_LOSS_NO_RATE", "Loss check when no exchange rate is known: refuse (default) or accept"},
	// The system base currency: the one currency prices are compared in.
	{"YAKU_BASE_CURRENCY", "System base currency prices are compared in (USD)"},
	{"YAKU_LOG_LEVEL", "Log level: debug, info, warn, error or none (info)"},
	{"YAKU_LOG_MAX_SIZE_MB", "Start a new log file past this size, MB; 0 = none (100)"},
	{"YAKU_LOG_RETENTION_DAYS", "Delete log files older than this, days; 0 = keep (30)"},
	{"YAKU_LOG_REDACT", "Hide phone numbers and codes in logs; off = show (on)"},
	{"YAKU_STATUS_PUSH_SEC", "Seconds between monitoring snapshots (default 5)"},
	{"YAKU_MONITOR_HISTORY", "off = do not save monitoring history"},
	{"YAKU_MONITOR_RETENTION_DAYS", "Days every 5-second report is kept; 0 = all (3)"},
	{"YAKU_MONITOR_CPU_PCT", "Event when host CPU goes over this % (90)"},
	{"YAKU_MONITOR_EVENTS", "Monitoring events on or off (on)"},
	{"YAKU_MONITOR_HOLD", "Intervals a condition must last to start or end an event (3)"},
	{"YAKU_MONITOR_LATENCY_MS", "Event when acceptance p95 goes over this, in ms (50)"},
	{"YAKU_MONITOR_MEM_PCT", "Event when host memory goes over this % (90)"},
	{"YAKU_MONITOR_QUEUE_AGE_SEC", "Event when a queue's oldest ready message waits longer (300)"},
	{"YAKU_MONITOR_QUEUE_DEPTH", "Event when more messages wait in one queue than this (10000)"},
	{"YAKU_MONITOR_STORE_PING_MS", "A MySQL or Redis round trip slower than this is a warning (100)"},
	{"YAKU_MONITOR_MYSQL_CONN_PCT", "MySQL connections over this % of max_connections is a warning (80)"},
	{"YAKU_MONITOR_REDIS_MEM_PCT", "Redis memory over this % of maxmemory is a warning (80)"},
	{"YAKU_MONITOR_REPLICA_LAG_SEC", "A MySQL replica further behind than this, in seconds, is a warning (30)"},
	{"YAKU_MONITOR_REJECT_MIN_TPS", "Refusal share judged only at this many messages/s (1)"},
	{"YAKU_MONITOR_REJECT_PCT", "Event when refused share of incoming goes over this % (20)"},
	{"YAKU_MONITOR_SPIKE_FACTOR", "Event when incoming is this many times its usual level (3)"},
	{"YAKU_MONITOR_SUMMARY_DAYS", "Days monitoring day summaries and events are kept (10)"},
	{"YAKU_MONITOR_TRAFFIC_MIN_TPS", "Traffic stop/spike judged above this messages/s (1)"},

	{"YAKU_CAPTURE_MAX_RECORDS", "Records the portal keeps per flow capture (20000)"},
	{"YAKU_PCAP_DIR", "Folder for packet captures (default <log dir>/pcap)"},
	{"YAKU_PCAP_INTERFACE", "Network interface tcpdump listens on (default any)"},
	{"YAKU_PCAP_MAX_MB", "Stop a packet capture at this size, MB; 0 = none (256)"},
	{"YAKU_PCAP_TCPDUMP", "tcpdump program name or full path (default tcpdump)"},
})
