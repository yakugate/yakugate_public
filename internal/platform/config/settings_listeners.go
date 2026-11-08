// The settings of the ports the roles listen on.
package config

var _ = settings("Listeners", []setting{
	{"YAKU_API_ADDR", "Submit API port for clients (default :8080)"},
	{"YAKU_SMPP_ADDR", "SMPP port for clients (default :2775); off = no plain port, TLS only"},
	{"YAKU_DLR_ADDR", "Port where vendors post delivery receipts (default :8090)"},
	{"YAKU_MO_ADDR", "Port where vendors post incoming messages (default :8091)"},
	{"YAKU_ADMIN_ADDR", "Health/admin port of a role: /healthz, /readyz, /status; unset = none"},
	// One admin port per role (cmd/yakugate useRoleAdminAddr); Describe
	// says what each is from its name.
	{"YAKU_ADMIN_ADDR_ABUSE_MONITOR", ""},
	{"YAKU_ADMIN_ADDR_BACKUP", ""},
	{"YAKU_ADMIN_ADDR_BILLING", ""},
	{"YAKU_ADMIN_ADDR_BLACKHOLE", ""},
	{"YAKU_ADMIN_ADDR_DB_WORKER", ""},
	{"YAKU_ADMIN_ADDR_DLR", ""},
	{"YAKU_ADMIN_ADDR_EXPIRY", ""},
	{"YAKU_ADMIN_ADDR_HTTP_SERVER", ""},
	{"YAKU_ADMIN_ADDR_HTTP_VENDOR", ""},
	{"YAKU_ADMIN_ADDR_MO", ""},
	{"YAKU_ADMIN_ADDR_RETENTION", ""},
	{"YAKU_ADMIN_ADDR_RETRY", ""},
	{"YAKU_ADMIN_ADDR_SMPP_SERVER", ""},
	{"YAKU_ADMIN_ADDR_SMPP_VENDOR", ""},
	{"YAKU_HEALTH_ADDR", "Health-only port (/healthz, /readyz) of a role; unset = none"},
	// Outbound HTTP (vendor calls, customer webhooks): idle connections kept
	// per host (internal/platform/outhttp, 2026-10-13).
	{"YAKU_HTTP_IDLE_PER_HOST", "Idle connections kept per host for vendor calls and webhooks (512)"},
	{"YAKU_CORS_ORIGINS", "Web sites allowed to call the API from a browser, comma list"},
	{"YAKU_TRUSTED_PROXIES", "Your own proxies (IPs/CIDRs) whose X-Forwarded-For is trusted"},
})
