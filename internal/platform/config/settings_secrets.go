// The settings of keys and secrets.
package config

var _ = settings("Keys and secrets", []setting{
	{"YAKU_ADMIN_KEY", "Key for /admin/* calls and the control port; unset = /admin off"},
	{"YAKU_ADMIN_USER", "User name for /admin/* calls (with YAKU_ADMIN_PASSWORD)"},
	{"YAKU_ADMIN_PASSWORD", "Password for /admin/* calls (with YAKU_ADMIN_USER)"},
	{"YAKU_STATUS_KEY", "Key for the read-only /status page; unset = /status off"},
	{"YAKU_TOKEN_PEPPER", "Secret mixed into stored token digests; keep it secret"},
	{"YAKU_CRYPTO_AT_REST", "on = encrypt message text in new records"},
	{"YAKU_CRYPTO_FIELDS", "Which fields are encrypted, comma list (default body)"},
	{"YAKU_CRYPTO_KEK_REF", "Where the gateway's master key is kept (file:// or env://)"},
	{"YAKU_KMS_GCP_ENABLED", "on = companies may use their own Google Cloud KMS key"},
	{"YAKU_KMS_GCP_CREDENTIALS_REF", "Where the Google service-account JSON is kept"},
})
