// The settings of the TLS ports.
package config

var _ = settings("TLS", []setting{
	{"YAKU_API_TLS", "off = no HTTPS port even when the certificate is set"},
	{"YAKU_API_TLS_ADDR", "HTTPS port for the submit API; needs the certificate and key too"},
	{"YAKU_API_TLS_CERT", "Certificate file for the HTTPS port"},
	{"YAKU_API_TLS_KEY", "Private key file for the HTTPS port"},
	{"YAKU_API_TLS_CLIENT_CA", "CA bundle; set = HTTPS clients must show a certificate"},
	{"YAKU_SMPP_TLS", "off = no SMPP-over-TLS port even when the certificate is set"},
	{"YAKU_SMPP_TLS_ADDR", "SMPP-over-TLS port for clients; unset = none"},
	{"YAKU_SMPP_TLS_CERT", "Certificate file for the SMPP-over-TLS port"},
	{"YAKU_SMPP_TLS_KEY", "Private key file for the SMPP-over-TLS port"},
	{"YAKU_SMPP_TLS_CLIENT_CA", "CA bundle; set = SMPP-TLS clients must show a certificate"},
})
