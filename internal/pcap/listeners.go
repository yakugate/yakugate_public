// Package pcap is Process 13 Type 4: a time-boxed packet capture of one
// gateway listener port. [SCOPE P13-T4]
//
// PO rulings 2026-09-17:
//   - operators only, the WHOLE port (no per-tenant filter);
//   - a window of 1, 5 or 15 minutes, and a reason is required;
//   - the file is encrypted at rest with the gateway key, and with no gateway
//     key the capture is refused (fail closed);
//   - the file is deleted 24 hours after the window closes.
//
// The capture is taken by the host's own tcpdump. This package never installs
// anything and never uses sudo: if tcpdump is missing, or the gateway's user
// may not capture, the request fails with a plain message.
package pcap

import (
	"strconv"
	"strings"
)

// Listener is one gateway port an operator may capture.
type Listener struct {
	Key   string `json:"key"`   // stable name the portal sends back
	Label string `json:"label"` // what the operator reads
	Port  int    `json:"port"`
}

// listenerSettings maps each listener to the setting that holds its listen
// address and the default the owning role uses when the setting is empty.
// A default of "" means the listener is off unless the setting is present.
var listenerSettings = []struct{ key, label, setting, def string }{
	{"http-server", "HTTP API (http-server)", "YAKU_API_ADDR", ":8080"},
	{"http-server-tls", "HTTP API over TLS (http-server)", "YAKU_API_TLS_ADDR", ""},
	{"smpp-server", "SMPP (smpp-server)", "YAKU_SMPP_ADDR", ":2775"},
	{"smpp-server-tls", "SMPP over TLS (smpp-server)", "YAKU_SMPP_TLS_ADDR", ""},
	{"dlr", "Receipt webhooks (dlr)", "YAKU_DLR_ADDR", ":8090"},
	{"mo", "Incoming message webhooks (mo)", "YAKU_MO_ADDR", ":8091"},
}

// Listeners reads the gateway's listener ports from its settings. getenv is
// os.Getenv in the engine (yakugate.config is loaded into the environment).
func Listeners(getenv func(string) string) []Listener {
	var out []Listener
	for _, l := range listenerSettings {
		addr := getenv(l.setting)
		if addr == "" {
			addr = l.def
		}
		if port := portOf(addr); port > 0 {
			out = append(out, Listener{Key: l.key, Label: l.label, Port: port})
		}
	}
	return out
}

// Find returns the listener with this key.
func Find(list []Listener, key string) (Listener, bool) {
	for _, l := range list {
		if l.Key == key {
			return l, true
		}
	}
	return Listener{}, false
}

// portOf pulls the port out of ":8080" or "host:8080".
func portOf(addr string) int {
	i := strings.LastIndex(addr, ":")
	if i < 0 {
		return 0
	}
	n, err := strconv.Atoi(addr[i+1:])
	if err != nil || n <= 0 || n > 65535 {
		return 0
	}
	return n
}
