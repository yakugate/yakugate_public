package monitor

import (
	"os"
	"strconv"
	"sync"
)

// Deployment identity — which gateway is this, and where.
//
// Hadal carries a `deployment` block naming the install ("ZONG"), its host and
// its ports. That matters the moment there is more than one deployment, which
// for an SMS gateway is immediately: an operator runs one per carrier
// relationship, and a dashboard that cannot say WHICH gateway it is showing is
// a dashboard nobody trusts during an incident.
//
// Read once from the environment. It cannot change while the process runs.
type Deployment struct {
	// ID is the operator's own name for this install (YAKU_DEPLOYMENT_ID).
	// Falls back to the hostname, so it is never empty — an unnamed
	// deployment on a screen is worse than a hostname.
	ID   string `json:"id"`
	Host string `json:"host"`
	// Ports are the surfaces this deployment exposes, so an operator reading
	// the dashboard can reach them without hunting through a config file.
	APIPort   int `json:"api_port,omitempty"`
	AdminPort int `json:"admin_port,omitempty"`
	SMPPPort  int `json:"smpp_port,omitempty"`
	// Env distinguishes production from staging on the same screen. Getting
	// this wrong is how someone drains the wrong gateway.
	Env string `json:"env,omitempty"`
	// Version is the build this process is running, so "did the fix deploy?"
	// is answerable from the dashboard rather than from ssh.
	Version string `json:"version,omitempty"`
}

var (
	depOnce  sync.Once
	depCache Deployment
)

// DeploymentInfo returns the cached identity.
func DeploymentInfo() Deployment {
	depOnce.Do(func() {
		host, _ := os.Hostname()
		d := Deployment{
			ID:        os.Getenv("YAKU_DEPLOYMENT_ID"),
			Host:      host,
			Env:       os.Getenv("YAKU_ENV"),
			Version:   os.Getenv("YAKU_VERSION"),
			APIPort:   portOf(os.Getenv("YAKU_API_ADDR")),
			AdminPort: portOf(os.Getenv("YAKU_ADMIN_ADDR")),
			SMPPPort:  portOf(os.Getenv("YAKU_SMPP_ADDR")),
		}
		if d.ID == "" {
			d.ID = host
		}
		depCache = d
	})
	return depCache
}

// portOf pulls the port out of a ":8080" or "host:8080" listen address.
func portOf(addr string) int {
	if addr == "" {
		return 0
	}
	for i := len(addr) - 1; i >= 0; i-- {
		if addr[i] == ':' {
			n, err := strconv.Atoi(addr[i+1:])
			if err != nil {
				return 0
			}
			return n
		}
	}
	return 0
}
