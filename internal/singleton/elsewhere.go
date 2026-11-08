package singleton

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/deployment"
	"github.com/yakugate/yakugate/internal/platform/config"
)

// The supervised side of the lease (Saleem, 2026-10-01: "don't start, auto
// takeover"). A role the whole-gateway supervisor starts learns at start
// whether another deployment already runs it, and if so says where on one
// stderr line and exits with ExitElsewhere; the supervisor shows it as
// running elsewhere and starts it again after a minute. Both sides read this
// file: the role writes the markers, internal/supervisor parses them.

// EnvSupervised is set to "1" in the environment of every role the
// whole-gateway supervisor starts (cmd/yakugate/gateway.go). It is read from
// the environment the process was STARTED with, never from yakugate.config, so a
// role run by hand stands by as it always has.
const EnvSupervised = "YAKU_SUPERVISED"

// ExitElsewhere is the exit status of a supervised gateway-wide role that
// found another session holding its lease. 75 is EX_TEMPFAIL (sysexits.h):
// "not really an error — try again later", which is exactly this. Distinct
// from 1 (a failed start) and 2 (a Go panic), so the supervisor never takes a
// crash for it.
const ExitElsewhere = 75

// The two marker lines, alone on a line of the role's stderr:
//
//	YAKU-ELSEWHERE role=billing deployment=lhr-1 host=gw2 pid=4121
//	YAKU-ELSEWHERE role=billing deployment=lhr-1 why=home
//	YAKU-ELSEWHERE role=billing why=nolist
//	YAKU-HERE role=billing
//
// ELSEWHERE comes just before the ExitElsewhere exit (deployment/host/pid
// left out when the holder is not known). `why` says why it is not here:
// none = another session holds the lease; home = the register lists it on
// that deployment; nolist = the register cannot be read (2026-10-26). HERE
// comes once the start-up check has found the role is this deployment's (or
// nobody's) and nobody else running it, or could not ask the lock (fail
// open): the supervisor shows the role running from then on.
const (
	MarkerElsewhere = "YAKU-ELSEWHERE"
	MarkerHere      = "YAKU-HERE"
)

// Why a role is not running here, on its ELSEWHERE line ("" = another session
// holds the lease).
const (
	WhyHome   = "home"   // the register lists it on another deployment
	WhyNoList = "nolist" // the register cannot be read
)

// gatewayWide are the roles whose New builds a Lease — the ones that run in
// one deployment at a time. TestGatewayWideMatchesTheRoles keeps this list and
// the singleton.New calls together.
var gatewayWide = map[string]bool{"abuse-monitor": true, "backup": true, "retention": true}

// GatewayWide reports whether a role holds a gateway-wide lease.
func GatewayWide(role string) bool { return gatewayWide[role] }

// Supervised reports whether the whole-gateway supervisor started this
// process.
func Supervised() bool {
	for _, kv := range config.OriginalEnviron() {
		if k, v, ok := strings.Cut(kv, "="); ok && k == EnvSupervised {
			return strings.TrimSpace(v) == "1"
		}
	}
	return false
}

// Holder is who runs a gateway-wide role, as its singleton_holders row says.
// The zero value is "not known".
type Holder struct {
	Deployment string
	Host       string
	PID        int
}

// String is the holder in words, for "runs on …": "lhr-1 (host gw2, pid
// 4121)"; just the host when the deployment is unnamed (deployment.ID falls
// back to the hostname); "another deployment" when not known.
func (h Holder) String() string {
	if h.Deployment == "" && h.Host == "" {
		return "another deployment"
	}
	var detail []string
	name := h.Deployment
	if name == "" || name == h.Host {
		name = "host " + h.Host
	} else if h.Host != "" {
		detail = append(detail, "host "+h.Host)
	}
	if h.PID > 0 {
		detail = append(detail, "pid "+strconv.Itoa(h.PID))
	}
	if len(detail) > 0 {
		name += " (" + strings.Join(detail, ", ") + ")"
	}
	return name
}

// Elsewhere is the error a supervised role's Held returns when another
// session holds the lease. It ends the process with ExitElsewhere.
type Elsewhere struct {
	Role   string
	Holder Holder
	// Home: the role belongs on Holder's deployment (the deployments
	// register lists it there): it never runs here (2026-10-26).
	Home bool
	// NoList: the register cannot be read; the role waits.
	NoList bool
}

func (e *Elsewhere) Error() string {
	switch {
	case e.NoList:
		return fmt.Sprintf("%s waits: the deployments register cannot be read, so it is not known which deployment runs it — looking again every minute", e.Role)
	case e.Home:
		return fmt.Sprintf("%s belongs on %s (the deployments register lists it there) and does not run here, even while %s is down — to run it here, move it in Deployments → Expected roles",
			e.Role, e.Holder.Deployment, e.Holder.Deployment)
	}
	return fmt.Sprintf("%s runs on %s — leaving; the gateway looks again every minute", e.Role, e.Holder)
}

// ExitCode makes Elsewhere a worker.Exit: worker.Run returns it rather than
// stepping on.
func (e *Elsewhere) ExitCode() int { return ExitElsewhere }

// Marker is the ELSEWHERE line for this error.
func (e *Elsewhere) Marker() string {
	switch {
	case e.NoList:
		return WaitMarker(e.Role)
	case e.Home:
		return HomeMarker(e.Role, e.Holder.Deployment)
	}
	return ElsewhereMarker(e.Role, e.Holder)
}

// HomeMarker is the line of a role the register lists on another deployment.
func HomeMarker(role, home string) string {
	return elsewhereLine(role, Holder{Deployment: home}, WhyHome)
}

// WaitMarker is the line of a role whose register cannot be read.
func WaitMarker(role string) string { return elsewhereLine(role, Holder{}, WhyNoList) }

// ElsewhereMarker is the line a role prints before leaving because another
// session holds its lease.
func ElsewhereMarker(role string, h Holder) string { return elsewhereLine(role, h, "") }

func elsewhereLine(role string, h Holder, why string) string {
	var b strings.Builder
	b.WriteString(MarkerElsewhere + " role=" + word(role))
	if h.Deployment != "" {
		b.WriteString(" deployment=" + word(h.Deployment))
	}
	if h.Host != "" {
		b.WriteString(" host=" + word(h.Host))
	}
	if h.PID > 0 {
		b.WriteString(" pid=" + strconv.Itoa(h.PID))
	}
	if why != "" {
		b.WriteString(" why=" + why)
	}
	return b.String()
}

// HereMarker is the line a role prints when it stays.
func HereMarker(role string) string { return MarkerHere + " role=" + word(role) }

// word keeps a value to one field of a marker line: no spaces, no '='.
func word(s string) string {
	return strings.Map(func(r rune) rune {
		if r == '=' || r == '"' || r <= ' ' {
			return '_'
		}
		return r
	}, s)
}

// Marker is one marker line, read back.
type Marker struct {
	Elsewhere bool // false = HERE
	Role      string
	Holder    Holder
	Why       string // WhyHome, WhyNoList or ""
}

// ParseMarker reads a marker line; ok=false for any other line.
func ParseMarker(line string) (Marker, bool) {
	f := strings.Fields(strings.TrimSpace(line))
	if len(f) == 0 || (f[0] != MarkerElsewhere && f[0] != MarkerHere) {
		return Marker{}, false
	}
	m := Marker{Elsewhere: f[0] == MarkerElsewhere}
	for _, kv := range f[1:] {
		k, v, ok := strings.Cut(kv, "=")
		if !ok {
			continue
		}
		switch k {
		case "role":
			m.Role = v
		case "deployment":
			m.Holder.Deployment = v
		case "host":
			m.Holder.Host = v
		case "pid":
			m.Holder.PID, _ = strconv.Atoi(v)
		case "why":
			m.Why = v
		}
	}
	return m, true
}

// AtStart is a supervised gateway-wide role's first look, before it builds
// anything. First the deployments register: a role listed on another
// deployment leaves (HomeMarker), and one whose register cannot be read leaves
// to wait (WaitMarker) — 2026-10-26. Then, for a role listed here or nowhere,
// is the lease held by another session right now? It only LOOKS (IS_USED_LOCK)
// — the role takes the lease on its first Step as always, and a deployment
// that wins it in between is met there by Held. leave=false when it may run
// here and the lease is free or could not be asked (fail open: it is this
// deployment's role, or nobody's). line is the marker to print either way.
func AtStart(ctx context.Context, leases *dal.Leases, role string) (line string, leave bool) {
	return atStart(ctx, leases, role, deployment.ID())
}

// atStart is AtStart for deployment `me` (tests run two deployments in one
// process).
func atStart(ctx context.Context, leases *dal.Leases, role, me string) (line string, leave bool) {
	if leases == nil || leases.Locks == nil {
		return HereMarker(role), false
	}
	home, known := homeOf(ctx, leases.Homes, role)
	if !known {
		return WaitMarker(role), true
	}
	if home != "" && home != me {
		return HomeMarker(role, home), true
	}
	owner, held, err := leases.Locks.LockHolder(ctx, lockName(role))
	if err != nil {
		return HereMarker(role), false
	}
	if !held {
		return HereMarker(role), false
	}
	h := holderOf(ctx, leases.Homes, role, owner)
	if home == me && h.Deployment != me {
		// The home asks for its role back and STAYS (2026-10-18): Held
		// stands by for homeStandBy while the holder (an older build that
		// took it over) hands it back.
		askBack(ctx, leases.Homes, role, me)
		return HereMarker(role), false
	}
	return ElsewhereMarker(role, h), true
}

// describe says who holds a role's lease now; the zero Holder when nobody
// does any more or the row cannot say.
func describe(ctx context.Context, locks dal.Locks, homes dal.RoleHomes, role string) Holder {
	owner, held, err := locks.LockHolder(ctx, lockName(role))
	if err != nil || !held {
		return Holder{}
	}
	return holderOf(ctx, homes, role, owner)
}

// holderOf reads the singleton_holders row, trusting it only when it names
// the session that holds the lock (owner, from IS_USED_LOCK). A row left by a
// holder that died — or one that took the lease before the table existed — is
// stale, and stale is "another deployment", never a wrong name.
func holderOf(ctx context.Context, homes dal.RoleHomes, role string, owner int64) Holder {
	r, err := homes.RoleHolder(ctx, role)
	if err != nil || r.Session != owner {
		return Holder{}
	}
	return Holder{Deployment: r.Deployment, Host: r.Host, PID: r.PID}
}
