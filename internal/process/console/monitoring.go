package console

import (
	"bufio"
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/yakugate/yakugate/internal/monitor"
)

// Monitoring: the gateway at a glance (PO 2026-09-21). [SCOPE P14-MONITOR]
// The summary is monitor.Summarize — the same one the admin listener's HTML
// page draws — over the snapshots every role pushes: live from Redis when it
// is on, else the newest rows of the monitoring history.

func (p *Process) monitoringMenu(ctx context.Context, sc *bufio.Scanner) {
	for {
		o, err := p.overview(ctx)
		if err != nil {
			p.refusal(err)
			if p.mon.at.IsZero() {
				return
			}
		} else {
			p.drawOverview(o)
		}
		v := &p.mon
		liveLabel := "Live for a minute — ask the gateway itself at every refresh"
		if v.live() {
			liveLabel = fmt.Sprintf("Stop Live (%s left)", monitor.Ago(time.Until(v.liveUntil).Round(time.Second)))
		}
		opts := []opt{
			item("1", "Refresh"),
			item("2", liveLabel),
			item("3", "Go to a time…"),
			item("4", "Events — what happened, each with the gateway before, at and after it"),
			item("5", "Days — one summary per day"),
			item("6", "Queues — every queue, and its last hour"),
			item("7", "MySQL & Redis — each server, and its last hour"),
		}
		if len(v.known) > 0 {
			opts = append(opts, item("8", "Gateway: "+v.gatewayLabel()+" — choose"))
		}
		if !v.at.IsZero() {
			opts = append(opts, item("9", "Back to the latest reports"))
		}
		opts = append(opts, item("0", "Back"))
		choice, ok := p.choose(sc, "Choose", "", opts)
		if !ok {
			return
		}
		switch choice {
		case "1", "":
		case "2":
			if v.live() {
				v.liveUntil = time.Time{}
			} else {
				v.at, v.liveUntil = time.Time{}, time.Now().Add(liveFor)
			}
		case "3":
			if t, ok := p.askTime(sc); ok {
				v.at, v.liveUntil = t, time.Time{}
			}
		case "4":
			if t, ok := p.eventsScreen(ctx, sc); ok {
				v.at, v.liveUntil = t, time.Time{}
			}
		case "5":
			if t, ok := p.daysScreen(ctx, sc); ok {
				v.at, v.liveUntil = t, time.Time{}
			}
		case "6":
			p.queuesScreen(ctx, sc)
		case "7":
			p.storesScreen(ctx, sc)
		case "8":
			p.chooseGateway(sc)
		case "9":
			v.at = time.Time{}
		case "0":
			return
		default:
			p.errf("  %q is not one of the choices.\n", choice)
		}
	}
}

func (p *Process) drawOverview(o monitor.Overview) {
	p.printf("\n  Monitoring — %s UTC, %s, %s\n", o.Time.Format("2006-01-02 15:04:05"), p.mon.gatewayLabel(), o.Source)
	// Every gateway and machine, when there are several (2026-10-05).
	if len(o.Deployments) > 1 {
		p.printf("\n  Gateways\n")
		for _, d := range o.Deployments {
			line := fmt.Sprintf("    %-12s %-11s %d of %d roles ready  on %s\n", d.ID, d.Env, d.Ready, d.Roles, strings.Join(d.Hosts, ", "))
			if d.Ready < d.Roles {
				p.errf("%s", line)
			} else {
				p.printf("%s", line)
			}
		}
	}
	if len(o.Hosts) > 1 {
		p.printf("\n  Machines\n")
		for _, h := range o.Hosts {
			p.printf("    %-24s CPU %5.1f%%  memory %5.1f%%  disk %5.1f%%  load %.2f\n", trim(h.Name, 24), h.CPUPct, h.MemPct, h.DiskPct, h.Load1)
		}
	}

	p.printf("\n  Needs attention\n")
	if len(o.Problems) == 0 {
		p.okf("    ✓ nothing\n")
	}
	for _, pr := range o.Problems {
		p.errf("    ✗ %s\n", pr)
	}

	p.printf("\n  Roles (%d running)\n", len(o.Roles))
	p.printf("    %-14s %-7s %-9s %-9s %s\n", "role", "ready", "up for", "reported", "process")
	for _, r := range o.Roles {
		inst := r.Instance // host-pid; the host is on its own line below
		if r.Host != "" && strings.HasPrefix(inst, r.Host+"-") {
			inst = "pid " + strings.TrimPrefix(inst, r.Host+"-")
		}
		line := fmt.Sprintf("    %-14s %-7s %-9s %-9s %s\n", r.Role, yesNo(r.Ready), monitor.Ago(r.Uptime),
			monitor.Ago(r.Seen)+" ago", trim(inst, 40))
		if r.Ready {
			p.printf("%s", line)
		} else {
			p.errf("%s", line)
		}
	}

	t := o.Traffic
	p.printf("\n  Traffic                 per second (last minute)    since start\n")
	for _, row := range []struct {
		name string
		c    monitor.CountRate
	}{
		{"Accepted", t.Accepted}, {"Refused", t.Rejected}, {"Sent to vendors", t.Sent},
		{"Vendor refused", t.SendFailed}, {"Receipts", t.Receipts}, {"Incoming (MO)", t.MO},
	} {
		p.printf("    %-20s %10.1f %30d\n", row.name, row.c.PerS, row.c.Total)
	}

	waiting := 0
	for _, q := range o.Queues {
		if q.Depth > 0 {
			waiting++
		}
	}
	p.printf("\n  Queues (%d vendor(s), %d with messages waiting)\n", len(o.Queues), waiting)
	shown := 0
	for _, q := range o.Queues {
		if q.Depth == 0 || shown == 10 {
			continue
		}
		shown++
		p.printf("    %-20s %8d waiting (%d ready, %d held), the oldest for %s; in %s/s, out %s/s\n", trim(q.Vendor, 20), q.Depth,
			q.Ready, q.Held, monitor.Ago(time.Duration(q.OldestSec)*time.Second), perS(q.InPerS), perS(q.OutPerS))
	}
	if waiting == 0 {
		p.printf("    all empty\n")
	}
	// Customers' receipts and MO waiting (D-127).
	shown = 0
	for _, q := range o.CustomerQueues {
		if q.Depth == 0 || shown == 10 {
			continue
		}
		if shown == 0 {
			p.printf("\n  Waiting for customers\n")
		}
		shown++
		p.printf("    %-20s %8d waiting, the oldest for %s; out %s/s\n", trim(queueName(q), 20), q.Depth,
			monitor.Ago(time.Duration(q.OldestSec)*time.Second), perS(q.OutPerS))
	}

	// MySQL and Redis as they report themselves (D-129).
	if len(o.Stores) > 0 {
		p.printf("\n  Stores\n")
		for _, s := range o.Stores {
			kind := map[string]string{"mysql": "MySQL", "redis": "Redis"}[s.Kind]
			p.printf("    %-5s %-24s %-8s %6.1f ms  %s\n", kind, trim(s.Name, 24), s.Health, s.PingMS, s.Figures)
		}
	}

	if s := o.SMPP; s.VendorsTotal > 0 {
		p.printf("\n  SMPP vendors: %d of %d bound; sessions %d of %d up", s.VendorsBound, s.VendorsTotal, s.SessionsBound, s.SessionsTotal)
		if s.SessionsDegraded > 0 {
			p.printf(", %d failing", s.SessionsDegraded)
		}
		p.printf("\n")
	}
	if b := o.Billing; b != nil {
		p.printf("  Billing queue: %d waiting (the oldest %d s); %d refused by MySQL; %d booked\n", b.Waiting, b.OldestSec, b.Delayed, b.Booked)
	}
	if h := o.Host; h != nil {
		p.printf("  Host %s: CPU %.0f%% (load %.2f), memory %.0f%%, disk %.0f%%\n", h.Name, h.CPUPct, h.Load1, h.MemPct, h.DiskPct)
	}
	p.printf("\n")
}

func sweepAgo(at string, now time.Time) string {
	t, err := time.Parse(time.RFC3339, strings.TrimSpace(at))
	if err != nil {
		return "never"
	}
	return monitor.Ago(now.Sub(t)) + " ago"
}
