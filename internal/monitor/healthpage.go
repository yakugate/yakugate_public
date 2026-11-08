package monitor

import (
	"context"
	"html/template"
	"net/http"
	"strconv"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/platform/version"
)

// The admin listener's HTML health page (PO 2026-09-21: "basic monitoring …
// via the HTTP health endpoint in HTML"). GET / or /health on YAKU_ADMIN_ADDR.
//
// Anyone who can reach the port sees THIS process: its role, whether it is
// ready and for how long — what /healthz and /readyz already say. The gateway
// overview (every role, traffic, queues, problems) is behind the status key,
// like /status: ?key=… or the yaku-status-key header. Without a key set it is
// not shown at all, as /status is not.
//
// It refreshes itself every 5 seconds, or as the page's dropdown says
// (?every=, 5 s to 5 min). /healthz and /readyz are unchanged —
// load balancers and orchestrators keep their plain answers.

var (
	pageRedis   dal.LiveStatus
	pageHistory dal.MonitorStatements
)

// SetOverviewSources tells the health page where the other roles' snapshots
// are: Redis when it is on, else the monitoring history in the hot store.
func SetOverviewSources(rdb dal.LiveStatus, history dal.MonitorStatements) {
	pageRedis, pageHistory = rdb, history
}

type pageData struct {
	Role, Instance, Version, Uptime, Now string
	Ready                                bool
	Overview                             *Overview
	Note                                 string
	Key                                  string
	Every                                int // seconds between refreshes
	Choices                              []refreshChoice
}

// refreshChoice is one entry of the page's refresh dropdown.
type refreshChoice struct {
	Sec   int
	Label string
}

// The page's refresh intervals (PO 2026-09-21: "a dropdown from 5 sec to 5
// min"). ?every=<seconds> picks one; anything else is the first.
var refreshChoices = []refreshChoice{{5, "5 s"}, {10, "10 s"}, {15, "15 s"}, {30, "30 s"},
	{60, "1 min"}, {120, "2 min"}, {300, "5 min"}}

// refreshEvery reads ?every=, keeping only the listed intervals.
func refreshEvery(r *http.Request) int {
	n, err := strconv.Atoi(r.URL.Query().Get("every"))
	if err == nil {
		for _, c := range refreshChoices {
			if c.Sec == n {
				return n
			}
		}
	}
	return refreshChoices[0].Sec
}

func healthPage(role, instance, statusKey string, start time.Time, w any) http.HandlerFunc {
	return func(rw http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" && r.URL.Path != "/health" {
			http.NotFound(rw, r)
			return
		}
		d := pageData{Role: role, Instance: instance, Version: version.String(), Ready: isReady(w),
			Uptime: Ago(time.Since(start)), Now: time.Now().UTC().Format("2006-01-02 15:04:05"),
			Every: refreshEvery(r), Choices: refreshChoices}
		key := r.URL.Query().Get("key")
		if key == "" {
			key = r.Header.Get("yaku-status-key")
		}
		switch {
		case statusKey == "":
			d.Note = "The gateway overview is off: set YAKU_STATUS_KEY to see every role, traffic and queues here."
		case key != statusKey:
			d.Note = "Add ?key=<status key> to this address to see the whole gateway."
		default:
			d.Key = key
			ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
			defer cancel()
			o, err := pageOverview(ctx)
			if err != nil {
				d.Note = "The overview could not be read: " + err.Error()
			} else {
				d.Overview = &o
			}
		}
		rw.Header().Set("Content-Type", "text/html; charset=utf-8")
		rw.Header().Set("Cache-Control", "no-store")
		_ = pageTmpl.Execute(rw, d)
	}
}

func pageOverview(ctx context.Context) (Overview, error) {
	if pageRedis != nil {
		// Half the time at most, so a slow Redis leaves the history its turn.
		rctx, cancel := context.WithTimeout(ctx, 1500*time.Millisecond)
		snaps, err := ClusterStatus(rctx, pageRedis)
		cancel()
		if err == nil {
			return Summarize(snaps, "live from Redis", time.Now()), nil
		}
	}
	if pageHistory != nil {
		snaps, err := FromHistory(ctx, pageHistory, 20*time.Second)
		if err != nil {
			return Overview{}, err
		}
		return Summarize(snaps, "from the monitoring history", time.Now()), nil
	}
	return Overview{}, errNoSource
}

type pageErr string

func (e pageErr) Error() string { return string(e) }

const errNoSource = pageErr("this process has neither Redis nor a hot store to read other roles from")

var pageTmpl = template.Must(template.New("health").Funcs(template.FuncMap{
	"ago":  func(d time.Duration) string { return Ago(d) },
	"secs": func(s int64) string { return Ago(time.Duration(s) * time.Second) },
	"rate": fmtRate,
	"since": func(at string) string {
		t, err := time.Parse(time.RFC3339, at)
		if err != nil {
			return "never"
		}
		return Ago(time.Since(t)) + " ago"
	},
}).Parse(`<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta http-equiv="refresh" content="{{.Every}}">
<title>Yakugate health — {{.Role}}</title>
<link rel="icon" href="data:,">
<style>
:root{--bg:#f6f7f9;--card:#fff;--ink:#1d2330;--dim:#667085;--line:#e4e7ec;--ok:#12805c;--bad:#c0362c;--warn:#b54708}
@media (prefers-color-scheme:dark){:root{--bg:#14161b;--card:#1d2027;--ink:#e6e8ec;--dim:#98a2b3;--line:#2e333d;--ok:#4ade80;--bad:#f87171;--warn:#fbbf24}}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--ink);font:15px/1.45 system-ui,-apple-system,"Segoe UI",sans-serif}
main{max-width:1000px;margin:0 auto;padding:20px 16px 40px}
h1{font-size:20px;margin:0 0 4px}h2{font-size:15px;margin:0 0 10px}
.meta{color:var(--dim);font-size:13px;margin-bottom:16px}
.card{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:14px 16px;margin-bottom:14px}
.badge{display:inline-block;padding:2px 10px;border-radius:999px;font-size:13px;font-weight:600}
.ok{color:var(--ok)}.bad{color:var(--bad)}.warn{color:var(--warn)}
.badge.ok{background:color-mix(in srgb,var(--ok) 15%,transparent)}.badge.bad{background:color-mix(in srgb,var(--bad) 15%,transparent)}
table{width:100%;border-collapse:collapse;font-size:14px}th,td{text-align:left;padding:6px 8px;border-bottom:1px solid var(--line)}
th{color:var(--dim);font-weight:600;font-size:12px;text-transform:uppercase;letter-spacing:.03em}.n{text-align:right;font-variant-numeric:tabular-nums}
ul{margin:0;padding-left:18px}.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:14px}
.big{font-size:22px;font-weight:600;font-variant-numeric:tabular-nums}.small{color:var(--dim);font-size:13px}
.scroll{overflow-x:auto}
.top{display:flex;flex-wrap:wrap;align-items:center;justify-content:space-between;gap:10px}
select,button{font:inherit;color:var(--ink);background:var(--card);border:1px solid var(--line);border-radius:6px;padding:3px 6px}
</style></head><body><main>
<div class="top">
<h1>Yakugate — {{.Role}} <span class="badge {{if .Ready}}ok{{else}}bad{{end}}">{{if .Ready}}ready{{else}}not ready{{end}}</span></h1>
<form method="get" class="small">
{{if .Key}}<input type="hidden" name="key" value="{{.Key}}">{{end}}
<label>Refresh every
<select name="every" onchange="this.form.submit()">
{{range .Choices}}<option value="{{.Sec}}"{{if eq .Sec $.Every}} selected{{end}}>{{.Label}}</option>{{end}}
</select></label>
<button type="submit">Set</button>
</form>
</div>
<div class="meta">{{.Instance}} · up {{.Uptime}} · {{.Version}} · {{.Now}} UTC</div>
{{if .Note}}<div class="card small">{{.Note}}</div>{{end}}
{{with .Overview}}
<div class="card"><h2>Needs attention</h2>
{{if .Problems}}<ul>{{range .Problems}}<li class="bad">{{.}}</li>{{end}}</ul>{{else}}<div class="ok">✓ Nothing — every role is ready and no queue is stuck.</div>{{end}}
</div>
<div class="grid">
<div class="card"><div class="small">Accepted / s</div><div class="big">{{rate .Traffic.Accepted.PerS}}</div><div class="small">{{.Traffic.Accepted.Total}} since start · {{.Traffic.Rejected.Total}} refused</div></div>
<div class="card"><div class="small">Sent to vendors / s</div><div class="big">{{rate .Traffic.Sent.PerS}}</div><div class="small">{{.Traffic.Sent.Total}} since start · {{.Traffic.SendFailed.Total}} refused by vendors</div></div>
<div class="card"><div class="small">Receipts / s</div><div class="big">{{rate .Traffic.Receipts.PerS}}</div><div class="small">{{.Traffic.Receipts.Total}} since start · {{.Traffic.MO.Total}} incoming</div></div>
</div>
<div class="card scroll"><h2>Roles ({{len .Roles}} running)</h2>
<table><tr><th>Role</th><th>Ready</th><th>Up for</th><th>Reported</th><th>Instance</th></tr>
{{range .Roles}}<tr><td>{{.Role}}</td><td class="{{if .Ready}}ok{{else}}bad{{end}}">{{if .Ready}}yes{{else}}no{{end}}</td><td>{{ago .Uptime}}</td><td>{{ago .Seen}} ago</td><td class="small">{{.Instance}}</td></tr>{{end}}
</table></div>
{{if .Stores}}<div class="card scroll"><h2>Stores</h2>
<table><tr><th>Store</th><th>Health</th><th class="n">Round trip</th><th>Figures</th></tr>
{{range .Stores}}<tr><td>{{.Kind}} {{.Name}}</td><td>{{.Health}}{{range .Reasons}}<br><small>{{.}}</small>{{end}}</td><td class="n">{{printf "%.1f" .PingMS}} ms</td><td>{{.Figures}}</td></tr>{{end}}
</table></div>{{end}}
<div class="card scroll"><h2>Queues</h2>
{{if .Queues}}<table><tr><th>Vendor</th><th class="n">Waiting</th><th class="n">Ready</th><th class="n">Held</th><th class="n">In/s</th><th class="n">Out/s</th><th class="n">Oldest</th></tr>
{{range .Queues}}<tr><td>{{.Vendor}}</td><td class="n">{{.Depth}}</td><td class="n">{{.Ready}}</td><td class="n">{{.Held}}</td><td class="n">{{printf "%.1f" .InPerS}}</td><td class="n">{{printf "%.1f" .OutPerS}}</td><td class="n">{{if .Depth}}{{secs .OldestSec}}{{else}}—{{end}}</td></tr>{{end}}
</table>{{else}}<div class="small">No queue reported.</div>{{end}}</div>
<div class="grid">
{{if .SMPP.VendorsTotal}}<div class="card"><h2>SMPP vendors</h2><div class="{{if lt .SMPP.VendorsBound .SMPP.VendorsTotal}}bad{{else}}ok{{end}}">{{.SMPP.VendorsBound}} of {{.SMPP.VendorsTotal}} bound</div><div class="small">sessions {{.SMPP.SessionsBound}} of {{.SMPP.SessionsTotal}} up{{if .SMPP.SessionsDegraded}}, {{.SMPP.SessionsDegraded}} failing{{end}}</div></div>{{end}}
{{with .Billing}}<div class="card"><h2>Billing queue</h2><div class="{{if or (gt .Delayed 0) (ge .OldestSec 120)}}bad{{else}}ok{{end}}">{{.Waiting}} waiting{{if gt .Delayed 0}} · {{.Delayed}} refused by MySQL{{end}}</div><div class="small">{{.Booked}} booked · oldest {{.OldestSec}} s</div></div>{{end}}
{{with .Host}}<div class="card"><h2>Host</h2><div>{{.Name}}</div><div class="small">CPU {{printf "%.0f" .CPUPct}}% (load {{printf "%.2f" .Load1}}) · memory {{printf "%.0f" .MemPct}}% · disk {{printf "%.0f" .DiskPct}}%</div></div>{{end}}
</div>
<div class="small">Overview {{.Source}} at {{.Time.Format "15:04:05"}} UTC. Plain answers for machines: <code>/healthz</code>, <code>/readyz</code>, <code>/status</code>.</div>
{{end}}
</main></body></html>`))

// fmtRate writes a per-second rate: one decimal below 100, whole above.
func fmtRate(f float64) string {
	if f >= 100 {
		return strconv.FormatFloat(f, 'f', 0, 64)
	}
	return strconv.FormatFloat(f, 'f', 1, 64)
}
