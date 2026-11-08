package monitor

// MySQL and Redis, as they report themselves (Saleem 2026-10-05, D-129). The
// http-server reads them (internal/storewatch) and publishes `stores`; this
// lifts the figures their history and events need into the `stores` column
// (hot/0056) and reads them back.

import (
	"encoding/json"
	"fmt"
)

// storePoint is one store in one sample (the column's short names).
type storePoint struct {
	K  string  `json:"k"`            // mysql | redis
	A  string  `json:"a,omitempty"`  // address, or where the settings point
	OK int     `json:"ok"`           // reachable
	W  int     `json:"w"`            // 0 ok, 1 warning, 2 down
	P  float64 `json:"p,omitempty"`  // round trip, ms
	C  float64 `json:"c,omitempty"`  // MySQL: connections % of max
	N  float64 `json:"n,omitempty"`  // MySQL: connections
	M  float64 `json:"m,omitempty"`  // Redis: memory %
	CL float64 `json:"cl,omitempty"` // Redis: clients
	Q  float64 `json:"q,omitempty"`  // queries (MySQL) / commands (Redis) a second
	S  float64 `json:"s,omitempty"`  // MySQL: slow queries a second
	E  float64 `json:"e,omitempty"`  // Redis: keys evicted a second
	H  float64 `json:"h,omitempty"`  // cache hit %
	L  float64 `json:"l,omitempty"`  // replica lag, s
	U  int64   `json:"u,omitempty"`  // uptime, s
	X  string  `json:"x,omitempty"`  // the first reason it is not ok
}

var healthLevel = map[string]int{"ok": 0, "warning": 1, "down": 2}

// liftStores builds the `stores` column from a decoded snapshot; nil when it
// has none.
func liftStores(snap map[string]any) []byte {
	sec, _ := snap["stores"].(map[string]any)
	if sec == nil {
		return nil
	}
	out := map[string]storePoint{}
	first := func(m map[string]any) string {
		if rs, ok := m["reasons"].([]any); ok && len(rs) > 0 {
			if s, ok := rs[0].(string); ok {
				return truncate(s, 160)
			}
		}
		if e, ok := m["error"].(string); ok {
			return truncate(e, 160)
		}
		return ""
	}
	okInt := func(m map[string]any) int {
		if b, _ := m["ok"].(bool); b {
			return 1
		}
		return 0
	}
	for _, e := range listOf(sec["mysql"]) {
		key, _ := e["key"].(string)
		if key == "" {
			continue
		}
		conn, _ := e["connections"].(map[string]any)
		rates, _ := e["rates"].(map[string]any)
		rep, _ := e["replica"].(map[string]any)
		p := storePoint{K: sqlKind(e), A: str(e, "addr"), OK: okInt(e), W: healthLevel[str(e, "health")], P: toFloat(e["ping_ms"]),
			C: toFloat(conn["pct"]), N: toFloat(conn["current"]), Q: toFloat(rates["queries"]), S: toFloat(rates["slow"]),
			U: toInt64(e["uptime_sec"]), X: first(e)}
		if h := toFloat(rates["buffer_pool_hit_pct"]); h >= 0 {
			p.H = h
		}
		if rep != nil {
			p.L = toFloat(rep["lag_sec"])
		}
		out[key] = p
	}
	for _, e := range listOf(sec["redis"]) {
		key, _ := e["key"].(string)
		if key == "" {
			continue
		}
		mem, _ := e["memory"].(map[string]any)
		rates, _ := e["rates"].(map[string]any)
		repl, _ := e["replication"].(map[string]any)
		p := storePoint{K: "redis", A: str(e, "where"), OK: okInt(e), W: healthLevel[str(e, "health")], P: toFloat(e["ping_ms"]),
			M: toFloat(mem["pct"]), CL: toFloat(e["clients"]), Q: toFloat(e["ops_per_sec"]), E: toFloat(rates["evicted"]),
			L: toFloat(repl["max_lag_sec"]), U: toInt64(e["uptime_sec"]), X: first(e)}
		if h := toFloat(rates["hit_pct"]); h >= 0 {
			p.H = h
		}
		out[key] = p
	}
	if len(out) == 0 {
		return nil
	}
	b, err := json.Marshal(out)
	if err != nil {
		return nil
	}
	return b
}

func listOf(v any) []map[string]any {
	arr, _ := v.([]any)
	out := make([]map[string]any, 0, len(arr))
	for _, x := range arr {
		if m, ok := x.(map[string]any); ok {
			out = append(out, m)
		}
	}
	return out
}

// parseStores reads the column back; nil for NULL or anything unreadable.
func parseStores(b []byte) map[string]storePoint {
	if len(b) == 0 {
		return nil
	}
	var m map[string]storePoint
	if json.Unmarshal(b, &m) != nil {
		return nil
	}
	return m
}

// sqlKind is a SQL server's kind as storewatch reports it: mysql, or mssql
// (SQL Server, since 2 Nov 2026); an older report says none and is MySQL.
func sqlKind(e map[string]any) string {
	if k := str(e, "kind"); k != "" {
		return k
	}
	return "mysql"
}

// storeLabel names a store in an event: "MySQL 127.0.0.1:3306", "SQL Server
// 127.0.0.1:1433", "Redis main".
func storeLabel(key string, p storePoint) string {
	kind := map[string]string{"mysql": "MySQL", "mssql": "SQL Server", "redis": "Redis"}[p.K]
	if kind == "" {
		kind = "Store"
	}
	if p.A != "" {
		return fmt.Sprintf("%s %s", kind, p.A)
	}
	return fmt.Sprintf("%s %s", kind, key)
}

// strs reads a JSON list of strings.
func strs(v any) []string {
	arr, _ := v.([]any)
	var out []string
	for _, x := range arr {
		if s, ok := x.(string); ok && s != "" {
			out = append(out, s)
		}
	}
	return out
}
