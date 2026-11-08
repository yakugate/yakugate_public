package daltest

import (
	"context"
	"strings"
	"testing"

	"github.com/yakugate/yakugate/internal/dal"
)

// RunSQLServerProbe is a SQL server probe's contract (dal.SQLServerProbe): a
// reachable database answers, its server's figures are read, and its size is
// known. Read only.
func RunSQLServerProbe(t *testing.T, p dal.SQLServerProbe) {
	ctx := context.Background()
	if p.Addr() == "" || p.Database() == "" {
		t.Fatalf("addr %q database %q", p.Addr(), p.Database())
	}
	if err := p.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	st := p.Global(ctx, "STATUS", []string{"Uptime", "Threads_connected"})
	if st["Uptime"] == "" || st["Threads_connected"] == "" {
		t.Fatalf("status %v", st)
	}
	if v := p.Global(ctx, "VARIABLES", []string{"version", "max_connections"}); v["version"] == "" {
		t.Fatalf("variables %v", v)
	}
	p.Deadlocks(ctx) // may be refused (PROCESS); never fails the read
	if r := p.Replica(ctx); r.IsReplica && !r.Known {
		t.Fatalf("replica %+v", r)
	}
	sizes, err := p.SchemaSizes(ctx, []string{p.Database()})
	if err != nil || sizes[p.Database()][1] == 0 {
		t.Fatalf("sizes %v %v", sizes, err)
	}
}

// RunRedisServerProbe is a Redis probe's contract (dal.RedisServerProbe).
func RunRedisServerProbe(t *testing.T, p dal.RedisServerProbe) {
	ctx := context.Background()
	if err := p.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	info, err := p.Info(ctx)
	if err != nil || !strings.Contains(info, "redis_version:") {
		t.Fatalf("info %v", err)
	}
	if p.Addr() == "" {
		t.Fatal("no address")
	}
	if n := len(p.Sentinels()); n != 0 {
		t.Fatalf("%d Sentinels with none set", n)
	}
}
