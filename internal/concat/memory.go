package concat

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/yakugate/yakugate/internal/model"
)

// Memory is the assemble-first buffer in the process's own memory
// (YAKU_CONCAT_MT_STORE / YAKU_CONCAT_MO_STORE = memory; Saleem, 28 Oct 2026:
// "each gateway's own heap memory, shared by all its sessions"). Every session
// of the process shares it; another process, another gateway, or this one
// after a restart does not see it. A group whose parts were lost that way is
// never completed: the parts that arrived after the restart run out as
// incomplete, like any other group.
type Memory struct {
	timeout time.Duration
	logf    func(format string, args ...any)
	mo      bool
	scope   func(int64) int64

	mu     sync.Mutex
	groups map[string]*memGroup
}

type memGroup struct {
	first time.Time
	parts map[int]*model.Message // by ConcatSeq
}

// NewMemory is the MT buffer (a client's long message) in memory.
func NewMemory(timeout time.Duration, logf func(string, ...any)) *Memory {
	if timeout <= 0 {
		timeout = 60 * time.Second
	}
	if logf == nil {
		logf = func(string, ...any) {}
	}
	return &Memory{timeout: timeout, logf: logf, groups: map[string]*memGroup{}}
}

// NewMemoryMO is NewMemory for incoming (MO) multi-part messages.
func NewMemoryMO(timeout time.Duration, logf func(string, ...any)) *Memory {
	m := NewMemory(timeout, logf)
	m.mo = true
	return m
}

func (b *Memory) key(m *model.Message) string {
	if b.mo {
		return moKey(m, b.scope)
	}
	return GroupKey(m)
}

// AddPart implements Buffer. A part seen twice keeps its first copy.
func (b *Memory) AddPart(_ context.Context, m *model.Message) (bool, *model.Message, error) {
	cp := *m
	key := b.key(m)
	b.mu.Lock()
	g := b.groups[key]
	if g == nil {
		g = &memGroup{first: time.Now(), parts: map[int]*model.Message{}}
		b.groups[key] = g
	}
	if _, seen := g.parts[m.ConcatSeq]; !seen {
		g.parts[m.ConcatSeq] = &cp
	}
	if len(g.parts) < m.ConcatTotal {
		b.mu.Unlock()
		return false, nil, nil
	}
	delete(b.groups, key)
	b.mu.Unlock()
	return true, Assemble(partsOf(g)), nil
}

// ReleaseExpired implements Buffer (choice A: recorded expired, dropped).
func (b *Memory) ReleaseExpired(_ context.Context) ([]*model.Message, error) {
	cutoff := time.Now().Add(-b.timeout)
	var gone []*memGroup
	var keys []string
	b.mu.Lock()
	for k, g := range b.groups {
		if g.first.Before(cutoff) {
			gone = append(gone, g)
			keys = append(keys, k)
			delete(b.groups, k)
		}
	}
	b.mu.Unlock()
	var expired []*model.Message
	for i, g := range gone {
		parts := partsOf(g)
		rep := Assemble(parts)
		rep.Status = model.StatusExpired
		if b.mo {
			rep.ConcatTotal = parts[0].ConcatTotal // "k of n parts arrived"
		}
		b.logf("concat group %s incomplete after %s — recording expired, dropping %d part(s)", keys[i], b.timeout, len(parts))
		expired = append(expired, rep)
	}
	return expired, nil
}

// Pending reports how many groups wait and how long the oldest has waited.
func (b *Memory) Pending(context.Context) (groups, oldestAgeSec int64, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	var oldest time.Time
	for _, g := range b.groups {
		if oldest.IsZero() || g.first.Before(oldest) {
			oldest = g.first
		}
	}
	if !oldest.IsZero() {
		oldestAgeSec = int64(time.Since(oldest).Seconds())
	}
	return int64(len(b.groups)), oldestAgeSec, nil
}

func partsOf(g *memGroup) []*model.Message {
	parts := make([]*model.Message, 0, len(g.parts))
	for _, p := range g.parts {
		parts = append(parts, p)
	}
	sort.Slice(parts, func(i, j int) bool { return parts[i].ConcatSeq < parts[j].ConcatSeq })
	return parts
}
