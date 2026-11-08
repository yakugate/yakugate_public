package hlr

import (
	"context"
	"github.com/yakugate/yakugate/internal/dal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/yakugate/yakugate/internal/netcode"
)

// Ranges answers two questions from the operators table (2026-09-14):
//
//   - Range: which operator was this number's range issued to? It does not
//     know about ported numbers — our HLR data and live lookups do.
//   - Country: which country is it in? From the issuing range when there is
//     one, else from the calling code when only one country uses it (+1 and
//     +7 are shared, so they give no answer).
//
// It matches exactly as the Operators screen's number lookup does
// (p13-backend/src/services/numberLookup.js): ranges are digit strings of one
// length each; a range as long as the number compares whole, a SHORTER one is
// a prefix block compared on the number's leading digits, a longer one cannot
// cover it; the longest covering range wins, then the narrowest.
//
// The table is ~349k rows, held in memory as numbers (about 11 MB) and
// reloaded when its row count or newest update changes.
type Ranges struct {
	mu     sync.RWMutex
	byLen  map[int]*rangeSet
	lens   []int // range lengths present, longest first
	codes  []callingCode
	nets   []network
	loaded time.Time
	sig    string // row count + newest update, to skip unchanged reloads
	logf   func(string, ...any)
}

// network is one range's network; name and ccc (6 Nov 2026) travel with a
// range answer into the message and its lookup log row.
type network struct{ mcc, mnc, name, ccc string }

type rangeSpan struct {
	from, to uint64
	net      uint32 // index into nets
}

// rangeSet is every range of one length, sorted by start, with the running
// maximum end so a lookup can stop scanning back as soon as no earlier range
// can reach the number.
type rangeSet struct {
	spans  []rangeSpan
	maxEnd []uint64
}

type callingCode struct {
	ccc string
	mcc string // "" when the code is shared by several countries
}

// NewRanges returns an empty index; Load or Start fills it.
func NewRanges(logf func(string, ...any)) *Ranges {
	if logf == nil {
		logf = func(string, ...any) {}
	}
	return &Ranges{byLen: map[int]*rangeSet{}, logf: logf}
}

// Size is the number of ranges held.
func (r *Ranges) Size() int {
	if r == nil {
		return 0
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	n := 0
	for _, s := range r.byLen {
		n += len(s.spans)
	}
	return n
}

// Range returns the network the number's range was issued to.
func (r *Ranges) Range(msisdn string) (mcc, mnc string, ok bool) {
	if r == nil {
		return "", "", false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if n, hit := r.find(msisdn); hit {
		return n.mcc, n.mnc, true
	}
	return "", "", false
}

// Differs says the number's range in the operators table names a network
// OTHER than mcc/mnc (6 Nov 2026, Saleem: "if mnc and mcc for a number is in
// the operators list, and the HLR provider says a different mnc/mcc, then it
// is ported"). A number outside every range, or an answer with no MCC, never
// differs.
func (r *Ranges) Differs(msisdn, mcc, mnc string) bool {
	if r == nil || mcc == "" {
		return false
	}
	rmcc, rmnc, ok := r.Range(msisdn)
	return ok && (rmcc != mcc || netcode.Norm(rmnc) != netcode.Norm(mnc))
}

// Named returns the operator's name and the country calling code of the
// number's range (6 Nov 2026), as the operators table has them.
func (r *Ranges) Named(msisdn string) (name, ccc string, ok bool) {
	if r == nil {
		return "", "", false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if n, hit := r.find(msisdn); hit {
		return n.name, n.ccc, true
	}
	return "", "", false
}

// Country returns only the number's country code (MCC).
func (r *Ranges) Country(msisdn string) (mcc string, ok bool) {
	if r == nil {
		return "", false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if n, hit := r.find(msisdn); hit && n.mcc != "" {
		return n.mcc, true
	}
	for _, c := range r.codes { // longest code first
		if strings.HasPrefix(msisdn, c.ccc) {
			return c.mcc, c.mcc != ""
		}
	}
	return "", false
}

func (r *Ranges) find(msisdn string) (network, bool) {
	for _, l := range r.lens {
		if l > len(msisdn) || l > 19 {
			continue
		}
		head, err := strconv.ParseUint(msisdn[:l], 10, 64)
		if err != nil {
			return network{}, false
		}
		set := r.byLen[l]
		// Last span starting at or before head.
		i := sort.Search(len(set.spans), func(i int) bool { return set.spans[i].from > head }) - 1
		best, bestWidth := -1, uint64(0)
		for ; i >= 0 && set.maxEnd[i] >= head; i-- {
			sp := set.spans[i]
			if sp.to >= head {
				if w := sp.to - sp.from; best < 0 || w < bestWidth {
					best, bestWidth = int(sp.net), w
				}
			}
		}
		if best >= 0 {
			return r.nets[best], true
		}
	}
	return network{}, false
}

// rangeRow is one operators row as the loader reads it.
type rangeRow struct{ ccc, mcc, mnc, from, to, name string }

// build replaces the index with rows.
func (r *Ranges) build(rows []rangeRow, sig string) {
	netIdx := map[network]uint32{}
	var nets []network
	byLen := map[int]*rangeSet{}
	cccMCC := map[string]map[string]bool{}
	for _, row := range rows {
		if row.ccc != "" && row.mcc != "" {
			if cccMCC[row.ccc] == nil {
				cccMCC[row.ccc] = map[string]bool{}
			}
			cccMCC[row.ccc][row.mcc] = true
		}
		l := len(row.from)
		if l == 0 || l != len(row.to) || l > 19 {
			continue
		}
		f, err1 := strconv.ParseUint(row.from, 10, 64)
		t, err2 := strconv.ParseUint(row.to, 10, 64)
		if err1 != nil || err2 != nil || t < f {
			continue
		}
		nw := network{mcc: row.mcc, mnc: netcode.Norm(row.mnc), name: row.name, ccc: row.ccc}
		id, seen := netIdx[nw]
		if !seen {
			id = uint32(len(nets))
			nets = append(nets, nw)
			netIdx[nw] = id
		}
		if byLen[l] == nil {
			byLen[l] = &rangeSet{}
		}
		byLen[l].spans = append(byLen[l].spans, rangeSpan{from: f, to: t, net: id})
	}
	var lens []int
	for l, set := range byLen {
		sort.Slice(set.spans, func(i, j int) bool { return set.spans[i].from < set.spans[j].from })
		set.maxEnd = make([]uint64, len(set.spans))
		var m uint64
		for i, sp := range set.spans {
			if sp.to > m {
				m = sp.to
			}
			set.maxEnd[i] = m
		}
		lens = append(lens, l)
	}
	sort.Sort(sort.Reverse(sort.IntSlice(lens)))
	var codes []callingCode
	for ccc, mccs := range cccMCC {
		c := callingCode{ccc: ccc}
		if len(mccs) == 1 {
			for m := range mccs {
				c.mcc = m
			}
		}
		codes = append(codes, c)
	}
	sort.Slice(codes, func(i, j int) bool {
		if len(codes[i].ccc) != len(codes[j].ccc) {
			return len(codes[i].ccc) > len(codes[j].ccc)
		}
		return codes[i].ccc < codes[j].ccc
	})
	r.mu.Lock()
	r.byLen, r.lens, r.codes, r.nets, r.sig, r.loaded = byLen, lens, codes, nets, sig, time.Now()
	r.mu.Unlock()
}

// Load reads the operators table from the config store (through the DAL). It
// skips the read when the table has not changed since the last load.
func (r *Ranges) Load(ctx context.Context, db dal.HLRLookupConfig) error {
	if db == nil {
		return errNoConfigStore
	}
	n, newest, err := db.NumberRangesMark(ctx)
	if err != nil {
		return err
	}
	sig := strconv.FormatInt(n, 10) + "/" + newest
	r.mu.RLock()
	same := sig == r.sig
	r.mu.RUnlock()
	if same {
		return nil
	}
	rows, err := db.NumberRanges(ctx, n)
	if err != nil {
		return err
	}
	out := make([]rangeRow, 0, n)
	for _, rr := range rows {
		row := rangeRow{ccc: rr.CCC, mcc: rr.MCC, mnc: rr.MNC, from: rr.From, to: rr.To, name: rr.OperatorName}
		row.ccc, row.from, row.to = strings.TrimSpace(row.ccc), strings.TrimSpace(row.from), strings.TrimSpace(row.to)
		out = append(out, row)
	}
	r.build(out, sig)
	r.logf("[hlr-ranges] %d number ranges loaded", r.Size())
	return nil
}

// Start loads now and then checks for changes every interval until ctx ends.
// A failed load keeps the previous index.
func (r *Ranges) Start(ctx context.Context, db dal.HLRLookupConfig, every time.Duration) {
	if every <= 0 {
		every = 5 * time.Minute
	}
	load := func() {
		lctx, cancel := context.WithTimeout(ctx, 60*time.Second)
		defer cancel()
		if err := r.Load(lctx, db); err != nil {
			r.logf("[hlr-ranges] load failed (keeping the previous ranges): %v", err)
		}
	}
	load()
	go func() {
		t := time.NewTicker(every)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				load()
			}
		}
	}()
}
