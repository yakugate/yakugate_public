package mo

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
	mosvc "github.com/yakugate/yakugate/internal/mo"
	"github.com/yakugate/yakugate/internal/model"
	"github.com/yakugate/yakugate/internal/platform/config"
)

// The MO webhook says which network the subscriber is on and which vendor the
// message came from (Saleem 4 Nov 2026). This file finds both for a push: our
// HLR data for the sender's number (what we already hold, never a paid live
// lookup) and the vendor's cid and name.

// numberLookFor is our HLR data as this role reads it, read through the DAL
// (this role's lane may not import internal/hlr): with Redis, the main
// Redis's copy of each number, as the accepting roles read it; else the HLR
// store's shard tables. nil when neither can be opened: pushes then carry
// only the network the vendor gave.
func numberLookFor(cfg config.Config, logf func(string, ...any)) mosvc.NumberLook {
	if cfg.RedisEnabled() {
		c, err := dal.OpenHLRCopies(cfg)
		if err != nil {
			logf("HLR copies unavailable (%v): MO webhooks carry only the network the vendor gives", err)
			return nil
		}
		return redisNumberLook(c)
	}
	b, dsn := cfg.HLRStore()
	d, ok := dal.Lookup(b)
	if dsn == "" || !ok || d.HLRNumbers == nil {
		logf("no HLR store: MO webhooks carry only the network the vendor gives")
		return nil
	}
	h, err := d.HLRNumbers(dsn)
	if err != nil {
		logf("HLR store unavailable (%v): MO webhooks carry only the network the vendor gives", err)
		return nil
	}
	return shardNumberLook(h)
}

// hlrCopy is a number's copy in the main Redis: internal/hlr's Entry as JSON
// (the fields have no tags, so they are named as in Go).
type hlrCopy struct {
	MCC, MNC        string
	Ported, Deleted bool
}

// redisNumberLook reads hlr:<msisdn>. A miss or an error is "not known".
func redisNumberLook(c dal.HLRCopies) mosvc.NumberLook {
	return func(msisdn string) (string, string, bool, bool) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		raw, found, err := c.GetHLR(ctx, msisdn)
		if err != nil || !found {
			return "", "", false, false
		}
		var e hlrCopy
		if json.Unmarshal(raw, &e) != nil || e.Deleted || e.MCC == "" {
			return "", "", false, false
		}
		return e.MCC, e.MNC, e.Ported, true
	}
}

// shardNumberLook reads the number's shard, hlr_<first 4 digits> (the rule
// of hlr.Shard).
func shardNumberLook(h dal.HLRNumbers) mosvc.NumberLook {
	return func(msisdn string) (string, string, bool, bool) {
		shard := msisdn
		if len(shard) > 4 {
			shard = shard[:4]
		}
		r, ok := h.Lookup(shard, msisdn)
		if !ok || r.Deleted || r.MCC == "" {
			return "", "", false, false
		}
		return r.MCC, r.MNC, r.Ported, true
	}
}

// vendorNames keeps every vendor's cid and name, read again at most every
// vendorNamesEvery, and at once (at most every few seconds) for a vendor it
// does not know yet.
type vendorNames struct {
	read func(ctx context.Context) (map[int64]dal.VendorName, error)
	mu   sync.Mutex
	m    map[int64]dal.VendorName
	at   time.Time
}

const (
	vendorNamesEvery = 60 * time.Second
	vendorNamesMiss  = 5 * time.Second
)

// of is the vendor's cid and name; empty when not known.
func (v *vendorNames) of(ctx context.Context, id int64) dal.VendorName {
	if v == nil || v.read == nil || id <= 0 {
		return dal.VendorName{}
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	got, ok := v.m[id]
	age := time.Since(v.at)
	if age >= vendorNamesEvery || (!ok && age >= vendorNamesMiss) {
		// Read under the lock: one read at a time, and a slow config store
		// holds pushes for no longer than one read.
		if m, err := v.read(ctx); err == nil {
			v.m, got = m, m[id]
		}
		v.at = time.Now()
	}
	return got
}

// pushFacts is an MO's network and vendor for its webhook.
func (p *Process) pushFacts(ctx context.Context, m model.Message) mosvc.PushFacts {
	vn := p.vendors.of(ctx, m.VendorID)
	return mosvc.FactsFor(m, p.numbers, vn.CID, vn.Name)
}
