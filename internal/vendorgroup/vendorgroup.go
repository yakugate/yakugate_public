// Package vendorgroup says which vendors are one carrier when a receipt is
// matched to its message: the vendors of one company (vendors.company_id).
//
// Saleem 2026-10-14: a carrier whose send bind (TX) and receive bind (RX) are
// two vendor rows sends the message on one and its receipt on the other, so a
// receipt is matched by the company and the vendor's message id, then the
// handset number, then our own message id (internal/dlr, groups.go). A
// vendor with no company is a company of its own, matched as before.
//
// Groups implements dlr.VendorGroups. A role loads it with its vendors and
// reloads it when they are reloaded.
package vendorgroup

import (
	"context"
	"sort"
	"sync"

	"github.com/yakugate/yakugate/internal/dal"
)

// Groups is the vendors' companies, as last loaded.
type Groups struct {
	mu      sync.RWMutex
	company map[int64]int64   // vendor -> its company (absent = none)
	members map[int64][]int64 // company -> its vendors, sorted
}

// New is an empty Groups: every vendor its own until Load.
func New() *Groups { return &Groups{} }

// Load reads every vendor's company from the config store
// (dal.VendorCompanies). On an error the groups stay as they were.
func (g *Groups) Load(ctx context.Context, db dal.VendorCompanies) error {
	if g == nil || db == nil {
		return nil
	}
	all, err := db.VendorCompanies(ctx)
	if err != nil {
		return err
	}
	byVendor := map[int64]int64{}
	for v, c := range all {
		if c > 0 {
			byVendor[v] = c
		}
	}
	g.Set(byVendor)
	return nil
}

// Set replaces the groups: vendor id -> company id (a vendor not named has
// none).
func (g *Groups) Set(byVendor map[int64]int64) {
	members := map[int64][]int64{}
	for v, c := range byVendor {
		members[c] = append(members[c], v)
	}
	for _, vs := range members {
		sort.Slice(vs, func(i, j int) bool { return vs[i] < vs[j] })
	}
	g.mu.Lock()
	g.company, g.members = byVendor, members
	g.mu.Unlock()
}

// Scope is the id a vendor's receipts are matched under: minus its company's
// id, or its own id when it has no company.
func (g *Groups) Scope(vendorID int64) int64 {
	if g == nil {
		return vendorID
	}
	g.mu.RLock()
	c := g.company[vendorID]
	g.mu.RUnlock()
	if c > 0 {
		return -c
	}
	return vendorID
}

// Members is every vendor of the vendor's company, or the vendor alone.
func (g *Groups) Members(vendorID int64) []int64 {
	if g == nil {
		return []int64{vendorID}
	}
	g.mu.RLock()
	defer g.mu.RUnlock()
	c := g.company[vendorID]
	if c == 0 {
		return []int64{vendorID}
	}
	return g.members[c]
}
