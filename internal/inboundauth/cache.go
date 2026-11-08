package inboundauth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/yakugate/yakugate/internal/callbackmap"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/refdata"
	"github.com/yakugate/yakugate/internal/webhookauth"
)

// Cache holds every inbound door in memory and refreshes it on a ticker — the
// same shape as internal/vendorstatus and internal/receiptpolicy, and for the
// same reason: this is read on every callback and must never be a query.
//
// It joins two tables on purpose. `inbound_endpoints` is the new per-kind row
// (config/0121); `vendors.inbound_config` is what a gateway has configured
// today, and it keeps answering for a door that has no credential of its own
// yet. An upgrade therefore changes nothing until an operator sets the new
// credentials, which is the only way to ship this to a live gateway.
type Cache struct {
	db dal.InboundDoors

	mu sync.RWMutex
	// byKey is "<cid>|<kind>" -> door. The cid comes from the URL, so this is
	// the lookup every callback does.
	byKey map[string]Endpoint
	// byPath is a door's own path (PathKey) -> its byKey key (issue 20,
	// config/0152), for a callback to a path that is not /dlr/ or /mo/.
	byPath map[string]string
	// byVendor is "<vendor id>|<kind>" -> its byKey key: a receipt applied
	// later from the orphans is read with its door's status words.
	byVendor map[string]string
	// byDialectPath is "<kind>|<a dlr / mo dialect's own URL>" (PathKey) ->
	// the byKey keys of the doors of that kind reading with that dialect
	// (27 Oct): the doors a callback to that URL may be for.
	byDialectPath map[string][]string
	// unconfigured lists the doors nothing guards, for the once-per-change
	// warning (an operator's only clue why a vendor's callbacks are refused).
	unconfigured []string
	// loaded is false until the first successful read. See Loaded.
	loaded bool
}

// New builds an empty cache bound to the CONFIG store (dal.EngineConfig's
// Doors). A nil db gives a cache that knows no doors, so every callback is
// refused — the same "no config means refused" rule, applied to a store we
// cannot read.
func NewCache(db dal.InboundDoors) *Cache {
	return &Cache{db: db, byKey: map[string]Endpoint{}, byPath: map[string]string{}, byVendor: map[string]string{},
		byDialectPath: map[string][]string{}}
}

// CacheOf is a cache that already holds these doors and reads no store — for
// a test of an edge, and for a tool that judges a callback with no config
// store behind it. It never reloads.
func CacheOf(doors ...Endpoint) *Cache {
	c := &Cache{byKey: map[string]Endpoint{}, loaded: true}
	for _, e := range doors {
		c.byKey[Key(e.VendorCID, e.Kind)] = e
		if !e.Configured() {
			c.unconfigured = append(c.unconfigured, Key(e.VendorCID, e.Kind)) // as Reload lists them
		}
	}
	c.byPath, c.byVendor, c.byDialectPath = indexes(c.byKey)
	return c
}

// Reload replaces the cached doors.
func (c *Cache) Reload(ctx context.Context) error {
	if c == nil || c.db == nil {
		return nil
	}
	// The mapping columns (config/0125) are read when they exist. A role
	// that starts before the migration has run must still load every door —
	// losing the doors would refuse every vendor's callbacks — so a store
	// without them is read the old way, with no mappings.
	// Since config/0128 a door's mapping, reply and credential places come
	// from its DIALECT; before that, from the door's own columns (0127, then
	// 0125). Newest shape first, then each older one (the DAL asks: Doors).
	rows, shape, err := c.db.Doors(ctx)
	withDialect := shape >= shapeDialect
	var cut dal.RowsError
	if errors.As(err, &cut) {
		return cut.Err // a row that would not scan, or a read cut short
	}
	if err != nil {
		return fmt.Errorf("inbound endpoints: %w", err)
	}
	byKey := map[string]Endpoint{}
	legacyByCID := map[string]webhookauth.Config{}
	vendorIDByCID := map[string]int64{}
	for _, row := range rows {
		var e Endpoint
		e.VendorID = row.VendorID
		cid, legacyRaw, kind := row.CID, row.LegacyRaw, row.Kind
		jwtRaw, ipRaw, placesRaw := row.JWTRaw, row.IPRaw, row.PlacesRaw
		disabled, id := row.Disabled, row.ID
		tokenHash, tokenSrc, tokenHdr := row.TokenHash, row.TokenSrc, row.TokenHdr
		username, password, throughput := row.Username, row.Password, row.Throughput
		reqRaw, replyRaw := row.ReqRaw, row.ReplyRaw
		dialectKind, dialectName := row.DialectKind, row.DialectName
		dialectFormat, dialectWrapper := row.DialectFormat, row.DialectWrapper
		dialectKeep, dialectURL, customPath := row.DialectKeep, row.DialectURL, row.CustomPath
		legacyByCID[cid] = webhookauth.Parse(legacyRaw)
		vendorIDByCID[cid] = e.VendorID
		if kind == "" {
			continue // a vendor with no rows yet: only its legacy config exists
		}
		e.ID, e.VendorCID, e.Kind = id, cid, kind
		e.Disabled = disabled != 0
		e.TokenHash, e.TokenSource, e.TokenHeader = tokenHash, tokenSrc, tokenHdr
		e.Username, e.Password, e.MaxThroughput = username, password, throughput
		var jwtCfg refdata.JWTConfig
		if err := json.Unmarshal([]byte(jwtRaw), &jwtCfg); err == nil {
			// Only the key: a door's JWT has no verification settings (PO
			// 2026-09-28). It is checked for its signature, an exp it carries,
			// and a subject naming this door — never for an issuer, audience,
			// subject claim or lifetime rule an older row may still hold.
			e.JWT = refdata.JWTConfig{Alg: jwtCfg.Alg, Secret: jwtCfg.Secret, SecretRef: jwtCfg.SecretRef,
				SecretRefPrev: jwtCfg.SecretRefPrev, PublicKey: jwtCfg.PublicKey}
		}
		_ = json.Unmarshal([]byte(ipRaw), &e.AllowedIPs)
		e.Legacy = legacyByCID[cid]
		if withDialect && dialectKind != "" && dialectKind != kind {
			// A receipt dialect on an MO door (or the reverse) would read the
			// wrong fields; the door keeps the default reading and says why.
			e.MappingErr = fmt.Sprintf("dialect %q is for %s callbacks, not %s", dialectName, dialectKind, kind)
			reqRaw, replyRaw, placesRaw = "", "", ""
			dialectFormat, dialectWrapper, dialectKeep = "", "", 0
			dialectURL = ""
		}
		e.Dialect = dialectName
		// A dialect is one model for every kind since 2026-10-08: the request
		// is a list of fields with its request_format and request_wrapper, the
		// reply a response with its keep_unmapped — and a row still stored in
		// the older shape is read as it always was (callbackmap/legacy.go).
		q, qerr := callbackmap.ParseRequestWith(reqRaw, kind,
			callbackmap.Options{Format: dialectFormat, Wrapper: dialectWrapper})
		if qerr != nil {
			e.MappingErr = qerr.Error()
		} else {
			e.Request = q
		}
		rp, rerr := callbackmap.ParseReplyWith(replyRaw, dialectKeep != 0)
		if rerr != nil {
			e.MappingErr = strings.TrimPrefix(e.MappingErr+"; "+rerr.Error(), "; ")
		} else {
			e.Reply = rp
		}
		if qerr == nil && rerr == nil {
			if err := callbackmap.CheckSize(q, rp); err != nil {
				// Too large to be a dialect: the door keeps the default
				// reading rather than half of one.
				e.MappingErr, e.Request, e.Reply = err.Error(), nil, nil
			}
		}
		if placesRaw != "" {
			var pl Places
			if err := json.Unmarshal([]byte(placesRaw), &pl); err == nil && pl.Check() == nil {
				e.Places = pl
			} else {
				// A place that cannot be read would leave the vendor unable to
				// prove itself; the credentials are still checked, where they
				// were read before places existed, and MappingErr says why.
				e.MappingErr = strings.TrimPrefix(e.MappingErr+"; credential places not loaded", "; ")
			}
		}
		if customPath != "" {
			// A path is kept as both doors clean it; one that does not
			// read (written by hand) is not used, and the log says why —
			// the door still answers on its default path.
			if p, perr := CleanCustomPath(customPath); perr != nil {
				e.PathNote = strings.TrimPrefix(e.PathNote+"; custom path not used: "+perr.Error(), "; ")
			} else {
				e.CustomPath = p
			}
		}
		// The dialect's own URL (27 Oct): the door is one of those a
		// callback to it may be for. A url that does not read (written by
		// hand) is not used, and the log says why.
		if p, perr := DialectURL(dialectURL); perr != nil {
			e.PathNote = strings.TrimPrefix(e.PathNote+"; dialect url not used: "+perr.Error(), "; ")
		} else {
			e.DialectURL = p
		}
		byKey[Key(cid, kind)] = e
	}
	// A vendor with no row for a kind still has a door — the legacy one — so
	// it is filled in here rather than 404ing as an unknown vendor.
	for cid, legacy := range legacyByCID {
		for _, kind := range Kinds() {
			if _, ok := byKey[Key(cid, kind)]; ok {
				continue
			}
			byKey[Key(cid, kind)] = Endpoint{VendorID: vendorIDByCID[cid], VendorCID: cid, Kind: kind, Legacy: legacy}
		}
	}
	var unconfigured []string
	for k, e := range byKey {
		if !e.Configured() {
			unconfigured = append(unconfigured, k)
		}
	}
	byPath, byVendor, byDialectPath := indexes(byKey)
	c.mu.Lock()
	c.byKey, c.byPath, c.byVendor, c.unconfigured, c.loaded = byKey, byPath, byVendor, unconfigured, true
	c.byDialectPath = byDialectPath
	c.mu.Unlock()
	return nil
}

// indexes makes the lookups beside byKey: by a door's own path, by its
// vendor's id, and by its dialect's own URL. Of two doors that claim one path
// (the store's unique key does not allow it; a test might) neither is kept: a
// callback must never reach the wrong vendor's door.
//
// Two doors on one dialect URL that share a token, or a username, cannot be
// told apart there by it: a callback to the URL that carries it is refused
// (Guard.Identify), and each door's PathNote says so, for the roles' log.
// byKey is changed for that note only.
func indexes(byKey map[string]Endpoint) (byPath, byVendor map[string]string, byDialectPath map[string][]string) {
	byPath, byVendor, byDialectPath = map[string]string{}, map[string]string{}, map[string][]string{}
	clash := map[string]bool{}
	for k, e := range byKey {
		if e.VendorID != 0 {
			byVendor[strconv.FormatInt(e.VendorID, 10)+"|"+e.Kind] = k
		}
		if e.DialectURL != "" && e.ID != 0 {
			dk := dialectPathKey(e.Kind, e.DialectURL)
			byDialectPath[dk] = append(byDialectPath[dk], k)
		}
		if e.CustomPath == "" {
			continue
		}
		pk := PathKey(e.CustomPath)
		if _, taken := byPath[pk]; taken {
			clash[pk] = true
		}
		byPath[pk] = k
	}
	for pk := range clash {
		delete(byPath, pk)
	}
	for _, keys := range byDialectPath {
		sort.Strings(keys) // one order, whatever the map's
		sameCredential(byKey, keys)
	}
	return byPath, byVendor, byDialectPath
}

// sameCredential notes, on each door of one dialect URL, another door there
// with the same token or username.
func sameCredential(byKey map[string]Endpoint, keys []string) {
	note := func(k, what string, other Endpoint) {
		e := byKey[k]
		e.PathNote = strings.TrimPrefix(e.PathNote+"; its "+what+" is also vendor "+other.VendorCID+
			"'s on the dialect URL "+e.DialectURL+" — a callback there that carries it is refused; give each door its own", "; ")
		byKey[k] = e
	}
	for i, ki := range keys {
		for _, kj := range keys[i+1:] {
			a, b := byKey[ki], byKey[kj]
			if a.TokenHash != "" && a.TokenHash == b.TokenHash {
				note(ki, "token", b)
				note(kj, "token", a)
			}
			if a.Username != "" && a.Username == b.Username {
				note(ki, "username", b)
				note(kj, "username", a)
			}
		}
	}
}

func dialectPathKey(kind, path string) string { return kind + "|" + PathKey(path) }

// Loaded reports whether a read has completed at least once.
//
// It is what lets an edge tell "no such vendor" from "we do not know yet".
// Without it both look like a cache miss, and the edge has to pick one answer
// for two very different situations: refusing a real vendor while the cache
// warms, or telling a stranger which vendors exist.
func (c *Cache) Loaded() bool {
	if c == nil {
		return false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.loaded
}

// Key is the cache key for one door.
func Key(cid, kind string) string { return strings.ToLower(cid) + "|" + kind }

// For returns the door for a vendor cid and kind. ok=false means the cid
// names no vendor we know — answered as a 404 by the callers, since telling a
// caller which vendors exist is itself information.
func (c *Cache) For(cid, kind string) (Endpoint, bool) {
	if c == nil {
		return Endpoint{}, false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	e, ok := c.byKey[Key(cid, kind)]
	return e, ok
}

// ForPath finds the door of this kind whose own path a callback came to
// (issue 20): the whole path, or — for a vendor that sends its token in the
// path — the path less its last part, which is then the token. Letter case
// is ignored, as the store compares two paths. ok=false = no door of this
// kind names the path (a door of the other kind is not this role's).
func (c *Cache) ForPath(path, kind string) (e Endpoint, urlToken string, ok bool) {
	if c == nil {
		return Endpoint{}, "", false
	}
	p := strings.TrimRight(path, "/")
	if p == "" {
		return Endpoint{}, "", false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	if k, found := c.byPath[PathKey(p)]; found {
		if e := c.byKey[k]; e.Kind == kind {
			return e, "", true
		}
		return Endpoint{}, "", false
	}
	if i := strings.LastIndexByte(p, '/'); i > 0 {
		if k, found := c.byPath[PathKey(p[:i])]; found {
			if e := c.byKey[k]; e.Kind == kind {
				return e, p[i+1:], true
			}
		}
	}
	return Endpoint{}, "", false
}

// ForDialectPath finds the doors of this kind whose dialect's own URL a
// callback came to (27 Oct): the whole path, or the path less its last part,
// which is then the token. They are the candidates; the caller's credentials
// say which one it is (Guard.Identify). ok=false = no dialect of this kind
// has the path as its URL.
func (c *Cache) ForDialectPath(path, kind string) (doors []Endpoint, urlToken string, ok bool) {
	if c == nil {
		return nil, "", false
	}
	p := strings.TrimRight(path, "/")
	if p == "" {
		return nil, "", false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	pick := func(keys []string) []Endpoint {
		out := make([]Endpoint, 0, len(keys))
		for _, k := range keys {
			out = append(out, c.byKey[k])
		}
		return out
	}
	if keys, found := c.byDialectPath[dialectPathKey(kind, p)]; found {
		return pick(keys), "", true
	}
	if i := strings.LastIndexByte(p, '/'); i > 0 {
		if keys, found := c.byDialectPath[dialectPathKey(kind, p[:i])]; found {
			return pick(keys), p[i+1:], true
		}
	}
	return nil, "", false
}

// ForVendor returns a vendor's door of one kind by the vendor's id.
func (c *Cache) ForVendor(vendorID int64, kind string) (Endpoint, bool) {
	if c == nil {
		return Endpoint{}, false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	k, ok := c.byVendor[strconv.FormatInt(vendorID, 10)+"|"+kind]
	if !ok {
		return Endpoint{}, false
	}
	e, ok := c.byKey[k]
	return e, ok
}

// Unconfigured lists the doors nothing guards, newest reading first.
func (c *Cache) Unconfigured() []string {
	if c == nil {
		return nil
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return append([]string(nil), c.unconfigured...)
}

// MappingProblems says which doors of one kind have a dialect that could not
// be loaded, and why — one line a door, "<vendor cid>: <reason>", sorted.
//
// Such a door is not closed: it is read the way a door with no dialect is, so
// a vendor's callbacks keep arriving and are misread (a receipt by the common
// names, an empty 200 for an answer). Until 2026-10-08 the reason was kept on
// the door and shown nowhere; the dlr and mo roles now say it in their log,
// once per change.
func (c *Cache) MappingProblems(kind string) []string {
	if c == nil {
		return nil
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	var out []string
	for _, e := range c.byKey {
		if e.Kind == kind && e.MappingErr != "" {
			out = append(out, e.VendorCID+": "+e.MappingErr)
		}
	}
	sort.Strings(out)
	return out
}

// PathProblems says which doors of one kind have a path note (PathNote): a
// stored path not used, or a token or username another door on the same
// dialect URL has — one line a door, "<vendor cid>: <note>", sorted. The dlr
// and mo roles say it in their log, once per change.
func (c *Cache) PathProblems(kind string) []string {
	if c == nil {
		return nil
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	var out []string
	for _, e := range c.byKey {
		if e.Kind == kind && e.PathNote != "" {
			out = append(out, e.VendorCID+": "+e.PathNote)
		}
	}
	sort.Strings(out)
	return out
}

// Start loads now and every interval until ctx ends. A failed refresh keeps
// the previous doors: losing the config store must not turn every vendor's
// callbacks away, nor let them all in.
func (c *Cache) Start(ctx context.Context, every time.Duration, logf func(string, ...any)) {
	if c == nil || c.db == nil {
		return
	}
	if err := c.Reload(ctx); err != nil && logf != nil {
		logf("%v", err)
	}
	go func() {
		t := time.NewTicker(every)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if err := c.Reload(ctx); err != nil && logf != nil {
					logf("%v", err)
				}
			}
		}
	}()
}

// The shapes of the doors' store, oldest first (the DAL reads the columns a
// shape has; a column of a newer shape is "" or 0). hmac_config is not read:
// the signature scheme was retired (2026-09-28).
const (
	shapeNone    = dal.DoorShapeNone    // config/0121: credentials only
	shapeMaps    = dal.DoorShapeMaps    // config/0125: + the door's request/response mapping
	shapePlaces  = dal.DoorShapePlaces  // config/0127: + the door's credential places
	shapeDialect = dal.DoorShapeDialect // config/0128: all three from the door's dialect
	shapePath    = dal.DoorShapePath    // config/0152: + the door's own path
)
