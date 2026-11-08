package provision

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/msisdn"
	"github.com/yakugate/yakugate/internal/netcode"
)

// Where a rule sends: ONE destination per rule (Saleem 2026-09-30) — any
// destination, a whole country, or one network. It is stored as the rule's
// match_mccmncs: [] for any, ["410"] for a country, ["410004"] for a
// network, the MNC written as the operators table writes it. [SCOPE P14-ROUTE]

// DestKind is the kind of a destination.
type DestKind string

const (
	DestAny     DestKind = "any"
	DestCountry DestKind = "country"
	DestNetwork DestKind = "network"
)

// Destination is where a rule's messages go. The names are filled by the
// store (ResolveDestination, the plan readers) and are for people only.
type Destination struct {
	Kind DestKind
	MCC  string // country and network: 3 digits
	MNC  string // network only, as the operators table writes it ("004")

	Country string // "Pakistan"
	CCC     string // calling code, "92"
	Network string // "CMPAK (Zong)"
	Series  string // the network's first number series start, "923100000000"
}

// AnyDestination is every number, including numbers whose country is not found.
func AnyDestination() Destination { return Destination{Kind: DestAny} }

var (
	destMCCRe = regexp.MustCompile(`^[0-9]{3}$`)
	destNetRe = regexp.MustCompile(`^([0-9]{3})[\s\-/]?([0-9]{1,3})$`)
)

// ParseDestination reads a destination as typed or stored: "" / "any" / "*"
// is any destination; "410" a country (its MCC); "41004", "410004",
// "410-04" or "410 004" a network. Names are not filled.
func ParseDestination(s string) (Destination, error) {
	t := strings.ToLower(strings.TrimSpace(s))
	switch t {
	case "", "any", "*", "everything", "everything else", "any destination":
		return AnyDestination(), nil
	}
	if destMCCRe.MatchString(t) {
		return Destination{Kind: DestCountry, MCC: t}, nil
	}
	if g := destNetRe.FindStringSubmatch(t); g != nil {
		return Destination{Kind: DestNetwork, MCC: g[1], MNC: g[2]}, nil
	}
	return Destination{}, refusef("%q is not a destination: write any, a country code (MCC, 410) or a network (MCC and MNC, 410-04)", s)
}

// String is the code form ParseDestination reads: "any", "410", "410-004".
func (d Destination) String() string {
	switch d.Kind {
	case DestCountry:
		return d.MCC
	case DestNetwork:
		return d.MCC + "-" + d.MNC
	}
	return "any"
}

// Networks is the destination as routes.match_mccmncs.
func (d Destination) Networks() []string {
	switch d.Kind {
	case DestCountry:
		return []string{d.MCC}
	case DestNetwork:
		return []string{d.MCC + d.MNC}
	}
	return nil
}

// DestinationOf reads a rule's stored networks as its one destination.
// ok=false when they name several (a rule made before 2026-09-30).
func DestinationOf(networks []string) (Destination, bool) {
	var clean []string
	for _, n := range networks {
		if n = strings.TrimSpace(n); n != "" {
			clean = append(clean, n)
		}
	}
	switch len(clean) {
	case 0:
		return AnyDestination(), true
	case 1:
		mcc, mnc := netcode.Split(clean[0])
		if mnc == "" {
			return Destination{Kind: DestCountry, MCC: mcc}, true
		}
		return Destination{Kind: DestNetwork, MCC: mcc, MNC: mnc}, true
	}
	return Destination{}, false
}

// Same reports whether two destinations are the same place (an MNC written
// "04" and "004" is one network).
func (d Destination) Same(o Destination) bool {
	if d.Kind != o.Kind {
		return false
	}
	switch d.Kind {
	case DestCountry:
		return d.MCC == o.MCC
	case DestNetwork:
		return d.MCC == o.MCC && netcode.SameMNC(d.MNC, o.MNC)
	}
	return true
}

// Covers reports whether d takes every number o takes.
func (d Destination) Covers(o Destination) bool {
	switch d.Kind {
	case DestAny:
		return o.Kind != ""
	case DestCountry:
		return (o.Kind == DestCountry || o.Kind == DestNetwork) && o.MCC == d.MCC
	case DestNetwork:
		return o.Kind == DestNetwork && d.Same(o)
	}
	return false
}

func (d Destination) countryName() string {
	if d.Country != "" {
		return d.Country
	}
	return "MCC " + d.MCC
}

func (d Destination) networkName() string {
	if d.Network != "" {
		return d.Network
	}
	return "network " + d.MCC + "-" + d.MNC
}

// Short names the destination in a few words: "Any destination",
// "Pakistan", "CMPAK (Zong)".
func (d Destination) Short() string {
	switch d.Kind {
	case DestCountry:
		return d.countryName()
	case DestNetwork:
		return d.networkName()
	case DestAny:
		return "Any destination"
	}
	return "not chosen"
}

// Label names it in full: "CMPAK (Zong) (Pakistan 410-004)".
func (d Destination) Label() string {
	switch d.Kind {
	case DestCountry:
		return d.countryName()
	case DestNetwork:
		c := d.Country
		if c == "" {
			c = "MCC " + d.MCC
		}
		return fmt.Sprintf("%s (%s %s-%s)", d.networkName(), c, d.MCC, d.MNC)
	case DestAny:
		return "Any destination"
	}
	return "not chosen"
}

// Reads is what the gateway will read from this destination (the editor's
// line under the picker).
func (d Destination) Reads() string {
	switch d.Kind {
	case DestAny:
		return "Catches every number, including numbers whose country is not found."
	case DestCountry:
		n := d.countryName()
		return "Catches every " + n + " number that no network rule takes, and " + n + " numbers whose network is not found."
	case DestNetwork:
		c := d.countryName()
		return "Catches numbers the gateway places on " + d.networkName() + ". A " + c + " number whose network is not found goes to the '" +
			c + "' rule or 'Everything else' instead."
	}
	return ""
}

// ── the operators table, summarised (cached) ─────────────────────────────

type catNetwork struct {
	MCC, MNC, CCC, Country, Name, First string
	Ranges                              int
}

type catalog struct {
	at        time.Time
	networks  []catNetwork          // by country name, then network name
	byNet     map[string]int        // mcc + netcode.Norm(mnc) → index
	countries map[string]catNetwork // mcc → country (MNC empty)
	codes     map[string][]string   // calling code → the MCCs its rows name
}

var (
	catMu    sync.Mutex
	catCache = map[dal.ConfigStore]*catalog{}
)

// catalogTTL is how long the summary is reused: the table changes on imports,
// not per request, and the engine itself reloads ranges every 5 minutes.
const catalogTTL = 10 * time.Minute

// operatorCatalog is every network and country in the operators table, read
// through the covering index (≈0.5 s for 349k rows) and kept for ten minutes.
func operatorCatalog(ctx context.Context, db dal.ConfigStore) (*catalog, error) {
	catMu.Lock()
	c := catCache[db]
	catMu.Unlock()
	if c != nil && time.Since(c.at) < catalogTTL {
		return c, nil
	}
	rows, err := db.OperatorCatalog(ctx)
	if err != nil {
		if re, ok := err.(dal.RowsError); ok {
			return nil, re.Err
		}
		return nil, fmt.Errorf("read the operators table: %w", err)
	}
	c = &catalog{at: time.Now(), byNet: map[string]int{}, countries: map[string]catNetwork{}, codes: map[string][]string{}}
	seenCode := map[string]bool{}
	for _, row := range rows {
		n := catNetwork{CCC: row.CCC, MCC: row.MCC, MNC: row.MNC, Country: row.Country, Name: row.Name, Ranges: row.Ranges}
		n.First = row.First.String
		if n.MCC == "" {
			continue
		}
		if n.CCC != "" && !seenCode[n.CCC+"|"+n.MCC] {
			seenCode[n.CCC+"|"+n.MCC] = true
			c.codes[n.CCC] = append(c.codes[n.CCC], n.MCC)
		}
		if co, ok := c.countries[n.MCC]; !ok || (co.Country == "" && n.Country != "") {
			c.countries[n.MCC] = catNetwork{MCC: n.MCC, CCC: n.CCC, Country: n.Country}
		}
		if n.MNC == "" {
			continue
		}
		key := n.MCC + netcode.Norm(n.MNC)
		if i, ok := c.byNet[key]; ok {
			// One network under two calling codes or MNC spellings: keep the
			// spelling with the most ranges, count them all.
			old := c.networks[i]
			if n.Ranges > old.Ranges {
				n.Ranges += old.Ranges
				if old.First != "" && (n.First == "" || old.First < n.First) {
					n.First = old.First
				}
				c.networks[i] = n
			} else {
				old.Ranges += n.Ranges
				if n.First != "" && (old.First == "" || n.First < old.First) {
					old.First = n.First
				}
				c.networks[i] = old
			}
			continue
		}
		c.byNet[key] = len(c.networks)
		c.networks = append(c.networks, n)
	}
	sort.SliceStable(c.networks, func(i, j int) bool {
		a, b := c.networks[i], c.networks[j]
		if ca, cb := strings.ToLower(a.Country), strings.ToLower(b.Country); ca != cb {
			return ca < cb
		}
		if na, nb := strings.ToLower(a.Name), strings.ToLower(b.Name); na != nb {
			return na < nb
		}
		return a.MNC < b.MNC
	})
	for i, n := range c.networks {
		c.byNet[n.MCC+netcode.Norm(n.MNC)] = i
	}
	for code := range c.codes {
		sort.Strings(c.codes[code])
	}
	catMu.Lock()
	catCache[db] = c
	catMu.Unlock()
	return c, nil
}

// fill names a destination from the catalog. ok=false: the table does not
// know it.
func (c *catalog) fill(d Destination) (Destination, bool) {
	switch d.Kind {
	case DestCountry:
		co, ok := c.countries[d.MCC]
		if ok {
			d.Country, d.CCC = co.Country, co.CCC
		}
		return d, ok
	case DestNetwork:
		if co, ok := c.countries[d.MCC]; ok {
			d.Country, d.CCC = co.Country, co.CCC
		}
		i, ok := c.byNet[d.MCC+netcode.Norm(d.MNC)]
		if !ok {
			return d, false
		}
		n := c.networks[i]
		d.MNC, d.Network, d.Series = n.MNC, n.Name, n.First
		if n.Country != "" {
			d.Country = n.Country
		}
		if n.CCC != "" {
			d.CCC = n.CCC
		}
		return d, true
	}
	return d, true
}

// networksOf lists a country's networks by name.
func (c *catalog) networksOf(mcc string) []Destination {
	var out []Destination
	for _, n := range c.networks {
		if n.MCC == mcc {
			out = append(out, Destination{Kind: DestNetwork, MCC: n.MCC, MNC: n.MNC, Country: n.Country, CCC: n.CCC, Network: n.Name, Series: n.First})
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return strings.ToLower(out[i].Network) < strings.ToLower(out[j].Network) })
	return out
}

// ResolveDestination checks a destination against the operators table and
// fills its names; a network comes back with its MNC written as the table
// writes it ("04" → "004"). A country or network the table does not know is
// refused: a rule for it would catch nothing the gateway can place.
func (s Store) ResolveDestination(ctx context.Context, d Destination) (Destination, error) {
	if d.Kind == DestAny || d.Kind == "" {
		if d.Kind == "" {
			return d, refusef("choose where these messages go: any destination, a country or a network")
		}
		return d, nil
	}
	c, err := operatorCatalog(ctx, s.DB)
	if err != nil {
		return d, err
	}
	out, ok := c.fill(d)
	if !ok {
		if d.Kind == DestCountry {
			return d, refusef("no network in the operators table has country code (MCC) %s. Add the country's networks under Operators first", d.MCC)
		}
		return d, refusef("there is no network %s-%s in the operators table. Add it under Operators first", d.MCC, d.MNC)
	}
	return out, nil
}

// CountryNetworks lists a country's networks from the operators table, by
// name — the "No rule of their own" list and the copy targets.
func (s Store) CountryNetworks(ctx context.Context, mcc string) ([]Destination, error) {
	c, err := operatorCatalog(ctx, s.DB)
	if err != nil {
		return nil, err
	}
	return c.networksOf(mcc), nil
}

// DestinationHit is one line of the destination search.
type DestinationHit struct {
	Destination
	What string // the line to show: "Pakistan · +92 · MCC 410"
}

// DestinationList is a search's answer. Note says why nothing (or not
// everything) was found, when there is something to say.
type DestinationList struct {
	Hits []DestinationHit
	Note string
}

// ListDestinations searches what a rule can send to: "any destination",
// countries (by name, dialling code or MCC) and networks (by operator name
// or MCC-MNC) from the operators table — and, for digits that start a
// number, the networks whose number series those digits lead into (the
// same reading as p13-backend numberLookup.js seriesByPrefix).
func (s Store) ListDestinations(ctx context.Context, query string, limit int) (DestinationList, error) {
	if limit <= 0 {
		limit = 20
	}
	var out DestinationList
	c, err := operatorCatalog(ctx, s.DB)
	if err != nil {
		return out, err
	}
	q := strings.TrimSpace(query)
	ql := strings.ToLower(q)
	full := false
	add := func(h DestinationHit) {
		for _, x := range out.Hits {
			if x.Same(h.Destination) {
				return
			}
		}
		if len(out.Hits) >= limit {
			full = true
			return
		}
		out.Hits = append(out.Hits, h)
	}
	digits := strings.Map(func(r rune) rune {
		if strings.ContainsRune("+ -().", r) {
			return -1
		}
		return r
	}, q)
	isDigits := digits != "" && strings.Trim(digits, "0123456789") == ""

	// The start of a number first: the networks whose series it leads into
	// (the prototype's "From the number").
	if isDigits && len(digits) >= 3 {
		if strings.HasPrefix(digits, "0") {
			out.Note = "A number that starts with 0 is not placed in any country: type it with its country code, for example 92 for Pakistan."
			return out, nil
		}
		series, err := s.seriesByPrefix(ctx, digits, limit)
		if err != nil {
			return out, err
		}
		for _, sr := range series {
			d, _ := c.fill(Destination{Kind: DestNetwork, MCC: sr.mcc, MNC: sr.mnc})
			if d.Network == "" {
				d.Network = sr.name
			}
			if d.Country == "" {
				d.Country = sr.country
			}
			add(DestinationHit{Destination: d, What: fmt.Sprintf("%s… is in %s's series %s–%s (%s %s-%s)",
				digits, d.networkName(), sr.from, sr.to, d.countryName(), d.MCC, d.MNC)})
		}
	}
	if q == "" || q == "*" || strings.Contains("any destination everything else every number", ql) {
		add(DestinationHit{Destination: AnyDestination(),
			What: "Any destination: every number, including numbers whose country is not found."})
	}

	// Countries: by name, dialling code or MCC.
	var mccs []string
	for mcc := range c.countries {
		mccs = append(mccs, mcc)
	}
	sort.Slice(mccs, func(i, j int) bool {
		a, b := strings.ToLower(c.countries[mccs[i]].Country), strings.ToLower(c.countries[mccs[j]].Country)
		if a != b {
			return a < b
		}
		return mccs[i] < mccs[j]
	})
	for _, mcc := range mccs {
		co := c.countries[mcc]
		hit := q == "" ||
			(!isDigits && strings.Contains(strings.ToLower(co.Country), ql)) ||
			(isDigits && (strings.HasPrefix(co.CCC, digits) || strings.HasPrefix(mcc, digits) ||
				(co.CCC != "" && strings.HasPrefix(digits, co.CCC) && len(digits) <= len(co.CCC)+1)))
		if !hit {
			continue
		}
		d, _ := c.fill(Destination{Kind: DestCountry, MCC: mcc})
		what := d.countryName()
		if d.CCC != "" {
			what += " · +" + d.CCC
		}
		add(DestinationHit{Destination: d, What: what + " · MCC " + mcc})
	}
	if q == "" {
		return out, nil
	}

	// Networks: by name (or their country's), or by their code.
	var code Destination
	if d, err := ParseDestination(q); err == nil && d.Kind == DestNetwork {
		code = d
	}
	for _, n := range c.networks {
		hit := (!isDigits && (strings.Contains(strings.ToLower(n.Name), ql) || strings.Contains(strings.ToLower(n.Country), ql))) ||
			(code.Kind == DestNetwork && n.MCC == code.MCC && netcode.SameMNC(n.MNC, code.MNC)) ||
			(isDigits && (strings.HasPrefix(n.MCC+netcode.Norm(n.MNC), digits) || strings.HasPrefix(n.MCC+n.MNC, digits)))
		if !hit {
			continue
		}
		d := Destination{Kind: DestNetwork, MCC: n.MCC, MNC: n.MNC, Country: n.Country, CCC: n.CCC, Network: n.Name, Series: n.First}
		what := d.networkName() + " · " + d.countryName() + " · " + n.MCC + "-" + n.MNC
		if len(n.First) >= 5 {
			what += " · " + n.First[:5] + "…"
		}
		add(DestinationHit{Destination: d, What: what})
	}
	if len(out.Hits) == 0 && out.Note == "" {
		out.Note = fmt.Sprintf("Nothing matches %q. Try a country, a network, a dialling code or 410-04.", q)
	} else if full {
		out.Note = fmt.Sprintf("More than %d match %q: type more to narrow it.", limit, q)
	}
	return out, nil
}

// ── number series ────────────────────────────────────────────────────────

type seriesRow struct {
	id                           int64
	ccc, mcc, mnc, name, country string
	from, to                     string
}

// seriesCovers: can a number in this range start with the digits p?
// (numberLookup.js seriesCovers)
func seriesCovers(r seriesRow, p string) bool {
	f, t := r.from, r.to
	if f == "" || len(f) != len(t) || f > t || strings.Trim(f+t, "0123456789") != "" {
		return false
	}
	k := len(p)
	if len(f) < k {
		k = len(f)
	}
	head := p[:k]
	return f[:k] <= head && head <= t[:k]
}

// seriesAround reads the ranges near p: the 60 starting at or below it (a
// range that already contains p sits there; the deepest nesting in the table
// is 25) and, for a prefix, up to 300 that start with it.
func (s Store) seriesAround(ctx context.Context, p string, withStarting bool) ([]seriesRow, error) {
	var out []seriesRow
	add := func(rows []dal.NumberSeriesRow) {
		for _, r := range rows {
			out = append(out, seriesRow{id: r.ID, ccc: r.CCC, mcc: r.MCC, mnc: r.MNC, name: r.Name, country: r.Country, from: r.From, to: r.To})
		}
	}
	rows, err := s.DB.NumberSeriesBelow(ctx, p)
	if err != nil {
		return nil, fmt.Errorf("read number series: %w", err)
	}
	add(rows)
	if withStarting {
		rows, err := s.DB.NumberSeriesLike(ctx, p+"%")
		if err != nil {
			return nil, fmt.Errorf("read number series: %w", err)
		}
		add(rows)
	}
	return out, nil
}

// seriesByPrefix is one series per network the digits can lead into: a
// whole number's own block first (longest, then narrowest), then number
// order (numberLookup.js groupSeries).
func (s Store) seriesByPrefix(ctx context.Context, p string, limit int) ([]seriesRow, error) {
	rows, err := s.seriesAround(ctx, p, true)
	if err != nil {
		return nil, err
	}
	type group struct {
		best     seriesRow
		complete bool
	}
	seen := map[int64]bool{}
	groups := map[string]*group{}
	var order []string
	for _, r := range rows {
		if seen[r.id] || !seriesCovers(r, p) {
			continue
		}
		seen[r.id] = true
		key := r.name + "|" + r.mcc + "|" + netcode.Norm(r.mnc)
		complete := len(p) >= len(r.from)
		g := groups[key]
		if g == nil {
			groups[key] = &group{best: r, complete: complete}
			order = append(order, key)
			continue
		}
		b := g.best
		better := false
		switch {
		case complete != g.complete:
			better = complete
		case complete:
			better = len(r.from) > len(b.from) || (len(r.from) == len(b.from) && narrower(r, b))
		default:
			better = r.from < b.from
		}
		if better {
			g.best, g.complete = r, complete
		}
	}
	sort.SliceStable(order, func(i, j int) bool {
		a, b := groups[order[i]], groups[order[j]]
		if a.complete != b.complete {
			return a.complete
		}
		return a.best.from < b.best.from
	})
	var out []seriesRow
	for _, k := range order {
		if len(out) >= limit {
			break
		}
		out = append(out, groups[k].best)
	}
	return out, nil
}

// narrower: a is a narrower block than b (same length).
func narrower(a, b seriesRow) bool {
	wa, wb := width(a), width(b)
	if wa != wb {
		return wa < wb
	}
	return a.from < b.from
}

func width(r seriesRow) uint64 {
	var f, t uint64
	fmt.Sscan(r.from, &f)
	fmt.Sscan(r.to, &t)
	return t - f
}

// covers: the range covers the whole number n (numberLookup.js covers,
// hlr.Ranges): a range as long as the number compares whole, a shorter one
// is a prefix block, a longer one cannot cover it.
func (r seriesRow) covers(n string) bool {
	f, t := r.from, r.to
	if f == "" || len(f) != len(t) || len(f) > len(n) {
		return false
	}
	head := n[:len(f)]
	return f <= head && head <= t
}

// NumberPlace is where the operators table places one number.
type NumberPlace struct {
	Input   string
	Digits  string // cleaned as the engine cleans it (msisdn.Clean)
	Problem string // why it cannot be placed or tested; "" when it can

	MCC, MNC       string // "" when not found
	Country, CCC   string
	Network        string
	From, To       string // the series it falls in
	Found          string // one line: what the lookup found, in plain words
	CountryFromCCC bool   // the country came from the calling code, not a series
}

// Destination is the number as a destination: its network, else its
// country, else any.
func (p NumberPlace) Destination() Destination {
	switch {
	case p.MNC != "":
		return Destination{Kind: DestNetwork, MCC: p.MCC, MNC: p.MNC, Country: p.Country, CCC: p.CCC, Network: p.Network}
	case p.MCC != "":
		return Destination{Kind: DestCountry, MCC: p.MCC, Country: p.Country, CCC: p.CCC}
	}
	return AnyDestination()
}

// PlaceNumber cleans a number the way the engine does and finds its country
// and network from the operators' number series — the longest, then the
// narrowest covering range, as hlr.Ranges and the Operators screen do; the
// country from the calling code when no series covers it and only one
// country uses that code. A national number (0300…) is not placed: the
// gateway refuses it (INVALID_MSISDN, msisdn.National) and adds no country code.
func (s Store) PlaceNumber(ctx context.Context, raw string) (NumberPlace, error) {
	p := NumberPlace{Input: raw, Digits: msisdn.Clean(raw)}
	if strings.TrimSpace(raw) == "" {
		p.Problem = "Type a number to see which rule catches it and which vendor sends it."
		return p, nil
	}
	if bad := strings.Trim(p.Digits, "0123456789"); bad != "" {
		p.Problem = fmt.Sprintf("'%s' can't be part of a number. Use digits; a leading +, spaces and dashes are fine.", string([]rune(bad)[:1]))
		return p, nil
	}
	lead := strings.TrimLeft(msisdnDigits(raw), " ")
	if strings.HasPrefix(lead, "0") && len(lead) > 1 && lead[1] != '0' {
		p.Problem = "Add the country code, for example 92 for Pakistan. A number that starts with a single 0 is a national number: the gateway refuses it (INVALID_MSISDN) and adds no country code."
		return p, nil
	}
	if len(p.Digits) > 15 {
		p.Problem = "Too long: a number has at most 15 digits with the country code."
		return p, nil
	}
	if len(p.Digits) < 4 {
		p.Problem = "Too short: type the whole number with its country code."
		return p, nil
	}
	rows, err := s.seriesAround(ctx, p.Digits, false)
	if err != nil {
		return p, err
	}
	var best *seriesRow
	for i := range rows {
		r := rows[i]
		if !r.covers(p.Digits) {
			continue
		}
		if best == nil || len(r.from) > len(best.from) || (len(r.from) == len(best.from) && narrower(r, *best)) {
			best = &rows[i]
		}
	}
	c, err := operatorCatalog(ctx, s.DB)
	if err != nil {
		return p, err
	}
	if best != nil {
		p.MCC, p.MNC, p.From, p.To = best.mcc, best.mnc, best.from, best.to
		d, _ := c.fill(Destination{Kind: DestNetwork, MCC: best.mcc, MNC: best.mnc})
		p.Country, p.CCC, p.Network = d.Country, d.CCC, d.Network
		if p.Network == "" {
			p.Network = best.name
		}
		if p.Country == "" {
			p.Country = best.country
		}
		if best.mnc == "" {
			p.MNC = ""
		}
	} else {
		// The longest calling code that starts the number, and its MCC only
		// when one country uses it (+1 and +7 are shared: no answer).
		var code string
		for cc := range c.codes {
			if strings.HasPrefix(p.Digits, cc) && (len(cc) > len(code) || (len(cc) == len(code) && cc < code)) {
				code = cc
			}
		}
		if code != "" && len(c.codes[code]) == 1 {
			p.MCC, p.CCC, p.CountryFromCCC = c.codes[code][0], code, true
			d, _ := c.fill(Destination{Kind: DestCountry, MCC: p.MCC})
			p.Country = d.Country
		}
	}
	switch {
	case p.MNC != "":
		p.Found = fmt.Sprintf("%s · %s · %s-%s · series %s–%s. From the number series it falls in.", orMCC(p.Country, p.MCC), p.Network, p.MCC, p.MNC, p.From, p.To)
	case p.MCC != "":
		p.Found = fmt.Sprintf("%s · network not found. Network rules do not apply; country rules and 'Everything else' can take it.", orMCC(p.Country, p.MCC))
	default:
		p.Found = "Country not found. Only 'Everything else' can take it."
	}
	return p, nil
}

func orMCC(country, mcc string) string {
	if country != "" {
		return country
	}
	return "MCC " + mcc
}

// msisdnDigits is the number's digits before the leading zeros are stripped
// (numberLookup.js normaliseDigits), to see a national 0.
func msisdnDigits(raw string) string {
	var b strings.Builder
	for _, r := range raw {
		switch {
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		case r >= '٠' && r <= '٩':
			b.WriteRune('0' + (r - '٠'))
		case r >= '۰' && r <= '۹':
			b.WriteRune('0' + (r - '۰'))
		}
	}
	return b.String()
}
