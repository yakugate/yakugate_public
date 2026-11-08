package provision

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/yakugate/yakugate/internal/dal"
)

// Operators (table operators): the mobile networks, and the number ranges
// each was issued. [SCOPE P14-OPERATORS]
//
// One row is one RANGE of one network: ccc (the country calling code), mcc +
// mnc (the network), its name/country/region, and from_series..to_series. A
// network with several blocks has several rows. The engine reads ccc, mcc,
// mnc and the two ends (internal/hlr/ranges.go) to say which network a number
// was issued to — the HLR "range" source, the country stage, routing by
// network — and reloads within 5 minutes of a change. The portal's number
// lookup also reads mccmnc.
//
// The Admin API's operators resource checks field lengths and nothing else.
// The console is stricter, never looser (one database, two doors):
//   - mcc is 3 digits, mnc 2 or 3, ccc 1 to 3 — digits only;
//   - a range's two ends are digits, the same length (5 to 15), from <= to —
//     the engine compares them as numbers of one length, so anything else is
//     a range that silently matches nothing;
//   - mccmnc is always mcc+mnc, and hlr_shard_key the ends' common prefix —
//     the shape every one of the 349k loaded rows has;
//   - the same range twice is refused (the unique key cannot see NULLs), and
//     so is a range overlapping another of the same length: two networks
//     both claiming a number is a question the engine answers by accident;
//   - a network cannot be deleted while a routing rule or a price names it.

// Network is one mcc/mnc, summarised over its ranges.
type Network struct {
	MCC, MNC, CCC         string
	Name, Country, Region string
	Ranges                int
}

// NumberRange is one operators row.
type NumberRange struct {
	ID       int64
	From, To string
	Network
}

// OperatorCounts is how many networks and ranges there are.
func (s Store) OperatorCounts(ctx context.Context) (networks, ranges int, err error) {
	return s.DB.OperatorCounts(ctx)
}

// FindNetworks lists networks whose country, name, mcc or mcc+mnc contains
// or starts with q (case-insensitive), at most limit.
func (s Store) FindNetworks(ctx context.Context, q string, limit int) ([]Network, error) {
	q = strings.TrimSpace(q)
	if limit <= 0 {
		limit = 50
	}
	rows, err := s.DB.FindNetworkRows(ctx, q, limit)
	var re dal.RowsError
	if err != nil && !errors.As(err, &re) {
		return nil, fmt.Errorf("find networks: %w", err)
	}
	if err != nil && rows == nil {
		return nil, re.Err
	}
	var out []Network
	for _, r := range rows {
		out = append(out, networkOf(r))
	}
	if err != nil {
		return out, re.Err // the read was cut short: what was read, and why
	}
	return out, nil
}

// networkOf is a network as the store read it.
func networkOf(r dal.NetworkRow) Network {
	return Network{MCC: r.MCC, MNC: r.MNC, CCC: r.CCC, Name: r.Name, Country: r.Country, Region: r.Region, Ranges: r.Ranges}
}

// GetNetwork reads one network's summary.
func (s Store) GetNetwork(ctx context.Context, mcc, mnc string) (Network, error) {
	row, err := s.DB.NetworkSummary(ctx, mcc, mnc)
	n := networkOf(row)
	n.MCC, n.MNC = mcc, mnc
	if err != nil {
		return n, fmt.Errorf("read network %s-%s: %w", mcc, mnc, err)
	}
	if n.Ranges == 0 {
		return n, refusef("there is no network %s-%s", mcc, mnc)
	}
	return n, nil
}

// SameNetwork finds a network already written with another spelling of this
// mnc ("1", "01" and "001" are one network to the gateway, internal/netcode),
// so a second spelling is never created beside it. ok=false: none exists.
func (s Store) SameNetwork(ctx context.Context, mcc, mnc string) (Network, bool, error) {
	f := mncForms(mnc)
	found, err := s.DB.NetworkSpelling(ctx, mcc, [3]string{f[0], f[1], f[2]})
	if err == dal.ErrNoRows {
		return Network{}, false, nil
	}
	if err != nil {
		return Network{}, false, err
	}
	n, err := s.GetNetwork(ctx, mcc, found)
	return n, err == nil, err
}

// NetworkRanges lists a network's ranges, those starting with prefix first
// (all when prefix is empty), at most limit, and how many there are in all.
func (s Store) NetworkRanges(ctx context.Context, mcc, mnc, prefix string, limit int) ([]NumberRange, int, error) {
	if limit <= 0 {
		limit = 50
	}
	total, err := s.DB.NetworkRangeCount(ctx, mcc, mnc, prefix)
	if err != nil {
		return nil, 0, err
	}
	rows, err := s.DB.NetworkRangeRows(ctx, mcc, mnc, prefix, limit)
	var re dal.RowsError
	if err != nil && !errors.As(err, &re) {
		return nil, 0, err
	}
	if err != nil && rows == nil {
		return nil, 0, re.Err
	}
	var out []NumberRange
	for _, r := range rows {
		out = append(out, rangeOf(r))
	}
	if err != nil {
		return out, total, re.Err // the read was cut short: what was read, and why
	}
	return out, total, nil
}

// rangeOf is a range as the store read it.
func rangeOf(r dal.NumberRangeRow) NumberRange {
	return NumberRange{ID: r.ID, From: r.From, To: r.To, Network: Network{MCC: r.MCC, MNC: r.MNC, CCC: r.CCC,
		Name: r.Name, Country: r.Country, Region: r.Region}}
}

func digits(s string, min, max int) bool {
	if len(s) < min || len(s) > max {
		return false
	}
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

// commonPrefix is the digits two range ends share — the hlr_shard_key every
// loaded row carries.
func commonPrefix(a, b string) string {
	i := 0
	for i < len(a) && i < len(b) && a[i] == b[i] {
		i++
	}
	return a[:i]
}

// ValidateRange checks one range before it is written. It returns a warning
// (not a refusal) when the numbers do not start with the calling code: some
// real ranges do not (American Samoa is +1 684 under calling code 685).
func ValidateRange(r NumberRange) (warning string, err error) {
	if !digits(r.MCC, 3, 3) {
		return "", refusef("the country code (MCC) is 3 digits, e.g. 410 — %q is not", r.MCC)
	}
	if !digits(r.MNC, 2, 3) {
		return "", refusef("the network code (MNC) is 2 or 3 digits, e.g. 01 — %q is not", r.MNC)
	}
	if !digits(r.CCC, 1, 3) {
		return "", refusef("the calling code is 1 to 3 digits, e.g. 92 — %q is not", r.CCC)
	}
	if strings.TrimSpace(r.Name) == "" || strings.TrimSpace(r.Country) == "" {
		return "", refusef("a network needs a name and a country")
	}
	if len([]rune(r.Name)) > 100 || len([]rune(r.Country)) > 100 || len([]rune(r.Region)) > 100 {
		return "", refusef("the name, country and region are at most 100 characters each")
	}
	if !digits(r.From, 5, 15) || !digits(r.To, 5, 15) {
		return "", refusef("a range's two ends are numbers of 5 to 15 digits, written in full with the calling code (e.g. 923000000000)")
	}
	// A number the gateway reads is international (E.164), which never starts
	// with 0: a range in national form (0300…) matches nothing (QA
	// BUG-YK-OPS-001). The Admin API refuses the same (resources.js operators).
	if strings.HasPrefix(r.From, "0") || strings.HasPrefix(r.To, "0") {
		return "", refusef("write the range in international form, starting with the calling code — %s…, not 0…: a number with a leading 0 is national and the gateway never sees one", r.CCC)
	}
	if len(r.From) != len(r.To) {
		return "", refusef("the two ends must have the same number of digits (%d and %d): the gateway compares them as numbers of one length", len(r.From), len(r.To))
	}
	if r.From > r.To {
		return "", refusef("the range starts after it ends (%s > %s)", r.From, r.To)
	}
	if !strings.HasPrefix(r.From, r.CCC) {
		warning = fmt.Sprintf("the range does not start with calling code %s — right for a few places (American Samoa is +1 684), a mistake anywhere else", r.CCC)
	}
	return warning, nil
}

// AddRange adds one range; a new mcc/mnc makes a new network. It refuses the
// same range twice and a range overlapping another of the same length, and
// audits as operators.create.
func (s Store) AddRange(ctx context.Context, r NumberRange) (int64, error) {
	r.Name, r.Country, r.Region = strings.TrimSpace(r.Name), strings.TrimSpace(r.Country), strings.TrimSpace(r.Region)
	if _, err := ValidateRange(r); err != nil {
		return 0, err
	}
	if n, ok, err := s.SameNetwork(ctx, r.MCC, r.MNC); err != nil {
		return 0, err
	} else if ok && n.MNC != r.MNC {
		return 0, refusef("%s-%s is already known as %s-%s (%s): add the range there — the gateway treats both spellings as one network", r.MCC, r.MNC, n.MCC, n.MNC, n.Name)
	}
	o, err := s.DB.OverlappingRange(ctx, len(r.From), r.From, r.To)
	id, other, oFrom, oTo := o.ID, o.Other, o.From, o.To
	switch {
	case err == nil:
		if oFrom == r.From && oTo == r.To {
			return 0, refusef("this exact range is already there (#%d, %s)", id, other)
		}
		return 0, refusef("this range overlaps %s..%s (#%d, %s): a number in both would belong to whichever the gateway meets first. Shorten one of them", oFrom, oTo, id, other)
	case err != dal.ErrNoRows:
		return 0, err
	}
	id, err = s.DB.InsertRange(ctx, dal.NumberRangeRow{
		From: r.From, To: r.To, MCC: r.MCC, MNC: r.MNC, CCC: r.CCC,
		Name: r.Name, Country: r.Country, Region: r.Region,
	}, commonPrefix(r.From, r.To))
	if err != nil {
		return 0, fmt.Errorf("add range: %w", err)
	}
	return id, s.audit(ctx, auditEntry{
		Action: "operators.create", Entity: "operator", Feature: "operators", EntityID: id,
		Label: r.MCC + r.MNC, Summary: "Created operator from the console",
		Detail: map[string]any{"after": rangeDetail(r)},
	})
}

// RenameNetwork changes a network's name, country or region on every one of
// its ranges (key: name | country | region), audited as operators.update.
func (s Store) RenameNetwork(ctx context.Context, mcc, mnc, key, value string) error {
	n, err := s.GetNetwork(ctx, mcc, mnc)
	if err != nil {
		return err
	}
	value = strings.TrimSpace(value)
	col := map[string]string{"name": "operator_name", "country": "country", "region": "region"}[key]
	if col == "" {
		return refusef("%q is not a setting on a network", key)
	}
	if (key == "name" || key == "country") && value == "" {
		return refusef("a network needs a %s", key)
	}
	if len([]rune(value)) > 100 {
		return refusef("at most 100 characters")
	}
	rows, err := s.DB.SetNetworkColumn(ctx, col, value, mcc, mnc)
	if err != nil {
		return fmt.Errorf("rename network: %w", err)
	}
	before := map[string]any{"name": n.Name, "country": n.Country, "region": n.Region}
	return s.audit(ctx, auditEntry{
		Action: "operators.update", Entity: "operator", Feature: "operators",
		Label: mcc + mnc, Summary: fmt.Sprintf("Updated operator %s on %d range(s) from the console", key, rows),
		Detail: map[string]any{"mccmnc": mcc + mnc, "field": col, "before": before[key], "after": value},
	})
}

// DeleteRange removes one range, audited as operators.delete.
func (s Store) DeleteRange(ctx context.Context, id int64) error {
	row, err := s.DB.RangeByID(ctx, id)
	r := rangeOf(row)
	if err == dal.ErrNoRows {
		return refusef("there is no range #%d", id)
	}
	if err != nil {
		return err
	}
	if err := s.DB.DeleteRangeRow(ctx, id); err != nil {
		return fmt.Errorf("delete range: %w", err)
	}
	return s.audit(ctx, auditEntry{
		Action: "operators.delete", Entity: "operator", Feature: "operators", EntityID: id,
		Label: r.MCC + r.MNC, Summary: "Deleted operator from the console",
		Detail: map[string]any{"before": rangeDetail(r)},
	})
}

// NetworkInUse says what still names a network: routing rules that match it
// and prices set for it. A network in use cannot be deleted.
func (s Store) NetworkInUse(ctx context.Context, mcc, mnc string) (routes, prices int, err error) {
	// match_mccmncs holds "41001"-style strings; compare the two usual
	// spellings of an mnc (01 and 001).
	var variants []string
	for _, f := range mncForms(mnc) {
		variants = append(variants, mcc+f)
	}
	for _, v := range variants {
		var n int
		if n, err = s.DB.RoutesMatchingNetwork(ctx, v); err != nil {
			return
		}
		routes += n
	}
	prices, err = s.DB.OpenPricesForNetwork(ctx, mcc, [3]string{mncForms(mnc)[0], mncForms(mnc)[1], mncForms(mnc)[2]})
	return
}

// DeleteNetwork removes every range of a network, refused while a routing
// rule or an open price names it. Audited as one operators.delete.
func (s Store) DeleteNetwork(ctx context.Context, mcc, mnc string) (int64, error) {
	n, err := s.GetNetwork(ctx, mcc, mnc)
	if err != nil {
		return 0, err
	}
	routes, prices, err := s.NetworkInUse(ctx, mcc, mnc)
	if err != nil {
		return 0, err
	}
	if routes > 0 || prices > 0 {
		return 0, refusef("%s (%s-%s) is still in use: %d routing rule(s) match it and %d price(s) are set for it. Change those first",
			n.Name, mcc, mnc, routes, prices)
	}
	gone, err := s.DB.DeleteNetworkRows(ctx, mcc, mnc)
	if err != nil {
		return 0, fmt.Errorf("delete network: %w", err)
	}
	return gone, s.audit(ctx, auditEntry{
		Action: "operators.delete", Entity: "operator", Feature: "operators",
		Label: mcc + mnc, Summary: fmt.Sprintf("Deleted operator %s-%s (%d ranges) from the console", mcc, mnc, gone),
		Detail: map[string]any{"mccmnc": mcc + mnc, "name": n.Name, "country": n.Country, "ranges": gone},
	})
}

func rangeDetail(r NumberRange) map[string]any {
	return map[string]any{"ccc": r.CCC, "mcc": r.MCC, "mnc": r.MNC, "mccmnc": r.MCC + r.MNC,
		"operator_name": r.Name, "country": r.Country, "region": nullStr(r.Region),
		"from_series": r.From, "to_series": r.To, "hlr_shard_key": commonPrefix(r.From, r.To)}
}

// FormatCount writes 349322 as 349,322.
func FormatCount(n int) string {
	s := strconv.Itoa(n)
	for i := len(s) - 3; i > 0; i -= 3 {
		s = s[:i] + "," + s[i:]
	}
	return s
}

// mncForms is an mnc as it may be written elsewhere: as given, two digits and
// three ("1", "01", "001" all name one network). Always three entries.
func mncForms(mnc string) []string {
	n := strings.TrimLeft(mnc, "0")
	if n == "" {
		n = "0"
	}
	pad := func(w int) string {
		if len(n) >= w {
			return n
		}
		return strings.Repeat("0", w-len(n)) + n
	}
	return []string{mnc, pad(2), pad(3)}
}
