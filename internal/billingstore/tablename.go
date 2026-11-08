package billingstore

// What a log table's name MEANS — shared, because every reader has to agree
// (D-136, 2026-10-14; by DAY since 26 Oct 2026, issue 25.3).
//
// A record is filed by its CUSTOMER company and its day:
// <kind>_<company>_<YYYYMMDD>, kind one of mt, sub, dlr, dlr_sub, mo, mo_sub,
// mo_out, hlr, rejects, orphans. Company 0 holds what has no customer company (refusals
// before login, every orphan). The day of an mt, sub, dlr, mo or mo_out row is
// its message id's UTC day (a ULID carries its time): a receipt, a vendor's
// answer and an MO's outcome are filed with their message. Retention reads the
// name to decide what may be archived.
//
// The monthly tables of D-136 (<kind>_<company>_<YYYYMM>) and every layout
// before them are not ours any more: nothing parses them, the operator drops
// them.

import (
	"fmt"
	"regexp"
	"strconv"
	"time"

	"github.com/oklog/ulid/v2"
)

// The kinds of log table.
const (
	KindMT      = "mt"
	KindSub     = "sub" // the vendor's answers, per part, per try (issue 25.4)
	KindDLR     = "dlr"
	KindMO      = "mo"
	KindMOOut   = "mo_out" // what became of an incoming message
	KindHLR     = "hlr"
	KindRejects = "rejects"
	KindOrphans = "orphans"
	// KindMOSub and KindDLRSub are every try to give a client an incoming
	// message or a receipt, bind or webhook, and what the client answered
	// (hot/0076, D-140, 5 Nov 2026): the MO's and the receipt's sub_, filed
	// by their message's day. mo_out is no longer written (its days are read).
	KindMOSub  = "mo_sub"
	KindDLRSub = "dlr_sub"
)

// Kinds lists every kind, in the order a reader lists them.
var Kinds = []string{KindMT, KindSub, KindDLR, KindMO, KindMOOut, KindHLR, KindRejects, KindOrphans, KindMOSub, KindDLRSub}

// Templates names the template each kind is made from (hot/0066):
// tpl_day_<kind>, so no LIKE pattern of a family matches its template.
var Templates = map[string]string{
	KindMT: "tpl_day_mt", KindSub: "tpl_day_sub", KindDLR: "tpl_day_dlr", KindMO: "tpl_day_mo",
	KindMOOut: "tpl_day_mo_out", KindHLR: "tpl_day_hlr", KindRejects: "tpl_day_rejects", KindOrphans: "tpl_day_orphans",
	KindMOSub: "tpl_day_mo_sub", KindDLRSub: "tpl_day_dlr_sub",
}

// RecordTableRe matches a log table's name: kind, company, year, month, day.
// mo_out and mo_sub come before mo, dlr_sub before dlr, so mo_out_5_20261027
// is never read as kind mo (a company is digits only, so the order is also
// what keeps the names apart).
var RecordTableRe = regexp.MustCompile(`^(mt|sub|dlr_sub|dlr|mo_out|mo_sub|mo|hlr|rejects|orphans)_(\d{1,19})_(\d{4})(\d{2})(\d{2})$`)

// dayLayout is how a day is written in a table's name.
const dayLayout = "20060102"

// RecordTableName is the table a record of this kind, company and moment is
// filed in. The day is UTC.
func RecordTableName(kind string, company int64, at time.Time) string {
	if company < 0 {
		company = 0
	}
	return fmt.Sprintf("%s_%d_%s", kind, company, at.UTC().Format(dayLayout))
}

// DayStart is the first moment (UTC) of the day n days after t's own (n < 0:
// before it).
func DayStart(t time.Time, n int) time.Time {
	t = t.UTC()
	return time.Date(t.Year(), t.Month(), t.Day()+n, 0, 0, 0, 0, time.UTC)
}

// MonthStart is the first moment (UTC) of the month n months after t's own
// (n < 0: before it). Counting from the 1st matters: t.AddDate(0, -1, 0) on
// the 31st of October is the 1st of October, not a day in September.
func MonthStart(t time.Time, n int) time.Time {
	t = t.UTC()
	return time.Date(t.Year(), t.Month()+time.Month(n), 1, 0, 0, 0, 0, time.UTC)
}

// TableOf is the table of a message's record: its company and the day of its
// id. now stands in for an id that is not a ULID (tests).
func TableOf(kind string, company int64, msgID string, now time.Time) string {
	return RecordTableName(kind, company, IDTime(msgID, now))
}

// IDTime is when a message id was made (a ULID carries it), else now. An id
// whose time is more than a minute ahead or more than 366 days old is not
// believed — the same rule the record writer has always used for accepted_at.
func IDTime(id string, now time.Time) time.Time {
	u, err := ulid.ParseStrict(id)
	if err != nil {
		return now.UTC()
	}
	t := ulid.Time(u.Time()).UTC()
	if t.After(now.Add(time.Minute)) || t.Before(now.AddDate(0, 0, -366)) {
		return now.UTC()
	}
	return t
}

// SwapKind is the table of another kind for the same company and day: the
// receipts of an mt table are in SwapKind(mt, KindDLR). "" for a name that is
// not a log table.
func SwapKind(table, kind string) string {
	rt, ok := ParseRecordTable(table)
	if !ok {
		return ""
	}
	return fmt.Sprintf("%s_%d_%s", kind, rt.CompanyID, rt.Day)
}

// ParseRecordTable reads a log table's name. ok=false means the name is not
// one of ours — a template, a table of an older layout (a month table of
// D-136 among them), or something else entirely — and the caller should leave
// it alone.
func ParseRecordTable(name string) (RecordTable, bool) {
	m := RecordTableRe.FindStringSubmatch(name)
	if m == nil {
		return RecordTable{}, false
	}
	co, err := strconv.ParseInt(m[2], 10, 64)
	if err != nil {
		return RecordTable{}, false
	}
	day, err := time.Parse(dayLayout, m[3]+m[4]+m[5])
	if err != nil {
		return RecordTable{}, false // no 31st of September, no month 13
	}
	return RecordTable{Name: name, Kind: m[1], CompanyID: co, Day: m[3] + m[4] + m[5], Date: day.UTC()}, true
}

// TableFamily says which kind a log table is ("" for any other name), so a
// caller can pick the right path without re-parsing the name.
func TableFamily(name string) string {
	if rt, ok := ParseRecordTable(name); ok {
		return rt.Kind
	}
	return ""
}
