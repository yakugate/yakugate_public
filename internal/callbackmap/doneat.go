package callbackmap

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// When a receipt says the message reached the phone (2026-09-28).
//
// A vendor's receipt carries the moment the handset took the message — its
// "done date". Until now nothing read it: dlrlog.done_date (hot/0016) stayed
// empty and every receipt's done date was the time it reached US, so a vendor
// that delivers in two seconds and reports ten minutes later looked like a
// ten-minute route. The dialect's request list now names where that time is
// (the done_at field) and how it is written (that field's "format" and
// "zone" — time_format and time_zone in the older stored shape), because
// every vendor writes it differently.
//
// format:
//
//	auto      (default) a number is Unix seconds, or milliseconds when it is
//	          13 digits; otherwise the ISO / SMPP shapes below are tried,
//	          then the web and mail dates (2026-10-01): RFC 1123 as HTTP
//	          writes it, "Thu, 01 Oct 2026 06:14:00 GMT", and its RFC 5322 /
//	          RFC 850 / asctime cousins
//	unix      Unix seconds: 1790580406
//	unix_ms   Unix milliseconds: 1790580406123
//	smpp      SMPP 3.4 receipt date: yyMMddHHmm or yyMMddHHmmss
//	iso       ISO 8601 / RFC 3339: 2026-09-28T07:26:46Z, 2026-09-28 07:26:46
//	a pattern made of YYYY YY MM DD HH mm ss (and any separators):
//	          "DD/MM/YYYY HH:mm:ss"
//
// zone is the zone of a time written without one — "UTC" (default), an
// offset "+05:00" or a zone name "Asia/Karachi". A Unix time has no zone.
const (
	TimeAuto   = "auto"
	TimeUnix   = "unix"
	TimeUnixMs = "unix_ms"
	TimeSMPP   = "smpp"
	TimeISO    = "iso"
)

// TimeFormats lists the named formats (a pattern is also accepted).
func TimeFormats() []string { return []string{TimeAuto, TimeUnix, TimeUnixMs, TimeSMPP, TimeISO} }

var isoLayouts = []string{
	time.RFC3339Nano, time.RFC3339,
	"2006-01-02T15:04:05.999999999", "2006-01-02T15:04:05",
	"2006-01-02 15:04:05.999999999Z07:00", "2006-01-02 15:04:05Z07:00",
	"2006-01-02 15:04:05.999999999", "2006-01-02 15:04:05",
	"2006-01-02T15:04", "2006-01-02 15:04",
}

// webLayouts are the dates web and mail headers carry, tried by auto after
// ISO. A one-digit day ("2") reads "1" and "01" alike, so RFC 1123's
// two-digit form needs no line of its own. A numeric zone is tried before a
// zone word, because Go's word layout also swallows "+0000".
var webLayouts = []string{
	"Mon, 2 Jan 2006 15:04:05 -0700", "Mon, 2 Jan 2006 15:04:05 MST",
	"2 Jan 2006 15:04:05 -0700", "2 Jan 2006 15:04:05 MST",
	"Mon, 2 Jan 2006 15:04 -0700", "Mon, 2 Jan 2006 15:04 MST",
	"Mon, 2 Jan 2006 15:04:05", "2 Jan 2006 15:04:05",
	time.RubyDate, time.UnixDate, time.RFC850, time.ANSIC,
}

// zoneOffsetRe finds a written offset — "+0500", "+05:00", "+5", "GMT+3",
// "UTC-04:30" — so it can be rewritten as "+HHMM". Go reads "GMT+3" with
// the right clock but the wrong moment (three hours off), and "+5" not at all.
var zoneOffsetRe = regexp.MustCompile(`(^|\s)(?:GMT|UTC|UT)?([+-])(\d{1,2})(?::?(\d{2}))?(\s|$)`)

func numericZone(v string) string {
	return zoneOffsetRe.ReplaceAllStringFunc(v, func(m string) string {
		p := zoneOffsetRe.FindStringSubmatch(m)
		h, _ := strconv.Atoi(p[3])
		mm := p[4]
		if mm == "" {
			mm = "00"
		}
		if h > 14 {
			return m
		}
		return fmt.Sprintf("%s%s%02d%s%s", p[1], p[2], h, mm, p[5])
	})
}

// webOf reads a web or mail date. matched=false when no layout fits. A zone
// written as a word must be one the door can place — GMT, UT or UTC, or a
// name the door's time_zone knows ("PKT" with Asia/Karachi). Go would read
// any other word as UTC, quietly hours wrong, so that is refused instead.
func webOf(v string, loc *time.Location) (t time.Time, matched bool, err error) {
	// RFC 5322 lets a comment follow the zone: "+0000 (UTC)".
	if i := strings.LastIndex(v, " ("); i > 0 && strings.HasSuffix(v, ")") {
		v = strings.TrimSpace(v[:i])
	}
	v = numericZone(v)
	for _, l := range webLayouts {
		got, perr := time.ParseInLocation(l, v, loc)
		if perr != nil {
			continue
		}
		if got.Location() != loc && got.Location() != time.UTC {
			switch name, _ := got.Zone(); name {
			case "", "GMT", "UT", "UTC":
			default:
				return time.Time{}, true, fmt.Errorf("%q names the zone %q, which this door cannot place — set the delivery time's zone to the one it means (for example Asia/Karachi)", v, name)
			}
		}
		return got, true, nil
	}
	return time.Time{}, false, nil
}

// patternTokens turns an operator's pattern into Go's layout, longest first.
var patternTokens = []struct{ tok, layout string }{
	{"YYYY", "2006"}, {"YY", "06"}, {"MM", "01"}, {"DD", "02"},
	{"HH", "15"}, {"mm", "04"}, {"ss", "05"},
}

// patternLayout converts "DD/MM/YYYY HH:mm:ss" into Go's layout, or "" when
// the text is not a pattern (it names no year, month and day).
func patternLayout(p string) string {
	var b strings.Builder
	seen := map[string]bool{}
	for i := 0; i < len(p); {
		matched := false
		for _, t := range patternTokens {
			if strings.HasPrefix(p[i:], t.tok) {
				b.WriteString(t.layout)
				seen[t.tok] = true
				i += len(t.tok)
				matched = true
				break
			}
		}
		if matched {
			continue
		}
		c := p[i]
		// Letters other than the tokens would be read by Go as layout parts
		// ("Jan", "Mon", "PM"); they are not part of this vocabulary.
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') {
			return ""
		}
		b.WriteByte(c)
		i++
	}
	if !(seen["YYYY"] || seen["YY"]) || !seen["MM"] || !seen["DD"] {
		return ""
	}
	return b.String()
}

// checkTimeSettings is what ParseRequest refuses about the done_at field's
// format and zone.
func checkTimeSettings(format, zone string) error {
	switch strings.TrimSpace(format) {
	case "", TimeAuto, TimeUnix, TimeUnixMs, TimeSMPP, TimeISO:
	default:
		if patternLayout(format) == "" {
			return fmt.Errorf("request mapping: the delivery time's format %q is not one of %s, nor a pattern like DD/MM/YYYY HH:mm:ss",
				format, strings.Join(TimeFormats(), ", "))
		}
	}
	if _, err := loadZone(zone); err != nil {
		return fmt.Errorf("request mapping: the delivery time's zone %q: %v", zone, err)
	}
	return nil
}

// loadZone reads "", "UTC", "+05:00" / "-0330" or a zone name.
func loadZone(z string) (*time.Location, error) {
	z = strings.TrimSpace(z)
	if z == "" || strings.EqualFold(z, "UTC") || z == "Z" {
		return time.UTC, nil
	}
	if z[0] == '+' || z[0] == '-' {
		d := strings.ReplaceAll(z[1:], ":", "")
		if len(d) == 2 {
			d += "00"
		}
		if len(d) != 4 {
			return nil, fmt.Errorf("an offset is written +05:00")
		}
		h, err1 := strconv.Atoi(d[:2])
		m, err2 := strconv.Atoi(d[2:])
		if err1 != nil || err2 != nil || h > 14 || m > 59 {
			return nil, fmt.Errorf("an offset is written +05:00")
		}
		secs := h*3600 + m*60
		if z[0] == '-' {
			secs = -secs
		}
		return time.FixedZone(z, secs), nil
	}
	loc, err := time.LoadLocation(z)
	if err != nil {
		return nil, fmt.Errorf("not a time zone (use UTC, +05:00 or a name like Asia/Karachi)")
	}
	return loc, nil
}

// ParseTime reads one vendor time by the door's format and zone.
func ParseTime(v, format, zone string) (time.Time, error) {
	v = strings.TrimSpace(v)
	if v == "" {
		return time.Time{}, fmt.Errorf("no time")
	}
	loc, err := loadZone(zone)
	if err != nil {
		return time.Time{}, err
	}
	switch strings.TrimSpace(format) {
	case TimeUnix:
		return unixOf(v, false)
	case TimeUnixMs:
		return unixOf(v, true)
	case TimeSMPP:
		return smppOf(v, loc)
	case TimeISO:
		return isoOf(v, loc)
	case "", TimeAuto:
		if allDigits(v) {
			switch len(v) {
			case 13:
				return unixOf(v, true)
			case 12:
				return smppOf(v, loc)
			case 10:
				// Ten digits are either Unix seconds (1790580406) or an SMPP
				// yyMMddHHmm (2609280726). Unix seconds that land in a sane
				// window win; a yyMMddHHmm read as seconds lands decades away.
				if t, err := unixOf(v, false); err == nil && plausible(t) {
					return t, nil
				}
				return smppOf(v, loc)
			}
			return unixOf(v, false)
		}
		if t, err := isoOf(v, loc); err == nil {
			return t, nil
		}
		if t, ok, err := webOf(v, loc); ok {
			return t, err
		}
		return time.Time{}, fmt.Errorf(`%q is not a date this door can read on its own (Unix, ISO 8601, SMPP or a web date like "Thu, 01 Oct 2026 06:14:00 GMT") — set the delivery time's format to a pattern like DD/MM/YYYY HH:mm:ss`, v)
	default:
		layout := patternLayout(format)
		if layout == "" {
			return time.Time{}, fmt.Errorf("the delivery time's format %q is not a pattern", format)
		}
		return time.ParseInLocation(layout, v, loc)
	}
}

func allDigits(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return s != ""
}

func unixOf(v string, ms bool) (time.Time, error) {
	if f, err := strconv.ParseFloat(v, 64); err == nil && !allDigits(v) {
		// 1790580406.5 — a fraction of a second.
		if ms {
			return time.UnixMilli(int64(f)).UTC(), nil
		}
		return time.Unix(int64(f), int64((f-float64(int64(f)))*1e9)).UTC(), nil
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return time.Time{}, fmt.Errorf("%q is not a Unix time", v)
	}
	if ms {
		return time.UnixMilli(n).UTC(), nil
	}
	return time.Unix(n, 0).UTC(), nil
}

func smppOf(v string, loc *time.Location) (time.Time, error) {
	switch len(v) {
	case 10:
		return time.ParseInLocation("0601021504", v, loc)
	case 12:
		return time.ParseInLocation("060102150405", v, loc)
	}
	return time.Time{}, fmt.Errorf("%q is not an SMPP date (yyMMddHHmm or yyMMddHHmmss)", v)
}

func isoOf(v string, loc *time.Location) (time.Time, error) {
	for _, l := range isoLayouts {
		if t, err := time.ParseInLocation(l, v, loc); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("%q is not an ISO 8601 date — set the delivery time's format to auto, or to a pattern like DD/MM/YYYY HH:mm:ss", v)
}

// plausible is a time a receipt could really carry: not years away.
func plausible(t time.Time) bool {
	now := time.Now()
	return t.After(now.AddDate(-1, 0, 0)) && t.Before(now.AddDate(0, 0, 1))
}

// DoneAt is when this callback says the message reached the phone, read from
// the mapped done_at field. ok=false when the field is not mapped or empty;
// err says why a value that IS there could not be read.
func (q *Request) DoneAt(v Values) (t time.Time, ok bool, err error) {
	if q == nil {
		return time.Time{}, false, nil
	}
	raw := strings.TrimSpace(v["done_at"])
	if raw == "" {
		return time.Time{}, false, nil
	}
	t, err = ParseTime(raw, q.TimeFormat, q.TimeZone)
	if err != nil {
		return time.Time{}, false, err
	}
	return t.UTC(), true, nil
}
