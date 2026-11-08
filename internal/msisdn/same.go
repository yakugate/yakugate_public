package msisdn

// sameDigits is how many of a number's last digits must agree for two to be
// one handset: a delivery receipt may carry the number in its national or
// its international form (03001234567 / 923001234567 / +923001234567), and
// the last seven are the subscriber's in every form.
const sameDigits = 7

// Same reports whether two numbers are one handset: their digits, the last
// sameDigits of them when both have that many. An empty one is not a
// different number — there is nothing to compare (used to tell apart two
// messages that two vendors of one company gave the same id, Saleem
// 2026-10-14).
func Same(a, b string) bool {
	da, db := onlyDigits(a), onlyDigits(b)
	if da == "" || db == "" {
		return true
	}
	if len(da) >= sameDigits && len(db) >= sameDigits {
		return da[len(da)-sameDigits:] == db[len(db)-sameDigits:]
	}
	return da == db
}

func onlyDigits(s string) string {
	b := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		if c := s[i]; c >= '0' && c <= '9' {
			b = append(b, c)
		}
	}
	return string(b)
}

// Tail is the part of a number Same compares: its last sameDigits digits, or
// all of them when it has fewer; "" when it has none. Two numbers Same calls
// one handset have the same Tail when both have sameDigits digits — the key a
// receipt's facts are filed under by number (D-136).
func Tail(s string) string {
	d := onlyDigits(s)
	if len(d) > sameDigits {
		return d[len(d)-sameDigits:]
	}
	return d
}
