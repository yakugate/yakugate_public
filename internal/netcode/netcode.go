// Package netcode compares mobile network codes written in different ways.
//
// The same network is stored in several forms across the gateway: the
// operators table pads the MNC to three digits ("410"+"003"), our HLR data and
// the API's operator field use two ("41003"), and routes and rate cards made
// in the portal hold whichever form the screen offered at the time ("410001"
// next to "41001"). Compared as strings they never meet, so a route set to
// "410003" never matched a number our HLR data calls "41003".
//
// The MCC is always three digits, so a combined code splits cleanly; the MNC
// is then compared as a number, the way the Operators screen's number lookup
// already does.
package netcode

import "strings"

// Split cuts a combined MCC+MNC ("41003", "410003") into its MCC (the first
// three digits) and MNC (the rest). A code of three digits or fewer is an MCC
// alone.
func Split(combined string) (mcc, mnc string) {
	combined = strings.TrimSpace(combined)
	if len(combined) <= 3 {
		return combined, ""
	}
	return combined[:3], combined[3:]
}

// SameMNC reports whether two MNCs name the same network: "3", "03" and "003"
// are equal. Two empty MNCs are equal; an empty one never equals a set one.
func SameMNC(a, b string) bool {
	a, b = strings.TrimSpace(a), strings.TrimSpace(b)
	if a == "" || b == "" {
		return a == b
	}
	return trimZeros(a) == trimZeros(b)
}

// Matches reports whether a combined route code ("410003", "41003", or an MCC
// alone, "410") covers a message's MCC and MNC. An MCC-only code covers every
// network in that country.
func Matches(combined, mcc, mnc string) bool {
	cm, cn := Split(combined)
	if cm == "" || cm != strings.TrimSpace(mcc) {
		return false
	}
	if cn == "" {
		return true
	}
	return SameMNC(cn, mnc)
}

// Norm writes an MNC the way our HLR data and the API do: at least two digits,
// without extra leading zeros ("003" -> "03", "260" -> "260", "7" -> "07").
func Norm(mnc string) string {
	mnc = strings.TrimSpace(mnc)
	if mnc == "" {
		return ""
	}
	t := trimZeros(mnc)
	if len(t) < 2 {
		t = strings.Repeat("0", 2-len(t)) + t
	}
	return t
}

func trimZeros(s string) string {
	t := strings.TrimLeft(s, "0")
	if t == "" {
		return "0"
	}
	return t
}
