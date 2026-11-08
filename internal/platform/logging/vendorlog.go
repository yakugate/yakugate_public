package logging

// Per-vendor daily log files (PO 2026-09-04: "each account and vendor having
// separate log file each day").
//
// The account half has worked since that ruling — P1 and P2 write
// account_<name>_<date>.log. The VENDOR half never did: P3 and P4 log
// everything through Printf, which is system-scoped, so a deployment has one
// system file with every vendor's traffic mixed into it and no
// vendor_<cid>_<date>.log at all. Nothing was missing from the machinery —
// VendorScope and the file naming were already here; the two vendor processes
// simply never named a scope.
//
// It matters beyond tidiness. Per-vendor log level, rotation size and
// retention (P13 backlog L-01) are configured against a vendor, and they have
// nothing to act on while every line a vendor produces is filed as "system".
//
// Written TWICE on purpose, mirroring what P2 does for an account: once into
// the vendor's own file, so a customer-facing operator can be shown one
// vendor's day without seeing anyone else's, and once into the system stream
// tagged with the cid, so the console and the whole-gateway view still read
// exactly as before. Nothing is lost from where it used to be.

import "fmt"

// VendorLog writes records that belong to one vendor.
type VendorLog struct {
	l   *Logger
	cid string
}

// ForVendor returns a writer for one vendor's records. A nil logger or an
// empty cid yields a writer whose calls are no-ops, so a caller that has not
// resolved its vendor yet does not have to guard every line.
func (l *Logger) ForVendor(cid string) VendorLog { return VendorLog{l: l, cid: cid} }

func (v VendorLog) write(lv Level, typ, msg string) {
	if v.l == nil || v.cid == "" {
		return
	}
	v.l.write(lv, VendorScope(v.cid), typ, msg, nil)
	// The system copy carries the cid as a field, because on a system record
	// nothing else names the vendor — the scope column says "system".
	v.l.write(lv, SystemScope, typ, msg, Fields{"vendor": v.cid})
}

// Infof records something that happened normally: a bind coming up, a batch
// going out.
func (v VendorLog) Infof(typ, format string, a ...any) {
	v.write(LevelInfo, typ, fmt.Sprintf(format, a...))
}

// Warnf records something that went wrong but is expected to recover — a
// failed dial before the next retry, a chunk left on the queue.
func (v VendorLog) Warnf(typ, format string, a ...any) {
	v.write(LevelWarn, typ, fmt.Sprintf(format, a...))
}

// Errorf records something an operator has to act on: a vendor that cannot be
// configured, credentials that will not resolve, a bind given up on.
func (v VendorLog) Errorf(typ, format string, a ...any) {
	v.write(LevelError, typ, fmt.Sprintf(format, a...))
}
