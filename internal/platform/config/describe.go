package config

import "strings"

// Describe is one short line saying what a setting does, for the console's
// Settings screens (Saleem 2026-10-01: "explain each variable in short") —
// "" for a name the gateway does not read. Defaults are named where there is
// one. yakugate.config.example has the same settings in groups (common, the
// whole gateway, shared, one process each), one line and the default each.
//
// The lines are registered with the settings, by group (settings_*.go);
// TestEveryKnownNameIsDescribed fails when a setting has none — describe a
// new setting when you add it.
func Describe(name string) string {
	if i, ok := registeredAt[name]; ok && registered[i].doc != "" {
		return registered[i].doc
	}
	// The prefix is split in two: TestKnownNamesCoverSource takes any whole
	// "YAKU_…" literal in the source for a setting's name.
	if role, ok := strings.CutPrefix(name, "YAKU_ADMIN_ADDR"+"_"); ok && Known(name) {
		r := strings.ToLower(strings.ReplaceAll(role, "_", "-"))
		return "Admin/health port of the " + r + " role only (beats YAKU_ADMIN_ADDR)"
	}
	return ""
}
