package config

import "fmt"

// The settings the gateway reads: each "YAKU_…" name in the engine's source,
// plus the five built at run time from a prefix (httpserver/tls.go reads
// YAKU_<prefix>_TLS* with prefix API) and one YAKU_ADMIN_ADDR_<ROLE> per role
// (cmd/yakugate useRoleAdminAddr). `yakugate config set` refuses any other
// name unless it is already in the file or --new is given, so a typo cannot
// sit in yakugate.config unnoticed.
//
// Each group's file (settings_<group>.go) registers its own settings with
// a package-level `var _ = settings("Group", []setting{{name, doc}, …})`, so
// two new settings in two groups never edit the same lines (30 Oct 2026;
// until then all were in one list here and one map in describe.go).
//
// TestKnownNamesCoverSource fails when a new setting is added to the source
// and not registered; TestEveryKnownNameIsDescribed when it has no line.

// setting is one setting: its name and its one line for the console's
// Settings screens. A YAKU_ADMIN_ADDR_<ROLE> has no line of its own:
// Describe makes it from the name.
type setting struct {
	name, doc string
}

// settingGroups are the groups in the order yakugate.config.example and
// the console list them. A new group is added here; a new setting goes in
// its group's file.
var settingGroups = []string{
	"Stores",
	"Whole gateway and roles",
	"Listeners",
	"TLS",
	"Keys and secrets",
	"Accepting messages",
	"HLR",
	"SMPP",
	"Receipts and incoming",
	"Retry and quality",
	"Billing and abuse",
	"Keeping and backing up data",
	"Logs, monitoring, captures",
}

// registered is every setting in the order the group files registered
// them; registeredAt finds one by name. Package-level variables are set
// before any init runs, and Known and the rest are called after, so they
// see every group.
var (
	registered   []groupSetting
	registeredAt = map[string]int{}
)

type groupSetting struct {
	group string
	setting
}

// settings registers a group's settings. A name registered twice, or a
// group not in settingGroups, is a mistake in the source: it stops the
// binary at start, as a doubled key in a map literal stopped the build.
func settings(group string, list []setting) bool {
	known := false
	for _, g := range settingGroups {
		known = known || g == group
	}
	if !known {
		panic(fmt.Sprintf("config: settings of group %q, which is not in settingGroups", group))
	}
	for _, s := range list {
		if _, dup := registeredAt[s.name]; dup || s.name == "" {
			panic(fmt.Sprintf("config: setting %q registered twice or with no name", s.name))
		}
		registeredAt[s.name] = len(registered)
		registered = append(registered, groupSetting{group, s})
	}
	return true
}
