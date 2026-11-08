// Package version is what the binary says about itself (VERSIONS.md §1,
// ratified PO 2026-09-17): a semantic version constant, plus the commit and
// build date injected at build time (Makefile ldflags). 1.0.0 is First Live
// Traffic (2026-10-02), released as 1.0.0-beta; tags start there.
package version

import "fmt"

var (
	Version = "1.2.0" // bumped by hand per VERSIONS.md §1.2
	Commit  = "unknown"   // -X github.com/yakugate/yakugate/internal/platform/version.Commit=<short sha>
	Date    = "unknown"   // -X github.com/yakugate/yakugate/internal/platform/version.Date=<UTC stamp>
)

// String is "1.2.0 (abc1234, 2026-10-30T13:00:00Z)".
func String() string { return fmt.Sprintf("%s (%s, %s)", Version, Commit, Date) }
