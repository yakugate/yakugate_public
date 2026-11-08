//go:build !darwin

package monitor

// fillHostGaps is darwin's; everywhere else /proc (or the Windows build) has
// already answered, and there is nothing to fill in.
func fillHostGaps(*System) {}
