//go:build windows

package monitor

// diskSpace has nothing to report on Windows: the disk list comes from
// /proc/mounts, which only Linux has, so it is never asked.
func diskSpace(string) (total, avail uint64, ok bool) { return 0, 0, false }
