//go:build !windows && !linux

package hlr

// availableMemoryBytes is unknown on this platform; the precheck then only
// logs the estimate and proceeds.
func availableMemoryBytes() uint64 { return 0 }
