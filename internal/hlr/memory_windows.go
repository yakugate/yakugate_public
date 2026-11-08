//go:build windows

package hlr

import (
	"syscall"
	"unsafe"
)

// availableMemoryBytes returns available physical memory (0 = unknown).
func availableMemoryBytes() uint64 {
	var ms struct {
		Length               uint32
		MemoryLoad           uint32
		TotalPhys            uint64
		AvailPhys            uint64
		TotalPageFile        uint64
		AvailPageFile        uint64
		TotalVirtual         uint64
		AvailVirtual         uint64
		AvailExtendedVirtual uint64
	}
	ms.Length = uint32(unsafe.Sizeof(ms))
	proc := syscall.NewLazyDLL("kernel32.dll").NewProc("GlobalMemoryStatusEx")
	r, _, _ := proc.Call(uintptr(unsafe.Pointer(&ms)))
	if r == 0 {
		return 0
	}
	return ms.AvailPhys
}
