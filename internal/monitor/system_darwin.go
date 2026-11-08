//go:build darwin

// Host figures on macOS.
//
// system.go reads everything from /proc, which is the right call for the
// platform this gateway runs on: every production box is Linux, and host
// stats there are a handful of files rather than a dependency carried by all
// fifteen roles. On a Mac none of those files exist, every read fails, and —
// because the collector degrades to a zero value rather than an error, which
// is the right choice for a monitoring collector — the snapshot went out with
// cpu.usage_percent 0, every memory figure 0 and uptime 0. The portal drew
// them faithfully, so a developer's Monitoring screen reported an idle machine
// with no memory, which is worse than reporting nothing: 0% and "not measured"
// looked identical.
//
// This fills what macOS can answer, through sysctl (golang.org/x/sys/unix is
// already a direct dependency, by way of x/term). What it cannot answer it
// leaves alone, and ReadSystem marks those unmeasured so the screen says "—".
//
// NOT measured here, deliberately:
//   - cpu.usage_percent and the memory used/free split come from mach calls
//     (host_statistics / HOST_CPU_LOAD_INFO, HOST_VM_INFO) which need cgo. The
//     engine builds without cgo and that is worth more than a CPU percentage
//     on a developer's laptop. Load average answers the same question — "is
//     this box keeping up" — and it IS available.
//   - Disk and network per interface: readDisks/readNet already use statfs and
//     work on darwin.
package monitor

import (
	"time"

	"golang.org/x/sys/unix"
)

// fillHostGaps adds what sysctl knows and /proc could not give us. It only
// ever writes a field that is still at its zero value, so a reading that was
// genuinely measured is never overwritten.
func fillHostGaps(s *System) {
	if s.UptimeSec == 0 {
		if tv, err := unix.SysctlTimeval("kern.boottime"); err == nil && tv.Sec > 0 {
			if up := time.Since(time.Unix(tv.Sec, 0)); up > 0 {
				s.UptimeSec = int64(up.Seconds())
			}
		}
	}
	if s.Memory.TotalBytes == 0 {
		if n, err := unix.SysctlUint64("hw.memsize"); err == nil {
			s.Memory.TotalBytes = n
		}
	}
	if s.Memory.SwapTotalBytes == 0 {
		// xsw_usage: total, avail, used, pagesize (uint64s), then two ints.
		if b, err := unix.SysctlRaw("vm.swapusage"); err == nil && len(b) >= 24 {
			le := func(o int) uint64 {
				var v uint64
				for i := 7; i >= 0; i-- {
					v = v<<8 | uint64(b[o+i])
				}
				return v
			}
			s.Memory.SwapTotalBytes = le(0)
			s.Memory.SwapUsedBytes = le(16)
		}
	}
	if s.CPU.Cores == 0 {
		if n, err := unix.SysctlUint32("hw.ncpu"); err == nil {
			s.CPU.Cores = int(n)
		}
	}
	if s.CPU.Model == "" {
		if v, err := unix.Sysctl("machdep.cpu.brand_string"); err == nil {
			s.CPU.Model = v
		}
	}
	if s.CPU.Load1 == 0 && s.CPU.Load5 == 0 && s.CPU.Load15 == 0 {
		// struct loadavg { fixpt_t ldavg[3]; long fscale; } — fixpt_t is
		// uint32, and the averages are the raw values over fscale.
		if b, err := unix.SysctlRaw("vm.loadavg"); err == nil && len(b) >= 20 {
			u32 := func(o int) uint32 {
				return uint32(b[o]) | uint32(b[o+1])<<8 | uint32(b[o+2])<<16 | uint32(b[o+3])<<24
			}
			scale := float64(u32(12))
			if scale <= 0 {
				scale = 2048 // FSCALE on darwin
			}
			s.CPU.Load1 = float64(u32(0)) / scale
			s.CPU.Load5 = float64(u32(4)) / scale
			s.CPU.Load15 = float64(u32(8)) / scale
		}
	}
	if s.OS.Release == "" {
		if v, err := unix.Sysctl("kern.osproductversion"); err == nil {
			s.OS.Release = v
		}
	}
	if s.OS.Kernel == "" {
		if v, err := unix.Sysctl("kern.osrelease"); err == nil {
			s.OS.Kernel = v
		}
	}
	if s.OS.Distro == "" {
		s.OS.Distro = "macOS"
	}
}
