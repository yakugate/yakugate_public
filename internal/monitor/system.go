// System metrics for the monitoring snapshot — the machine the role runs on,
// not the traffic through it.
//
// Yakugate reported nothing about its host until now, which meant the first
// question of any slowdown — "was it us or the box?" — had no answer. The
// Hadal reference carries CPU, memory, storage, network and process heap, and
// that set is the right one.
//
// Read straight from /proc and syscalls rather than adding a metrics library.
// The engine has one dependency for each thing it genuinely needs a library
// for; host stats on Linux are a handful of files, internal/hlr already reads
// /proc/meminfo the same way, and a dependency here would be carried by every
// one of the fifteen roles for the sake of some string parsing.
//
// Every field degrades to a zero value rather than an error. A snapshot with
// an unreadable disk figure is still worth having, and a monitoring collector
// that fails because it could not stat a filesystem is worse than useless.

package monitor

import (
	"bufio"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

// System is the host and process view in one snapshot.
type System struct {
	Hostname string `json:"hostname"`
	OS       OS     `json:"os"`
	// UptimeSec is the MACHINE's uptime, not the process's — the process has
	// its own start time on the snapshot. A role restarting on a box that did
	// not restart tells you something different from both restarting.
	UptimeSec int64  `json:"uptime_sec"`
	CPU       CPU    `json:"cpu"`
	Memory    Memory `json:"memory"`
	Disk      []Disk `json:"disk,omitempty"`
	Net       []Net  `json:"net,omitempty"`
	Process   Proc   `json:"process"`
}

// OS identifies the operating system. Read once and cached — none of it can
// change while the process runs, and re-reading /etc/os-release every five
// seconds for a value that is fixed at boot is pure waste.
type OS struct {
	Platform string `json:"platform"` // linux
	Distro   string `json:"distro"`   // Ubuntu
	Release  string `json:"release"`  // 26.04.1 LTS
	Kernel   string `json:"kernel"`   // 6.x.y-generic
	Arch     string `json:"arch"`     // amd64
}

// CPU is the host processor picture.
type CPU struct {
	Cores int `json:"cores"`
	// Model and MHz identify the silicon. Worth carrying because "the gateway
	// is slower on box B" is usually answered by the two machines not being
	// the same machine.
	Model string  `json:"model"`
	MHz   float64 `json:"mhz"`
	// TemperatureC is 0 when the host exposes no thermal zone — a VM, a
	// container, or most cloud instances. Zero here means UNKNOWN, not cold.
	TemperatureC float64 `json:"temperature_c"`
	// UsagePercent is measured BETWEEN snapshots, so the first sample after a
	// process starts reports 0 rather than a meaningless instant reading.
	// /proc/stat counts ticks since boot; a single read tells you the average
	// since boot, which is never what anyone means by "CPU usage".
	UsagePercent float64 `json:"usage_percent"`
	// UsageKnown separates "0%" from "nobody measured it" (2026-10-05). The
	// collector degrades to zero values on purpose, which is right — but a
	// consumer then cannot tell an idle box from an unreadable one, and on a
	// platform with no /proc that is every field at once.
	UsageKnown bool `json:"usage_known"`
	// Load1/5/15 are the kernel's own load averages, which unlike a percentage
	// say whether work is QUEUING. A box at 100% with load 1 is busy; at 100%
	// with load 30 it is drowning, and those need different reactions.
	Load1  float64 `json:"load_1"`
	Load5  float64 `json:"load_5"`
	Load15 float64 `json:"load_15"`
}

// Memory is host memory in bytes.
type Memory struct {
	TotalBytes uint64 `json:"total_bytes"`
	// FreeBytes and CachedBytes are reported alongside Available because they
	// answer different questions. Free is memory nobody holds; Cached is the
	// page cache, which is "in use" but instantly reclaimable. Reporting Free
	// alone makes every healthy database host look full.
	FreeBytes      uint64  `json:"free_bytes"`
	CachedBytes    uint64  `json:"cached_bytes"`
	AvailableBytes uint64  `json:"available_bytes"`
	UsedBytes      uint64  `json:"used_bytes"`
	UsagePercent   float64 `json:"usage_percent"`
	// As CPU.UsageKnown: a reported 0% means idle only if this is true.
	UsageKnown     bool   `json:"usage_known"`
	SwapTotalBytes uint64 `json:"swap_total_bytes"`
	SwapUsedBytes  uint64 `json:"swap_used_bytes"`
}

// Disk is one mounted filesystem.
type Disk struct {
	// Device is the block device (/dev/nvme0n1p2), Mount the path. Both,
	// because two mounts can share a device and an operator thinks in whichever
	// one their monitoring alert quoted.
	Device         string  `json:"device"`
	Mount          string  `json:"mount"`
	FSType         string  `json:"fstype"`
	TotalBytes     uint64  `json:"total_bytes"`
	AvailableBytes uint64  `json:"available_bytes"`
	UsagePercent   float64 `json:"usage_percent"`
}

// Net is one interface's cumulative counters. Cumulative, not per-second: a
// reader comparing two snapshots can derive the rate, and storing the raw
// counter means a missed sample does not silently under-report the gap.
type Net struct {
	Iface string `json:"iface"`
	// IPv4, MAC, SpeedMbps and Kind describe the interface itself. SpeedMbps
	// is -1 when the kernel cannot report it, which is normal for virtual and
	// wireless interfaces — that is UNKNOWN, not "zero speed".
	IPv4      string `json:"ipv4,omitempty"`
	MAC       string `json:"mac,omitempty"`
	SpeedMbps int64  `json:"speed_mbps"`
	Kind      string `json:"kind"` // wired | wireless | virtual
	RxBytes   uint64 `json:"rx_bytes"`
	TxBytes   uint64 `json:"tx_bytes"`
	RxErrors  uint64 `json:"rx_errors"`
	TxErrors  uint64 `json:"tx_errors"`
	// RxDropped/TxDropped matter more than errors on a gateway: a dropped
	// packet under load is the shape of an overloaded NIC or a full socket
	// buffer, which is exactly the failure a busy SMPP bind produces.
	RxDropped uint64 `json:"rx_dropped"`
	TxDropped uint64 `json:"tx_dropped"`
}

// Proc is this Go process itself.
type Proc struct {
	PID        int `json:"pid"`
	Goroutines int `json:"goroutines"`
	// RSSBytes is resident set size — what the OS says this process occupies.
	// The Go heap figures below are what the RUNTIME thinks it has; the two
	// differ, sometimes a lot, and a gap between them is how you spot memory
	// the runtime has not returned to the OS.
	RSSBytes       uint64 `json:"rss_bytes"`
	HeapAllocBytes uint64 `json:"heap_alloc_bytes"`
	HeapSysBytes   uint64 `json:"heap_sys_bytes"`
	// StackSysBytes and OtherSysBytes are Go's equivalents of the Node
	// "external"/"arrayBuffers" split — memory held outside the heap.
	StackSysBytes uint64 `json:"stack_sys_bytes"`
	OtherSysBytes uint64 `json:"other_sys_bytes"`
	// HeapUsagePercent is alloc/sys — how full the runtime's own arena is.
	HeapUsagePercent float64 `json:"heap_usage_percent"`
	// GCPauseTotalMs is cumulative. A gateway that starts stalling at p95
	// while its GC pause total climbs is telling you where to look.
	GCPauseTotalMs uint64 `json:"gc_pause_total_ms"`
	NumGC          uint32 `json:"num_gc"`
}

// cpuSampler holds the previous /proc/stat reading so usage can be a real
// interval measurement rather than an average since boot.
type cpuSampler struct {
	mu           sync.Mutex
	lastTotal    uint64
	lastIdle     uint64
	lastSampleAt time.Time
}

var cpus cpuSampler

// ReadSystem gathers one host + process sample.
func ReadSystem() System {
	host, _ := os.Hostname()
	s := System{
		Hostname:  host,
		OS:        readOS(),
		UptimeSec: readUptimeSec(),
		CPU:       readCPU(),
		Memory:    readMemory(),
		Disk:      readDisks(),
		Net:       readNet(),
	}
	// What this platform answers by another route than /proc (macOS: sysctl).
	// A no-op on Linux and Windows, and it only ever writes a field still at
	// its zero value.
	fillHostGaps(&s)
	// WHAT COULD NOT BE MEASURED SAYS SO. An unreadable figure used to go out
	// as 0, so a reader could not tell an idle CPU from one nobody measured —
	// on a Mac the whole System panel reported a machine at 0% with no memory.
	// `latency.known` already carries this distinction for latency; CPU and
	// memory carry it now too (set by their own readers, which are the only
	// code that knows whether a measurement happened), and the portal shows
	// "—" rather than a confident zero.
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	s.Process = Proc{
		PID:            os.Getpid(),
		Goroutines:     runtime.NumGoroutine(),
		RSSBytes:       readRSSBytes(),
		HeapAllocBytes: m.HeapAlloc,
		HeapSysBytes:   m.HeapSys,
		StackSysBytes:  m.StackSys,
		OtherSysBytes:  m.MSpanSys + m.MCacheSys + m.BuckHashSys + m.GCSys + m.OtherSys,
		GCPauseTotalMs: m.PauseTotalNs / 1e6,
		NumGC:          m.NumGC,
	}
	if m.HeapSys > 0 {
		s.Process.HeapUsagePercent = float64(m.HeapAlloc) / float64(m.HeapSys) * 100
	}
	return s
}

func readUptimeSec() int64 {
	b, err := os.ReadFile("/proc/uptime")
	if err != nil {
		return 0
	}
	f := strings.Fields(string(b))
	if len(f) == 0 {
		return 0
	}
	v, _ := strconv.ParseFloat(f[0], 64)
	return int64(v)
}

func readCPU() CPU {
	c := CPU{Cores: runtime.NumCPU()}
	c.Model, c.MHz = cpuInfoOnce()
	c.TemperatureC = readCPUTemp()
	c.Load1, c.Load5, c.Load15 = readLoadAvg()
	total, idle, ok := readCPUTicks()
	if !ok {
		// No tick source on this platform (no /proc/stat): the percentage is
		// not measured, and UsageKnown stays false so nobody reads the zero as
		// an idle box.
		return c
	}
	cpus.mu.Lock()
	defer cpus.mu.Unlock()
	if !cpus.lastSampleAt.IsZero() && total > cpus.lastTotal {
		dTotal := total - cpus.lastTotal
		dIdle := idle - cpus.lastIdle
		if dTotal > 0 {
			c.UsagePercent = float64(dTotal-dIdle) / float64(dTotal) * 100
			// Only now is the figure a real interval measurement. The first
			// sample after start deliberately has none.
			c.UsageKnown = true
		}
	}
	cpus.lastTotal, cpus.lastIdle, cpus.lastSampleAt = total, idle, time.Now()
	return c
}

// readCPUTicks sums the aggregate "cpu" line of /proc/stat.
func readCPUTicks() (total, idle uint64, ok bool) {
	f, err := os.Open("/proc/stat")
	if err != nil {
		return 0, 0, false
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) < 5 || fields[0] != "cpu" {
			continue
		}
		for i, v := range fields[1:] {
			n, err := strconv.ParseUint(v, 10, 64)
			if err != nil {
				continue
			}
			total += n
			// Fields 4 and 5 (idle, iowait) both count as not-working. iowait
			// is included deliberately: a gateway blocked on its database is
			// not busy, and calling that CPU usage would send someone to
			// optimise code when the disk is the problem.
			if i == 3 || i == 4 {
				idle += n
			}
		}
		return total, idle, true
	}
	return 0, 0, false
}

func readLoadAvg() (float64, float64, float64) {
	b, err := os.ReadFile("/proc/loadavg")
	if err != nil {
		return 0, 0, 0
	}
	f := strings.Fields(string(b))
	if len(f) < 3 {
		return 0, 0, 0
	}
	a, _ := strconv.ParseFloat(f[0], 64)
	c, _ := strconv.ParseFloat(f[1], 64)
	d, _ := strconv.ParseFloat(f[2], 64)
	return a, c, d
}

func readMemory() Memory {
	vals := map[string]uint64{}
	f, err := os.Open("/proc/meminfo")
	if err != nil {
		// Nothing read: every field stays zero and UsageKnown stays false, so
		// the screen says "—" instead of "0% of 0 bytes".
		return Memory{}
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) < 2 {
			continue
		}
		kb, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			continue
		}
		vals[strings.TrimSuffix(fields[0], ":")] = kb * 1024
	}
	m := Memory{
		TotalBytes:     vals["MemTotal"],
		FreeBytes:      vals["MemFree"],
		CachedBytes:    vals["Cached"],
		AvailableBytes: vals["MemAvailable"],
		SwapTotalBytes: vals["SwapTotal"],
	}
	if m.TotalBytes > 0 && m.AvailableBytes <= m.TotalBytes {
		// Used means "not available", NOT total-free. Free excludes the page
		// cache, which on a database host is most of memory and is reclaimable
		// — reporting that as used makes every box look permanently full.
		m.UsedBytes = m.TotalBytes - m.AvailableBytes
		m.UsagePercent = float64(m.UsedBytes) / float64(m.TotalBytes) * 100
		m.UsageKnown = true
	}
	if free := vals["SwapFree"]; m.SwapTotalBytes >= free {
		m.SwapUsedBytes = m.SwapTotalBytes - free
	}
	return m
}

// readDisks reports the filesystems that carry this engine's data.
//
// Filtered by filesystem TYPE, not by device path, and that distinction is not
// cosmetic. On this dev machine /proc/mounts lists 40 filesystems of which 38
// are snap packages: they are loop devices under /dev/, so a "/dev/ prefix"
// filter lets every one through, and each reports 100% USED because a squashfs
// image is read-only and packed solid. The snapshot would have been 95% noise
// and would have shown three dozen alarming full disks that mean nothing at
// all — on a dashboard someone is meant to page off.
func readDisks() []Disk {
	seen := map[string]bool{}
	var out []Disk
	f, err := os.Open("/proc/mounts")
	if err != nil {
		return nil
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) < 3 || !strings.HasPrefix(fields[0], "/dev/") {
			continue
		}
		if !realFilesystem(fields[2]) {
			continue
		}
		mount := fields[1]
		if seen[mount] {
			continue
		}
		seen[mount] = true
		total, avail, ok := diskSpace(mount)
		if !ok || total == 0 {
			continue
		}
		d := Disk{Device: fields[0], Mount: mount, FSType: fields[2], TotalBytes: total, AvailableBytes: avail}
		d.UsagePercent = float64(total-avail) / float64(total) * 100
		out = append(out, d)
	}
	return out
}

func readNet() []Net {
	f, err := os.Open("/proc/net/dev")
	if err != nil {
		return nil
	}
	defer f.Close()
	var out []Net
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		i := strings.Index(line, ":")
		if i < 0 {
			continue // the two header lines
		}
		iface := strings.TrimSpace(line[:i])
		if iface == "lo" {
			continue // loopback traffic is our own and tells nobody anything
		}
		fields := strings.Fields(line[i+1:])
		if len(fields) < 12 {
			continue
		}
		n := Net{Iface: iface}
		ifaceDetail(&n)
		n.RxBytes, _ = strconv.ParseUint(fields[0], 10, 64)
		n.RxErrors, _ = strconv.ParseUint(fields[2], 10, 64)
		n.RxDropped, _ = strconv.ParseUint(fields[3], 10, 64)
		n.TxBytes, _ = strconv.ParseUint(fields[8], 10, 64)
		n.TxErrors, _ = strconv.ParseUint(fields[10], 10, 64)
		n.TxDropped, _ = strconv.ParseUint(fields[11], 10, 64)
		out = append(out, n)
	}
	return out
}

// realFilesystems are the types that hold data we could actually run out of.
// Anything else — squashfs snap images, tmpfs, overlays, virtual filesystems —
// either cannot fill up or filling up means nothing.
var realFilesystems = map[string]bool{
	"ext2": true, "ext3": true, "ext4": true,
	"xfs": true, "btrfs": true, "zfs": true,
	"vfat": true, "ntfs": true, "ntfs3": true,
	"f2fs": true, "jfs": true, "reiserfs": true,
	"nfs": true, "nfs4": true,
}

func realFilesystem(fstype string) bool { return realFilesystems[fstype] }

// --- identity, read once ----------------------------------------------------

var (
	osOnce  sync.Once
	osCache OS
	cpuOnce sync.Once
	cpuName string
	cpuMHz  float64
)

// readOS identifies the operating system. Cached: nothing here changes while
// the process lives, and parsing /etc/os-release every five seconds across
// fifteen roles is work for no information.
func readOS() OS {
	osOnce.Do(func() {
		o := OS{Platform: runtime.GOOS, Arch: runtime.GOARCH}
		if b, err := os.ReadFile("/etc/os-release"); err == nil {
			for _, line := range strings.Split(string(b), "\n") {
				k, v, ok := strings.Cut(line, "=")
				if !ok {
					continue
				}
				v = strings.Trim(v, `"`)
				switch k {
				case "NAME":
					o.Distro = v
				case "VERSION":
					o.Release = v
				}
			}
		}
		if b, err := os.ReadFile("/proc/sys/kernel/osrelease"); err == nil {
			o.Kernel = strings.TrimSpace(string(b))
		}
		osCache = o
	})
	return osCache
}

// cpuInfoOnce reads the processor model and its nominal clock. Cached for the
// same reason as readOS — and note the MHz here is the value at first read;
// modern CPUs scale constantly, so treat it as identity, not a live gauge.
func cpuInfoOnce() (string, float64) {
	cpuOnce.Do(func() {
		f, err := os.Open("/proc/cpuinfo")
		if err != nil {
			return
		}
		defer f.Close()
		sc := bufio.NewScanner(f)
		for sc.Scan() {
			k, v, ok := strings.Cut(sc.Text(), ":")
			if !ok {
				continue
			}
			k, v = strings.TrimSpace(k), strings.TrimSpace(v)
			switch k {
			case "model name":
				if cpuName == "" {
					cpuName = v
				}
			case "cpu MHz":
				if cpuMHz == 0 {
					cpuMHz, _ = strconv.ParseFloat(v, 64)
				}
			}
			if cpuName != "" && cpuMHz != 0 {
				return
			}
		}
	})
	return cpuName, cpuMHz
}

// readCPUTemp returns the hottest thermal zone in Celsius, or 0 when the host
// exposes none.
//
// ZERO MEANS UNKNOWN. Most VMs and cloud instances have no thermal zone at
// all, and a dashboard that renders "0 °C" for them is stating something false
// — the renderer must treat 0 as absent.
func readCPUTemp() float64 {
	zones, err := filepath.Glob("/sys/class/thermal/thermal_zone*/temp")
	if err != nil || len(zones) == 0 {
		return 0
	}
	var hottest float64
	for _, z := range zones {
		b, err := os.ReadFile(z)
		if err != nil {
			continue
		}
		milli, err := strconv.ParseFloat(strings.TrimSpace(string(b)), 64)
		if err != nil {
			continue
		}
		// The kernel reports millidegrees. A zone reading absurdly high is a
		// sensor fault rather than a fire, and would panic a dashboard.
		if c := milli / 1000; c > hottest && c < 150 {
			hottest = c
		}
	}
	return hottest
}

// readRSSBytes is resident set size from /proc/self/statm, in bytes.
func readRSSBytes() uint64 {
	b, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		return 0
	}
	f := strings.Fields(string(b))
	if len(f) < 2 {
		return 0
	}
	pages, err := strconv.ParseUint(f[1], 10, 64)
	if err != nil {
		return 0
	}
	return pages * uint64(os.Getpagesize())
}

// ifaceDetail fills in the address, MAC, link speed and kind of one interface.
func ifaceDetail(n *Net) {
	base := "/sys/class/net/" + n.Iface + "/"
	if b, err := os.ReadFile(base + "address"); err == nil {
		n.MAC = strings.TrimSpace(string(b))
	}
	// Speed is -1 when the kernel will not say, which is normal for wireless
	// and virtual links. Reported as -1 rather than 0 so "unknown" and "down"
	// stay distinguishable.
	n.SpeedMbps = -1
	if b, err := os.ReadFile(base + "speed"); err == nil {
		if v, err := strconv.ParseInt(strings.TrimSpace(string(b)), 10, 64); err == nil {
			n.SpeedMbps = v
		}
	}
	switch {
	case dirExists(base + "wireless"), dirExists(base + "phy80211"):
		n.Kind = "wireless"
	case dirExists(base + "device"):
		n.Kind = "wired"
	default:
		n.Kind = "virtual"
	}
	if ifc, err := net.InterfaceByName(n.Iface); err == nil {
		if addrs, err := ifc.Addrs(); err == nil {
			for _, a := range addrs {
				if ipn, ok := a.(*net.IPNet); ok && ipn.IP.To4() != nil {
					n.IPv4 = ipn.IP.String()
					break
				}
			}
		}
	}
}

func dirExists(p string) bool {
	fi, err := os.Stat(p)
	return err == nil && fi.IsDir()
}
