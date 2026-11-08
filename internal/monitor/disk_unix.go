//go:build !windows

package monitor

import "syscall"

// diskSpace is a mounted filesystem's size and the space free to us, in bytes.
func diskSpace(mount string) (total, avail uint64, ok bool) {
	var st syscall.Statfs_t
	if syscall.Statfs(mount, &st) != nil {
		return 0, 0, false
	}
	return st.Blocks * uint64(st.Bsize), st.Bavail * uint64(st.Bsize), true
}
