//go:build windows

package supervisor

import (
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"time"

	"golang.org/x/sys/windows"
)

// Windows has no process groups to signal and no SIGTERM a console process
// can catch, so the gateway's own processes are handled with what Windows
// has: a process tree (taskkill /T) and the process's exit code.

// StopStarted ends a gateway this process started that never answered, with
// every role it started: taskkill on the tree, forced after 5 s.
func StopStarted(pid int) {
	_ = exec.Command("taskkill", "/T", "/PID", strconv.Itoa(pid)).Run()
	for i := 0; i < 50; i++ {
		if done, _ := exited(pid); done {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	_ = exec.Command("taskkill", "/T", "/F", "/PID", strconv.Itoa(pid)).Run()
	for i := 0; i < 20; i++ {
		if done, _ := exited(pid); done {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// stillActive is GetExitCodeProcess's answer for a process that has not ended.
const stillActive = 259

// exited reports whether pid has ended, and how in words. A process that
// cannot be opened at all is gone.
func exited(pid int) (bool, string) {
	h, err := windows.OpenProcess(windows.PROCESS_QUERY_LIMITED_INFORMATION, false, uint32(pid))
	if err != nil {
		return true, "ended"
	}
	defer windows.CloseHandle(h)
	var code uint32
	if err := windows.GetExitCodeProcess(h, &code); err != nil {
		return false, ""
	}
	if code == stillActive {
		return false, ""
	}
	return true, fmt.Sprintf("exit %d", code)
}

// detach starts the gateway in a process group of its own, with no console,
// so it outlives the console that started it.
func detach(cmd *exec.Cmd) {
	cmd.SysProcAttr = &windows.SysProcAttr{CreationFlags: windows.CREATE_NEW_PROCESS_GROUP | windows.DETACHED_PROCESS}
}

// askToStop ends a role. Windows cannot deliver SIGTERM to a console
// process, so there is no "finish what you are doing" to ask for: the role
// is ended at once, as a crash would end it. Everything a role writes is
// durable before it is acknowledged, so this loses no message.
func askToStop(proc *os.Process) { _ = proc.Kill() }
