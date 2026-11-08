//go:build !windows

package supervisor

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"syscall"
	"time"
)

// StopStarted ends a gateway this process started that never answered, so a
// failed start leaves nothing running. It runs in its own session (pgid =
// pid), so its roles go with it: SIGTERM first, SIGKILL after 5 s.
func StopStarted(pid int) {
	_ = syscall.Kill(-pid, syscall.SIGTERM)
	for i := 0; i < 50; i++ {
		if done, _ := exited(pid); done {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	_ = syscall.Kill(-pid, syscall.SIGKILL)
	for i := 0; i < 20; i++ {
		if done, _ := exited(pid); done {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// exited reports whether pid has ended, and how in words, reaping it when it
// is our child.
func exited(pid int) (bool, string) {
	var ws syscall.WaitStatus
	wpid, err := syscall.Wait4(pid, &ws, syscall.WNOHANG, nil)
	if wpid == pid {
		return true, exitWords(ws)
	}
	if errors.Is(err, syscall.ECHILD) {
		// Not our child (already reaped): ask the kernel whether it exists.
		return syscall.Kill(pid, 0) != nil, "ended"
	}
	return false, ""
}

func exitWords(ws syscall.WaitStatus) string {
	switch {
	case ws.Exited():
		return fmt.Sprintf("exit %d", ws.ExitStatus())
	case ws.Signaled():
		return "killed by " + ws.Signal().String()
	}
	return "ended"
}

// detach starts the gateway in a session of its own, so it outlives the
// console that started it and StopStarted can end its whole group.
func detach(cmd *exec.Cmd) { cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true} }

// askToStop asks a role to finish what it is doing and stop (SIGTERM, which
// every role handles).
func askToStop(proc *os.Process) { _ = proc.Signal(syscall.SIGTERM) }
