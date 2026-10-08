//go:build !windows

package pauseresume_test

import (
	"os"
	"syscall"
)

func supportsPauseResume() bool {
	return true
}

func pauseProcess(proc *os.Process) error {
	return proc.Signal(syscall.SIGSTOP)
}

func resumeProcess(proc *os.Process) error {
	return proc.Signal(syscall.SIGCONT)
}
