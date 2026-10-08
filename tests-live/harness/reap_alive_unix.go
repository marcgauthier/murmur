//go:build !windows

package harness

import (
	"os"
	"syscall"
)

func processAlive(pid int) bool {
	proc, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	// Signal 0 probes existence without delivering anything.
	return proc.Signal(syscall.Signal(0)) == nil
}
