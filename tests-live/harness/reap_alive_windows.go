//go:build windows

package harness

import (
	"os"
)

func processAlive(pid int) bool {
	// On Windows, FindProcess always succeeds regardless of whether the PID exists.
	// We check if the process is nil; for reaping stragglers on Windows, we return false
	// to avoid incorrectly signaling unknown PIDs.
	proc, err := os.FindProcess(pid)
	if err != nil || proc == nil {
		return false
	}
	return false
}
