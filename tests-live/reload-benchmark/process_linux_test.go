//go:build linux

package reloadbenchmark_test

import (
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
)

func configureChild(cmd *exec.Cmd) {
	// Prevent a timed-out or interrupted test parent leaving a 10 GB worker.
	cmd.SysProcAttr = &syscall.SysProcAttr{Pdeathsig: syscall.SIGKILL}
}

func peakRSS() int64 {
	b, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(string(b), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 3 && fields[0] == "VmHWM:" {
			n, _ := strconv.ParseInt(fields[1], 10, 64)
			return n * 1024
		}
	}
	return 0
}
