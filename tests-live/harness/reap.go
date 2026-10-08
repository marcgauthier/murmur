package harness

import (
	"os"
	"strings"
)

// reapStragglers SIGKILLs any daemon process this cluster started that
// is still alive after the normal stop loop (a wedged shutdown or a
// test bug that orphaned a process). It only touches PIDs whose
// command line identifies them as test daemons, so a recycled PID is
// never killed. Best effort: every error is logged, none is fatal.
func (c *Cluster) reapStragglers() {
	for _, node := range c.Nodes {
		node.pidMu.Lock()
		pids := node.Pids
		node.Pids = nil
		node.pidMu.Unlock()
		for _, pid := range pids {
			if pid <= 0 {
				continue
			}
			if !processAlive(pid) {
				continue
			}
			if !processCmdlineContains(pid, "testnode") {
				continue
			}
			proc, err := os.FindProcess(pid)
			if err != nil {
				continue
			}
			if err := proc.Kill(); err != nil {
				c.T.Logf("reap straggler pid=%d: %v", pid, err)
				continue
			}
			c.T.Logf("reaped straggler daemon pid=%d (%s)", pid, node.Label)
		}
	}
}

// processCmdlineContains reports whether the process command line
// contains substr. Unknown (non-Linux, unreadable) reads conservatively
// false so the sweeper never kills what it cannot identify.
func processCmdlineContains(pid int, substr string) bool {
	raw, err := os.ReadFile(pidCmdlinePath(pid))
	if err != nil {
		return false
	}
	for _, arg := range strings.Split(string(raw), "\x00") {
		if strings.Contains(arg, substr) {
			return true
		}
	}
	return false
}
