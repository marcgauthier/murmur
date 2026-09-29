package harness

import "fmt"

func pidCmdlinePath(pid int) string {
	return fmt.Sprintf("/proc/%d/cmdline", pid)
}
