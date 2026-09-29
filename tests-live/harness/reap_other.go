//go:build !linux

package harness

// pidCmdlinePath has no /proc off Linux; the read fails and the
// sweeper conservatively skips unidentified processes.
func pidCmdlinePath(pid int) string {
	return "/nonexistent-pid-cmdline"
}
