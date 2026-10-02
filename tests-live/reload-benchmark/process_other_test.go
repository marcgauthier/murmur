//go:build !linux

package reloadbenchmark_test

import "os/exec"

func configureChild(cmd *exec.Cmd) {}

// Go heap statistics do not account for CGO SQLite allocations.
func peakRSS() int64 { return 0 }
