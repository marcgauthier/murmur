package harness

import (
	"os"
	"os/exec"
	"testing"
	"time"
)

// TestReapStragglersKillsOnlyTestDaemons proves the cleanup sweep
// reaps stranded daemon PIDs while leaving other processes alone.
// It copies /bin/sleep under a testnode-matching name: no daemon,
// no ports, no timing flakes beyond process spawn.
func TestReapStragglersKillsOnlyTestDaemons(t *testing.T) {
	sleepBin, err := exec.LookPath("sleep")
	if err != nil {
		t.Skip("sleep not available")
	}
	raw, err := os.ReadFile(sleepBin)
	if err != nil {
		t.Fatal(err)
	}
	fakeDaemon := t.TempDir() + "/faketestnode-sleeper"
	if err := os.WriteFile(fakeDaemon, raw, 0755); err != nil {
		t.Fatal(err)
	}

	startSleeper := func(bin string) *exec.Cmd {
		cmd := exec.Command(bin, "60")
		if err := cmd.Start(); err != nil {
			t.Fatalf("start %s: %v", bin, err)
		}
		return cmd
	}
	daemon := startSleeper(fakeDaemon)
	other := startSleeper(sleepBin)
	defer func() {
		_ = other.Process.Kill()
		_ = other.Wait()
		_ = daemon.Process.Kill()
		_ = daemon.Wait()
	}()

	c := &Cluster{T: t, Name: "reap-unit", Nodes: []*Node{
		{Label: "node1", Pids: []int{daemon.Process.Pid}},
		{Label: "node2", Pids: []int{other.Process.Pid}},
	}}
	c.reapStragglers()

	// Wait (not signal 0: a killed-but-unwaited child is a zombie and
	// signal 0 reports zombies as alive).
	waitDone := make(chan error, 1)
	go func() { waitDone <- daemon.Wait() }()
	select {
	case <-waitDone:
	case <-time.After(5 * time.Second):
		t.Fatalf("straggler pid=%d still alive after sweep", daemon.Process.Pid)
	}
	if !processAlive(other.Process.Pid) {
		t.Fatalf("non-daemon pid=%d was killed by the sweep", other.Process.Pid)
	}
	// Unknown and foreign PIDs are skipped, never fatal.
	c2 := &Cluster{T: t, Name: "reap-unit2", Nodes: []*Node{
		{Label: "node1", Pids: []int{1, os.Getpid(), -5, 0}},
	}}
	c2.reapStragglers()
}
