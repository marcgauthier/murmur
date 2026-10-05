// Fault-injection helpers for the endurance-chaos suite: tc/netem network
// impairment, libfaketime clock skew, disk-pressure filler files, metric
// scrapes, and key-config rewrites.
//
// Every helper here is safe to call from any goroutine: helpers return
// errors and never fail the test. Daemon lifecycle (Stop/Start/Unlock/Wait)
// stays on the test goroutine in the chaos director.
package endurancechaos_test

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/marcgauthier/murmur/tests-live/harness"
)

var faultHTTP = &http.Client{Timeout: 10 * time.Second}

// --- tc/netem impairment (loopback, replication ports only) ---
//
// Same scoping as the impaired-network suite: impairment attaches to lo as
// root qdisc 77: (prio) with the netem/tbf child on band 77:3; u32 filters
// steer only packets whose source OR destination port is a cluster
// replication port into the impaired band. API, metrics, and all other
// loopback traffic take the default band untouched.

func tcCapable() (bool, string) {
	if _, err := exec.LookPath("tc"); err != nil {
		return false, "tc binary not found"
	}
	out, err := exec.Command("tc", "qdisc", "show", "dev", "lo").CombinedOutput()
	if err != nil {
		return false, fmt.Sprintf("tc qdisc show failed: %v", strings.TrimSpace(string(out)))
	}
	show := string(out)
	if strings.Contains(show, "77:") {
		if out, err := exec.Command("tc", "qdisc", "del", "dev", "lo", "root").CombinedOutput(); err != nil {
			return false, fmt.Sprintf("cannot clear stale qdisc 77:: %v", strings.TrimSpace(string(out)))
		}
		return tcCapable()
	}
	if strings.Contains(show, "qdisc") && !strings.Contains(show, "noqueue") {
		return false, fmt.Sprintf("lo already has a root qdisc (%s); refusing to disturb shared box", strings.TrimSpace(show))
	}
	if out, err := exec.Command("tc", "qdisc", "add", "dev", "lo", "root", "handle", "77:", "prio").CombinedOutput(); err != nil {
		return false, fmt.Sprintf("tc qdisc add failed (need CAP_NET_ADMIN): %v", strings.TrimSpace(string(out)))
	}
	if out, err := exec.Command("tc", "qdisc", "del", "dev", "lo", "root").CombinedOutput(); err != nil {
		return false, fmt.Sprintf("tc probe cleanup failed: %v", strings.TrimSpace(string(out)))
	}
	return true, ""
}

func tcApply(replPorts []int, child string, args ...string) error {
	run := func(what string, argv ...string) error {
		if out, err := exec.Command("tc", argv...).CombinedOutput(); err != nil {
			return fmt.Errorf("tc %s: %v (%s)", what, err, strings.TrimSpace(string(out)))
		}
		return nil
	}
	if err := run("root", "qdisc", "replace", "dev", "lo", "root", "handle", "77:", "prio", "bands", "3"); err != nil {
		tcClear()
		return err
	}
	childArgs := append([]string{"qdisc", "replace", "dev", "lo", "parent", "77:3", "handle", "773:", child}, args...)
	if err := run("child", childArgs...); err != nil {
		tcClear()
		return err
	}
	for _, port := range replPorts {
		p := strconv.Itoa(port)
		if err := run("filter-dport", "filter", "add", "dev", "lo", "protocol", "ip", "parent", "77:0",
			"prio", "77", "u32", "match", "ip", "dport", p, "0xffff", "flowid", "77:3"); err != nil {
			tcClear()
			return err
		}
		if err := run("filter-sport", "filter", "add", "dev", "lo", "protocol", "ip", "parent", "77:0",
			"prio", "77", "u32", "match", "ip", "sport", p, "0xffff", "flowid", "77:3"); err != nil {
			tcClear()
			return err
		}
	}
	return nil
}

func tcClear() {
	out, err := exec.Command("tc", "qdisc", "show", "dev", "lo").CombinedOutput()
	if err != nil || !strings.Contains(string(out), "77:") {
		return
	}
	_ = exec.Command("tc", "qdisc", "del", "dev", "lo", "root").Run()
}

func clusterReplPorts(c *harness.Cluster) ([]int, error) {
	var ports []int
	for _, node := range c.Nodes {
		_, p, err := net.SplitHostPort(node.ReplAddr)
		if err != nil {
			return nil, fmt.Errorf("parse repl addr %q: %v", node.ReplAddr, err)
		}
		v, err := strconv.Atoi(p)
		if err != nil {
			return nil, fmt.Errorf("parse repl port %q: %v", p, err)
		}
		ports = append(ports, v)
	}
	return ports, nil
}

// --- libfaketime clock skew ---

func findFaketimeLib() string {
	if env := os.Getenv("FAKETIME_LIB"); env != "" {
		if _, err := os.Stat(env); err == nil {
			return env
		}
	}
	globs := []string{
		"/usr/lib/x86_64-linux-gnu/faketime/libfaketime.so*",
		"/usr/lib/aarch64-linux-gnu/faketime/libfaketime.so*",
		"/usr/lib64/faketime/libfaketime.so*",
		"/usr/lib/faketime/libfaketime.so*",
		"/usr/local/lib/faketime/libfaketime.so*",
		"/opt/*/lib/faketime/libfaketime.so*",
	}
	for _, g := range globs {
		if hits, _ := filepath.Glob(g); len(hits) > 0 {
			for _, h := range hits {
				if strings.HasSuffix(h, ".so.1") {
					return h
				}
			}
			return hits[0]
		}
	}
	if out, err := exec.Command("ldconfig", "-p").CombinedOutput(); err == nil {
		for _, line := range strings.Split(string(out), "\n") {
			if !strings.Contains(line, "libfaketime") {
				continue
			}
			fields := strings.Fields(line)
			candidate := fields[len(fields)-1]
			if _, err := os.Stat(candidate); err == nil {
				return candidate
			}
		}
	}
	return ""
}

func daemonIsStatic(bin string) bool {
	out, err := exec.Command("file", "-b", bin).CombinedOutput()
	if err != nil {
		return false
	}
	return strings.Contains(string(out), "statically linked")
}

func effectiveTestBinary() string {
	if override := harness.GetEnv("MURMUR_BIN"); override != "" {
		return override
	}
	wd, err := os.Getwd()
	if err != nil {
		return ""
	}
	for {
		if _, err := os.Stat(filepath.Join(wd, "go.mod")); err == nil {
			return filepath.Join(wd, "tests-live", "bin", "testnode")
		}
		parent := filepath.Dir(wd)
		if parent == wd {
			return ""
		}
		wd = parent
	}
}

// startNodeWithEnv mirrors Cluster.StartNode but launches the daemon with
// extra environment (libfaketime). Must run on the test goroutine: the node
// stays owned by the cluster and StopNode/Cleanup handle it normally.
func startNodeWithEnv(c *harness.Cluster, idx int, env []string) error {
	node := c.Nodes[idx]
	f, err := os.OpenFile(node.LogFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("open log file %s: %v", node.LogFile, err)
	}
	node.LogFileWriter = f
	bin := node.BinaryPath
	if bin == "" {
		bin = c.BinaryPath
	}
	cmd := exec.Command(bin, "agent", "--config", node.ConfigFile)
	cmd.Stdout = f
	cmd.Stderr = f
	cmd.Env = append(os.Environ(), env...)
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start node %s with env: %v", node.Label, err)
	}
	node.Process = cmd
	return nil
}

func nodeHLC(apiAddr string) (uint64, error) {
	resp, err := faultHTTP.Get("https://" + apiAddr + "/v1/status")
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	var st struct {
		HLC uint64 `json:"hlc"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&st); err != nil {
		return 0, err
	}
	return st.HLC, nil
}

func nodeConnectedPeers(apiAddr string) (int, error) {
	resp, err := faultHTTP.Get("https://" + apiAddr + "/v1/status")
	if err != nil {
		return -1, err
	}
	defer resp.Body.Close()
	var st struct {
		ConnectedPeers int `json:"connected_peers"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&st); err != nil {
		return -1, err
	}
	return st.ConnectedPeers, nil
}

// --- disk pressure ---

func dirFreeBytes(path string) (uint64, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return 0, err
	}
	return uint64(st.Bavail) * uint64(st.Bsize), nil
}

// writeFiller consumes mb megabytes at path with incompressible-ish bytes
// (a counter stream compresses poorly enough at this scale to hold the
// space against sparse-file optimization).
func writeFiller(path string, mb int) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	buf := make([]byte, 1<<20)
	var x uint64 = 0x9e3779b97f4a7c15
	for m := 0; m < mb; m++ {
		for i := 0; i < len(buf); i += 8 {
			x ^= x << 13
			x ^= x >> 7
			x ^= x << 17
			v := x
			for b := 0; b < 8 && i+b < len(buf); b++ {
				buf[i+b] = byte(v)
				v >>= 8
			}
		}
		if _, err := f.Write(buf); err != nil {
			_ = os.Remove(path)
			return err
		}
	}
	return f.Sync()
}

func removeFiller(path string) {
	_ = os.Remove(path)
}

// --- metrics ---

func scrapeMetrics(apiAddr string) (string, error) {
	resp, err := faultHTTP.Get("https://" + apiAddr + "/metrics")
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", err
	}
	return string(raw), nil
}

// metricInt parses a counter that may appear bare (`name value`) or labeled
// (`name{...} value`). It reports false when the series is absent.
func metricInt(body, name string) (int64, bool) {
	for _, line := range strings.Split(body, "\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 {
			continue
		}
		head := fields[0]
		if head != name && !strings.HasPrefix(head, name+"{") {
			continue
		}
		if v, err := strconv.ParseInt(fields[1], 10, 64); err == nil {
			return v, true
		}
		if f, err := strconv.ParseFloat(fields[1], 64); err == nil {
			return int64(f), true
		}
	}
	return 0, false
}

func nodeMetric(apiAddr, name string) (int64, error) {
	body, err := scrapeMetrics(apiAddr)
	if err != nil {
		return 0, err
	}
	v, ok := metricInt(body, name)
	if !ok {
		return 0, fmt.Errorf("counter %s not present in /metrics", name)
	}
	return v, nil
}

// --- key rotation bookkeeping ---

// rewriteKeyConfig points a node's config at rotated key material so the
// next restart unlocks with the new key instead of the retired one.
func rewriteKeyConfig(path, keyID, keyHex string) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var cfg map[string]any
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return err
	}
	cfg["key_id"] = keyID
	cfg["key_hex"] = keyHex
	out, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, out, 0644)
}

// sleepInterruptible sleeps d, waking early when stop closes. It reports
// false when interrupted.
func sleepInterruptible(d time.Duration, stop <-chan struct{}) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-stop:
		return false
	case <-t.C:
		return true
	}
}
