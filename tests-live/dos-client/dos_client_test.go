// DoS-client acceptance: while an attacker floods the victim's HTTPS API
// with half-open TLS handshakes, slow-loris headers, and certified trickle
// bodies — and sprays garbage plus half-open QUIC handshakes at replication
// — the node must keep serving its legit peer (writes converge both ways
// within latency bounds, fresh HTTPS requests stay fast) and attacker state
// must stay bounded (garbage creates no sessions, unauthenticated requests
// are rejected, goroutines recover after teardown).
package dosclient_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	db "github.com/marcgauthier/murmur"
	"github.com/marcgauthier/murmur/schema"
	"github.com/marcgauthier/murmur/tests-live/harness"
	"github.com/marcgauthier/murmur/transport"
)

var dosSchema = &db.SchemaConfig{Version: 1, Tables: []schema.TableSchema{{
	Name: "dos_rows",
	Columns: []schema.ColumnSchema{
		{Name: "id", Type: schema.ColBlob},
		{Name: "name", Type: schema.ColText, Nullable: true},
	},
}}}

func envSeconds(name string, def int) time.Duration {
	if v := harness.GetEnv(name); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return time.Duration(n) * time.Second
		}
	}
	return time.Duration(def) * time.Second
}

func envInt(name string, def int) int {
	if v := harness.GetEnv(name); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			return n
		}
	}
	return def
}

func TestDoSClientFlood(t *testing.T) {
	attackFor := envSeconds("MURMUR_DOS_CLIENT_ATTACK_SECONDS", 60)
	nTCPIdle := envInt("MURMUR_DOS_CLIENT_TCP_HALFOPEN", 48)
	nTCPPartial := envInt("MURMUR_DOS_CLIENT_TCP_PARTIAL", 16)
	nTLSIdle := envInt("MURMUR_DOS_CLIENT_TLS_IDLE", 16)
	nLoris := envInt("MURMUR_DOS_CLIENT_SLOW_LORIS", 8)
	nTrickle := envInt("MURMUR_DOS_CLIENT_TRICKLE", 4)
	nQUICHalf := envInt("MURMUR_DOS_CLIENT_QUIC_HALFOPEN", 24)

	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name:        "dos-client",
		NumNodes:    2,
		AwaitUnlock: true,
		Schema:      dosSchema,
	})
	victim, peer := 0, 1
	api, repl := cluster.Nodes[victim].APIAddr, cluster.Nodes[victim].ReplAddr

	// --- Positive control (pre-attack): both directions converge.
	if err := cluster.ExecSQL(peer, "INSERT INTO dos_rows (id, name) VALUES (?, ?)", fmt.Sprintf("%032x", 1), "honest-1"); err != nil {
		t.Fatalf("pre-attack insert: %v", err)
	}
	waitConverged(t, cluster, 1, 30*time.Second)
	if err := cluster.ExecSQL(victim, "INSERT INTO dos_rows (id, name) VALUES (?, ?)", fmt.Sprintf("%032x", 2), "honest-2"); err != nil {
		t.Fatalf("pre-attack insert on victim: %v", err)
	}
	waitConverged(t, cluster, 2, 30*time.Second)

	// Let the mesh settle so session counters are stable before baselining.
	waitSessionsStable(t, api, 15*time.Second)
	baseSessions := replSessions(t, api)
	baseRoutines := goroutineCount(t, api)
	t.Logf("baselines: sessions=%.0f goroutines=%d", baseSessions, baseRoutines)

	// --- Phase 1: garbage UDP is rejected cheaply (no sessions, no harm).
	garbageStop := make(chan struct{})
	var garbageWG sync.WaitGroup
	for g := 0; g < 2; g++ {
		garbageWG.Add(1)
		go func() {
			defer garbageWG.Done()
			conn, err := net.Dial("udp", repl)
			if err != nil {
				return
			}
			defer conn.Close()
			buf := make([]byte, 1200)
			ticker := time.NewTicker(10 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-garbageStop:
					return
				case <-ticker.C:
					rand.Read(buf)
					_, _ = conn.Write(buf[:100+rand.Intn(1100)])
				}
			}
		}()
	}
	for i := 0; i < 5; i++ {
		if err := cluster.ExecSQL(peer, "INSERT INTO dos_rows (id, name) VALUES (?, ?)", fmt.Sprintf("%032x", 100+i), "garbage-hb"); err != nil {
			close(garbageStop)
			garbageWG.Wait()
			t.Fatalf("heartbeat write during garbage: %v", err)
		}
		time.Sleep(2 * time.Second)
	}
	close(garbageStop)
	garbageWG.Wait()
	waitConverged(t, cluster, 7, 30*time.Second)
	if got := replSessions(t, api); got != baseSessions {
		t.Fatalf("garbage UDP moved sessions %.0f -> %.0f (junk created server state)", baseSessions, got)
	}
	t.Logf("garbage phase: sessions stable at %.0f, heartbeats converged", baseSessions)

	// --- Phase 2: protocol-floor rejections (cheap, synchronous).
	rawNoCert := func() string {
		conn, err := tls.Dial("tcp", api, &tls.Config{InsecureSkipVerify: true})
		if err != nil {
			t.Fatalf("no-cert TLS dial: %v", err)
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
		body := `{"query":"SELECT 1"}`
		fmt.Fprintf(conn, "POST /v1/exec HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\nContent-Length: %d\r\nConnection: close\r\n\r\n%s", len(body), body)
		raw, _ := io.ReadAll(conn)
		return string(raw)
	}
	if head := rawNoCert(); !strings.HasPrefix(head, "HTTP/1.1 401") {
		t.Fatalf("unauthenticated exec head = %.60q, want 401", head)
	}
	func() {
		conn, err := tls.Dial("tcp", api, &tls.Config{InsecureSkipVerify: true, MaxVersion: tls.VersionTLS10})
		if err == nil {
			defer conn.Close()
			err = conn.Handshake()
		}
		if err == nil {
			t.Fatal("TLS 1.0 handshake accepted, want rejection (TLS 1.2 floor)")
		}
		t.Logf("TLS 1.0 correctly refused: %v", err)
	}()

	// --- Phase 3: sustained multi-vector flood.
	atk := &flood{
		t:          t,
		api:        api,
		repl:       repl,
		expect:     cluster.Nodes[victim].NodeID,
		caPEM:      cluster.CA.CertPEM,
		tlsDir:     cluster.Nodes[victim].TLSDir,
		stopCh:     make(chan struct{}),
		tcpIdle:    nTCPIdle,
		tcpPartial: nTCPPartial,
		tlsIdle:    nTLSIdle,
		loris:      nLoris,
		trickle:    nTrickle,
		quicHalf:   nQUICHalf,
	}
	atk.start()
	t.Logf("flood running for %v (tcp=%d/%d tls=%d loris=%d trickle=%d quic=%d)",
		attackFor, nTCPIdle, nTCPPartial, nTLSIdle, nLoris, nTrickle, nQUICHalf)
	// Confirm the flood is actually holding server state before asserting
	// availability under it (non-vacuous: the victim must feel pressure).
	waitGoroutinesAbove(t, api, baseRoutines+20, 30*time.Second)
	heldRoutines := goroutineCount(t, api)
	t.Logf("victim under pressure: goroutines %d -> %d (attacker held %d/%d conns)",
		baseRoutines, heldRoutines, atk.held.Load(), atk.attempted.Load())
	if held, attempted := atk.held.Load(), atk.attempted.Load(); attempted == 0 || held*2 < attempted {
		atk.stop()
		t.Fatalf("flood too weak to prove anything: held %d/%d attacker conns", held, attempted)
	}

	deadline := time.Now().Add(attackFor)
	// Legit HTTPS on the victim stays fast during the flood.
	for i := 0; i < 5 && time.Now().Before(deadline); i++ {
		start := time.Now()
		cluster.WaitNodeReady(victim)
		if dt := time.Since(start); dt > 5*time.Second {
			atk.stop()
			t.Fatalf("healthz on flooded victim took %v, want < 5s", dt)
		}
		if _, err := cluster.QueryRowCount(victim, "dos_rows"); err != nil {
			atk.stop()
			t.Fatalf("legit query on flooded victim: %v", err)
		}
		time.Sleep(2 * time.Second)
	}
	// Legit exec directly on the flooded victim succeeds fast.
	start := time.Now()
	if err := cluster.ExecSQL(victim, "INSERT INTO dos_rows (id, name) VALUES (?, ?)", fmt.Sprintf("%032x", 200), "during-flood-victim"); err != nil {
		atk.stop()
		t.Fatalf("exec on flooded victim: %v", err)
	}
	if dt := time.Since(start); dt > 10*time.Second {
		atk.stop()
		t.Fatalf("exec on flooded victim took %v, want < 10s", dt)
	}
	// Attacker's half-opens must not have attached as replication sessions.
	if got := replSessions(t, api); got > baseSessions+2 {
		atk.stop()
		t.Fatalf("sessions grew %.0f -> %.0f during flood (attacker state attaching?)", baseSessions, got)
	}
	// The legit peer's write converges on the victim within the latency bound.
	if err := cluster.ExecSQL(peer, "INSERT INTO dos_rows (id, name) VALUES (?, ?)", fmt.Sprintf("%032x", 201), "during-flood-peer"); err != nil {
		atk.stop()
		t.Fatalf("peer write during flood: %v", err)
	}
	waitConvergedDeadline(t, cluster, 9, deadline, "peer write during flood")
	t.Logf("mid-flood convergence ok with %v of attack left", time.Until(deadline).Round(time.Second))
	// Hold the flood for the remainder so budgets face the full duration.
	if d := time.Until(deadline); d > 0 {
		time.Sleep(d)
	}
	atk.stop()

	// --- Phase 4: teardown recovery is bounded and complete.
	waitGoroutinesNear(t, api, baseRoutines+25, 90*time.Second)
	t.Logf("goroutines recovered to %d (baseline %d)", goroutineCount(t, api), baseRoutines)
	if got := replSessions(t, api); got > baseSessions+2 {
		t.Fatalf("post-attack sessions %.0f exceeds baseline %.0f + 2 (leaked attacker state)", got, baseSessions)
	}
	waitConnectedPeers(t, cluster, victim, 1, 30*time.Second)

	// --- Positive control (post-attack): fast convergence, equal digests.
	if err := cluster.ExecSQL(peer, "INSERT INTO dos_rows (id, name) VALUES (?, ?)", fmt.Sprintf("%032x", 202), "post-attack"); err != nil {
		t.Fatalf("post-attack insert: %v", err)
	}
	start = time.Now()
	waitConverged(t, cluster, 10, 15*time.Second)
	t.Logf("post-attack write converged in %v", time.Since(start).Round(100*time.Millisecond))
}

// flood holds one multi-vector DoS attack against a victim node.
type flood struct {
	t          *testing.T
	api        string
	repl       string
	expect     db.NodeID
	caPEM      []byte
	tlsDir     string
	stopCh     chan struct{}
	tcpIdle    int
	tcpPartial int
	tlsIdle    int
	loris      int
	trickle    int
	quicHalf   int
	wg         sync.WaitGroup
	once       sync.Once
	held       atomic.Int64
	attempted  atomic.Int64
}

func (f *flood) track(ok bool) {
	f.attempted.Add(1)
	if ok {
		f.held.Add(1)
	}
}

func (f *flood) halt() <-chan struct{} { return f.stopCh }

func (f *flood) stop() { f.once.Do(func() { close(f.stopCh); f.wg.Wait() }) }

func (f *flood) start() {
	// Raw TCP half-opens: connected, never speak TLS.
	for i := 0; i < f.tcpIdle; i++ {
		f.wg.Add(1)
		go func() {
			defer f.wg.Done()
			conn, err := net.Dial("tcp", f.api)
			f.track(err == nil)
			if err != nil {
				return
			}
			defer conn.Close()
			<-f.halt()
		}()
	}
	// Partial ClientHello: a few bytes, then silence.
	for i := 0; i < f.tcpPartial; i++ {
		f.wg.Add(1)
		go func() {
			defer f.wg.Done()
			conn, err := net.Dial("tcp", f.api)
			f.track(err == nil)
			if err != nil {
				return
			}
			defer conn.Close()
			_, _ = conn.Write([]byte{0x16, 0x03, 0x01, 0x00, 0x05})
			<-f.halt()
		}()
	}
	// Completed TLS handshakes (no client cert), then idle: no HTTP at all.
	for i := 0; i < f.tlsIdle; i++ {
		f.wg.Add(1)
		go func() {
			defer f.wg.Done()
			conn, err := tls.Dial("tcp", f.api, &tls.Config{InsecureSkipVerify: true})
			f.track(err == nil)
			if err != nil {
				return
			}
			defer conn.Close()
			<-f.halt()
		}()
	}
	// Slow-loris: TLS up, headers never complete, 1 byte per 2s.
	for i := 0; i < f.loris; i++ {
		f.wg.Add(1)
		go func(n int) {
			defer f.wg.Done()
			conn, err := tls.Dial("tcp", f.api, &tls.Config{InsecureSkipVerify: true})
			f.track(err == nil)
			if err != nil {
				return
			}
			defer conn.Close()
			fmt.Fprintf(conn, "POST /v1/exec HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\nContent-Length: 1048576\r\nX-Drip-%d: ", n)
			ticker := time.NewTicker(2 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-f.halt():
					return
				case <-ticker.C:
					_ = conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
					if _, err := conn.Write([]byte("a")); err != nil {
						return
					}
				}
			}
		}(i)
	}
	// Certified trickle bodies: stolen-credential attacker; complete headers
	// with a 1 GiB Content-Length, then 1 byte/s (handler stays in body read).
	certCfg := f.certifiedTLS()
	if certCfg != nil {
		for i := 0; i < f.trickle; i++ {
			f.wg.Add(1)
			go func() {
				defer f.wg.Done()
				conn, err := tls.Dial("tcp", f.api, certCfg)
				f.track(err == nil)
				if err != nil {
					return
				}
				defer conn.Close()
				fmt.Fprintf(conn, "POST /v1/exec HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\nContent-Length: 1073741824\r\nConnection: close\r\n\r\n{")
				ticker := time.NewTicker(time.Second)
				defer ticker.Stop()
				for {
					select {
					case <-f.halt():
						return
					case <-ticker.C:
						_ = conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
						if _, err := conn.Write([]byte(" ")); err != nil {
							return
						}
					}
				}
			}()
		}
	}
	// QUIC half-opens: valid-cert dials that open a stream and never send
	// Hello, held open; a top-up loop replaces conns the idle timeout reaps.
	creds := f.quicCreds()
	if creds != nil {
		var mu sync.Mutex
		var held []*transport.Session
		f.wg.Add(1)
		go func() {
			defer f.wg.Done()
			defer func() {
				mu.Lock()
				for _, s := range held {
					_ = s.Close()
				}
				mu.Unlock()
			}()
			ticker := time.NewTicker(5 * time.Second)
			defer ticker.Stop()
			for {
				mu.Lock()
				need := f.quicHalf - len(held)
				mu.Unlock()
				for i := 0; i < need; i++ {
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					sess, err := transport.Dial(ctx, f.repl, creds, f.expect)
					cancel()
					if err != nil {
						f.track(false)
						break
					}
					ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
					_, err = sess.OpenStream(ctx2)
					cancel2()
					if err != nil {
						_ = sess.Close()
						break
					}
					mu.Lock()
					held = append(held, sess)
					mu.Unlock()
					f.track(true)
				}
				select {
				case <-f.halt():
					return
				case <-ticker.C:
				}
			}
		}()
	}
	// Garbage UDP spray for the whole attack.
	for g := 0; g < 2; g++ {
		f.wg.Add(1)
		go func() {
			defer f.wg.Done()
			conn, err := net.Dial("udp", f.repl)
			if err != nil {
				return
			}
			defer conn.Close()
			buf := make([]byte, 1200)
			ticker := time.NewTicker(10 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-f.halt():
					return
				case <-ticker.C:
					rand.Read(buf)
					_, _ = conn.Write(buf[:100+rand.Intn(1100)])
				}
			}
		}()
	}
}

// certifiedTLS loads the victim's own node certificate to simulate a
// stolen-credential attacker. It returns nil (skipping that vector) only if
// the key material is unreadable.
func (f *flood) certifiedTLS() *tls.Config {
	cert, err := tls.LoadX509KeyPair(
		filepath.Join(f.tlsDir, "node.crt"),
		filepath.Join(f.tlsDir, "node.key"),
	)
	if err != nil {
		f.t.Logf("certified vector disabled: %v", err)
		return nil
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(f.caPEM) {
		f.t.Logf("certified vector disabled: bad CA")
		return nil
	}
	return &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots, Certificates: []tls.Certificate{cert}}
}

// quicCreds issues a fresh CA-signed identity for half-open QUIC dials.
func (f *flood) quicCreds() *transport.Credentials {
	// The cluster CA is not reachable here; reconstruct credentials from the
	// victim's own key material with a distinct in-memory identity is not
	// possible, so read the issued cert files directly: the QUIC handshake
	// only needs a CA-valid chain, and reusing the node identity is exactly
	// what a credential thief would present.
	certPEM, err := os.ReadFile(filepath.Join(f.tlsDir, "node.crt"))
	if err != nil {
		f.t.Logf("quic vector disabled: %v", err)
		return nil
	}
	keyPEM, err := os.ReadFile(filepath.Join(f.tlsDir, "node.key"))
	if err != nil {
		f.t.Logf("quic vector disabled: %v", err)
		return nil
	}
	creds, err := transport.CredentialsFromPEM(certPEM, keyPEM, f.caPEM, nil)
	if err != nil {
		f.t.Logf("quic vector disabled: %v", err)
		return nil
	}
	return creds
}

func waitConverged(t *testing.T, c *harness.Cluster, want int, timeout time.Duration) {
	t.Helper()
	waitConvergedDeadline(t, c, want, time.Now().Add(timeout), "converge")
}

func waitConvergedDeadline(t *testing.T, c *harness.Cluster, want int, deadline time.Time, what string) {
	t.Helper()
	for time.Now().Before(deadline) {
		ok := true
		var first string
		for i := range c.Nodes {
			n, err := c.QueryRowCount(i, "dos_rows")
			if err != nil || n != want {
				ok = false
				break
			}
			d, err := c.ComputeTableDigest(i, "dos_rows", "id")
			if err != nil {
				ok = false
				break
			}
			if i == 0 {
				first = d
			} else if d != first {
				ok = false
				break
			}
		}
		if ok {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("%s: nodes did not converge on %d dos_rows rows with equal digests in time", what, want)
}

func replSessions(t *testing.T, apiAddr string) float64 {
	t.Helper()
	resp, err := http.Get(fmt.Sprintf("https://%s/metrics", apiAddr))
	if err != nil {
		t.Fatalf("metrics: %v", err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range bytes.Split(raw, []byte("\n")) {
		if bytes.HasPrefix(line, []byte("spedsql_repl_sessions_opened_total")) {
			fields := bytes.Fields(line)
			v, err := strconv.ParseFloat(string(fields[len(fields)-1]), 64)
			if err != nil {
				t.Fatalf("parse sessions: %v", err)
			}
			return v
		}
	}
	t.Fatal("sessions counter missing from /metrics")
	return 0
}

func waitSessionsStable(t *testing.T, apiAddr string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	last := replSessions(t, apiAddr)
	stableSince := time.Now()
	for time.Now().Before(deadline) {
		time.Sleep(time.Second)
		cur := replSessions(t, apiAddr)
		if cur != last {
			last, stableSince = cur, time.Now()
			continue
		}
		if time.Since(stableSince) >= 5*time.Second {
			return
		}
	}
}

func goroutineCount(t *testing.T, apiAddr string) int {
	t.Helper()
	resp, err := http.Get(fmt.Sprintf("https://%s/v1/debug/stacks", apiAddr))
	if err != nil {
		t.Fatalf("stacks: %v", err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, line := range strings.Split(string(raw), "\n") {
		if strings.HasPrefix(line, "goroutine ") {
			n++
		}
	}
	return n
}

func waitGoroutinesAbove(t *testing.T, apiAddr string, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if got := goroutineCount(t, apiAddr); got >= want {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("victim goroutines never reached %d within %v (flood not holding?)", want, timeout)
}

func waitGoroutinesNear(t *testing.T, apiAddr string, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if got := goroutineCount(t, apiAddr); got <= want {
			return
		}
		time.Sleep(time.Second)
	}
	t.Fatalf("victim goroutines never fell to %d within %v (leaked attacker state?)", want, timeout)
}

func waitConnectedPeers(t *testing.T, c *harness.Cluster, idx, want int, timeout time.Duration) {
	t.Helper()
	url := fmt.Sprintf("https://%s/v1/status", c.Nodes[idx].APIAddr)
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := http.Get(url)
		if err == nil {
			var st struct {
				ConnectedPeers int `json:"connected_peers"`
			}
			raw, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if json.Unmarshal(raw, &st) == nil && st.ConnectedPeers == want {
				return
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("node %d connected_peers != %d within %v", idx, want, timeout)
}
