package bridge

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/marcgauthier/murmur/backup"
	"github.com/marcgauthier/murmur/ids"
)

func TestDirPublisherRoundTrip(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	pub, err := NewDirPublisher(dir, Limits{}.withDefaults())
	if err != nil {
		t.Fatal(err)
	}
	a := Artifact{Name: "b1.spb", Data: []byte("payload-bytes")}
	if err := pub.Publish(ctx, a); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, a.Name))
	if err != nil {
		t.Fatalf("published file missing: %v", err)
	}
	if !bytes.Equal(raw, a.Data) {
		t.Fatal("published bytes drift")
	}
	// Invalid artifacts never touch the store.
	for _, bad := range []Artifact{
		{Name: "../evil", Data: []byte("x")},
		{Name: "empty.spb"},
	} {
		if err := pub.Publish(ctx, bad); err == nil {
			t.Fatalf("published invalid %q", bad.Name)
		}
	}
	if entries, err := os.ReadDir(dir); err != nil || len(entries) != 1 {
		t.Fatalf("dir = %v, %v", entries, err)
	}
}

func TestHTTPPublisher(t *testing.T) {
	ctx := context.Background()
	var mu sync.Mutex
	got := make(map[string][]byte)
	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			http.Error(w, "want PUT", http.StatusMethodNotAllowed)
			return
		}
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "read", http.StatusBadRequest)
			return
		}
		mu.Lock()
		got[strings.TrimPrefix(r.URL.Path, "/")] = raw
		gotAuth = r.Header.Get("Authorization")
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	pub, err := NewHTTPPublisher(backup.HTTPSOptions{BaseURL: srv.URL, AuthBearer: "tok"}, Limits{}.withDefaults())
	if err != nil {
		t.Fatal(err)
	}
	a := Artifact{Name: "b2.spb", Data: []byte("https-payload")}
	if err := pub.Publish(ctx, a); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if !bytes.Equal(got["b2.spb"], a.Data) {
		t.Fatalf("server got %v", got)
	}
	if gotAuth != "Bearer tok" {
		t.Fatalf("auth = %q", gotAuth)
	}
}

// fakeFTPServer is a minimal PASV upload-only server for adapter tests.
type fakeFTPServer struct {
	ln    net.Listener
	mu    sync.Mutex
	files map[string][]byte
	done  chan struct{}
}

func startFakeFTP(t *testing.T) (addr string, files func() map[string][]byte) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := &fakeFTPServer{ln: ln, files: map[string][]byte{}, done: make(chan struct{})}
	go s.serve()
	t.Cleanup(func() { close(s.done); ln.Close() })
	return ln.Addr().String(), func() map[string][]byte {
		s.mu.Lock()
		defer s.mu.Unlock()
		cp := make(map[string][]byte, len(s.files))
		for k, v := range s.files {
			cp[k] = v
		}
		return cp
	}
}

func (s *fakeFTPServer) serve() {
	for {
		select {
		case <-s.done:
			return
		default:
		}
		conn, err := s.ln.Accept()
		if err != nil {
			return
		}
		go s.handle(conn)
	}
}

func (s *fakeFTPServer) handle(conn net.Conn) {
	defer conn.Close()
	fmt.Fprintf(conn, "220 fake\r\n")
	var dataLn net.Listener
	rd := make([]byte, 4096)
	readLine := func() string {
		var line []byte
		for {
			n, err := conn.Read(rd)
			if err != nil || n == 0 {
				return string(line)
			}
			line = append(line, rd[:n]...)
			if i := bytes.Index(line, []byte("\r\n")); i >= 0 {
				return string(line[:i])
			}
		}
	}
	for {
		line := readLine()
		if line == "" {
			return
		}
		parts := strings.SplitN(line, " ", 2)
		cmd := strings.ToUpper(parts[0])
		arg := ""
		if len(parts) > 1 {
			arg = parts[1]
		}
		switch cmd {
		case "USER":
			fmt.Fprintf(conn, "331 ok\r\n")
		case "PASS":
			fmt.Fprintf(conn, "230 ok\r\n")
		case "TYPE":
			fmt.Fprintf(conn, "200 ok\r\n")
		case "CWD", "MKD":
			fmt.Fprintf(conn, "250 ok\r\n")
		case "PASV":
			if dataLn != nil {
				dataLn.Close()
			}
			var err error
			dataLn, err = net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				fmt.Fprintf(conn, "425 no data\r\n")
				continue
			}
			port := dataLn.Addr().(*net.TCPAddr).Port
			fmt.Fprintf(conn, "227 ok (127,0,0,1,%d,%d)\r\n", port>>8, port&0xff)
		case "STOR":
			if dataLn == nil {
				fmt.Fprintf(conn, "425 no data\r\n")
				continue
			}
			fmt.Fprintf(conn, "150 go\r\n")
			dconn, err := dataLn.Accept()
			if err != nil {
				fmt.Fprintf(conn, "426 fail\r\n")
				continue
			}
			raw, _ := io.ReadAll(dconn)
			dconn.Close()
			dataLn.Close()
			dataLn = nil
			s.mu.Lock()
			s.files[arg] = raw
			s.mu.Unlock()
			fmt.Fprintf(conn, "226 done\r\n")
		case "QUIT":
			fmt.Fprintf(conn, "221 bye\r\n")
			return
		default:
			fmt.Fprintf(conn, "502 unknown\r\n")
		}
	}
}

func TestFTPPublisher(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	addr, files := startFakeFTP(t)
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatal(err)
	}
	var portNum int
	if _, err := fmt.Sscanf(port, "%d", &portNum); err != nil {
		t.Fatal(err)
	}
	opt := backup.FTPOptions{Host: host, Port: portNum, Username: "u", Password: "p", Timeout: 10 * time.Second}
	pub, err := NewFTPPublisher(opt, Limits{}.withDefaults())
	if err != nil {
		t.Fatal(err)
	}
	a := Artifact{Name: "b3.spb", Data: []byte("ftp-payload")}
	if err := pub.Publish(ctx, a); err != nil {
		t.Fatal(err)
	}
	got := files()
	if !bytes.Equal(got["b3.spb"], a.Data) {
		t.Fatalf("server got %v", got)
	}
}

type flakyPublisher struct {
	mu      sync.Mutex
	fails   int
	calls   int
	last    Artifact
	succeed bool
}

func (p *flakyPublisher) Publish(_ context.Context, a Artifact) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls++
	p.last = a
	if p.calls <= p.fails {
		return fmt.Errorf("boom %d", p.calls)
	}
	p.succeed = true
	return nil
}

func TestPublishWithRetry(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	a := Artifact{Name: "r.spb", Data: []byte("x")}
	flaky := &flakyPublisher{fails: 2}
	policy := RetryPolicy{MaxAttempts: 5, BaseDelay: time.Millisecond, MaxDelay: 5 * time.Millisecond}
	if err := PublishWithRetry(ctx, flaky, a, policy); err != nil {
		t.Fatal(err)
	}
	if flaky.calls != 3 || !flaky.succeed {
		t.Fatalf("calls = %d", flaky.calls)
	}
	doomed := &flakyPublisher{fails: 100}
	if err := PublishWithRetry(ctx, doomed, a, policy); err == nil {
		t.Fatal("exhausted retry succeeded")
	}
	if doomed.calls != 5 {
		t.Fatalf("calls = %d, want 5", doomed.calls)
	}
}

func testExporter(t *testing.T) *Exporter {
	t.Helper()
	lowDB := stubDB{ids.NewDBID()}
	exp, err := NewExporter(lowDB, Config{Role: RoleLowExporter, Domain: lowDB.id, Stream: "s"}, &stubPublisher{})
	if err != nil {
		t.Fatal(err)
	}
	return exp
}

func appendEvents(t *testing.T, o *Outbox, origin ids.NodeID, epoch uint64, hash [32]byte, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		b := testBatch()
		if _, err := o.Append(b, origin, uint64(i+1), epoch, hash); err != nil {
			t.Fatal(err)
		}
	}
}

func TestPublishPending(t *testing.T) {
	ctx := context.Background()
	exp := testExporter(t)
	origin := ids.NewNodeID()
	o, err := OpenOutbox(t.TempDir(), Limits{}.withDefaults())
	if err != nil {
		t.Fatal(err)
	}
	// Two runs: schema A x2 then schema B x1 (split by schema identity).
	appendEvents(t, o, origin, 1, [32]byte{0x1}, 2)
	appendEvents(t, o, origin, 2, [32]byte{0x2}, 1)

	var sealed [][2]uint64
	sealer := func(batches []Batch, first, last uint64, _ uint64, _ [32]byte) (Artifact, error) {
		sealed = append(sealed, [2]uint64{first, last})
		if len(batches) != int(last-first+1) {
			t.Fatalf("run [%d..%d] has %d batches", first, last, len(batches))
		}
		return Artifact{Name: "ok.spb", Data: []byte("bundle")}, nil
	}
	pub := &flakyPublisher{}
	policy := RetryPolicy{MaxAttempts: 1}
	n, err := PublishPending(ctx, exp, o, sealer, pub, policy)
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 || len(sealed) != 2 || sealed[0] != [2]uint64{1, 2} || sealed[1] != [2]uint64{3, 3} {
		t.Fatalf("bundles = %d %v", n, sealed)
	}
	if got := o.Pending(0); len(got) != 0 {
		t.Fatalf("pending = %d", len(got))
	}

	// Poison run: sealer failure quarantines its events and continues.
	o2, err := OpenOutbox(t.TempDir(), Limits{}.withDefaults())
	if err != nil {
		t.Fatal(err)
	}
	appendEvents(t, o2, origin, 1, [32]byte{0x1}, 1)
	appendEvents(t, o2, origin, 9, [32]byte{0x9}, 1)
	poison := func(batches []Batch, first, last uint64, _ uint64, _ [32]byte) (Artifact, error) {
		if first == 1 {
			return Artifact{}, fmt.Errorf("poison")
		}
		return Artifact{Name: "ok.spb", Data: []byte("b")}, nil
	}
	n, err = PublishPending(ctx, exp, o2, poison, &flakyPublisher{}, policy)
	if err != nil || n != 1 {
		t.Fatalf("poison drain = %d, %v", n, err)
	}
	pending := o2.Pending(0)
	if len(pending) != 1 || pending[0].Seq != 1 || pending[0].State != EventFailed {
		t.Fatalf("pending = %+v", pending)
	}

	// Transport failure stops the drain with events still queued.
	o3, err := OpenOutbox(t.TempDir(), Limits{}.withDefaults())
	if err != nil {
		t.Fatal(err)
	}
	appendEvents(t, o3, origin, 1, [32]byte{0x1}, 2)
	doomed := &flakyPublisher{fails: 100}
	n, err = PublishPending(ctx, exp, o3, sealer, doomed, policy)
	if err == nil || n != 0 {
		t.Fatalf("doomed drain = %d, %v", n, err)
	}
	if got := o3.Pending(0); len(got) != 2 {
		t.Fatalf("pending = %d", len(got))
	}
}

func TestBundleSealerNames(t *testing.T) {
	exp := testExporter(t)
	signer, err := GenerateSigningKey()
	if err != nil {
		t.Fatal(err)
	}
	recip, err := GenerateRecipientKey()
	if err != nil {
		t.Fatal(err)
	}
	seal := NewBundleSealer(exp, signer, recip.Public())
	b := testBatch()
	b.Sequence = 7 // PublishPending rewrites origin seqs to export numbering
	a, err := seal([]Batch{b}, 7, 7, 4, [32]byte{0x4})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(a.Name, "bundle-s-") || !strings.HasSuffix(a.Name, ".spb") {
		t.Fatalf("name = %q", a.Name)
	}
	// The artifact opens with matching trust.
	trust := NewTrustStore()
	if err := trust.AddSigner(signer.ID, "s"); err != nil {
		t.Fatal(err)
	}
	if err := trust.AddRecipient(recip); err != nil {
		t.Fatal(err)
	}
	opened, err := OpenBundle(a.Data, trust, Limits{}.withDefaults())
	if err != nil {
		t.Fatal(err)
	}
	if opened.Manifest.SeqFirst != 7 || opened.Manifest.Stream != "s" {
		t.Fatalf("manifest = %+v", opened.Manifest)
	}
	if len(opened.Batches) != 1 || opened.Batches[0].Sequence != 7 {
		t.Fatal("export seq not carried")
	}
}
