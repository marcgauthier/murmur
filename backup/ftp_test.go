package backup

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

// ftpScript programs the fake FTP server's responses. Zero values select the
// happy path; fields script individual failure modes.
type ftpScript struct {
	banner      string // default "220 fake ftp ready"
	userCode    int    // default 331 (230 = no password needed)
	passCode    int    // default 230
	typeFail    bool   // TYPE -> 500
	cwdFailures int    // first N CWDs -> 550 (then 250)
	cwdAlways   bool   // every CWD -> 550
	pasv        string // "ok" (default), "err", "malformed", "noparens"
	storCode    int    // default 150
	retrFinal   string // default "226 Transfer complete"
	tls         bool   // offer AUTH TLS with a self-signed cert
}

type fakeFTP struct {
	t      *testing.T
	ln     net.Listener
	script ftpScript
	cert   tls.Certificate

	mu    sync.Mutex
	files map[string][]byte

	wg   sync.WaitGroup
	done chan struct{}
}

func startFakeFTP(t *testing.T, script ftpScript) *fakeFTP {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f := &fakeFTP{
		t:      t,
		ln:     ln,
		script: script,
		files:  make(map[string][]byte),
		done:   make(chan struct{}),
	}
	if script.tls {
		f.cert = selfSignedCert(t)
	}
	f.wg.Add(1)
	go f.acceptLoop()
	t.Cleanup(func() {
		close(f.done)
		_ = ln.Close()
		f.wg.Wait()
	})
	return f
}

func selfSignedCert(t *testing.T) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "fake-ftp"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := tls.X509KeyPair(
		pemEncode("CERTIFICATE", der),
		pemEncode("EC PRIVATE KEY", keyDER),
	)
	if err != nil {
		t.Fatal(err)
	}
	return cert
}

func pemEncode(blockType string, der []byte) []byte {
	return pem.EncodeToMemory(&pem.Block{Type: blockType, Bytes: der})
}

func (f *fakeFTP) acceptLoop() {
	defer f.wg.Done()
	for {
		conn, err := f.ln.Accept()
		if err != nil {
			select {
			case <-f.done:
				return
			default:
				f.t.Logf("fake ftp accept: %v", err)
				return
			}
		}
		f.wg.Add(1)
		go func() {
			defer f.wg.Done()
			f.serve(conn)
		}()
	}
}

func (f *fakeFTP) serve(conn net.Conn) {
	defer conn.Close()
	script := f.script // per-connection copy: failure counters mutate
	_ = conn.SetDeadline(time.Now().Add(30 * time.Second))
	rd := bufio.NewReader(conn)
	wr := bufio.NewWriter(conn)
	send := func(format string, args ...any) {
		_, _ = fmt.Fprintf(wr, format+"\r\n", args...)
		_ = wr.Flush()
	}
	banner := script.banner
	if banner == "" {
		banner = "220 fake ftp ready"
	}
	send("%s", banner)

	var dataLn net.Listener
	var dataCh chan net.Conn
	closeData := func() {
		if dataLn != nil {
			_ = dataLn.Close()
			dataLn = nil
		}
		dataCh = nil
	}
	defer closeData()
	var tlsOn bool

	// acceptData takes the background-accepted data connection. The accept
	// runs ahead of the transfer command because FTPS clients complete
	// the data TLS handshake before sending STOR/RETR.
	acceptData := func() net.Conn {
		if dataCh == nil {
			return nil
		}
		ch := dataCh
		dataCh = nil
		select {
		case dc := <-ch:
			return dc
		case <-time.After(10 * time.Second):
			return nil
		}
	}
	armData := func(pln net.Listener) {
		closeData()
		dataLn = pln
		ch := make(chan net.Conn, 1)
		dataCh = ch
		go func() {
			_ = pln.(*net.TCPListener).SetDeadline(time.Now().Add(10 * time.Second))
			dc, err := pln.Accept()
			if err != nil {
				return
			}
			if tlsOn {
				tlsConn := tls.Server(dc, &tls.Config{Certificates: []tls.Certificate{f.cert}})
				if err := tlsConn.Handshake(); err != nil {
					_ = dc.Close()
					return
				}
				ch <- tlsConn
				return
			}
			ch <- dc
		}()
	}

	for {
		line, err := rd.ReadString('\n')
		if err != nil {
			return
		}
		cmd, arg, _ := strings.Cut(strings.TrimSpace(line), " ")
		cmd = strings.ToUpper(cmd)
		arg = strings.TrimSpace(arg)
		switch cmd {
		case "USER":
			code := script.userCode
			if code == 0 {
				code = 331
			}
			if code == 331 {
				send("331 Need password")
			} else if code == 230 {
				send("230 Logged in")
			} else {
				send("%d No such user", code)
			}
		case "PASS":
			code := script.passCode
			if code == 0 {
				code = 230
			}
			if code == 230 {
				send("230 Logged in")
			} else {
				send("%d Login failed", code)
			}
		case "AUTH":
			if script.tls && strings.ToUpper(arg) == "TLS" {
				send("234 AUTH TLS ok")
				tlsConn := tls.Server(conn, &tls.Config{Certificates: []tls.Certificate{f.cert}})
				if err := tlsConn.Handshake(); err != nil {
					return
				}
				conn = tlsConn
				_ = conn.SetDeadline(time.Now().Add(30 * time.Second))
				rd = bufio.NewReader(conn)
				wr = bufio.NewWriter(conn)
				tlsOn = true
			} else {
				send("500 AUTH not understood")
			}
		case "PBSZ", "PROT":
			send("200 OK")
		case "TYPE":
			if script.typeFail {
				send("500 TYPE failed")
			} else {
				send("200 Binary")
			}
		case "CWD":
			if script.cwdAlways || script.cwdFailures > 0 {
				if !f.script.cwdAlways {
					script.cwdFailures--
				}
				send("550 No such directory")
			} else {
				send("250 CWD ok")
			}
		case "MKD":
			send("257 Created")
		case "PASV":
			switch script.pasv {
			case "err":
				send("500 PASV failed")
			case "malformed":
				send("227 ok (1,2,3)")
			case "noparens":
				send("227 ok no parens here")
			default:
				pln, err := net.Listen("tcp", "127.0.0.1:0")
				if err != nil {
					send("425 Cannot open data connection")
					continue
				}
				armData(pln)
				port := pln.Addr().(*net.TCPAddr).Port
				send("227 Entering Passive Mode (127,0,0,1,%d,%d)", port>>8, port&0xff)
			}
		case "STOR":
			dc := acceptData()
			if dc == nil {
				send("425 No data connection")
				continue
			}
			code := script.storCode
			if code == 0 {
				code = 150
			}
			if code != 150 && code != 125 {
				_ = dc.Close()
				send("%d STOR rejected", code)
				continue
			}
			send("150 Opening data connection")
			payload, err := io.ReadAll(dc)
			_ = dc.Close()
			if err != nil {
				send("426 Transfer aborted")
				continue
			}
			f.mu.Lock()
			f.files[arg] = payload
			f.mu.Unlock()
			send("226 Transfer complete")
		case "RETR":
			f.mu.Lock()
			payload, ok := f.files[arg]
			f.mu.Unlock()
			if !ok {
				send("550 File not found")
				if dc := acceptData(); dc != nil {
					_ = dc.Close()
				}
				continue
			}
			dc := acceptData()
			if dc == nil {
				send("425 No data connection")
				continue
			}
			send("150 Opening data connection")
			_, werr := dc.Write(payload)
			_ = dc.Close()
			if werr != nil {
				send("426 Transfer aborted")
				continue
			}
			final := script.retrFinal
			if final == "" {
				final = "226 Transfer complete"
			}
			send("%s", final)
		case "NLST":
			dc := acceptData()
			if dc == nil {
				send("425 No data connection")
				continue
			}
			send("150 Opening data connection")
			f.mu.Lock()
			for name := range f.files {
				_, _ = fmt.Fprintf(dc, "%s\r\n", name)
			}
			f.mu.Unlock()
			_ = dc.Close()
			send("226 Transfer complete")
		case "DELE":
			f.mu.Lock()
			_, ok := f.files[arg]
			if ok {
				delete(f.files, arg)
			}
			f.mu.Unlock()
			if !ok {
				send("550 File not found")
			} else {
				send("250 Deleted")
			}
		case "QUIT":
			send("221 Bye")
			return
		default:
			send("500 Unknown command")
		}
	}
}

func (f *fakeFTP) addr() (host string, port int) {
	tcp := f.ln.Addr().(*net.TCPAddr)
	return "127.0.0.1", tcp.Port
}

func (f *fakeFTP) get(name string) ([]byte, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	b, ok := f.files[name]
	return b, ok
}

func ftpTestCtx(t *testing.T) (context.Context, context.CancelFunc) {
	t.Helper()
	return context.WithTimeout(context.Background(), 15*time.Second)
}

// TestFTPDestinationValidation pins constructor validation and defaults.
func TestFTPDestinationValidation(t *testing.T) {
	if _, err := NewFTPDestination(FTPOptions{}); err == nil {
		t.Fatal("empty host accepted")
	}
	d, err := NewFTPDestination(FTPOptions{Host: "example.com"})
	if err != nil {
		t.Fatal(err)
	}
	if d.Type() != "ftp" {
		t.Fatalf("Type = %q", d.Type())
	}
	if d.opt.Port != 21 || d.opt.Timeout == 0 {
		t.Fatalf("defaults = port %d timeout %v", d.opt.Port, d.opt.Timeout)
	}
}

// TestFTPWriteReadListDeleteRoundTrip stores, retrieves, lists, and deletes
// through the fake server, including the CWD/MKD retry path.
func TestFTPWriteReadListDeleteRoundTrip(t *testing.T) {
	ctx, cancel := ftpTestCtx(t)
	defer cancel()
	srv := startFakeFTP(t, ftpScript{cwdFailures: 1})
	host, port := srv.addr()
	d, err := NewFTPDestination(FTPOptions{
		Host: host, Port: port,
		Username: "u", Password: "p",
		RemoteDir: "/backups",
	})
	if err != nil {
		t.Fatal(err)
	}
	payload := "backup-bytes-0123456789"
	if err := d.WriteBackup(ctx, "db1-a.tar.gz", strings.NewReader(payload), int64(len(payload))); err != nil {
		t.Fatal(err)
	}
	if got, ok := srv.get("db1-a.tar.gz"); !ok || string(got) != payload {
		t.Fatalf("stored = %q/%v", got, ok)
	}
	rc, err := d.ReadBackup(ctx, "db1-a.tar.gz")
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	if err := rc.Close(); err != nil {
		t.Fatal(err)
	}
	if string(got) != payload {
		t.Fatalf("retrieved = %q", got)
	}
	// Double close is a no-op.
	if err := rc.Close(); err != nil {
		t.Fatalf("second Close = %v", err)
	}
	if err := d.WriteBackup(ctx, "db2-b.tar.gz", strings.NewReader("x"), 1); err != nil {
		t.Fatal(err)
	}
	srv.mu.Lock()
	srv.files["notes.txt"] = []byte("not a backup")
	srv.mu.Unlock()
	items, err := d.ListBackups(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 2 {
		t.Fatalf("unfiltered list = %d items, want 2 (.tar.gz only)", len(items))
	}
	items, err = d.ListBackups(ctx, "db1")
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || items[0].Name != "db1-a.tar.gz" {
		t.Fatalf("filtered list = %+v", items)
	}
	if err := d.DeleteBackup(ctx, "db1-a.tar.gz"); err != nil {
		t.Fatal(err)
	}
	if _, ok := srv.get("db1-a.tar.gz"); ok {
		t.Fatal("deleted file still present")
	}
	if err := d.DeleteBackup(ctx, "db1-a.tar.gz"); err == nil {
		t.Fatal("double delete succeeded")
	}
}

// TestFTPLoginVariants covers passwordless login and both authentication
// failure branches.
func TestFTPLoginVariants(t *testing.T) {
	ctx, cancel := ftpTestCtx(t)
	defer cancel()
	mk := func(t *testing.T, script ftpScript, user, pass string) *FTPDestination {
		t.Helper()
		srv := startFakeFTP(t, script)
		host, port := srv.addr()
		d, err := NewFTPDestination(FTPOptions{Host: host, Port: port, Username: user, Password: pass})
		if err != nil {
			t.Fatal(err)
		}
		return d
	}
	t.Run("no-password", func(t *testing.T) {
		d := mk(t, ftpScript{userCode: 230}, "instant", "")
		if err := d.DeleteBackup(ctx, "whatever"); err == nil {
			t.Fatal("expected DELE error for missing file, got nil")
		} else if !strings.Contains(err.Error(), "ftp DELE") {
			t.Fatalf("err = %v, want DELE failure (login itself must pass)", err)
		}
	})
	t.Run("bad-user", func(t *testing.T) {
		d := mk(t, ftpScript{userCode: 550}, "nouser", "p")
		if err := d.DeleteBackup(ctx, "x"); !errors.Is(err, ErrUnauthenticated) {
			t.Fatalf("err = %v, want unauthenticated", err)
		}
	})
	t.Run("bad-password", func(t *testing.T) {
		d := mk(t, ftpScript{passCode: 530}, "u", "wrong")
		if err := d.DeleteBackup(ctx, "x"); !errors.Is(err, ErrUnauthenticated) {
			t.Fatalf("err = %v, want unauthenticated", err)
		}
	})
	t.Run("anonymous-default", func(t *testing.T) {
		d := mk(t, ftpScript{}, "", "")
		if err := d.DeleteBackup(ctx, "missing"); err == nil {
			t.Fatal("expected DELE error, got nil")
		}
	})
}

// TestFTPDialFailures covers transport and handshake failures before login.
func TestFTPDialFailures(t *testing.T) {
	ctx, cancel := ftpTestCtx(t)
	defer cancel()
	t.Run("refused", func(t *testing.T) {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		port := ln.Addr().(*net.TCPAddr).Port
		_ = ln.Close()
		d, err := NewFTPDestination(FTPOptions{Host: "127.0.0.1", Port: port})
		if err != nil {
			t.Fatal(err)
		}
		if err := d.WriteBackup(ctx, "x", strings.NewReader("x"), 1); err == nil {
			t.Fatal("dial to closed port succeeded")
		}
	})
	t.Run("bad-banner", func(t *testing.T) {
		srv := startFakeFTP(t, ftpScript{banner: "500 Nope"})
		host, port := srv.addr()
		d, err := NewFTPDestination(FTPOptions{Host: host, Port: port})
		if err != nil {
			t.Fatal(err)
		}
		if err := d.DeleteBackup(ctx, "x"); err == nil {
			t.Fatal("bad banner accepted")
		}
	})
	t.Run("type-fails", func(t *testing.T) {
		srv := startFakeFTP(t, ftpScript{typeFail: true})
		host, port := srv.addr()
		d, err := NewFTPDestination(FTPOptions{Host: host, Port: port})
		if err != nil {
			t.Fatal(err)
		}
		if err := d.DeleteBackup(ctx, "x"); err == nil || !strings.Contains(err.Error(), "type I") {
			t.Fatalf("err = %v, want TYPE failure", err)
		}
	})
	t.Run("cwd-persistent", func(t *testing.T) {
		srv := startFakeFTP(t, ftpScript{cwdAlways: true})
		host, port := srv.addr()
		d, err := NewFTPDestination(FTPOptions{Host: host, Port: port, RemoteDir: "/nope"})
		if err != nil {
			t.Fatal(err)
		}
		if err := d.DeleteBackup(ctx, "x"); err == nil || !strings.Contains(err.Error(), "cwd") {
			t.Fatalf("err = %v, want CWD failure", err)
		}
	})
}

// TestFTPPasvFailures covers data-connection setup errors.
func TestFTPPasvFailures(t *testing.T) {
	ctx, cancel := ftpTestCtx(t)
	defer cancel()
	for _, tc := range []struct {
		name string
		pasv string
		want string
	}{
		{"rejected", "err", "PASV"},
		{"malformed", "malformed", "PASV fields"},
		{"no-parens", "noparens", "PASV response"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := startFakeFTP(t, ftpScript{pasv: tc.pasv})
			host, port := srv.addr()
			d, err := NewFTPDestination(FTPOptions{Host: host, Port: port})
			if err != nil {
				t.Fatal(err)
			}
			err = d.WriteBackup(ctx, "x", strings.NewReader("x"), 1)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want %q", err, tc.want)
			}
		})
	}
}

// TestFTPTransferFailures covers rejected transfers, missing files, and a
// bad final acknowledgment on RETR close.
func TestFTPTransferFailures(t *testing.T) {
	ctx, cancel := ftpTestCtx(t)
	defer cancel()
	mk := func(t *testing.T, script ftpScript) *FTPDestination {
		t.Helper()
		srv := startFakeFTP(t, script)
		host, port := srv.addr()
		d, err := NewFTPDestination(FTPOptions{Host: host, Port: port})
		if err != nil {
			t.Fatal(err)
		}
		return d
	}
	t.Run("stor-rejected", func(t *testing.T) {
		d := mk(t, ftpScript{storCode: 550})
		if err := d.WriteBackup(ctx, "x", strings.NewReader("x"), 1); err == nil {
			t.Fatal("rejected STOR succeeded")
		}
	})
	t.Run("retr-missing", func(t *testing.T) {
		d := mk(t, ftpScript{})
		if _, err := d.ReadBackup(ctx, "missing.tar.gz"); !errors.Is(err, ErrDestinationNotFound) {
			t.Fatalf("err = %v, want not-found", err)
		}
	})
	t.Run("retr-bad-final", func(t *testing.T) {
		srv := startFakeFTP(t, ftpScript{retrFinal: "250 odd but confusing"})
		host, port := srv.addr()
		d, err := NewFTPDestination(FTPOptions{Host: host, Port: port})
		if err != nil {
			t.Fatal(err)
		}
		srv.mu.Lock()
		srv.files["f.tar.gz"] = []byte("data")
		srv.mu.Unlock()
		rc, err := d.ReadBackup(ctx, "f.tar.gz")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := io.ReadAll(rc); err != nil {
			t.Fatal(err)
		}
		if err := rc.Close(); err == nil {
			t.Fatal("bad final ack accepted")
		}
	})
}

type errReader struct{ err error }

func (r errReader) Read([]byte) (int, error) { return 0, r.err }

type slowReader struct {
	remaining int
	chunk     []byte
}

func (r *slowReader) Read(p []byte) (int, error) {
	if r.remaining <= 0 {
		return 0, io.EOF
	}
	time.Sleep(5 * time.Millisecond)
	r.remaining--
	return copy(p, r.chunk), nil
}

// TestFTPWriteStreamErrors covers reader failures and mid-transfer
// cancellation of uploads.
func TestFTPWriteStreamErrors(t *testing.T) {
	ctx, cancel := ftpTestCtx(t)
	defer cancel()
	mk := func(t *testing.T) *FTPDestination {
		t.Helper()
		srv := startFakeFTP(t, ftpScript{})
		host, port := srv.addr()
		d, err := NewFTPDestination(FTPOptions{Host: host, Port: port})
		if err != nil {
			t.Fatal(err)
		}
		return d
	}
	t.Run("reader-error", func(t *testing.T) {
		d := mk(t)
		err := d.WriteBackup(ctx, "x", errReader{errors.New("boom")}, 0)
		if err == nil || !strings.Contains(err.Error(), "stream read") {
			t.Fatalf("err = %v, want stream-read failure", err)
		}
	})
	t.Run("cancelled", func(t *testing.T) {
		d := mk(t)
		cctx, ccancel := context.WithCancel(ctx)
		defer ccancel()
		resCh := make(chan error, 1)
		go func() {
			resCh <- d.WriteBackup(cctx, "x", &slowReader{remaining: 500, chunk: make([]byte, 1024)}, 0)
		}()
		time.Sleep(100 * time.Millisecond)
		ccancel()
		select {
		case err := <-resCh:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("err = %v, want context.Canceled", err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("cancelled upload did not return")
		}
	})
}

// TestFTPTLSRoundTrip exercises AUTH TLS plus TLS data connections.
func TestFTPTLSRoundTrip(t *testing.T) {
	ctx, cancel := ftpTestCtx(t)
	defer cancel()
	srv := startFakeFTP(t, ftpScript{tls: true})
	host, port := srv.addr()
	d, err := NewFTPDestination(FTPOptions{
		Host: host, Port: port,
		Username: "u", Password: "p",
		TLSConfig: &tls.Config{InsecureSkipVerify: true}, //nolint:gosec // test-only fake server
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := d.WriteBackup(ctx, "tls.tar.gz", strings.NewReader("secret"), 6); err != nil {
		t.Fatal(err)
	}
	rc, err := d.ReadBackup(ctx, "tls.tar.gz")
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	if err := rc.Close(); err != nil {
		t.Fatal(err)
	}
	if string(got) != "secret" {
		t.Fatalf("retrieved = %q", got)
	}
}
