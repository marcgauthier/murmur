package main

import (
	"bufio"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"strings"
	"sync"
	"time"
)

// Local certificate authority and server certificate are generated per run.
func serverTLS() (*tls.Config, []byte, error) {
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, nil, err
	}
	now := time.Now()
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "Example transport CA"}, NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	caDER, err := x509.CreateCertificate(rand.Reader, ca, ca, public, private)
	if err != nil {
		return nil, nil, err
	}
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, nil, err
	}
	leaf := &x509.Certificate{SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "localhost"}, DNSNames: []string{"localhost"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, leaf, ca, pub, private)
	if err != nil {
		return nil, nil, err
	}
	return &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{{Certificate: [][]byte{der, caDER}, PrivateKey: priv}}}, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}), nil
}

// This receiver implements only the FTP commands needed by PublishFTP, on
// loopback, for one upload. It is an example fixture, not a deployable FTP server.
type ftpReceiver struct {
	listener  net.Listener
	mu        sync.Mutex
	sockets   []net.Conn
	listeners []net.Listener
	received  chan []byte
	done      chan error
	once      sync.Once
	closed    bool
}

func newFTP(ctx context.Context, config *tls.Config) (*ftpReceiver, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	r := &ftpReceiver{listener: listener, received: make(chan []byte, 1), done: make(chan error, 1)}
	go func() { err := r.serve(ctx, config); r.done <- err }()
	return r, nil
}
func (r *ftpReceiver) track(conn net.Conn) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		conn.Close()
		return
	}
	r.sockets = append(r.sockets, conn)
}
func (r *ftpReceiver) Close() error {
	r.once.Do(func() {
		r.listener.Close()
		r.mu.Lock()
		defer r.mu.Unlock()
		r.closed = true
		for _, l := range r.listeners {
			l.Close()
		}
		for _, c := range r.sockets {
			c.Close()
		}
	})
	return nil
}
func (r *ftpReceiver) serve(ctx context.Context, config *tls.Config) error {
	control, err := r.listener.Accept()
	if err != nil {
		return err
	}
	r.track(control)
	defer control.Close()
	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(60 * time.Second)
	}
	control.SetDeadline(deadline)
	reader := bufio.NewReader(control)
	reply := func(s string) error { _, err := io.WriteString(control, s+"\r\n"); return err }
	if err = reply("220 local Murmur example receiver"); err != nil {
		return err
	}
	var transfer chan error
	secure := false
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return err
		}
		fields := strings.Fields(line)
		if len(fields) == 0 {
			return fmt.Errorf("empty fixture command")
		}
		verb := strings.ToUpper(fields[0])
		switch verb {
		case "AUTH":
			if config == nil {
				return fmt.Errorf("unexpected AUTH")
			}
			if err = reply("234 TLS ready"); err != nil {
				return err
			}
			conn := tls.Server(control, config)
			if err = conn.HandshakeContext(ctx); err != nil {
				return err
			}
			control = conn
			reader = bufio.NewReader(control)
			secure = true
		case "PBSZ", "PROT", "TYPE":
			err = reply("200 accepted")
		case "USER":
			err = reply("331 password required")
		case "PASS":
			err = reply("230 logged in")
		case "CWD":
			err = reply("250 example directory")
		case "MKD":
			err = reply("257 directory created")
		case "PASV":
			listener, listenErr := net.Listen("tcp", "127.0.0.1:0")
			if listenErr != nil {
				return listenErr
			}
			r.mu.Lock()
			if r.closed {
				listener.Close()
			} else {
				r.listeners = append(r.listeners, listener)
			}
			r.mu.Unlock()
			transfer = make(chan error, 1)
			go func(listener net.Listener, secure bool, complete chan error) {
				defer listener.Close()
				conn, err := listener.Accept()
				if err != nil {
					complete <- err
					return
				}
				r.track(conn)
				defer conn.Close()
				conn.SetDeadline(deadline)
				if secure {
					tlsConn := tls.Server(conn, config)
					if err = tlsConn.HandshakeContext(ctx); err != nil {
						complete <- err
						return
					}
					conn = tlsConn
				}
				// Bound fixture memory independently of native bundle limits.
				data, err := io.ReadAll(io.LimitReader(conn, 16<<20))
				if err == nil {
					r.received <- data
				}
				complete <- err
			}(listener, secure, transfer)
			port := listener.Addr().(*net.TCPAddr).Port
			err = reply(fmt.Sprintf("227 Entering Passive Mode (127,0,0,1,%d,%d)", port/256, port%256))
		case "STOR":
			if transfer == nil {
				return fmt.Errorf("STOR before PASV")
			}
			if err = reply("150 starting transfer"); err != nil {
				return err
			}
			select {
			case err = <-transfer:
				if err != nil {
					return err
				}
			case <-ctx.Done():
				return ctx.Err()
			}
			err = reply("226 transfer complete")
		case "QUIT":
			return reply("221 goodbye")
		default:
			return fmt.Errorf("unsupported fixture command %s", verb)
		}
		if err != nil {
			return err
		}
	}
}
