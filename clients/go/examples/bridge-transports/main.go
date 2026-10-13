package main

import (
	"bytes"
	"context"
	"errors"
	murmur "github.com/marcgauthier/murmur/clients/go"
	"github.com/marcgauthier/murmur/clients/go/examples/internal/demo"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"time"
)

func run(ctx context.Context, e *demo.Env) error {
	c, err := demo.NewCluster(1)
	if err != nil {
		return err
	}
	backend, err := demo.NewBackend(c.Database, c.Nodes[0], c.Configs[0])
	if err != nil {
		return err
	}
	e.Defer(backend.Handle.Close)
	secret, err := demo.Key()
	if err != nil {
		return err
	}
	recipient, err := murmur.RecipientPublic(secret)
	if err != nil {
		return err
	}
	bridge, err := murmur.OpenBridge(murmur.ExporterConfig(c.Database, e.Path("bundles"), "transport", c.Configs[0].SigningKey, recipient), backend.Handle)
	if err != nil {
		return err
	}
	e.Defer(bridge.Close)
	path, err := bridge.Export([]byte("sealed transport payload"))
	if err != nil {
		return err
	}
	expected, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	received := make(chan []byte, 1)
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			w.WriteHeader(405)
			return
		}
		data, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 16<<20))
		if err != nil {
			w.WriteHeader(400)
			return
		}
		received <- data
		w.WriteHeader(http.StatusCreated)
	})
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	server := &http.Server{Handler: handler, ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second, ErrorLog: log.New(io.Discard, "", 0)}
	go server.Serve(listener)
	e.Defer(server.Close)
	if err = bridge.PublishHTTP(path, "http://"+listener.Addr().String()+"/bundle"); err != nil {
		return err
	}
	select {
	case got := <-received:
		if err = demo.Require(bytes.Equal(got, expected), "HTTP altered bundle"); err != nil {
			return err
		}
	case <-ctx.Done():
		return ctx.Err()
	}
	tlsConfig, roots, err := serverTLS()
	if err != nil {
		return err
	}
	for _, secure := range []bool{false, true} {
		cfg := tlsConfig
		if !secure {
			cfg = nil
		}
		ftp, err := newFTP(ctx, cfg)
		if err != nil {
			return err
		}
		e.Defer(ftp.Close)
		scheme := "ftp"
		var options *murmur.FTPOptions
		if secure {
			scheme = "ftps"
			options = &murmur.FTPOptions{RootsPEM: roots, Timeout: 5 * time.Second}
		}
		if err = bridge.PublishFTP(path, scheme+"://"+ftp.listener.Addr().String()+"/incoming/demo.bundle", options); err != nil {
			return err
		}
		select {
		case got := <-ftp.received:
			if err = demo.Require(bytes.Equal(got, expected), "%s altered bundle", scheme); err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
		select {
		case err := <-ftp.done:
			if err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
		e.Print("%s uploaded and verified %d sealed bytes", scheme, len(expected))
	}
	tlsListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	https := &http.Server{Handler: handler, TLSConfig: tlsConfig, ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second, ErrorLog: log.New(io.Discard, "", 0)}
	go https.ServeTLS(tlsListener, "", "")
	e.Defer(https.Close)
	err = bridge.PublishHTTP(path, "https://"+tlsListener.Addr().String()+"/bundle")
	if err == nil {
		return errors.New("private-CA HTTPS unexpectedly trusted")
	}
	if !errors.Is(err, murmur.ErrUnavailable) {
		return err
	}
	e.Print("HTTPS rejected the generated CA as expected: PublishHTTP has no private-CA option")
	if e.HTTPSURL != "" {
		if err = bridge.PublishHTTP(path, e.HTTPSURL); err != nil {
			return err
		}
		e.Print("Trusted external HTTPS endpoint accepted the bundle")
	}
	return nil
}
func main() { demo.Main("bridge-transports", run) }
