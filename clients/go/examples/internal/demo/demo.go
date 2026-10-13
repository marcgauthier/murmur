// Package demo provides setup and cleanup shared by the executable examples.
// Feature-specific operations remain in each command's main.go.
package demo

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	murmur "github.com/marcgauthier/murmur/clients/go"
)

type Env struct {
	Dir       string
	Key       murmur.Key
	Out       io.Writer
	HTTPSURL  string
	BasicPath string
	cleanup   []func() error
}

func New(dir string, out io.Writer) (*Env, error) {
	e := &Env{Dir: dir, Out: out}
	if _, err := rand.Read(e.Key[:]); err != nil {
		return nil, err
	}
	return e, nil
}
func Key() (k murmur.Key, err error)            { _, err = rand.Read(k[:]); return }
func ID() (id murmur.NodeID, err error)         { _, err = rand.Read(id[:]); return }
func (e *Env) Path(name string) string          { return filepath.Join(e.Dir, name) }
func (e *Env) Print(format string, args ...any) { fmt.Fprintf(e.Out, format+"\n", args...) }
func (e *Env) Defer(fn func() error)            { e.cleanup = append(e.cleanup, fn) }
func (e *Env) Close() error {
	var err error
	for i := len(e.cleanup) - 1; i >= 0; i-- {
		err = errors.Join(err, e.cleanup[i]())
	}
	e.cleanup = nil
	return err
}
func (e *Env) Open(name string, opts murmur.Options) (*murmur.DB, error) {
	path := e.Path(name)
	if name == "db" && e.BasicPath != "" {
		path = e.BasicPath
	}
	db, err := murmur.Open(path, e.Key[:], opts)
	if err == nil {
		e.Defer(db.Close)
	}
	return db, err
}
func Options() murmur.Options { o := murmur.DefaultOptions(); o.Mode = murmur.CreateNew; return o }
func Wait(ctx context.Context, description string, fn func() (bool, error)) error {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("%s: %w", description, err)
		}
		ok, err := fn()
		if err != nil {
			return fmt.Errorf("%s: %w", description, err)
		}
		if ok {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("%s: %w", description, ctx.Err())
		case <-ticker.C:
		}
	}
}
func Require(ok bool, format string, args ...any) error {
	if !ok {
		return fmt.Errorf(format, args...)
	}
	return nil
}
func Int(db *murmur.DB, query string, args ...any) (int64, error) {
	var n int64
	err := db.Read(func(s *murmur.Sql) error { return s.QueryRow(query, args...).Scan(&n) })
	return n, err
}
func Text(db *murmur.DB, query string, args ...any) (string, error) {
	var v string
	err := db.Read(func(s *murmur.Sql) error { return s.QueryRow(query, args...).Scan(&v) })
	return v, err
}
func Embedding(values ...float32) []byte {
	b := make([]byte, len(values)*4)
	for i, v := range values {
		binary.LittleEndian.PutUint32(b[i*4:], mathBits(v))
	}
	return b
}
func Main(name string, run func(context.Context, *Env) error) {
	if err := command(name, run); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func command(name string, run func(context.Context, *Env) error) (result error) {
	flags := flag.NewFlagSet(name, flag.ContinueOnError)
	dir := flags.String("dir", "", "new working directory (must not exist)")
	keep := flags.Bool("keep", false, "retain example data after closing handles")
	timeout := flags.Duration("timeout", 60*time.Second, "maximum scenario duration")
	httpsURL := flags.String("https-url", "", "optional trusted HTTPS bridge upload endpoint")
	if err := flags.Parse(os.Args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if *httpsURL != "" && !strings.HasPrefix(*httpsURL, "https://") {
		return errors.New("https-url must start with https://")
	}
	if *httpsURL != "" && name != "bridge-transports" {
		return errors.New("https-url is only supported by bridge-transports")
	}
	if *timeout <= 0 {
		return errors.New("timeout must be positive")
	}
	if flags.NArg() > 0 && (name != "basic" || flags.NArg() != 1) {
		return errors.New("unexpected positional arguments")
	}
	var root string
	var err error
	if *dir == "" {
		root, err = os.MkdirTemp("", "murmur-"+name+"-")
	} else {
		root = *dir
		err = os.Mkdir(root, 0700)
	}
	if err != nil {
		return err
	}
	if !*keep {
		defer func() { result = errors.Join(result, os.RemoveAll(root)) }()
	}
	e, err := New(root, os.Stdout)
	if err != nil {
		return err
	}
	e.HTTPSURL = *httpsURL
	if flags.NArg() == 1 {
		e.BasicPath = flags.Arg(0)
	}
	defer func() { result = errors.Join(result, e.Close()) }()
	e.Print("%s: working directory %s", name, root)
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	// The watcher exits the executable if a native blocking operation cannot be
	// interrupted by context. Normal completion always closes resources first.
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-done:
			return
		case <-ctx.Done():
			fmt.Fprintln(os.Stderr, name+": deadline exceeded")
			os.Exit(1)
		}
	}()
	if err = errors.Join(run(ctx, e), e.Close()); err != nil {
		return err
	}
	e.Print("%s: verified successfully", name)
	if *keep {
		e.Print("Data retained at %s; ephemeral keys are not printed or saved.", root)
	}
	return nil
}
