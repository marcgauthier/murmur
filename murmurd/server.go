package murmurd

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	gmsmemory "github.com/dolthub/go-mysql-server/memory"
	gmserver "github.com/dolthub/go-mysql-server/server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/mysql"
	wire "github.com/jeroenrinzema/psql-wire"

	db "github.com/marcgauthier/murmur"
)

// Server is a running murmurd daemon: one murmur engine with a MySQL
// frontend, a PostgreSQL frontend, or both. Use New to open the
// engine and bind the listeners, Run to serve until ctx ends, Close
// to shut down.
type Server struct {
	cfg      *Config
	logger   *slog.Logger
	database *db.DB
	provider *Provider

	mysqlSrv *gmserver.Server
	pgSrv    *wire.Server
	pgLn     net.Listener

	schemaEpoch atomic.Uint64 // last epoch persisted to the sidecar

	closeOnce sync.Once
	closed    chan struct{}
}

// New opens the murmur engine and binds the enabled frontends. The
// listeners are bound (so Addrs are valid) but serving starts in Run.
func New(cfg *Config, logger *slog.Logger) (*Server, error) {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	mcfg, err := cfg.murmurConfig()
	if err != nil {
		return nil, err
	}
	// Reopen from the daemon-owned sidecar when it exists: the stored
	// schema advances on every CREATE TABLE and replicated adoption,
	// so config.toml alone cannot reopen a used data dir. Fresh data
	// dirs (no sidecar) boot from config as genesis.
	live, err := loadLiveSchema(cfg.Murmur.DataDir)
	if err != nil {
		return nil, err
	}
	if live != nil {
		mcfg.Schema.Version = live.Version
		mcfg.Schema.Tables = live.Tables
	}
	ctx := context.Background()
	database, err := db.Open(ctx, mcfg)
	if err != nil {
		if errors.Is(err, db.ErrSchemaMismatch) {
			return nil, fmt.Errorf("murmurd: open murmur engine: %w (schema drift: re-declare the missing tables/columns in the [schema] file, or restore data_dir/%s from backup)", err, liveSchemaFile)
		}
		return nil, fmt.Errorf("murmurd: open murmur engine: %w", err)
	}
	s := &Server{cfg: cfg, logger: logger, database: database, closed: make(chan struct{})}
	provider := NewProvider(database, frontendDBName(cfg), mcfg.Schema.Tables)
	provider.schemaDir = cfg.Murmur.DataDir
	provider.logger = logger
	s.provider = provider
	// The schema file is desired additive state: migrate in whatever
	// the live schema is missing, fail on conflicting redefinitions.
	desired, err := cfg.desiredTables()
	if err != nil {
		database.Close()
		return nil, err
	}
	if err := reconcileDesiredTables(ctx, database, desired, logger); err != nil {
		database.Close()
		return nil, err
	}
	epoch, err := persistLiveSchema(database, cfg.Murmur.DataDir)
	if err != nil {
		database.Close()
		return nil, err
	}
	s.schemaEpoch.Store(epoch)

	if cfg.MySQL.Enable {
		msrv, err := newMySQLServer(provider, cfg.MySQL.Addr)
		if err != nil {
			database.Close()
			return nil, err
		}
		s.mysqlSrv = msrv
		logger.Info("murmurd: mysql frontend bound", "addr", s.MySQLAddr())
	}
	if cfg.Postgres.Enable {
		psrv, pln, err := newPGListener(database, provider, cfg.Postgres.Database, cfg.Postgres.Addr, logger)
		if err != nil {
			database.Close()
			return nil, err
		}
		s.pgSrv, s.pgLn = psrv, pln
		logger.Info("murmurd: postgres frontend bound", "addr", s.PGAddr())
	}
	return s, nil
}

// frontendDBName picks the provider database name (both frontends
// share one provider; the MySQL name wins when both are enabled).
func frontendDBName(cfg *Config) string {
	if cfg.MySQL.Enable {
		return cfg.MySQL.Database
	}
	return cfg.Postgres.Database
}

// DB exposes the underlying engine (admin, tests).
func (s *Server) DB() *db.DB { return s.database }

// MySQLAddr is the bound MySQL address, or "" when disabled.
func (s *Server) MySQLAddr() string {
	if s.mysqlSrv == nil || s.mysqlSrv.Listener == nil {
		return ""
	}
	return s.mysqlSrv.Listener.Addr().String()
}

// PGAddr is the bound PostgreSQL address, or "" when disabled.
func (s *Server) PGAddr() string {
	if s.pgLn == nil {
		return ""
	}
	return s.pgLn.Addr().String()
}

// Run serves both frontends until ctx ends, then closes everything.
// It returns nil on context cancellation.
func (s *Server) Run(ctx context.Context) error {
	errCh := make(chan error, 2)
	var wg sync.WaitGroup
	if s.mysqlSrv != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := s.mysqlSrv.Start(); err != nil {
				errCh <- fmt.Errorf("murmurd: mysql frontend: %w", err)
			}
		}()
	}
	if s.pgSrv != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := s.pgSrv.Serve(s.pgLn); err != nil {
				errCh <- fmt.Errorf("murmurd: postgres frontend: %w", err)
			}
		}()
	}
	stopped := make(chan struct{})
	go func() {
		wg.Wait()
		close(stopped)
	}()
	// A replicated adoption advances the stored schema with no local
	// DDL; the watch refreshes the sidecar so the next boot reopens.
	watchCtx, stopWatch := context.WithCancel(context.Background())
	defer stopWatch()
	go s.watchSchema(watchCtx)
	select {
	case <-ctx.Done():
		_ = s.Close()
		<-stopped
		return nil
	case err := <-errCh:
		_ = s.Close()
		<-stopped
		return err
	case <-stopped:
		_ = s.Close()
		select {
		case err := <-errCh:
			return err
		default:
			return fmt.Errorf("murmurd: frontends stopped unexpectedly")
		}
	}
}

// Close stops the frontends and closes the engine. It is idempotent.
func (s *Server) Close() error {
	var first error
	s.closeOnce.Do(func() {
		defer close(s.closed)
		if s.mysqlSrv != nil {
			if err := s.mysqlSrv.Close(); err != nil {
				first = fmt.Errorf("murmurd: close mysql: %w", err)
			}
		}
		if s.pgSrv != nil {
			if err := s.pgSrv.Close(); err != nil && first == nil {
				first = fmt.Errorf("murmurd: close postgres: %w", err)
			}
		}
		if s.pgLn != nil {
			// The PG server serves a caller-owned listener: closing
			// it is what unblocks Serve.
			if err := s.pgLn.Close(); err != nil && first == nil {
				first = fmt.Errorf("murmurd: close postgres listener: %w", err)
			}
		}
		// Persist the final declaration (adoptions may have advanced
		// it since the last write) so the next boot reopens exactly.
		if _, err := persistLiveSchema(s.database, s.cfg.Murmur.DataDir); err != nil && first == nil {
			first = err
		}
		if err := s.database.Close(); err != nil && first == nil {
			first = fmt.Errorf("murmurd: close engine: %w", err)
		}
	})
	return first
}

// watchSchema refreshes the schema sidecar whenever the live epoch moves
// without local DDL (replicated adoption). Best-effort: Close persists
// the final declaration regardless.
func (s *Server) watchSchema(ctx context.Context) {
	t := time.NewTicker(10 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if s.database.Status().SchemaEpoch == s.schemaEpoch.Load() {
				continue
			}
			if got, err := persistLiveSchema(s.database, s.cfg.Murmur.DataDir); err != nil {
				s.logger.Warn("murmurd: schema sidecar refresh failed", "err", err)
			} else {
				s.schemaEpoch.Store(got)
			}
		}
	}
}

func newMySQLServer(provider *Provider, addr string) (*gmserver.Server, error) {
	engine := provider.Engine()
	builder := func(ctx context.Context, conn *mysql.Conn, remote string) (sql.Session, error) {
		return gmsmemory.NewSession(sql.NewBaseSession(), provider), nil
	}
	srv, err := gmserver.NewServer(gmserver.Config{
		Protocol: "tcp",
		Address:  addr,
	}, engine, sql.NewContext, builder, nil)
	if err != nil {
		return nil, fmt.Errorf("murmurd: mysql frontend: %w", err)
	}
	return srv, nil
}

func newPGListener(database *db.DB, provider *Provider, dbName, addr string, logger *slog.Logger) (*wire.Server, net.Listener, error) {
	srv, err := newPGServer(database, provider, dbName, logger)
	if err != nil {
		return nil, nil, err
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, nil, fmt.Errorf("murmurd: postgres listen %s: %w", addr, err)
	}
	return srv, ln, nil
}
