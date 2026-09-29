package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	db "github.com/nomadsql/replicateddb"
	"github.com/nomadsql/replicateddb/metrics"
	"github.com/nomadsql/replicateddb/schema"
	"github.com/nomadsql/replicateddb/service"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// NodeConfigFile represents the JSON configuration file for a node instance.
type NodeConfigFile struct {
	NodeID          string                 `json:"node_id"`
	DBID            string                 `json:"db_id,omitempty"`
	DataDir         string                 `json:"data_dir"`
	ListenAddr      string                 `json:"listen_addr"`
	APIAddr         string                 `json:"api_addr"`
	MetricsAddr     string                 `json:"metrics_addr,omitempty"`
	Bootstrap       []string               `json:"bootstrap,omitempty"`
	Peers           []PeerConfig           `json:"peers,omitempty"`
	AllowedPeers    []string               `json:"allowed_peers,omitempty"`
	AllowedNetworks []string               `json:"allowed_networks,omitempty"`
	AwaitUnlock     bool                   `json:"await_unlock"`
	KeyHex          string                 `json:"key_hex,omitempty"`
	KeyID           string                 `json:"key_id,omitempty"`
	SchemaPath      string                 `json:"schema_path,omitempty"`
	TLSCACertFile   string                 `json:"tls_ca_cert_file,omitempty"`
	TLSNodeCertFile string                 `json:"tls_node_cert_file,omitempty"`
	TLSNodeKeyFile  string                 `json:"tls_node_key_file,omitempty"`
	Schema          *db.SchemaConfig       `json:"schema,omitempty"`
	Files           *FilesConfigFile       `json:"files,omitempty"`
	Bridge          *BridgeConfigFile      `json:"bridge,omitempty"`
	Replication     *ReplicationConfigFile `json:"replication,omitempty"`
	Limits          *LimitsConfigFile      `json:"limits,omitempty"`
}

// ReplicationConfigFile carries optional retention overrides. Zero values
// select production defaults; positive values override them (used by live
// scenarios that must force log expiry and snapshot resync).
type ReplicationConfigFile struct {
	MinLogRetentionMs        int64  `json:"min_log_retention_ms,omitempty"`
	MaxOfflineLogRetentionMs int64  `json:"max_offline_log_retention_ms,omitempty"`
	MinRetainedBatches       uint64 `json:"min_retained_batches,omitempty"`
	Dissemination            string `json:"dissemination,omitempty"`
	// ProtocolVersionOverride/MinProtocolVersionOverride replace the
	// advertised handshake versions (interoperability testing only).
	ProtocolVersionOverride    uint16 `json:"protocol_version_override,omitempty"`
	MinProtocolVersionOverride uint16 `json:"min_protocol_version_override,omitempty"`
}

// LimitsConfigFile carries optional commit-path budget overrides. Zero
// values select production defaults; positive values override them (used
// by live scenarios that must deterministically trip budget rejection).
type LimitsConfigFile struct {
	MaxValueBytes       int   `json:"max_value_bytes,omitempty"`
	MaxTransactionBytes int64 `json:"max_transaction_bytes,omitempty"`
	MaxBatchMutations   int   `json:"max_batch_mutations,omitempty"`
}

type PeerConfig struct {
	NodeID string   `json:"node_id"`
	Addrs  []string `json:"addrs"`
}

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	subcmd := os.Args[1]
	switch subcmd {
	case "agent":
		runAgent(os.Args[2:])
	case "unlock":
		runUnlock(os.Args[2:])
	case "exec":
		runExec(os.Args[2:])
	case "query":
		runQuery(os.Args[2:])
	case "status":
		runStatus(os.Args[2:])
	default:
		// If first arg is a flag or file, default to agent
		if strings.HasPrefix(subcmd, "-") {
			runAgent(os.Args[1:])
		} else {
			printUsage()
			os.Exit(1)
		}
	}
}

func printUsage() {
	fmt.Fprintf(os.Stderr, `Usage: spedsql <command> [arguments]

Commands:
  agent   Run a SPeD-SQL node agent daemon
  unlock  Send encryption key to unlock an await-unlock node
  exec    Execute a SQL DDL/DML statement against an HTTP service API
  query   Run a SQL query against an HTTP service API
  status  Check node status via HTTP service API
`)
}

func runAgent(args []string) {
	fs := flag.NewFlagSet("agent", flag.ExitOnError)
	configPath := fs.String("config", "", "Path to JSON configuration file")
	nodeIDStr := fs.String("node-id", "", "Node UUID")
	dataDir := fs.String("data-dir", "", "Data directory for Pebble store and logs")
	listenAddr := fs.String("listen-addr", "127.0.0.1:7443", "Replication listen address")
	apiAddr := fs.String("api-addr", "127.0.0.1:8080", "HTTPS API listen address")
	metricsAddr := fs.String("metrics-addr", "", "Prometheus metrics listen address (optional, defaults to api-addr)")
	awaitUnlock := fs.Bool("await-unlock", false, "Wait for key via Remote Unlock HTTPS API")
	keyHex := fs.String("key-hex", "", "Hex-encoded 32-byte encryption key")
	keyID := fs.String("key-id", "default-key", "Encryption Key ID")
	schemaPath := fs.String("schema-path", "", "Directory containing *.sql schema files")
	logFile := fs.String("log-file", "", "Log output file (optional)")
	_ = fs.Parse(args)

	cfg := NodeConfigFile{
		NodeID:      *nodeIDStr,
		DataDir:     *dataDir,
		ListenAddr:  *listenAddr,
		APIAddr:     *apiAddr,
		MetricsAddr: *metricsAddr,
		AwaitUnlock: *awaitUnlock,
		KeyHex:      *keyHex,
		KeyID:       *keyID,
		SchemaPath:  *schemaPath,
	}

	if *configPath != "" {
		data, err := os.ReadFile(*configPath)
		if err != nil {
			log.Fatalf("read config file: %v", err)
		}
		if err := json.Unmarshal(data, &cfg); err != nil {
			log.Fatalf("parse config JSON: %v", err)
		}
		// CLI flags override config file if explicitly provided
		fs.Visit(func(f *flag.Flag) {
			switch f.Name {
			case "node-id":
				cfg.NodeID = *nodeIDStr
			case "data-dir":
				cfg.DataDir = *dataDir
			case "listen-addr":
				cfg.ListenAddr = *listenAddr
			case "api-addr":
				cfg.APIAddr = *apiAddr
			case "await-unlock":
				cfg.AwaitUnlock = *awaitUnlock
			}
		})
	}

	if *logFile != "" {
		_ = os.MkdirAll(filepath.Dir(*logFile), 0755)
		f, err := os.OpenFile(*logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			log.Fatalf("open log file: %v", err)
		}
		defer f.Close()
		log.SetOutput(io.MultiWriter(os.Stdout, f))
	}

	if cfg.DataDir == "" {
		cfg.DataDir = "./node-data"
	}
	_ = os.MkdirAll(filepath.Join(cfg.DataDir, "pebble"), 0755)

	var nodeID db.NodeID
	if cfg.NodeID != "" {
		id, err := db.ParseNodeID(cfg.NodeID)
		if err != nil {
			log.Fatalf("invalid node-id: %v", err)
		}
		nodeID = id
	} else {
		nodeID = db.NewNodeID()
	}

	var dbID db.DBID
	if cfg.DBID != "" {
		id, err := db.ParseDBID(cfg.DBID)
		if err != nil {
			log.Fatalf("invalid db-id: %v", err)
		}
		dbID = id
	}

	log.Printf("[SPEDSQL] Starting node %s on repl=%s api=%s data=%s await-unlock=%v",
		nodeID, cfg.ListenAddr, cfg.APIAddr, cfg.DataDir, cfg.AwaitUnlock)

	daemon := &NodeDaemon{
		cfg:    cfg,
		nodeID: nodeID,
		dbID:   dbID,
	}

	if err := daemon.Start(); err != nil {
		log.Fatalf("[SPEDSQL] Failed to start node: %v", err)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	log.Printf("[SPEDSQL] Received signal %v, shutting down...", sig)
	daemon.Close()
	log.Printf("[SPEDSQL] Node shutdown complete.")
}

type NodeDaemon struct {
	cfg        NodeConfigFile
	nodeID     db.NodeID
	dbID       db.DBID
	mu         sync.Mutex
	database   *db.DB
	httpServer *http.Server
	promReg    *prometheus.Registry
	bridge     *bridgeRuntime
}

func (d *NodeDaemon) apiTLSConfig() (*tls.Config, error) {
	if d.cfg.TLSCACertFile == "" || d.cfg.TLSNodeCertFile == "" || d.cfg.TLSNodeKeyFile == "" {
		return nil, fmt.Errorf("tls_ca_cert_file, tls_node_cert_file, and tls_node_key_file are required for the HTTPS API")
	}
	caPEM, err := os.ReadFile(d.cfg.TLSCACertFile)
	if err != nil {
		return nil, fmt.Errorf("read API client CA: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("API client CA file contains no valid certificate")
	}
	certPEM, err := os.ReadFile(d.cfg.TLSNodeCertFile)
	if err != nil {
		return nil, fmt.Errorf("read API server certificate: %w", err)
	}
	keyPEM, err := os.ReadFile(d.cfg.TLSNodeKeyFile)
	if err != nil {
		return nil, fmt.Errorf("read API server key: %w", err)
	}
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, fmt.Errorf("load API server key pair: %w", err)
	}
	return &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tls.RequestClientCert,
		ClientCAs:    roots,
		VerifyPeerCertificate: func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
			if len(rawCerts) == 0 {
				return nil // /healthz may be called without a client certificate.
			}
			peerCerts := make([]*x509.Certificate, 0, len(rawCerts))
			for _, raw := range rawCerts {
				peer, err := x509.ParseCertificate(raw)
				if err != nil {
					return fmt.Errorf("parse client certificate: %w", err)
				}
				peerCerts = append(peerCerts, peer)
			}
			intermediates := x509.NewCertPool()
			for _, peer := range peerCerts[1:] {
				intermediates.AddCert(peer)
			}
			_, err := peerCerts[0].Verify(x509.VerifyOptions{
				Roots: roots, Intermediates: intermediates,
				KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
			})
			return err
		},
	}, nil
}

func requireAPIClientCert(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" && r.Method == http.MethodGet {
			next.ServeHTTP(w, r)
			return
		}
		if r.TLS == nil || len(r.TLS.PeerCertificates) == 0 {
			http.Error(w, "valid client certificate required", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (d *NodeDaemon) Start() error {
	d.promReg = prometheus.NewRegistry()

	mux := http.NewServeMux()

	// Metrics endpoint
	mux.Handle("/metrics", promhttp.HandlerFor(d.promReg, promhttp.HandlerOpts{}))

	// The daemon API is protected by the verified client certificate on this
	// listener. /healthz is the sole HTTPS route that permits no client cert.
	mux.HandleFunc("/v1/admin/unlock", d.handleAdminUnlock)
	mux.HandleFunc("/v1/admin/status", d.handleAdminStatus)
	mux.HandleFunc("/v1/admin/lock", d.handleAdminLock)
	mux.HandleFunc("/v1/admin/add_peer", d.handleAdminAddPeer)
	mux.HandleFunc("/v1/admin/remove_peer", d.handleAdminRemovePeer)

	mux.HandleFunc("/v1/status", d.handleServiceStatus)
	mux.HandleFunc("/v1/query", d.handleServiceQuery)
	mux.HandleFunc("/v1/exec", d.handleServiceExec)
	mux.HandleFunc("/v1/subscribe", d.handleServiceSubscribe)

	mux.HandleFunc("/v1/files/upload", d.handleFilesUpload)
	mux.HandleFunc("/v1/files/download", d.handleFilesDownload)
	mux.HandleFunc("/v1/files/status", d.handleFilesStatus)
	mux.HandleFunc("/v1/files/search", d.handleFilesSearch)
	mux.HandleFunc("/v1/files/delete", d.handleFilesDelete)
	mux.HandleFunc("/v1/files/fetch", d.handleFilesFetch)
	mux.HandleFunc("/v1/files/fetch-stats", d.handleFilesFetchStats)
	mux.HandleFunc("/v1/admin/bridge/export", d.handleBridgeExport)
	mux.HandleFunc("/v1/admin/bridge/import", d.handleBridgeImport)
	mux.HandleFunc("/v1/admin/bridge/status", d.handleBridgeStatus)
	mux.HandleFunc("/v1/admin/bridge/provenance", d.handleBridgeProvenance)
	mux.HandleFunc("/v1/admin/bridge/release", d.handleBridgeRelease)
	mux.HandleFunc("/v1/admin/migrate", d.handleAdminMigrate)
	mux.HandleFunc("/v1/admin/rotate-key", d.handleAdminRotateKey)
	mux.HandleFunc("/v1/admin/encryption-status", d.handleAdminEncryptionStatus)

	// Fallback health check
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})

	apiTLS, err := d.apiTLSConfig()
	if err != nil {
		return fmt.Errorf("configure API TLS: %w", err)
	}
	d.httpServer = &http.Server{
		Addr:      d.cfg.APIAddr,
		Handler:   requireAPIClientCert(mux),
		TLSConfig: apiTLS,
	}

	ln, err := net.Listen("tcp", d.cfg.APIAddr)
	if err != nil {
		return fmt.Errorf("listen API addr %s: %w", d.cfg.APIAddr, err)
	}

	go func() {
		if err := d.httpServer.ServeTLS(ln, "", ""); err != nil && err != http.ErrServerClosed {
			log.Printf("[SPEDSQL] HTTPS API server error: %v", err)
		}
	}()

	if !d.cfg.AwaitUnlock {
		var keyBytes []byte
		if d.cfg.KeyHex != "" {
			k, err := hex.DecodeString(d.cfg.KeyHex)
			if err != nil {
				return fmt.Errorf("decode key_hex: %w", err)
			}
			keyBytes = k
		} else {
			keyBytes = []byte("0123456789abcdef0123456789abcdef")
		}
		keyID := d.cfg.KeyID
		if keyID == "" {
			keyID = "default-key"
		}
		if _, err := d.openDatabase(context.Background(), keyBytes, keyID); err != nil {
			return fmt.Errorf("open database: %w", err)
		}
	}

	return nil
}

func (d *NodeDaemon) openDatabase(ctx context.Context, key []byte, keyID string) (*db.DB, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.database != nil {
		return d.database, nil
	}

	pebbleDir := filepath.Join(d.cfg.DataDir, "pebble")
	_ = os.MkdirAll(pebbleDir, 0755)

	dbCfg := db.Config{
		Path:   pebbleDir,
		NodeID: d.nodeID,
		DBID:   d.dbID,
		Pebble: db.DefaultPebbleConfig(),
		Encryption: db.EncryptionConfig{
			Key:   append([]byte(nil), key...),
			KeyID: keyID,
		},
		Replication: db.ReplicationConfig{
			ListenAddr:      d.cfg.ListenAddr,
			AllowedNetworks: append([]string(nil), d.cfg.AllowedNetworks...),
			Bootstrap:       append([]string(nil), d.cfg.Bootstrap...),
			Membership: db.MembershipConfig{
				Bootstrap:     append([]string(nil), d.cfg.Bootstrap...),
				AdvertiseAddr: d.cfg.ListenAddr,
			},
		},
	}
	for _, peerID := range d.cfg.AllowedPeers {
		id, err := db.ParseNodeID(peerID)
		if err != nil {
			return nil, fmt.Errorf("invalid allowed peer node id %q: %w", peerID, err)
		}
		dbCfg.Replication.AllowedPeers = append(dbCfg.Replication.AllowedPeers, id)
	}

	// Load TLS credentials if available
	if d.cfg.TLSCACertFile != "" && d.cfg.TLSNodeCertFile != "" && d.cfg.TLSNodeKeyFile != "" {
		caPEM, err := os.ReadFile(d.cfg.TLSCACertFile)
		if err != nil {
			return nil, fmt.Errorf("read ca cert: %w", err)
		}
		nodeCertPEM, err := os.ReadFile(d.cfg.TLSNodeCertFile)
		if err != nil {
			return nil, fmt.Errorf("read node cert: %w", err)
		}
		nodeKeyPEM, err := os.ReadFile(d.cfg.TLSNodeKeyFile)
		if err != nil {
			return nil, fmt.Errorf("read node key: %w", err)
		}
		dbCfg.Replication.TLS = &db.TLSCredential{
			CertPEM: nodeCertPEM,
			KeyPEM:  nodeKeyPEM,
			CAPEM:   caPEM,
		}
	}

	// Configure schema
	if d.cfg.Schema != nil && len(d.cfg.Schema.Tables) > 0 {
		dbCfg.Schema = *d.cfg.Schema
	} else {
		dbCfg.Schema = db.SchemaConfig{
			Version: 1,
			Tables: []schema.TableSchema{
				{
					Name: "items",
					Columns: []schema.ColumnSchema{
						{Name: "id", Type: schema.ColBlob},
						{Name: "name", Type: schema.ColText, Nullable: true},
					},
				},
				{
					Name: "live_records",
					Columns: []schema.ColumnSchema{
						{Name: "id", Type: schema.ColBlob},
						{Name: "source", Type: schema.ColText, Nullable: true},
						{Name: "value", Type: schema.ColText, Nullable: true},
						{Name: "created_at", Type: schema.ColInteger, Nullable: true},
					},
				},
				{
					Name: "sync_bench_records",
					Columns: []schema.ColumnSchema{
						{Name: "id", Type: schema.ColBlob},
						{Name: "node_id", Type: schema.ColText, Nullable: true},
						{Name: "value", Type: schema.ColText, Nullable: true},
						{Name: "created_at", Type: schema.ColInteger, Nullable: true},
					},
				},
				{
					Name: "partition_rows",
					Columns: []schema.ColumnSchema{
						{Name: "id", Type: schema.ColBlob},
						{Name: "name", Type: schema.ColText, Nullable: true},
					},
				},
				{
					Name: "chaos_rows",
					Columns: []schema.ColumnSchema{
						{Name: "id", Type: schema.ColBlob},
						{Name: "name", Type: schema.ColText, Nullable: true},
					},
				},
				{
					Name: "crash_rows",
					Columns: []schema.ColumnSchema{
						{Name: "id", Type: schema.ColBlob},
						{Name: "value", Type: schema.ColText, Nullable: true},
					},
				},
			},
		}
	}

	// Fast replication intervals for tests
	dbCfg.Replication.SendInterval = 15 * time.Millisecond
	dbCfg.Replication.DialInterval = 50 * time.Millisecond
	dbCfg.Replication.AckInterval = 50 * time.Millisecond
	if rc := d.cfg.Replication; rc != nil {
		if rc.MinLogRetentionMs > 0 {
			dbCfg.Replication.MinLogRetention = time.Duration(rc.MinLogRetentionMs) * time.Millisecond
		}
		if rc.MaxOfflineLogRetentionMs > 0 {
			dbCfg.Replication.MaxOfflineLogRetention = time.Duration(rc.MaxOfflineLogRetentionMs) * time.Millisecond
		}
		if rc.MinRetainedBatches > 0 {
			dbCfg.Replication.MinRetainedBatches = rc.MinRetainedBatches
		}
		if rc.Dissemination != "" {
			dbCfg.Replication.Dissemination = db.DisseminationMode(rc.Dissemination)
		}
		if rc.ProtocolVersionOverride != 0 {
			dbCfg.Replication.ProtocolVersionOverride = rc.ProtocolVersionOverride
		}
		if rc.MinProtocolVersionOverride != 0 {
			dbCfg.Replication.MinProtocolVersionOverride = rc.MinProtocolVersionOverride
		}
	}
	if lc := d.cfg.Limits; lc != nil {
		if lc.MaxValueBytes > 0 {
			dbCfg.MaxReplicatedValueBytes = lc.MaxValueBytes
		}
		if lc.MaxTransactionBytes > 0 {
			dbCfg.MaxTransactionBytes = lc.MaxTransactionBytes
		}
		if lc.MaxBatchMutations > 0 {
			dbCfg.MaxBatchMutations = lc.MaxBatchMutations
		}
	}

	// Configure initial peers
	for _, p := range d.cfg.Peers {
		peerNodeID, err := db.ParseNodeID(p.NodeID)
		if err == nil {
			dbCfg.Replication.Peers = append(dbCfg.Replication.Peers, db.Peer{
				NodeID: peerNodeID,
				Addrs:  p.Addrs,
			})
		}
	}

	// Configure replicated files when enabled.
	if d.cfg.Files != nil && d.cfg.Files.Enabled {
		fc := d.cfg.Files
		objKey, err := hex.DecodeString(fc.ObjectKeyHex)
		if err != nil {
			return nil, fmt.Errorf("decode files object_key_hex: %w", err)
		}
		dbCfg.Files.Enabled = true
		dbCfg.Files.ObjectKey = objKey
		dbCfg.Files.FetchAddr = fc.FetchAddr
		for _, p := range fc.FetchPeers {
			if peerNodeID, err := db.ParseNodeID(p.NodeID); err == nil {
				dbCfg.Files.FetchPeers = append(dbCfg.Files.FetchPeers, db.Peer{
					NodeID: peerNodeID,
					Addrs:  p.Addrs,
				})
			}
		}
		if fc.FetchIntervalMs != 0 {
			dbCfg.Files.FetchInterval = time.Duration(fc.FetchIntervalMs) * time.Millisecond
		}
		if fc.FetchTimeoutMs != 0 {
			dbCfg.Files.FetchTimeout = time.Duration(fc.FetchTimeoutMs) * time.Millisecond
		}
		if fc.MaxFileBytes != 0 {
			dbCfg.Files.MaxFileBytes = fc.MaxFileBytes
		}
	}

	instance, err := db.Open(ctx, dbCfg)
	if err != nil {
		return nil, fmt.Errorf("db.Open: %w", err)
	}

	// Apply schema directory SQL files if present
	if d.cfg.SchemaPath != "" {
		if files, err := filepath.Glob(filepath.Join(d.cfg.SchemaPath, "*.sql")); err == nil {
			for _, file := range files {
				content, err := os.ReadFile(file)
				if err == nil {
					stmts := strings.Split(string(content), ";")
					for _, stmt := range stmts {
						stmt = strings.TrimSpace(stmt)
						if stmt != "" {
							_, _ = instance.ExecContext(ctx, stmt)
						}
					}
				}
			}
		}
	}

	// Add configured peers, honoring persisted exclusions: an explicit
	// RemovePeer survives restarts by design, and only an explicit
	// AddPeer (which clears the exclusion) may re-admit the peer.
	for _, p := range d.cfg.Peers {
		peerNodeID, err := db.ParseNodeID(p.NodeID)
		if err != nil {
			continue
		}
		if instance.IsPeerExcluded(peerNodeID) {
			continue
		}
		_ = instance.AddPeer(ctx, db.Peer{NodeID: peerNodeID, Addrs: p.Addrs})
	}

	// Register Prometheus metrics collector
	collector := metrics.NewCollector(instance.Status)
	_ = d.promReg.Register(collector)

	d.database = instance

	if err := d.initBridge(instance); err != nil {
		_ = instance.Close()
		d.database = nil
		return nil, fmt.Errorf("init bridge: %w", err)
	}

	log.Printf("[SPEDSQL] Database unlocked and online. NodeID: %s", instance.NodeID())
	return instance, nil
}

// HTTP Handler delegations
func (d *NodeDaemon) handleAdminUnlock(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var body struct {
		Key    string `json:"key"`
		KeyHex string `json:"key_hex"`
		KeyID  string `json:"key_id"`
		Cipher string `json:"cipher"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, fmt.Sprintf("invalid json: %v", err), http.StatusBadRequest)
		return
	}

	var keyBytes []byte
	if body.KeyHex != "" {
		k, err := hex.DecodeString(body.KeyHex)
		if err != nil || len(k) != 32 {
			http.Error(w, "invalid key_hex", http.StatusBadRequest)
			return
		}
		keyBytes = k
	} else if len(body.Key) == 32 {
		keyBytes = []byte(body.Key)
	} else if len(body.Key) == 64 {
		k, err := hex.DecodeString(body.Key)
		if err == nil && len(k) == 32 {
			keyBytes = k
		} else {
			keyBytes = []byte(body.Key[:32])
		}
	} else {
		keyBytes = []byte("0123456789abcdef0123456789abcdef")
	}

	keyID := body.KeyID
	if keyID == "" {
		keyID = "remote-unlock-key"
	}

	_, err := d.openDatabase(r.Context(), keyBytes, keyID)
	if err != nil {
		http.Error(w, fmt.Sprintf("unlock failed: %v", err), http.StatusUnauthorized)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]any{"status": "ok", "unlocked": true})
}

func (d *NodeDaemon) handleAdminStatus(w http.ResponseWriter, r *http.Request) {
	d.mu.Lock()
	defer d.mu.Unlock()
	unlocked := d.database != nil
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"unlocked": unlocked,
		"node_id":  d.nodeID.String(),
	})
}

func (d *NodeDaemon) handleAdminLock(w http.ResponseWriter, r *http.Request) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.database == nil {
		http.Error(w, "already locked", http.StatusConflict)
		return
	}
	_ = d.database.Close()
	d.database = nil
	w.WriteHeader(http.StatusNoContent)
}

func (d *NodeDaemon) handleAdminAddPeer(w http.ResponseWriter, r *http.Request) {
	d.mu.Lock()
	database := d.database
	d.mu.Unlock()
	if database == nil {
		http.Error(w, "node is locked", http.StatusServiceUnavailable)
		return
	}
	var req struct {
		NodeID string   `json:"node_id"`
		Addrs  []string `json:"addrs"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}
	nodeID, err := db.ParseNodeID(req.NodeID)
	if err != nil {
		http.Error(w, "invalid node_id", http.StatusBadRequest)
		return
	}
	if err := database.AddPeer(r.Context(), db.Peer{NodeID: nodeID, Addrs: req.Addrs}); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (d *NodeDaemon) handleAdminRemovePeer(w http.ResponseWriter, r *http.Request) {
	d.mu.Lock()
	database := d.database
	d.mu.Unlock()
	if database == nil {
		http.Error(w, "node is locked", http.StatusServiceUnavailable)
		return
	}
	var req struct {
		NodeID string `json:"node_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}
	nodeID, err := db.ParseNodeID(req.NodeID)
	if err != nil {
		http.Error(w, "invalid node_id", http.StatusBadRequest)
		return
	}
	if err := database.RemovePeer(r.Context(), nodeID); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (d *NodeDaemon) handleServiceStatus(w http.ResponseWriter, r *http.Request) {
	d.mu.Lock()
	database := d.database
	d.mu.Unlock()
	if database == nil {
		http.Error(w, "node is locked/awaiting unlock", http.StatusServiceUnavailable)
		return
	}
	st := database.Status()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"state":           st.State.String(),
		"node_id":         st.NodeID.String(),
		"db_id":           st.DBID.String(),
		"hlc":             st.HLC,
		"local_seq":       st.LocalSeq,
		"schema_epoch":    st.SchemaEpoch,
		"peer_count":      st.PeerCount,
		"connected_peers": st.ConnectedPeers,
		"selected_peers":  st.SelectedPeers,
		"uptime_millis":   st.Uptime.Milliseconds(),
	})
}

func (d *NodeDaemon) handleServiceQuery(w http.ResponseWriter, r *http.Request) {
	d.mu.Lock()
	database := d.database
	d.mu.Unlock()
	if database == nil {
		http.Error(w, "node is locked", http.StatusServiceUnavailable)
		return
	}
	var req struct {
		Query string `json:"query"`
		Args  []any  `json:"args"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("bad json: %v", err), http.StatusBadRequest)
		return
	}
	normArgs := normalizeArgs(req.Args)
	rows, err := database.QueryContext(r.Context(), req.Query, normArgs...)
	if err != nil {
		http.Error(w, fmt.Sprintf("query error: %v", err), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	cols := rows.Columns()
	var result [][]any
	for rows.Next() {
		dest := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range dest {
			ptrs[i] = &dest[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			http.Error(w, fmt.Sprintf("scan error: %v", err), http.StatusInternalServerError)
			return
		}
		result = append(result, dest)
	}
	if result == nil {
		result = [][]any{}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"columns": cols,
		"rows":    result,
	})
}

func (d *NodeDaemon) handleServiceExec(w http.ResponseWriter, r *http.Request) {
	d.mu.Lock()
	database := d.database
	d.mu.Unlock()
	if database == nil {
		http.Error(w, "node is locked", http.StatusServiceUnavailable)
		return
	}
	var req struct {
		Query string `json:"query"`
		Args  []any  `json:"args"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("bad json: %v", err), http.StatusBadRequest)
		return
	}
	normArgs := normalizeArgs(req.Args)
	res, err := database.ExecContext(r.Context(), req.Query, normArgs...)
	if err != nil {
		http.Error(w, fmt.Sprintf("exec error: %v", err), http.StatusInternalServerError)
		return
	}
	rowsAffected, _ := res.RowsAffected()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"rows_affected": rowsAffected,
	})
}

func normalizeArgs(args []any) []any {
	out := make([]any, len(args))
	for i, a := range args {
		switch v := a.(type) {
		case string:
			if len(v) == 32 {
				if b, err := hex.DecodeString(v); err == nil && len(b) == 16 {
					out[i] = b
					continue
				}
			}
			out[i] = v
		default:
			out[i] = v
		}
	}
	return out
}

func (d *NodeDaemon) handleServiceSubscribe(w http.ResponseWriter, r *http.Request) {
	d.mu.Lock()
	database := d.database
	d.mu.Unlock()
	if database == nil {
		http.Error(w, "node is locked", http.StatusServiceUnavailable)
		return
	}
	// Native server-sent-events subscription over the daemon API. The outer
	// mTLS middleware authenticates this route like every other /v1 endpoint.
	query := r.URL.Query().Get("query")
	if query == "" {
		http.Error(w, "query is required", http.StatusBadRequest)
		return
	}
	args, err := service.DecodeJSONArgs(r.URL.Query().Get("args"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var opts db.SubscriptionOptions
	if resume := r.URL.Query().Get("resume"); resume != "" {
		cursor, err := strconv.ParseUint(resume, 10, 64)
		if err != nil {
			http.Error(w, "invalid resume cursor", http.StatusBadRequest)
			return
		}
		opts.ResumeFromCursor = cursor
	}
	sub, err := database.SubscribeWithOptions(r.Context(), query, opts, args...)
	if err != nil {
		http.Error(w, fmt.Sprintf("subscribe error: %v", err), http.StatusInternalServerError)
		return
	}
	defer sub.Close()
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	enc := json.NewEncoder(w)
	ctx := r.Context()
	emit := func(eventType string, cursor uint64, columns []string, rows [][]service.Value, errText string) bool {
		out := map[string]any{"type": eventType, "cursor": cursor}
		if columns != nil {
			out["columns"] = columns
		}
		if rows != nil {
			out["rows"] = rows
		}
		if errText != "" {
			out["error"] = errText
		}
		if _, err := w.Write([]byte("data: ")); err != nil {
			return false
		}
		if err := enc.Encode(out); err != nil {
			return false
		}
		if _, err := w.Write([]byte("\n")); err != nil {
			return false
		}
		flusher.Flush()
		return true
	}
	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-sub.Events():
			if !ok {
				return
			}
			if ev.Err != nil {
				emit("error", ev.Cursor, nil, nil, ev.Err.Error())
				return
			}
			raw := make([][]any, len(ev.Rows))
			for i, row := range ev.Rows {
				raw[i] = row.Values
			}
			encRows, err := service.MarshalRows(raw)
			if err != nil {
				emit("error", ev.Cursor, nil, nil, err.Error())
				return
			}
			if !emit(string(ev.Type), ev.Cursor, ev.Columns, encRows, "") {
				return
			}
		}
	}
}

func (d *NodeDaemon) Close() {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.httpServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = d.httpServer.Shutdown(ctx)
		cancel()
	}
	if d.database != nil {
		_ = d.database.Close()
		d.database = nil
	}
}

// Client CLI helper subcommands
type apiClientFlags struct {
	apiURL   *string
	caFile   *string
	certFile *string
	keyFile  *string
}

func addAPIClientFlags(fs *flag.FlagSet) apiClientFlags {
	return apiClientFlags{
		apiURL:   fs.String("api", "https://127.0.0.1:8080", "HTTPS API base URL"),
		caFile:   fs.String("ca-cert", "", "CA certificate used to verify the API server"),
		certFile: fs.String("client-cert", "", "Client certificate for API mTLS"),
		keyFile:  fs.String("client-key", "", "Client private key for API mTLS"),
	}
}

func (f apiClientFlags) client() (*http.Client, string, error) {
	u, err := url.Parse(*f.apiURL)
	if err != nil || u.Scheme != "https" || u.Host == "" {
		return nil, "", fmt.Errorf("API URL must be an absolute https URL")
	}
	if *f.caFile == "" || *f.certFile == "" || *f.keyFile == "" {
		return nil, "", fmt.Errorf("--ca-cert, --client-cert, and --client-key are required")
	}
	caPEM, err := os.ReadFile(*f.caFile)
	if err != nil {
		return nil, "", fmt.Errorf("read API CA: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		return nil, "", fmt.Errorf("API CA file contains no valid certificate")
	}
	cert, err := tls.LoadX509KeyPair(*f.certFile, *f.keyFile)
	if err != nil {
		return nil, "", fmt.Errorf("load API client certificate: %w", err)
	}
	return &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{TLSClientConfig: &tls.Config{
			MinVersion: tls.VersionTLS12, RootCAs: roots, Certificates: []tls.Certificate{cert},
		}},
	}, strings.TrimSuffix(*f.apiURL, "/"), nil
}

func runUnlock(args []string) {
	fs := flag.NewFlagSet("unlock", flag.ExitOnError)
	apiFlags := addAPIClientFlags(fs)
	keyHex := fs.String("key-hex", "", "Hex-encoded 32-byte encryption key")
	keyStr := fs.String("key", "", "String 32-byte key")
	_ = fs.Parse(args)
	client, apiURL, err := apiFlags.client()
	if err != nil {
		log.Fatal(err)
	}

	payload := map[string]string{}
	if *keyHex != "" {
		payload["key_hex"] = *keyHex
	} else if *keyStr != "" {
		payload["key"] = *keyStr
	} else {
		log.Fatal("--key-hex or --key is required")
	}
	body, _ := json.Marshal(payload)
	resp, err := client.Post(apiURL+"/v1/admin/unlock", "application/json", strings.NewReader(string(body)))
	if err != nil {
		log.Fatalf("unlock request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		b, _ := io.ReadAll(resp.Body)
		log.Fatalf("unlock failed with code %d: %s", resp.StatusCode, string(b))
	}
	fmt.Println("Node unlocked successfully.")
}

func runExec(args []string) {
	fs := flag.NewFlagSet("exec", flag.ExitOnError)
	apiFlags := addAPIClientFlags(fs)
	_ = fs.Parse(args)
	client, apiURL, err := apiFlags.client()
	if err != nil {
		log.Fatal(err)
	}
	if fs.NArg() < 1 {
		log.Fatal("SQL statement argument required")
	}
	sqlStmt := fs.Arg(0)
	payload, _ := json.Marshal(map[string]any{"query": sqlStmt})
	resp, err := client.Post(apiURL+"/v1/exec", "application/json", strings.NewReader(string(payload)))
	if err != nil {
		log.Fatalf("exec request failed: %v", err)
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		log.Fatalf("exec failed (%d): %s", resp.StatusCode, string(respBody))
	}
	fmt.Println(string(respBody))
}

func runQuery(args []string) {
	fs := flag.NewFlagSet("query", flag.ExitOnError)
	apiFlags := addAPIClientFlags(fs)
	_ = fs.Parse(args)
	client, apiURL, err := apiFlags.client()
	if err != nil {
		log.Fatal(err)
	}
	if fs.NArg() < 1 {
		log.Fatal("SQL query argument required")
	}
	sqlQuery := fs.Arg(0)
	payload, _ := json.Marshal(map[string]any{"query": sqlQuery})
	resp, err := client.Post(apiURL+"/v1/query", "application/json", strings.NewReader(string(payload)))
	if err != nil {
		log.Fatalf("query request failed: %v", err)
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		log.Fatalf("query failed (%d): %s", resp.StatusCode, string(respBody))
	}
	fmt.Println(string(respBody))
}

func runStatus(args []string) {
	fs := flag.NewFlagSet("status", flag.ExitOnError)
	apiFlags := addAPIClientFlags(fs)
	_ = fs.Parse(args)
	client, apiURL, err := apiFlags.client()
	if err != nil {
		log.Fatal(err)
	}
	resp, err := client.Get(apiURL + "/v1/status")
	if err != nil {
		log.Fatalf("status request failed: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	fmt.Println(string(body))
}
