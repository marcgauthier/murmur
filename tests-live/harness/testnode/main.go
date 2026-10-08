package main

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
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
	"runtime/pprof"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	db "github.com/marcgauthier/murmur"
	"github.com/marcgauthier/murmur/crypto"
	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/metrics"
	"github.com/marcgauthier/murmur/origin"
	"github.com/marcgauthier/murmur/schema"
	"github.com/marcgauthier/murmur/spool"
)

// NodeConfigFile represents the JSON configuration file for a node instance.
type NodeConfigFile struct {
	OriginSigningKeyFile   string                 `json:"origin_signing_key_file"`
	OriginPublicKeys       map[string]string      `json:"origin_public_keys"`
	TrustedSnapshotSources []string               `json:"trusted_snapshot_sources"`
	NodeID                 string                 `json:"node_id"`
	DBID                   string                 `json:"db_id,omitempty"`
	DataDir                string                 `json:"data_dir"`
	ListenAddr             string                 `json:"listen_addr"`
	APIAddr                string                 `json:"api_addr"`
	MetricsAddr            string                 `json:"metrics_addr,omitempty"`
	Bootstrap              []string               `json:"bootstrap,omitempty"`
	MembershipEnabled      bool                   `json:"membership_enabled,omitempty"`
	Peers                  []PeerConfig           `json:"peers,omitempty"`
	AllowedPeers           []string               `json:"allowed_peers,omitempty"`
	AllowedNetworks        []string               `json:"allowed_networks,omitempty"`
	AwaitUnlock            bool                   `json:"await_unlock"`
	KeyHex                 string                 `json:"key_hex,omitempty"`
	KeyID                  string                 `json:"key_id,omitempty"`
	TLSCACertFile          string                 `json:"tls_ca_cert_file,omitempty"`
	TLSNodeCertFile        string                 `json:"tls_node_cert_file,omitempty"`
	TLSNodeKeyFile         string                 `json:"tls_node_key_file,omitempty"`
	Schema                 *db.SchemaConfig       `json:"schema,omitempty"`
	TypedRecords           bool                   `json:"typed_records,omitempty"`
	TypedSchemaVersion     int                    `json:"typed_schema_version,omitempty"`
	TypedSchemaCrashPhase  string                 `json:"typed_schema_crash_phase,omitempty"`
	TypedContention        bool                   `json:"typed_contention,omitempty"`
	Files                  *FilesConfigFile       `json:"files,omitempty"`
	Bridge                 *BridgeConfigFile      `json:"bridge,omitempty"`
	Replication            *ReplicationConfigFile `json:"replication,omitempty"`
	Limits                 *LimitsConfigFile      `json:"limits,omitempty"`
	Spool                  *SpoolConfigFile       `json:"spool,omitempty"`
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

// SpoolConfigFile carries optional Spool storage overrides.
type SpoolConfigFile struct {
	TargetBlockBytes int   `json:"target_block_bytes,omitempty"`
	MaxBlockBytes    int   `json:"max_block_bytes,omitempty"`
	MaxPendingBytes  int64 `json:"max_pending_bytes,omitempty"`
	CacheBytes       int64 `json:"cache_bytes,omitempty"`
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
	case "migrate-origin-baseline":
		runOriginMigration(os.Args[2:])
	case "agent":
		runAgent(os.Args[2:])
	case "unlock":
		runUnlock(os.Args[2:])
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
	fmt.Fprintf(os.Stderr, `Usage: testnode <command> [arguments]

Commands:
  agent   Run a Murmur-SQL node agent daemon
  unlock  Send encryption key to unlock an await-unlock node
  status  Check node status via HTTP service API
`)
}

func runAgent(args []string) {
	fs := flag.NewFlagSet("agent", flag.ExitOnError)
	configPath := fs.String("config", "", "Path to JSON configuration file")
	nodeIDStr := fs.String("node-id", "", "Node UUID")
	dataDir := fs.String("data-dir", "", "Data directory for Spool store and logs")
	listenAddr := fs.String("listen-addr", "127.0.0.1:7443", "Replication listen address")
	apiAddr := fs.String("api-addr", "127.0.0.1:8080", "HTTPS API listen address")
	metricsAddr := fs.String("metrics-addr", "", "JSON metrics listen address (optional, defaults to api-addr)")
	awaitUnlock := fs.Bool("await-unlock", false, "Wait for key via Remote Unlock HTTPS API")
	keyHex := fs.String("key-hex", "", "Hex-encoded 32-byte encryption key")
	keyID := fs.String("key-id", "default-key", "Encryption Key ID")
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
	_ = os.MkdirAll(filepath.Join(cfg.DataDir, "data"), 0755)

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

	log.Printf("[MURMUR] Starting node %s on repl=%s api=%s data=%s await-unlock=%v",
		nodeID, cfg.ListenAddr, cfg.APIAddr, cfg.DataDir, cfg.AwaitUnlock)

	daemon := &NodeDaemon{
		cfg:    cfg,
		nodeID: nodeID,
		dbID:   dbID,
	}

	if err := daemon.Start(); err != nil {
		log.Fatalf("[MURMUR] Failed to start node: %v", err)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	log.Printf("[MURMUR] Received signal %v, shutting down...", sig)
	daemon.Close()
	log.Printf("[MURMUR] Node shutdown complete.")
}

type NodeDaemon struct {
	originRegistry  *origin.KeyRegistry
	originMigration bool
	cfg             NodeConfigFile
	nodeID          db.NodeID
	dbID            db.DBID
	mu              sync.Mutex
	database        *db.DB
	httpServer      *http.Server
	bridge          *bridgeRuntime
}

type liveTypedRecord struct {
	ID    ids.RowID `rime:"primary" json:"id"`
	Name  string    `json:"name"`
	Count int64     `json:"count"`
	Tags  []string  `json:"tags"`
	Peak  int64     `json:"peak"`
	Floor float64   `json:"floor"`
}

type liveContentionRecord struct {
	ID    ids.RowID `rime:"primary" json:"id"`
	Name  string    `json:"name"`
	Phone string    `json:"phone"`
	Score int64     `json:"score"`
}

type liveTypedLocalRecord struct {
	ID   ids.RowID `rime:"primary" json:"id"`
	Name string    `json:"name"`
}

type liveTypedRecordV2 struct {
	ID    ids.RowID `rime:"primary" json:"id"`
	Name  string    `json:"name"`
	Count int64     `json:"count"`
	Tags  []string  `json:"tags"`
	Peak  int64     `json:"peak"`
	Floor float64   `json:"floor"`
	Note  string    `json:"note"`
}

type liveTypedRecordRegion struct {
	ID     ids.RowID `rime:"primary" json:"id"`
	Name   string    `json:"name"`
	Count  int64     `json:"count"`
	Tags   []string  `json:"tags"`
	Peak   int64     `json:"peak"`
	Floor  float64   `json:"floor"`
	Region string    `json:"region"`
}

type liveTypedRecordV4 struct {
	ID     ids.RowID `rime:"primary" json:"id"`
	Name   string    `json:"name"`
	Count  int64     `json:"count"`
	Tags   []string  `json:"tags"`
	Peak   int64     `json:"peak"`
	Floor  float64   `json:"floor"`
	Note   string    `json:"note"`
	Region string    `json:"region"`
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
	mux := http.NewServeMux()

	// Metrics endpoint
	mux.Handle("/metrics", metrics.Handler(func() db.Status {
		d.mu.Lock()
		defer d.mu.Unlock()
		if d.database == nil {
			return db.Status{}
		}
		return d.database.Status()
	}))

	// The daemon API is protected by the verified client certificate on this
	// listener. /healthz is the sole HTTPS route that permits no client cert.
	mux.HandleFunc("/v1/admin/unlock", d.handleAdminUnlock)
	mux.HandleFunc("/v1/admin/status", d.handleAdminStatus)
	mux.HandleFunc("/v1/admin/gc", d.handleAdminGC)
	mux.HandleFunc("/v1/admin/lock", d.handleAdminLock)
	mux.HandleFunc("/v1/admin/add_peer", d.handleAdminAddPeer)
	mux.HandleFunc("/v1/admin/authorize_origin", d.handleAuthorizeOrigin)
	mux.HandleFunc("/v1/admin/remove_peer", d.handleAdminRemovePeer)

	mux.HandleFunc("/v1/status", d.handleServiceStatus)
	mux.HandleFunc("/v1/schema", d.handleServiceSchema)
	mux.HandleFunc("/v1/query", d.handleServiceQuery)
	mux.HandleFunc("/v1/exec", d.handleServiceExec)
	mux.HandleFunc("/v1/typed/insert", d.handleTypedInsert)
	mux.HandleFunc("/v1/typed/contention/insert", d.handleTypedContentionInsert)
	mux.HandleFunc("/v1/typed/contention/insert-many", d.handleTypedContentionInsertMany)
	mux.HandleFunc("/v1/typed/contention/update", d.handleTypedContentionUpdate)
	mux.HandleFunc("/v1/typed/contention/read", d.handleTypedContentionRead)
	mux.HandleFunc("/v1/typed/contention/delete", d.handleTypedContentionDelete)
	mux.HandleFunc("/v1/typed/contention/all", d.handleTypedContentionAll)
	mux.HandleFunc("/v1/typed/contention/prefix-count", d.handleTypedContentionPrefixCount)
	mux.HandleFunc("/v1/typed/contention/digest", d.handleTypedContentionDigest)
	mux.HandleFunc("/v1/typed/local-insert", d.handleTypedLocalInsert)
	mux.HandleFunc("/v1/typed/local-count", d.handleTypedLocalCount)
	mux.HandleFunc("/v1/typed/explicit-tx", d.handleTypedExplicitTx)
	mux.HandleFunc("/v1/typed/migrate-v2", d.handleTypedMigrateV2)
	mux.HandleFunc("/v1/typed/migrate-region", d.handleTypedMigrateRegion)
	mux.HandleFunc("/v1/typed/migrate-v4", d.handleTypedMigrateV4)
	mux.HandleFunc("/v1/typed/set-note", d.handleTypedSetNote)
	mux.HandleFunc("/v1/typed/set-region", d.handleTypedSetRegion)
	mux.HandleFunc("/v1/typed/branch-values", d.handleTypedBranchValues)
	mux.HandleFunc("/v1/typed/rename", d.handleTypedRename)
	mux.HandleFunc("/v1/typed/note", d.handleTypedNote)
	mux.HandleFunc("/v1/typed/schema-epoch", d.handleTypedSchemaEpoch)
	mux.HandleFunc("/v1/typed/count", d.handleTypedCount)
	mux.HandleFunc("/v1/typed/enabled-names", d.handleTypedEnabledNames)
	mux.HandleFunc("/v1/typed/names", d.handleTypedNames)
	mux.HandleFunc("/v1/typed/counter-add", d.handleTypedCounterAdd)
	mux.HandleFunc("/v1/typed/counter-value", d.handleTypedCounterValue)
	mux.HandleFunc("/v1/typed/set-add", d.handleTypedSetAdd)
	mux.HandleFunc("/v1/typed/set-remove", d.handleTypedSetRemove)
	mux.HandleFunc("/v1/typed/set-values", d.handleTypedSetValues)
	mux.HandleFunc("/v1/typed/extrema-update", d.handleTypedExtremaUpdate)
	mux.HandleFunc("/v1/typed/extrema-values", d.handleTypedExtremaValues)
	mux.HandleFunc("/v1/typed/watch", d.handleTypedWatch)

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
	mux.HandleFunc("/v1/admin/rotate-key", d.handleAdminRotateKey)
	mux.HandleFunc("/v1/admin/encryption-status", d.handleAdminEncryptionStatus)
	mux.HandleFunc("/v1/debug/peers", d.handleDebugPeers)
	mux.HandleFunc("/v1/debug/receipt", d.handleDebugReceipt)
	mux.HandleFunc("/v1/debug/stacks", d.handleDebugStacks)

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
			log.Printf("[MURMUR] HTTPS API server error: %v", err)
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

	dataDir := d.cfg.DataDir
	_ = os.MkdirAll(filepath.Join(dataDir, "data"), 0755)

	dbCfg := db.Config{
		Path:   dataDir,
		NodeID: d.nodeID,
		DBID:   d.dbID,
		Spool:  db.DefaultSpoolConfig(),
		Encryption: db.EncryptionConfig{
			Key:   append([]byte(nil), key...),
			KeyID: keyID,
		},
		Replication: db.ReplicationConfig{
			ListenAddr:      d.cfg.ListenAddr,
			AllowedNetworks: append([]string(nil), d.cfg.AllowedNetworks...),
			Bootstrap:       append([]string(nil), d.cfg.Bootstrap...),
			Membership: db.MembershipConfig{
				Enabled:       d.cfg.MembershipEnabled || len(d.cfg.Bootstrap) > 0,
				Bootstrap:     append([]string(nil), d.cfg.Bootstrap...),
				AdvertiseAddr: d.cfg.ListenAddr,
			},
		},
	}
	signingKey, err := os.ReadFile(d.cfg.OriginSigningKeyFile)
	if err != nil {
		return nil, fmt.Errorf("read origin signing key: %w", err)
	}
	trusted, err := origin.NewKeyRegistry(nil)
	if err != nil {
		return nil, err
	}
	for node, raw := range d.cfg.OriginPublicKeys {
		id, err := db.ParseNodeID(node)
		if err != nil {
			return nil, err
		}
		pub, err := hex.DecodeString(raw)
		if err != nil {
			return nil, err
		}
		if err := trusted.Add(id, ed25519.PublicKey(pub)); err != nil {
			return nil, err
		}
	}
	d.originRegistry = trusted
	dbCfg.OriginSigning = db.OriginSigningConfig{PrivateKey: ed25519.PrivateKey(signingKey), TrustedKeys: trusted}
	for _, node := range d.cfg.TrustedSnapshotSources {
		id, err := db.ParseNodeID(node)
		if err != nil {
			return nil, err
		}
		dbCfg.Replication.TrustedSnapshotSources = append(dbCfg.Replication.TrustedSnapshotSources, id)
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

	// Configure native record schemas in this test-only typed mode.
	if d.cfg.TypedRecords {
		var definition db.TableDefinition
		var err error
		if d.cfg.TypedSchemaVersion >= 2 {
			definition, err = db.Define[liveTypedRecordV2]("live_typed_records", 901, db.RecordOptions{
				PrimaryField:  "ID",
				FieldIDs:      map[string]uint32{"ID": 1, "Name": 2, "Count": 3, "Tags": 4, "Peak": 5, "Floor": 6, "Note": 7},
				MergePolicies: map[string]db.RecordMergePolicy{"Count": db.RecordMergeCounter, "Tags": db.RecordMergeORSet, "Peak": db.RecordMergeMax, "Floor": db.RecordMergeMin},
			})
		} else {
			definition, err = db.Define[liveTypedRecord]("live_typed_records", 901, db.RecordOptions{
				PrimaryField:  "ID",
				FieldIDs:      map[string]uint32{"ID": 1, "Name": 2, "Count": 3, "Tags": 4, "Peak": 5, "Floor": 6},
				MergePolicies: map[string]db.RecordMergePolicy{"Count": db.RecordMergeCounter, "Tags": db.RecordMergeORSet, "Peak": db.RecordMergeMax, "Floor": db.RecordMergeMin},
			})
		}
		if err != nil {
			return nil, fmt.Errorf("define live typed record schema: %w", err)
		}
		localDefinition, err := liveTypedLocalDefinition()
		if err != nil {
			return nil, fmt.Errorf("define live node-local record schema: %w", err)
		}
		dbCfg.Schema = db.SchemaConfig{Version: 1}
		dbCfg.Tables = []db.TableDefinition{definition, localDefinition}
		if d.cfg.TypedContention {
			contentionDefinition, err := db.Define[liveContentionRecord]("live_typed_contention", 903, db.RecordOptions{
				PrimaryField: "ID",
				FieldIDs:     map[string]uint32{"ID": 1, "Name": 2, "Phone": 3, "Score": 4},
			})
			if err != nil {
				return nil, fmt.Errorf("define live typed contention schema: %w", err)
			}
			dbCfg.Tables = append(dbCfg.Tables, contentionDefinition)
		}
	} else if d.cfg.Schema != nil && len(d.cfg.Schema.Tables) > 0 {
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
	if sc := d.cfg.Spool; sc != nil {
		if sc.TargetBlockBytes > 0 {
			dbCfg.Spool.TargetBlockBytes = sc.TargetBlockBytes
		}
		if sc.MaxBlockBytes > 0 {
			dbCfg.Spool.MaxBlockBytes = sc.MaxBlockBytes
		}
		if sc.MaxPendingBytes > 0 {
			dbCfg.Spool.MaxPendingBytes = sc.MaxPendingBytes
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

	if d.originMigration {
		return nil, db.MigrateOriginBaseline(ctx, dbCfg)
	}
	instance, err := db.Open(ctx, dbCfg)
	if err != nil {
		return nil, fmt.Errorf("db.Open: %w", err)
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

	d.database = instance

	if err := d.initBridge(instance); err != nil {
		_ = instance.Close()
		d.database = nil
		return nil, fmt.Errorf("init bridge: %w", err)
	}

	log.Printf("[MURMUR] Database unlocked and online. NodeID: %s", instance.NodeID())
	return instance, nil
}

func liveTypedLocalDefinition() (db.TableDefinition, error) {
	return db.Define[liveTypedLocalRecord]("live_node_local_records", 902, db.RecordOptions{
		PrimaryField: "ID",
		FieldIDs:     map[string]uint32{"ID": 1, "Name": 2},
		Scope:        db.TableScopeNodeLocal,
	})
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
		if isUnlockAuthFailure(err) {
			// Credential failures stay generic: the wrong-key and
			// wrapping-ID errors can differ, which would make this
			// endpoint a key/key-ID oracle. The detail is logged
			// server-side for operators instead.
			log.Printf("unlock failed for node %s: %v", d.nodeID, err)
			http.Error(w, "unlock failed", http.StatusUnauthorized)
			return
		}
		http.Error(w, fmt.Sprintf("unlock failed: %v", err), http.StatusUnauthorized)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]any{"status": "ok", "unlocked": true})
}

// isUnlockAuthFailure reports whether an openDatabase error is a credential
// failure (wrong key material or wrapping-key ID) as opposed to a state
// problem (schema mismatch, corruption). Only credential failures are
// normalized to a generic response; state problems keep their detail so
// callers can classify them.
func isUnlockAuthFailure(err error) bool {
	if errors.Is(err, crypto.ErrAuth) || errors.Is(err, spool.ErrWrongKey) {
		return true
	}
	// The registry reports an unresolvable recorded key ID as
	// "storage key %q unavailable" (see crypto.Registry).
	return strings.Contains(err.Error(), "unavailable")
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

func (d *NodeDaemon) handleAdminGC(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	d.mu.Lock()
	database := d.database
	d.mu.Unlock()
	if database == nil {
		http.Error(w, "node is locked", http.StatusServiceUnavailable)
		return
	}
	if err := database.GC(r.Context()); err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]bool{"triggered": true})
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
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]bool{"updated": true})
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
		"state":            st.State.String(),
		"node_id":          st.NodeID.String(),
		"db_id":            st.DBID.String(),
		"hlc":              st.HLC,
		"state_generation": st.StateGeneration,
		"local_seq":        st.LocalSeq,
		"schema_epoch":     st.SchemaEpoch,
		"peer_count":       st.PeerCount,
		"connected_peers":  st.ConnectedPeers,
		"selected_peers":   st.SelectedPeers,
		"uptime_millis":    st.Uptime.Milliseconds(),
	})
}

func (d *NodeDaemon) handleServiceSchema(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	database := d.databaseOrLocked(w)
	if database == nil {
		return
	}
	tables, err := database.SchemaTables()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(tables)
}

func (d *NodeDaemon) handleDebugPeers(w http.ResponseWriter, r *http.Request) {
	d.mu.Lock()
	database := d.database
	d.mu.Unlock()
	if database == nil {
		http.Error(w, "node is locked", http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(database.Peers())
}

// handleDebugStacks dumps all goroutine stacks. It takes no database or
// replication locks, so it responds even when the node is deadlocked;
// forensics fetch it on divergence to capture wedged mutex holders.
func (d *NodeDaemon) handleDebugStacks(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	_ = pprof.Lookup("goroutine").WriteTo(w, 2)
}

func (d *NodeDaemon) handleServiceQuery(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "SQL query API was removed; use typed RIME records", http.StatusGone)
}

func (d *NodeDaemon) handleServiceExec(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "SQL exec API was removed; use typed RIME records", http.StatusGone)
}

func (d *NodeDaemon) handleTypedInsert(w http.ResponseWriter, r *http.Request) {
	if !d.cfg.TypedRecords {
		http.Error(w, "typed test API is disabled", http.StatusNotFound)
		return
	}
	d.mu.Lock()
	database := d.database
	d.mu.Unlock()
	if database == nil {
		http.Error(w, "node is locked", http.StatusServiceUnavailable)
		return
	}
	var req struct {
		ID    string `json:"id"`
		Name  string `json:"name"`
		Count int64  `json:"count"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Name == "" {
		http.Error(w, "name is required", http.StatusBadRequest)
		return
	}
	rowID := ids.NewRowID()
	if req.ID != "" {
		parsed, err := uuid.Parse(req.ID)
		if err != nil {
			http.Error(w, "id must be a UUID", http.StatusBadRequest)
			return
		}
		rowID = ids.RowID(parsed)
	}
	var writeErr error
	if table, err := db.TableOf[liveTypedRecord](database, "live_typed_records"); err == nil {
		row := &liveTypedRecord{ID: rowID, Name: req.Name}
		writeErr = database.WriteTxContext(r.Context(), func(tx *db.Tx) error {
			if err := table.Insert(tx, row); err != nil {
				return err
			}
			if req.Count != 0 {
				return db.RecordCounterAdd(tx, table, row.ID, "Count", req.Count)
			}
			return nil
		})
	} else if table, bindErr := db.TableOf[liveTypedRecordV2](database, "live_typed_records"); bindErr == nil {
		row := &liveTypedRecordV2{ID: rowID, Name: req.Name}
		writeErr = database.WriteTxContext(r.Context(), func(tx *db.Tx) error {
			if err := table.Insert(tx, row); err != nil {
				return err
			}
			if req.Count != 0 {
				return db.RecordCounterAdd(tx, table, row.ID, "Count", req.Count)
			}
			return nil
		})
	} else {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if writeErr != nil {
		http.Error(w, writeErr.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"id": rowID.String()})
}

func (d *NodeDaemon) handleTypedContentionInsert(w http.ResponseWriter, r *http.Request) {
	database := d.databaseOrLocked(w)
	if database == nil {
		return
	}
	var req struct {
		ID    string `json:"id"`
		Name  string `json:"name"`
		Phone string `json:"phone"`
		Score int64  `json:"score"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.ID == "" {
		http.Error(w, "id is required", http.StatusBadRequest)
		return
	}
	parsed, err := uuid.Parse(req.ID)
	if err != nil {
		http.Error(w, "id must be a UUID", http.StatusBadRequest)
		return
	}
	table, err := db.TableOf[liveContentionRecord](database, "live_typed_contention")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	row := &liveContentionRecord{ID: ids.RowID(parsed), Name: req.Name, Phone: req.Phone, Score: req.Score}
	if err := database.WriteTxContext(r.Context(), func(tx *db.Tx) error { return table.Insert(tx, row) }); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (d *NodeDaemon) handleTypedContentionInsertMany(w http.ResponseWriter, r *http.Request) {
	database := d.databaseOrLocked(w)
	if database == nil {
		return
	}
	var req struct {
		Rows []struct {
			ID    string `json:"id"`
			Name  string `json:"name"`
			Phone string `json:"phone"`
			Score int64  `json:"score"`
		} `json:"rows"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || len(req.Rows) == 0 {
		http.Error(w, "rows are required", http.StatusBadRequest)
		return
	}
	values := make([]*liveContentionRecord, 0, len(req.Rows))
	for _, item := range req.Rows {
		parsed, err := uuid.Parse(item.ID)
		if err != nil {
			http.Error(w, "each row id must be a UUID", http.StatusBadRequest)
			return
		}
		values = append(values, &liveContentionRecord{ID: ids.RowID(parsed), Name: item.Name, Phone: item.Phone, Score: item.Score})
	}
	table, err := db.TableOf[liveContentionRecord](database, "live_typed_contention")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if err := database.WriteTxContext(r.Context(), func(tx *db.Tx) error { return table.InsertMany(tx, values) }); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (d *NodeDaemon) handleTypedContentionUpdate(w http.ResponseWriter, r *http.Request) {
	database := d.databaseOrLocked(w)
	if database == nil {
		return
	}
	var req struct {
		ID    string `json:"id"`
		Field string `json:"field"`
		Value string `json:"value"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.ID == "" || req.Field == "" {
		http.Error(w, "id, field, and value are required", http.StatusBadRequest)
		return
	}
	parsed, err := uuid.Parse(req.ID)
	if err != nil {
		http.Error(w, "id must be a UUID", http.StatusBadRequest)
		return
	}
	table, err := db.TableOf[liveContentionRecord](database, "live_typed_contention")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	err = database.WriteTxContext(r.Context(), func(tx *db.Tx) error {
		return table.Update(tx, ids.RowID(parsed), func(row *liveContentionRecord) error {
			switch req.Field {
			case "name":
				row.Name = req.Value
			case "phone":
				row.Phone = req.Value
			case "score":
				var score int64
				if _, scanErr := fmt.Sscan(req.Value, &score); scanErr != nil {
					return scanErr
				}
				row.Score = score
			default:
				return fmt.Errorf("unsupported contention field %q", req.Field)
			}
			return nil
		})
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (d *NodeDaemon) handleTypedContentionRead(w http.ResponseWriter, r *http.Request) {
	database := d.databaseOrLocked(w)
	if database == nil {
		return
	}
	var req struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.ID == "" {
		http.Error(w, "id is required", http.StatusBadRequest)
		return
	}
	parsed, err := uuid.Parse(req.ID)
	if err != nil {
		http.Error(w, "id must be a UUID", http.StatusBadRequest)
		return
	}
	table, err := db.TableOf[liveContentionRecord](database, "live_typed_contention")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	row, err := table.Get(ids.RowID(parsed))
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(struct {
		ID    string `json:"id"`
		Name  string `json:"name"`
		Phone string `json:"phone"`
		Score int64  `json:"score"`
	}{ID: row.ID.String(), Name: row.Name, Phone: row.Phone, Score: row.Score})
}

func (d *NodeDaemon) handleTypedContentionDelete(w http.ResponseWriter, r *http.Request) {
	database := d.databaseOrLocked(w)
	if database == nil {
		return
	}
	var req struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.ID == "" {
		http.Error(w, "id is required", http.StatusBadRequest)
		return
	}
	parsed, err := uuid.Parse(req.ID)
	if err != nil {
		http.Error(w, "id must be a UUID", http.StatusBadRequest)
		return
	}
	table, err := db.TableOf[liveContentionRecord](database, "live_typed_contention")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if err := database.WriteTxContext(r.Context(), func(tx *db.Tx) error {
		return table.Delete(tx, ids.RowID(parsed))
	}); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (d *NodeDaemon) handleTypedContentionAll(w http.ResponseWriter, r *http.Request) {
	database := d.databaseOrLocked(w)
	if database == nil {
		return
	}
	table, err := db.TableOf[liveContentionRecord](database, "live_typed_contention")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	rows, err := table.Where().Find()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	values := make([]struct {
		ID    string `json:"id"`
		Name  string `json:"name"`
		Phone string `json:"phone"`
		Score int64  `json:"score"`
	}, len(rows))
	for i, row := range rows {
		values[i].ID, values[i].Name, values[i].Phone, values[i].Score = row.ID.String(), row.Name, row.Phone, row.Score
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(values)
}

func (d *NodeDaemon) handleTypedContentionPrefixCount(w http.ResponseWriter, r *http.Request) {
	database := d.databaseOrLocked(w)
	if database == nil {
		return
	}
	var req struct {
		Prefix string `json:"prefix"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	table, err := db.TableOf[liveContentionRecord](database, "live_typed_contention")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	count, err := table.Where(db.StringFieldOf[liveContentionRecord](table, "Name").StartsWith(req.Prefix)).Count()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]int{"count": count})
}

func (d *NodeDaemon) handleTypedContentionDigest(w http.ResponseWriter, r *http.Request) {
	database := d.databaseOrLocked(w)
	if database == nil {
		return
	}
	table, err := db.TableOf[liveContentionRecord](database, "live_typed_contention")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	rows, err := table.Where().Find()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].ID.String() < rows[j].ID.String() })
	h := sha256.New()
	for _, row := range rows {
		_, _ = fmt.Fprintf(h, "%s\\x00%s\\x00%s\\x00%d\\n", row.ID.String(), row.Name, row.Phone, row.Score)
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"count": len(rows), "digest": hex.EncodeToString(h.Sum(nil))})
}

func (d *NodeDaemon) handleTypedLocalInsert(w http.ResponseWriter, r *http.Request) {
	if !d.cfg.TypedRecords {
		http.Error(w, "typed test API is disabled", http.StatusNotFound)
		return
	}
	database := d.databaseOrLocked(w)
	if database == nil {
		return
	}
	var req struct {
		Name string `json:"name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Name == "" {
		http.Error(w, "name is required", http.StatusBadRequest)
		return
	}
	table, err := db.TableOf[liveTypedLocalRecord](database, "live_node_local_records")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	row := &liveTypedLocalRecord{ID: ids.NewRowID(), Name: req.Name}
	if err := database.WriteTxContext(r.Context(), func(tx *db.Tx) error { return table.Insert(tx, row) }); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"id": row.ID.String()})
}

func (d *NodeDaemon) handleTypedLocalCount(w http.ResponseWriter, r *http.Request) {
	if !d.cfg.TypedRecords {
		http.Error(w, "typed test API is disabled", http.StatusNotFound)
		return
	}
	database := d.databaseOrLocked(w)
	if database == nil {
		return
	}
	var req struct {
		Name string `json:"name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	table, err := db.TableOf[liveTypedLocalRecord](database, "live_node_local_records")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	count, err := table.Where(db.FieldOf[liveTypedLocalRecord, string](table, "Name").Eq(req.Name)).Count()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]int{"count": count})
}

func (d *NodeDaemon) handleTypedExplicitTx(w http.ResponseWriter, r *http.Request) {
	if !d.cfg.TypedRecords {
		http.Error(w, "typed test API is disabled", http.StatusNotFound)
		return
	}
	database := d.databaseOrLocked(w)
	if database == nil {
		return
	}
	var req struct {
		Name string `json:"name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Name == "" {
		http.Error(w, "name is required", http.StatusBadRequest)
		return
	}
	table, err := db.TableOf[liveTypedRecord](database, "live_typed_records")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	committed := &liveTypedRecord{ID: ids.NewRowID(), Name: req.Name}
	tx, err := database.BeginTx(r.Context())
	if err == nil {
		err = table.Insert(tx, committed)
	}
	if err == nil {
		err = tx.Commit()
	} else if tx != nil {
		_ = tx.Rollback()
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	rolledBack := &liveTypedRecord{ID: ids.NewRowID(), Name: req.Name + "-rolled-back"}
	tx, err = database.BeginTx(r.Context())
	if err == nil {
		err = table.Insert(tx, rolledBack)
	}
	if err == nil {
		err = tx.Rollback()
	} else if tx != nil {
		_ = tx.Rollback()
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"committed": committed.ID.String(), "rolled_back": rolledBack.ID.String()})
}

func (d *NodeDaemon) handleTypedMigrateV2(w http.ResponseWriter, r *http.Request) {
	if !d.cfg.TypedRecords {
		http.Error(w, "typed test API is disabled", http.StatusNotFound)
		return
	}
	database := d.databaseOrLocked(w)
	if database == nil {
		return
	}
	definition, err := db.Define[liveTypedRecordV2]("live_typed_records", 901, db.RecordOptions{
		PrimaryField: "ID",
		FieldIDs: map[string]uint32{
			"ID": 1, "Name": 2, "Count": 3, "Tags": 4, "Peak": 5, "Floor": 6, "Note": 7,
		},
		MergePolicies: map[string]db.RecordMergePolicy{
			"Count": db.RecordMergeCounter, "Tags": db.RecordMergeORSet,
			"Peak": db.RecordMergeMax, "Floor": db.RecordMergeMin,
		},
	})
	if err == nil {
		var local db.TableDefinition
		local, err = liveTypedLocalDefinition()
		if err == nil {
			err = installSchemaCrashForTest(database, d.cfg.TypedSchemaCrashPhase)
		}
		if err == nil {
			err = database.MigrateRecords(r.Context(), []db.TableDefinition{definition, local})
		}
	}
	if err != nil {
		http.Error(w, fmt.Sprintf("typed migration failed: %v", err), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"migrated": true, "epoch": database.Status().SchemaEpoch})
}

func (d *NodeDaemon) handleTypedMigrateRegion(w http.ResponseWriter, r *http.Request) {
	if !d.cfg.TypedRecords {
		http.Error(w, "typed test API is disabled", http.StatusNotFound)
		return
	}
	database := d.databaseOrLocked(w)
	if database == nil {
		return
	}
	definition, err := db.Define[liveTypedRecordRegion]("live_typed_records", 901, db.RecordOptions{
		PrimaryField: "ID",
		FieldIDs: map[string]uint32{
			"ID": 1, "Name": 2, "Count": 3, "Tags": 4, "Peak": 5, "Floor": 6, "Region": 8,
		},
		MergePolicies: map[string]db.RecordMergePolicy{
			"Count": db.RecordMergeCounter, "Tags": db.RecordMergeORSet,
			"Peak": db.RecordMergeMax, "Floor": db.RecordMergeMin,
		},
	})
	if err == nil {
		var local db.TableDefinition
		local, err = liveTypedLocalDefinition()
		if err == nil {
			err = database.MigrateRecords(r.Context(), []db.TableDefinition{definition, local})
		}
	}
	if err != nil {
		http.Error(w, fmt.Sprintf("typed region migration failed: %v", err), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"migrated": true, "epoch": database.Status().SchemaEpoch})
}

func (d *NodeDaemon) handleTypedMigrateV4(w http.ResponseWriter, r *http.Request) {
	if !d.cfg.TypedRecords {
		http.Error(w, "typed test API is disabled", http.StatusNotFound)
		return
	}
	database := d.databaseOrLocked(w)
	if database == nil {
		return
	}
	definition, err := db.Define[liveTypedRecordV4]("live_typed_records", 901, db.RecordOptions{
		PrimaryField: "ID",
		FieldIDs: map[string]uint32{
			"ID": 1, "Name": 2, "Count": 3, "Tags": 4, "Peak": 5, "Floor": 6, "Note": 7, "Region": 8,
		},
		MergePolicies: map[string]db.RecordMergePolicy{
			"Count": db.RecordMergeCounter, "Tags": db.RecordMergeORSet,
			"Peak": db.RecordMergeMax, "Floor": db.RecordMergeMin,
		},
	})
	if err == nil {
		var local db.TableDefinition
		local, err = liveTypedLocalDefinition()
		if err == nil {
			err = database.MigrateRecords(r.Context(), []db.TableDefinition{definition, local})
		}
	}
	if err != nil {
		http.Error(w, fmt.Sprintf("typed v4 rebind failed: %v", err), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"migrated": true, "epoch": database.Status().SchemaEpoch})
}

func (d *NodeDaemon) handleTypedSetNote(w http.ResponseWriter, r *http.Request) {
	database := d.databaseOrLocked(w)
	if database == nil {
		return
	}
	var req struct {
		Name string `json:"name"`
		Note string `json:"note"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Name == "" {
		http.Error(w, "name and note are required", http.StatusBadRequest)
		return
	}
	table, err := db.TableOf[liveTypedRecordV2](database, "live_typed_records")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	row, err := table.Where(db.FieldOf[liveTypedRecordV2, string](table, "Name").Eq(req.Name)).First()
	if err == nil {
		err = database.WriteTxContext(r.Context(), func(tx *db.Tx) error {
			return table.Update(tx, row.ID, func(value *liveTypedRecordV2) error {
				value.Note = req.Note
				return nil
			})
		})
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]bool{"updated": true})
}

func (d *NodeDaemon) handleTypedSetRegion(w http.ResponseWriter, r *http.Request) {
	database := d.databaseOrLocked(w)
	if database == nil {
		return
	}
	var req struct {
		Name   string `json:"name"`
		Region string `json:"region"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Name == "" {
		http.Error(w, "name and region are required", http.StatusBadRequest)
		return
	}
	table, err := db.TableOf[liveTypedRecordRegion](database, "live_typed_records")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	row, err := table.Where(db.FieldOf[liveTypedRecordRegion, string](table, "Name").Eq(req.Name)).First()
	if err == nil {
		err = database.WriteTxContext(r.Context(), func(tx *db.Tx) error {
			return table.Update(tx, row.ID, func(value *liveTypedRecordRegion) error {
				value.Region = req.Region
				return nil
			})
		})
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (d *NodeDaemon) handleTypedBranchValues(w http.ResponseWriter, r *http.Request) {
	database := d.databaseOrLocked(w)
	if database == nil {
		return
	}
	var req struct {
		Name string `json:"name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Name == "" {
		http.Error(w, "name is required", http.StatusBadRequest)
		return
	}
	table, err := db.TableOf[liveTypedRecordV4](database, "live_typed_records")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	row, err := table.Where(db.FieldOf[liveTypedRecordV4, string](table, "Name").Eq(req.Name)).First()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"note": row.Note, "region": row.Region})
}

func (d *NodeDaemon) handleTypedRename(w http.ResponseWriter, r *http.Request) {
	database := d.databaseOrLocked(w)
	if database == nil {
		return
	}
	var req struct {
		Name    string `json:"name"`
		NewName string `json:"new_name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Name == "" || req.NewName == "" {
		http.Error(w, "name and new_name are required", http.StatusBadRequest)
		return
	}
	table, err := db.TableOf[liveTypedRecord](database, "live_typed_records")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	row, err := table.Where(db.FieldOf[liveTypedRecord, string](table, "Name").Eq(req.Name)).First()
	if err == nil {
		err = database.WriteTxContext(r.Context(), func(tx *db.Tx) error {
			return table.Update(tx, row.ID, func(value *liveTypedRecord) error {
				value.Name = req.NewName
				return nil
			})
		})
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]bool{"updated": true})
}

func (d *NodeDaemon) handleTypedNote(w http.ResponseWriter, r *http.Request) {
	database := d.databaseOrLocked(w)
	if database == nil {
		return
	}
	var req struct {
		Name string `json:"name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Name == "" {
		http.Error(w, "name is required", http.StatusBadRequest)
		return
	}
	table, err := db.TableOf[liveTypedRecordV2](database, "live_typed_records")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	row, err := table.Where(db.FieldOf[liveTypedRecordV2, string](table, "Name").Eq(req.Name)).First()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"note": row.Note})
}

func (d *NodeDaemon) handleTypedSchemaEpoch(w http.ResponseWriter, r *http.Request) {
	database := d.databaseOrLocked(w)
	if database == nil {
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]uint64{"epoch": database.Status().SchemaEpoch})
}

func (d *NodeDaemon) handleTypedCount(w http.ResponseWriter, r *http.Request) {
	if !d.cfg.TypedRecords {
		http.Error(w, "typed test API is disabled", http.StatusNotFound)
		return
	}
	d.mu.Lock()
	database := d.database
	d.mu.Unlock()
	if database == nil {
		http.Error(w, "node is locked", http.StatusServiceUnavailable)
		return
	}
	var req struct {
		Name string `json:"name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	count := 0
	var err error
	tableV1, errV1 := db.TableOf[liveTypedRecord](database, "live_typed_records")
	if errV1 == nil {
		count, err = tableV1.Where(db.FieldOf[liveTypedRecord, string](tableV1, "Name").Eq(req.Name)).Count()
	} else {
		tableV2, err := db.TableOf[liveTypedRecordV2](database, "live_typed_records")
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		count, err = tableV2.Where(db.FieldOf[liveTypedRecordV2, string](tableV2, "Name").Eq(req.Name)).Count()
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]int{"count": count})
}

// handleTypedEnabledNames is the application-level replacement for the old
// SQL enabled_contacts view. It deliberately recomputes from managed RIME
// records so the endpoint cannot bypass Spool durability or replication.
func (d *NodeDaemon) handleTypedEnabledNames(w http.ResponseWriter, r *http.Request) {
	database := d.databaseOrLocked(w)
	if database == nil {
		return
	}
	table, err := db.TableOf[liveTypedRecord](database, "live_typed_records")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	rows, err := table.Where(db.NumericFieldOf[liveTypedRecord, int64](table, "Count").Gt(0)).
		OrderByAsc(db.StringFieldOf[liveTypedRecord](table, "Name")).Find()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	names := make([]string, 0, len(rows))
	for _, row := range rows {
		names = append(names, row.Name)
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string][]string{"names": names})
}

func (d *NodeDaemon) handleTypedNames(w http.ResponseWriter, r *http.Request) {
	database := d.databaseOrLocked(w)
	if database == nil {
		return
	}
	var names []string
	if table, err := db.TableOf[liveTypedRecord](database, "live_typed_records"); err == nil {
		rows, queryErr := table.Where().OrderByAsc(db.StringFieldOf[liveTypedRecord](table, "Name")).Find()
		if queryErr != nil {
			http.Error(w, queryErr.Error(), http.StatusInternalServerError)
			return
		}
		names = make([]string, 0, len(rows))
		for _, row := range rows {
			names = append(names, row.Name)
		}
	} else {
		table, tableErr := db.TableOf[liveTypedRecordV2](database, "live_typed_records")
		if tableErr != nil {
			http.Error(w, tableErr.Error(), http.StatusInternalServerError)
			return
		}
		rows, queryErr := table.Where().OrderByAsc(db.StringFieldOf[liveTypedRecordV2](table, "Name")).Find()
		if queryErr != nil {
			http.Error(w, queryErr.Error(), http.StatusInternalServerError)
			return
		}
		names = make([]string, 0, len(rows))
		for _, row := range rows {
			names = append(names, row.Name)
		}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string][]string{"names": names})
}

func (d *NodeDaemon) handleTypedWatch(w http.ResponseWriter, r *http.Request) {
	if !d.cfg.TypedRecords {
		http.Error(w, "typed test API is disabled", http.StatusNotFound)
		return
	}
	d.mu.Lock()
	database := d.database
	d.mu.Unlock()
	if database == nil {
		http.Error(w, "node is locked", http.StatusServiceUnavailable)
		return
	}
	var req struct {
		Name string `json:"name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Name == "" {
		http.Error(w, "name is required", http.StatusBadRequest)
		return
	}
	table, err := db.TableOf[liveTypedRecord](database, "live_typed_records")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	sub, err := table.Subscribe(r.Context(), db.RecordSubscriptionOptions{BufferSize: 4}, db.FieldOf[liveTypedRecord, string](table, "Name").Eq(req.Name))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer sub.Close()
	w.Header().Set("Content-Type", "application/x-ndjson")
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	encoder := json.NewEncoder(w)
	for {
		select {
		case event, open := <-sub.Events():
			if !open {
				return
			}
			response := map[string]any{"type": event.Type, "count": len(event.Rows)}
			if event.Err != nil {
				response["error"] = event.Err.Error()
			}
			if err := encoder.Encode(response); err != nil {
				return
			}
			flusher.Flush()
			if event.Type != db.EventInitial {
				return
			}
		case <-r.Context().Done():
			return
		}
	}
}

func (d *NodeDaemon) handleTypedCounterAdd(w http.ResponseWriter, r *http.Request) {
	if !d.cfg.TypedRecords {
		http.Error(w, "typed test API is disabled", http.StatusNotFound)
		return
	}
	d.mu.Lock()
	database := d.database
	d.mu.Unlock()
	if database == nil {
		http.Error(w, "node is locked", http.StatusServiceUnavailable)
		return
	}
	var req struct {
		Name  string `json:"name"`
		Delta int64  `json:"delta"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Name == "" {
		http.Error(w, "name and delta are required", http.StatusBadRequest)
		return
	}
	table, err := db.TableOf[liveTypedRecord](database, "live_typed_records")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	row, err := table.Where(db.FieldOf[liveTypedRecord, string](table, "Name").Eq(req.Name)).First()
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	if err := database.WriteTxContext(r.Context(), func(tx *db.Tx) error {
		return db.RecordCounterAdd(tx, table, row.ID, "Count", req.Delta)
	}); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (d *NodeDaemon) handleTypedCounterValue(w http.ResponseWriter, r *http.Request) {
	if !d.cfg.TypedRecords {
		http.Error(w, "typed test API is disabled", http.StatusNotFound)
		return
	}
	d.mu.Lock()
	database := d.database
	d.mu.Unlock()
	if database == nil {
		http.Error(w, "node is locked", http.StatusServiceUnavailable)
		return
	}
	var req struct {
		Name string `json:"name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Name == "" {
		http.Error(w, "name is required", http.StatusBadRequest)
		return
	}
	var value int64
	if table, err := db.TableOf[liveTypedRecord](database, "live_typed_records"); err == nil {
		row, queryErr := table.Where(db.FieldOf[liveTypedRecord, string](table, "Name").Eq(req.Name)).First()
		if queryErr != nil {
			http.Error(w, queryErr.Error(), http.StatusNotFound)
			return
		}
		value = row.Count
	} else {
		table, tableErr := db.TableOf[liveTypedRecordV2](database, "live_typed_records")
		if tableErr != nil {
			http.Error(w, tableErr.Error(), http.StatusInternalServerError)
			return
		}
		row, queryErr := table.Where(db.FieldOf[liveTypedRecordV2, string](table, "Name").Eq(req.Name)).First()
		if queryErr != nil {
			http.Error(w, queryErr.Error(), http.StatusNotFound)
			return
		}
		value = row.Count
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]int64{"value": value})
}

func (d *NodeDaemon) handleTypedSetAdd(w http.ResponseWriter, r *http.Request) {
	d.handleTypedSetChange(w, r, true)
}
func (d *NodeDaemon) handleTypedSetRemove(w http.ResponseWriter, r *http.Request) {
	d.handleTypedSetChange(w, r, false)
}

func (d *NodeDaemon) handleTypedSetChange(w http.ResponseWriter, r *http.Request, add bool) {
	if !d.cfg.TypedRecords {
		http.Error(w, "typed test API is disabled", http.StatusNotFound)
		return
	}
	d.mu.Lock()
	database := d.database
	d.mu.Unlock()
	if database == nil {
		http.Error(w, "node is locked", http.StatusServiceUnavailable)
		return
	}
	var req struct {
		Name  string `json:"name"`
		Value string `json:"value"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Name == "" {
		http.Error(w, "name and value are required", http.StatusBadRequest)
		return
	}
	table, err := db.TableOf[liveTypedRecord](database, "live_typed_records")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	row, err := table.Where(db.FieldOf[liveTypedRecord, string](table, "Name").Eq(req.Name)).First()
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	err = database.WriteTxContext(r.Context(), func(tx *db.Tx) error {
		if add {
			return db.RecordSetAdd(tx, table, row.ID, "Tags", req.Value)
		}
		return db.RecordSetRemove(tx, table, row.ID, "Tags", req.Value)
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (d *NodeDaemon) handleTypedSetValues(w http.ResponseWriter, r *http.Request) {
	if !d.cfg.TypedRecords {
		http.Error(w, "typed test API is disabled", http.StatusNotFound)
		return
	}
	d.mu.Lock()
	database := d.database
	d.mu.Unlock()
	if database == nil {
		http.Error(w, "node is locked", http.StatusServiceUnavailable)
		return
	}
	var req struct {
		Name string `json:"name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Name == "" {
		http.Error(w, "name is required", http.StatusBadRequest)
		return
	}
	table, err := db.TableOf[liveTypedRecord](database, "live_typed_records")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	row, err := table.Where(db.FieldOf[liveTypedRecord, string](table, "Name").Eq(req.Name)).First()
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string][]string{"values": row.Tags})
}

func (d *NodeDaemon) handleTypedExtremaUpdate(w http.ResponseWriter, r *http.Request) {
	if !d.cfg.TypedRecords {
		http.Error(w, "typed test API is disabled", http.StatusNotFound)
		return
	}
	d.mu.Lock()
	database := d.database
	d.mu.Unlock()
	if database == nil {
		http.Error(w, "node is locked", http.StatusServiceUnavailable)
		return
	}
	var req struct {
		Name  string  `json:"name"`
		Peak  int64   `json:"peak"`
		Floor float64 `json:"floor"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Name == "" {
		http.Error(w, "name and extrema values are required", http.StatusBadRequest)
		return
	}
	table, err := db.TableOf[liveTypedRecord](database, "live_typed_records")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	row, err := table.Where(db.FieldOf[liveTypedRecord, string](table, "Name").Eq(req.Name)).First()
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	err = database.WriteTxContext(r.Context(), func(tx *db.Tx) error {
		if err := db.RecordMax(tx, table, row.ID, "Peak", req.Peak); err != nil {
			return err
		}
		return db.RecordMin(tx, table, row.ID, "Floor", req.Floor)
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (d *NodeDaemon) handleTypedExtremaValues(w http.ResponseWriter, r *http.Request) {
	if !d.cfg.TypedRecords {
		http.Error(w, "typed test API is disabled", http.StatusNotFound)
		return
	}
	d.mu.Lock()
	database := d.database
	d.mu.Unlock()
	if database == nil {
		http.Error(w, "node is locked", http.StatusServiceUnavailable)
		return
	}
	var req struct {
		Name string `json:"name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Name == "" {
		http.Error(w, "name is required", http.StatusBadRequest)
		return
	}
	table, err := db.TableOf[liveTypedRecord](database, "live_typed_records")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	row, err := table.Where(db.FieldOf[liveTypedRecord, string](table, "Name").Eq(req.Name)).First()
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"peak": row.Peak, "floor": row.Floor})
}

func (d *NodeDaemon) Close() {
	if d.httpServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = d.httpServer.Shutdown(ctx)
		cancel()
	}
	d.mu.Lock()
	database := d.database
	d.database = nil
	d.mu.Unlock()
	if database != nil {
		_ = database.Close()
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
