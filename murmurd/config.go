package murmurd

import (
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/pelletier/go-toml/v2"

	db "github.com/marcgauthier/spedsql"
	"github.com/marcgauthier/spedsql/schema"
)

// ExampleConfig is the documented config.example.toml, embedded for
// --generate-config.
//
//go:embed config.example.toml
var ExampleConfig string

// Config is the murmurd daemon configuration, loaded from config.toml.
// See config.example.toml for a documented example.
type Config struct {
	Murmur      MurmurConfig      `toml:"murmur"`
	MySQL       MySQLConfig       `toml:"mysql"`
	Postgres    PostgresConfig    `toml:"postgres"`
	Replication ReplicationConfig `toml:"replication"`
	Schema      SchemaConfig      `toml:"schema"`

	// schemaFileTables is the parsed [schema] file, loaded by
	// LoadConfig. It is the daemon's only schema source.
	schemaFileTables []schema.TableSchema
}

// MurmurConfig carries engine identity, storage, and encryption.
type MurmurConfig struct {
	DataDir string `toml:"data_dir"`
	// NodeID and DBID are optional: when empty they load from (or are
	// generated into) data_dir/node.json, so restarts keep a stable
	// identity. When set they must match a persisted node.json.
	NodeID string `toml:"node_id"`
	DBID   string `toml:"db_id"`

	Encryption EncryptionConfig `toml:"encryption"`
}

// EncryptionConfig carries the storage wrapping key. Exactly one of
// KeyHex and KeyFile must be set.
type EncryptionConfig struct {
	KeyID   string `toml:"key_id"`
	KeyHex  string `toml:"key_hex"`
	KeyFile string `toml:"key_file"`
}

// MySQLConfig carries the go-mysql-server frontend settings.
type MySQLConfig struct {
	Enable   bool   `toml:"enable"`
	Addr     string `toml:"addr"`
	Database string `toml:"database"`
}

// PostgresConfig carries the psql-wire frontend settings.
type PostgresConfig struct {
	Enable   bool   `toml:"enable"`
	Addr     string `toml:"addr"`
	Database string `toml:"database"`
}

// ReplicationConfig carries mesh settings. Replication stays disabled
// when Addr is empty and no peers/bootstrap entries exist; otherwise
// the TLS files are required.
type ReplicationConfig struct {
	Addr            string       `toml:"addr"`
	CACertFile      string       `toml:"ca_cert_file"`
	NodeCertFile    string       `toml:"node_cert_file"`
	NodeKeyFile     string       `toml:"node_key_file"`
	Bootstrap       []string     `toml:"bootstrap"`
	AllowedNetworks []string     `toml:"allowed_networks"`
	Peers           []PeerConfig `toml:"peers"`
}

// PeerConfig is one statically configured peer.
type PeerConfig struct {
	NodeID string   `toml:"node_id"`
	Addrs  []string `toml:"addrs"`
}

// SchemaConfig points at the user-provided schema.sql. The file is
// the daemon's only schema source: it bootstraps a fresh data_dir,
// and on every boot tables/columns it declares but the live schema
// lacks migrate in. Later DDL also goes through CREATE TABLE
// (additive only; murmur forbids destructive schema changes).
type SchemaConfig struct {
	// File is the schema.sql path, required. Relative paths
	// resolve from the config file's directory.
	File string `toml:"file"`
	// Tables is embedded-only: [[schema.tables]] in a daemon
	// config is rejected, use the schema file instead.
	Tables []TableConfig `toml:"tables"`
}

// TableConfig declares one replicated table.
type TableConfig struct {
	Name    string         `toml:"name"`
	Columns []ColumnConfig `toml:"columns"`
}

// ColumnConfig declares one column. Type is one of integer, real,
// text, blob. The primary key is always the column named "id" (the
// engine requires a single BLOB NOT NULL key); pk = true on any other
// column is rejected.
type ColumnConfig struct {
	Name     string `toml:"name"`
	Type     string `toml:"type"`
	Nullable bool   `toml:"nullable"`
	PK       bool   `toml:"pk"`
}

const nodeIdentityFile = "node.json"

// LoadConfig reads and validates path, resolves the node identity,
// and returns the daemon configuration.
func LoadConfig(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("murmurd: read config %s: %w", path, err)
	}
	var cfg Config
	// Defaults for the frontends; everything else is explicit.
	cfg.MySQL.Enable = true
	cfg.MySQL.Addr = "127.0.0.1:3306"
	cfg.MySQL.Database = "murmur"
	cfg.Postgres.Enable = true
	cfg.Postgres.Addr = "127.0.0.1:5432"
	cfg.Postgres.Database = "murmur"
	if err := toml.Unmarshal(raw, &cfg); err != nil {
		return nil, fmt.Errorf("murmurd: parse config %s: %w", path, err)
	}
	if err := cfg.loadSchemaFile(path); err != nil {
		return nil, err
	}
	if err := cfg.resolveIdentity(); err != nil {
		return nil, err
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// Validate checks the configuration without touching the data dir.
func (c *Config) Validate() error {
	if c.Murmur.DataDir == "" {
		return fmt.Errorf("murmurd: murmur.data_dir is required")
	}
	if c.Murmur.Encryption.KeyID == "" {
		return fmt.Errorf("murmurd: murmur.encryption.key_id is required")
	}
	if _, err := c.storageKey(); err != nil {
		return err
	}
	if !c.MySQL.Enable && !c.Postgres.Enable {
		return fmt.Errorf("murmurd: at least one of mysql/postgres frontends must be enabled")
	}
	if c.MySQL.Enable {
		if c.MySQL.Addr == "" || c.MySQL.Database == "" {
			return fmt.Errorf("murmurd: mysql.addr and mysql.database are required")
		}
	}
	if c.Postgres.Enable {
		if c.Postgres.Addr == "" || c.Postgres.Database == "" {
			return fmt.Errorf("murmurd: postgres.addr and postgres.database are required")
		}
	}
	if err := c.validateReplication(); err != nil {
		return err
	}
	if len(c.Schema.Tables) > 0 {
		return fmt.Errorf("murmurd: [[schema.tables]] is embedded-only; the daemon takes its schema from [schema] file (schema.sql)")
	}
	if c.Schema.File == "" {
		return fmt.Errorf("murmurd: [schema] file is required (the engine requires a replicated table at open; add more later with CREATE TABLE or by editing the file)")
	}
	tables, err := c.desiredTables()
	if err != nil {
		return err
	}
	if len(tables) == 0 {
		return fmt.Errorf("murmurd: schema file %q declares no tables", c.Schema.File)
	}
	return nil
}

// loadSchemaFile reads and parses [schema] file. Relative paths
// resolve from the config file's directory. It runs before
// Validate so validation sees the parsed declaration.
func (c *Config) loadSchemaFile(configPath string) error {
	if c.Schema.File == "" {
		return nil
	}
	path := c.Schema.File
	if !filepath.IsAbs(path) {
		path = filepath.Join(filepath.Dir(configPath), path)
	}
	tables, err := parseSchemaFile(path)
	if err != nil {
		return err
	}
	c.schemaFileTables = tables
	return nil
}

func (c *Config) validateReplication() error {
	r := &c.Replication
	mesh := r.Addr != "" || len(r.Peers) > 0 || len(r.Bootstrap) > 0
	if !mesh {
		return nil
	}
	missing := []string{}
	if r.CACertFile == "" {
		missing = append(missing, "ca_cert_file")
	}
	if r.NodeCertFile == "" {
		missing = append(missing, "node_cert_file")
	}
	if r.NodeKeyFile == "" {
		missing = append(missing, "node_key_file")
	}
	if len(missing) > 0 {
		return fmt.Errorf("murmurd: replication %s required when meshing (addr/peers/bootstrap set)", strings.Join(missing, ", "))
	}
	for i, p := range r.Peers {
		if _, err := db.ParseNodeID(p.NodeID); err != nil {
			return fmt.Errorf("murmurd: replication.peers[%d].node_id: %w", i, err)
		}
		if len(p.Addrs) == 0 {
			return fmt.Errorf("murmurd: replication.peers[%d] needs at least one addr", i)
		}
	}
	return nil
}

// storageKey loads the wrapping key from hex or file. It must be a
// valid storage-key length (16, 24, or 32 bytes).
func (c *Config) storageKey() ([]byte, error) {
	e := &c.Murmur.Encryption
	hexStr := strings.TrimSpace(e.KeyHex)
	if e.KeyFile != "" {
		if hexStr != "" {
			return nil, fmt.Errorf("murmurd: set exactly one of murmur.encryption.key_hex and key_file")
		}
		raw, err := os.ReadFile(e.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("murmurd: read key file: %w", err)
		}
		hexStr = strings.TrimSpace(string(raw))
	}
	if hexStr == "" {
		return nil, fmt.Errorf("murmurd: murmur.encryption.key_hex or key_file is required")
	}
	key, err := hex.DecodeString(hexStr)
	if err != nil {
		return nil, fmt.Errorf("murmurd: key_hex is not valid hex: %w", err)
	}
	switch len(key) {
	case 16, 24, 32:
		return key, nil
	default:
		return nil, fmt.Errorf("murmurd: storage key must be 16, 24, or 32 bytes, got %d", len(key))
	}
}

// desiredTables returns a copy of the schema-file declaration. IDs
// stay zero for deterministic derivation by the engine.
func (c *Config) desiredTables() ([]schema.TableSchema, error) {
	out := make([]schema.TableSchema, 0, len(c.schemaFileTables))
	for _, t := range c.schemaFileTables {
		cp := t
		cp.Columns = append([]schema.ColumnSchema(nil), t.Columns...)
		out = append(out, cp)
	}
	return out, nil
}

func columnType(s string) (schema.ColumnType, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "integer", "int", "bigint", "smallint":
		return schema.ColInteger, nil
	case "real", "float", "double":
		return schema.ColReal, nil
	case "text", "varchar", "char", "string":
		return schema.ColText, nil
	case "blob", "binary", "varbinary", "bytea":
		return schema.ColBlob, nil
	default:
		return 0, fmt.Errorf("unknown column type %q (want integer, real, text, or blob)", s)
	}
}

// nodeIdentity is the persisted stable identity.
type nodeIdentity struct {
	NodeID string `json:"node_id"`
	DBID   string `json:"db_id"`
}

// resolveIdentity loads data_dir/node.json, generates it on first
// boot, and reconciles it with any configured IDs. Configured IDs
// win on first boot and must match afterwards (fail closed).
func (c *Config) resolveIdentity() error {
	if c.Murmur.DataDir == "" {
		return fmt.Errorf("murmurd: murmur.data_dir is required")
	}
	path := filepath.Join(c.Murmur.DataDir, nodeIdentityFile)
	raw, err := os.ReadFile(path)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("murmurd: read %s: %w", path, err)
	}
	if os.IsNotExist(err) {
		nodeID := strings.TrimSpace(c.Murmur.NodeID)
		if nodeID == "" {
			nodeID = db.NewNodeID().String()
		} else if _, err := db.ParseNodeID(nodeID); err != nil {
			return fmt.Errorf("murmurd: murmur.node_id: %w", err)
		}
		dbID := strings.TrimSpace(c.Murmur.DBID)
		if dbID == "" {
			dbID = db.NewDBID().String()
		} else if _, err := db.ParseDBID(dbID); err != nil {
			return fmt.Errorf("murmurd: murmur.db_id: %w", err)
		}
		if err := os.MkdirAll(c.Murmur.DataDir, 0o755); err != nil {
			return fmt.Errorf("murmurd: create data dir: %w", err)
		}
		out, _ := json.MarshalIndent(nodeIdentity{NodeID: nodeID, DBID: dbID}, "", "  ")
		if err := os.WriteFile(path, append(out, '\n'), 0o644); err != nil {
			return fmt.Errorf("murmurd: persist %s: %w", path, err)
		}
		c.Murmur.NodeID, c.Murmur.DBID = nodeID, dbID
		return nil
	}
	var saved nodeIdentity
	if err := json.Unmarshal(raw, &saved); err != nil {
		return fmt.Errorf("murmurd: parse %s: %w", path, err)
	}
	if c.Murmur.NodeID != "" && c.Murmur.NodeID != saved.NodeID {
		return fmt.Errorf("murmurd: configured node_id %s != persisted %s (refusing to change identity)", c.Murmur.NodeID, saved.NodeID)
	}
	if c.Murmur.DBID != "" && c.Murmur.DBID != saved.DBID {
		return fmt.Errorf("murmurd: configured db_id %s != persisted %s (refusing to change identity)", c.Murmur.DBID, saved.DBID)
	}
	if _, err := db.ParseNodeID(saved.NodeID); err != nil {
		return fmt.Errorf("murmurd: persisted node_id invalid: %w", err)
	}
	if _, err := db.ParseDBID(saved.DBID); err != nil {
		return fmt.Errorf("murmurd: persisted db_id invalid: %w", err)
	}
	c.Murmur.NodeID, c.Murmur.DBID = saved.NodeID, saved.DBID
	return nil
}

// murmurConfig converts the daemon config to the embedded engine config.
func (c *Config) murmurConfig() (db.Config, error) {
	nodeID, err := db.ParseNodeID(c.Murmur.NodeID)
	if err != nil {
		return db.Config{}, fmt.Errorf("murmurd: node_id: %w", err)
	}
	dbID, err := db.ParseDBID(c.Murmur.DBID)
	if err != nil {
		return db.Config{}, fmt.Errorf("murmurd: db_id: %w", err)
	}
	key, err := c.storageKey()
	if err != nil {
		return db.Config{}, err
	}
	tables, err := c.desiredTables()
	if err != nil {
		return db.Config{}, err
	}
	cfg := db.Config{
		Path:   filepath.Join(c.Murmur.DataDir, "pebble"),
		NodeID: nodeID,
		DBID:   dbID,
		Encryption: db.EncryptionConfig{
			Key:   key,
			KeyID: c.Murmur.Encryption.KeyID,
		},
		Schema: db.SchemaConfig{Version: 1, Tables: tables},
	}
	r := &c.Replication
	mesh := r.Addr != "" || len(r.Peers) > 0 || len(r.Bootstrap) > 0
	if mesh {
		caPEM, err := os.ReadFile(r.CACertFile)
		if err != nil {
			return db.Config{}, fmt.Errorf("murmurd: read ca_cert_file: %w", err)
		}
		certPEM, err := os.ReadFile(r.NodeCertFile)
		if err != nil {
			return db.Config{}, fmt.Errorf("murmurd: read node_cert_file: %w", err)
		}
		keyPEM, err := os.ReadFile(r.NodeKeyFile)
		if err != nil {
			return db.Config{}, fmt.Errorf("murmurd: read node_key_file: %w", err)
		}
		cfg.Replication.ListenAddr = r.Addr
		cfg.Replication.TLS = &db.TLSCredential{CertPEM: certPEM, KeyPEM: keyPEM, CAPEM: caPEM}
		cfg.Replication.Bootstrap = append([]string(nil), r.Bootstrap...)
		cfg.Replication.AllowedNetworks = append([]string(nil), r.AllowedNetworks...)
		for _, p := range r.Peers {
			id, err := db.ParseNodeID(p.NodeID)
			if err != nil {
				return db.Config{}, fmt.Errorf("murmurd: peer node_id: %w", err)
			}
			cfg.Replication.Peers = append(cfg.Replication.Peers, db.Peer{NodeID: id, Addrs: append([]string(nil), p.Addrs...)})
		}
	}
	return cfg, nil
}
