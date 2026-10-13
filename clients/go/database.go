package murmur

import (
	"encoding/hex"
	"encoding/json"
	"sync/atomic"
)

type object struct{ id atomic.Uint64 }

func (o *object) handle() (uint64, error) {
	id := o.id.Load()
	if id == 0 {
		return 0, ErrClosed
	}
	return id, nil
}
func (o *object) call(op string, args map[string]any, out any) error {
	id, err := o.handle()
	if err != nil {
		return err
	}
	return invoke(id, op, args, out)
}
func (o *object) close(op string) error {
	id := o.id.Load()
	if id == 0 {
		return nil
	}
	err := invoke(id, op, nil, nil)
	if err == nil {
		o.id.CompareAndSwap(id, 0)
	}
	return err
}
func (o *object) set(id uint64) { o.id.Store(id) }

// DB shares one native SQLite owner with its clones. Close closes the shared database.
type DB struct{ object }

func Open(path string, key []byte, options Options) (*DB, error) {
	if len(key) != 32 {
		return nil, invalidError("master key must contain 32 bytes")
	}
	raw, err := json.Marshal(options)
	if err != nil {
		return nil, err
	}
	var opts map[string]any
	// Retain JSON integers: capacities and durations must not travel through float64.
	var data map[string]json.RawMessage
	if err = json.Unmarshal(raw, &data); err != nil {
		return nil, err
	}
	opts = map[string]any{}
	for k, v := range data {
		opts[k] = v
	}
	var progressToken, faultToken uint64
	var progressState *callbackState
	if options.OnOpenProgress != nil {
		progressToken, progressState = newCallback(func(op uint32, _ uint64, b []byte) ([]byte, error) {
			if op != 2 {
				return nil, invalidError("progress callback opcode")
			}
			var p OpenProgress
			if err := decodeJSON(b, &p); err != nil {
				return nil, err
			}
			options.OnOpenProgress(p)
			return nil, nil
		})
		opts["progress_callback"] = progressToken
	}
	if options.FaultHooks != nil {
		faultToken, _ = newCallback(func(op uint32, _ uint64, b []byte) ([]byte, error) {
			if op != 11 {
				return nil, invalidError("fault callback opcode")
			}
			var name string
			if err := json.Unmarshal(b, &name); err != nil {
				return nil, err
			}
			return nil, options.FaultHooks.Trip(name)
		})
		opts["fault_callback"] = faultToken
	}
	var id uint64
	err = invoke(0, "open", map[string]any{"path": path, "key": Bytes(key), "options": opts}, &id)
	if err != nil {
		releaseCallback(progressToken)
		releaseCallback(faultToken)
		return nil, err
	}
	db := &DB{}
	db.set(id)
	if progressState != nil && progressState.failure() != nil {
		_ = db.Close()
		return nil, progressState.failure()
	}
	return db, nil
}
func (d *DB) Clone() (*DB, error) {
	var id uint64
	if err := d.call("clone", nil, &id); err != nil {
		return nil, err
	}
	other := &DB{}
	other.set(id)
	return other, nil
}
func (d *DB) Close() error { return d.object.close("close") }

// Release drops this handle without closing other clones. Always Close the database explicitly.
func (d *DB) Release() error { return d.object.close("release") }
func runSQL(o *object, op string, fn func(*Sql) error) error {
	if fn == nil {
		return invalidError("nil SQL callback")
	}
	id, err := o.handle()
	if err != nil {
		return err
	}
	return callbackInvoke(id, op, nil, func(code uint32, scope uint64, _ []byte) ([]byte, error) {
		if code != 1 {
			return nil, invalidError("SQL callback opcode")
		}
		sql := scopedSQL(scope)
		defer func() { sql.alive.Store(false) }()
		return nil, fn(sql)
	}, nil)
}
func (d *DB) Read(fn func(*Sql) error) error        { return runSQL(&d.object, "read", fn) }
func (d *DB) Transaction(fn func(*Sql) error) error { return runSQL(&d.object, "transaction", fn) }
func (d *DB) Exec(query string, args ...any) (Result, error) {
	var result Result
	err := d.Transaction(func(sql *Sql) error { var err error; result, err = sql.Exec(query, args...); return err })
	return result, err
}

// Query materializes an owned result. Use Read with Sql.Query to iterate without buffering all rows.
func (d *DB) Query(query string, args ...any) (QueryResult, error) {
	var result QueryResult
	err := d.Read(func(sql *Sql) error {
		rows, err := sql.Query(query, args...)
		if err != nil {
			return err
		}
		defer rows.Close()
		result.Columns = rows.Columns()
		for rows.Next() {
			result.Rows = append(result.Rows, rows.Values())
		}
		return rows.Err()
	})
	return result, err
}

type QueryResult struct {
	Columns []string
	Rows    [][]Value
}

func (d *DB) Migrate(m Migration) error {
	return d.call("migrate", map[string]any{"sql": m.SQL, "mappings": nonNil(m.Mappings)}, nil)
}
func (d *DB) Flush() error          { return d.call("flush", nil, nil) }
func (d *DB) RefreshIndexes() error { return d.call("refresh_indexes", nil, nil) }
func (d *DB) SetSearchIndexes(indexes []SearchIndex) error {
	return d.call("set_search_indexes", map[string]any{"indexes": nonNil(indexes)}, nil)
}
func (d *DB) SetReplication(table string, enabled bool) error {
	return d.call("set_replication", map[string]any{"table": table, "enabled": enabled}, nil)
}
func (d *DB) ReplicationEnabled(table string) (bool, error) {
	var b bool
	err := d.call("replication_enabled", map[string]any{"table": table}, &b)
	return b, err
}
func (d *DB) Backup(destination string) error {
	return d.call("backup", map[string]any{"destination": destination}, nil)
}
func Restore(backup, destination string, key []byte, options RestoreOptions) error {
	return invoke(0, "restore", map[string]any{"backup": backup, "destination": destination, "key": Bytes(key), "new_database_id": options.NewDatabaseID, "new_node_id": options.NewNodeID}, nil)
}
func VerifyBackup(path string, key []byte) error {
	return invoke(0, "verify_backup", map[string]any{"path": path, "key": Bytes(key)}, nil)
}
func (d *DB) RotateDataKey() error { return d.call("rotate_data_key", nil, nil) }
func (d *DB) RotateMasterKey(key Key) error {
	return d.call("rotate_master_key", map[string]any{"key": key}, nil)
}
func (d *DB) RetryStorage() error { return d.call("retry_storage", nil, nil) }
func (d *DB) MergeState(state ReplicaState) error {
	return d.call("merge_state", map[string]any{"state": state}, nil)
}
func (d *DB) RetirePeer(node NodeID) error {
	return d.call("retire_peer", map[string]any{"node": node}, nil)
}
func nonNil[T any](v []T) []T {
	if v == nil {
		return []T{}
	}
	return v
}
func (f *FaultHooks) Trip(name string) error {
	if f == nil {
		return nil
	}
	var hook func() error
	switch name {
	case "append":
		hook = f.Append
	case "sync":
		hook = f.SegmentSync
	case "manifest":
		hook = f.ManifestPersist
	case "keyring":
		hook = f.KeyringPersist
	case "intent":
		hook = f.IntentPersist
	case "compact":
		hook = f.CompactionStage
	case "checkpoint":
		hook = f.CheckpointLink
	case "rename":
		hook = f.Rename
	case "dirsync":
		hook = f.DirSync
	case "delete":
		hook = f.Delete
	default:
		return nil
	}
	if hook != nil {
		return hook()
	}
	return nil
}
func NodeIDFromHex(s string) (NodeID, error) {
	var id NodeID
	bytes, err := hex.DecodeString(s)
	if err != nil {
		return id, err
	}
	if len(bytes) != len(id) {
		return id, invalidError("node ID must contain 32 hex characters")
	}
	copy(id[:], bytes)
	return id, nil
}

func (d *DB) Status() (Status, error) {
	var value Status
	err := d.call("status", nil, &value)
	return value, err
}

func (d *DB) OpenProgress() (*OpenProgress, error) {
	var value *OpenProgress
	err := d.call("open_progress", nil, &value)
	return value, err
}

func (d *DB) SQLiteMetrics() (SqliteMetrics, error) {
	var value SqliteMetrics
	err := d.call("sqlite_metrics", nil, &value)
	return value, err
}

func (d *DB) SpoolMetrics() (SpoolMetrics, error) {
	var value SpoolMetrics
	err := d.call("spool_metrics", nil, &value)
	return value, err
}

func (d *DB) Schema() (Schema, error) {
	var value Schema
	err := d.call("schema", nil, &value)
	return value, err
}

func (d *DB) DatabaseID() (NodeID, error) {
	var value NodeID
	err := d.call("database_id", nil, &value)
	return value, err
}

func (d *DB) NodeID() (NodeID, error) {
	var value NodeID
	err := d.call("node_id", nil, &value)
	return value, err
}

func (d *DB) KeyInventory() (KeyInventory, error) {
	var value KeyInventory
	err := d.call("key_inventory", nil, &value)
	return value, err
}

func (d *DB) ExportState() (ReplicaState, error) {
	var value ReplicaState
	err := d.call("export_state", nil, &value)
	return value, err
}

func (d *DB) RetentionStatus() (RetentionStatus, error) {
	var value RetentionStatus
	err := d.call("retention_status", nil, &value)
	return value, err
}

func (d *DB) CollectRetention() (uint64, error) {
	var value uint64
	err := d.call("collect_retention", nil, &value)
	return value, err
}

// Database is the full Rust Database equivalent; DB is its idiomatic Go name.
type Database = DB
