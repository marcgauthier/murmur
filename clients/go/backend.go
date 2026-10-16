package murmur

import (
	"bytes"
	"encoding/json"
	"sort"
)

type Record struct {
	Key   Bytes  `json:"key"`
	Value *Bytes `json:"value"`
}

func PutRecord(key, value []byte) Record {
	v := Bytes(append([]byte{}, value...))
	return Record{Key: append([]byte{}, key...), Value: &v}
}
func DeleteRecord(key []byte) Record { return Record{Key: append([]byte{}, key...)} }

type Content struct {
	SQLite  *Bytes   `json:"sqlite"`
	Records []Record `json:"records"`
	Origin  NodeID   `json:"origin"`
}

// Backend methods may run concurrently on Rust background threads. Implementations
// must serialize authoritative storage access and honor Rust's durability contract.
type Backend interface {
	Identity() (NodeID, NodeID)
	Get([]byte) ([]byte, bool, error)
	Keys([]byte) ([][]byte, error)
	Commit([]Record) error
	ImportBridge(BridgeHeader, []byte) (bool, error)
	ReleaseOwnership(string, []byte, *string) (bool, error)
	Publish([]Record) error
	Snapshot() ([]byte, error)
	SnapshotWithProgress() ([]byte, []PeerProgress, error)
	CreateSnapshot(NodeID, ReplicationConfig) (SnapshotManifest, error)
	AcknowledgeSnapshot(NodeID, NodeID, Key) error
	RetentionCheckpoint() error
	PruneHistory(NodeID, uint64) (uint64, error)
	ReclaimRetention() error
	Schema() ([]byte, error)
	Apply([][]byte, []Record) (bool, error)
	Observe(uint64, uint32) error
	AcceptSchema([]byte) (bool, error)
}

// BatchBackend optionally reads an ordered prefix in one storage operation.
// A nil budget requires one result per key; a non-nil budget permits a shorter
// prefix and one oversized first value. A nil result entry means missing;
// a pointer to empty Bytes represents an existing empty value.
type BatchBackend interface {
	GetMany(keys [][]byte, maxBytes *uint64) ([]*Bytes, error)
}

// PagedBackend optionally provides bounded traversal of authoritative keys.
// CancellableBackend optionally interrupts pending waits during replication close.
type CancellableBackend interface{ CancelPending() }

type PagedBackend interface {
	KeysPage(prefix, after []byte, limit uint64) ([][]byte, error)
}
type BackendHandle struct{ object }

func RegisterBackend(backend Backend) (*BackendHandle, error) {
	if backend == nil {
		return nil, invalidError("nil backend")
	}
	token, _ := newCallback(func(op uint32, _ uint64, b []byte) ([]byte, error) {
		if op != 10 {
			return nil, invalidError("backend callback")
		}
		var request struct {
			Method string          `json:"method"`
			Args   json.RawMessage `json:"args"`
		}
		if err := json.Unmarshal(b, &request); err != nil {
			return nil, err
		}
		result, err := backendCall(backend, request.Method, request.Args)
		if err != nil {
			return nil, err
		}
		return json.Marshal(result)
	})
	var id uint64
	if err := invoke(0, "backend_new", map[string]any{"callback": token}, &id); err != nil {
		releaseCallback(token)
		return nil, err
	}
	h := &BackendHandle{}
	h.set(id)
	return h, nil
}
func (h *BackendHandle) Close() error { return h.object.close("close") }
func (h *BackendHandle) BindSigner(config ReplicationConfig) error {
	return h.call("backend_bind_signer", map[string]any{"config": config}, nil)
}
func backendCall(backend Backend, method string, data []byte) (any, error) {
	var a struct {
		Key       Bytes             `json:"key"`
		Prefix    Bytes             `json:"prefix"`
		Records   []Record          `json:"records"`
		Header    BridgeHeader      `json:"header"`
		Payload   Bytes             `json:"payload"`
		Table     string            `json:"table"`
		Column    *string           `json:"column"`
		Recipient NodeID            `json:"recipient"`
		Config    ReplicationConfig `json:"config"`
		Transfer  NodeID            `json:"transfer"`
		Digest    Key               `json:"digest"`
		Origin    NodeID            `json:"origin"`
		Through   uint64            `json:"through"`
		Payloads  []Bytes           `json:"payloads"`
		Millis    uint64            `json:"millis"`
		Logical   uint32            `json:"logical"`
		Schema    Bytes             `json:"schema"`
	}
	if string(data) != "null" {
		if err := json.Unmarshal(data, &a); err != nil {
			return nil, err
		}
	}
	switch method {
	case "cancel_pending":
		if cancellable, ok := backend.(CancellableBackend); ok {
			cancellable.CancelPending()
		}
		return nil, nil
	case "identity":
		db, node := backend.Identity()
		return [2]NodeID{db, node}, nil
	case "get":
		b, found, err := backend.Get(a.Key)
		if !found {
			return nil, err
		}
		return Bytes(b), err
	case "get_many":
		var a struct {
			Keys     []Bytes `json:"keys"`
			MaxBytes *uint64 `json:"max_bytes"`
		}
		if err := json.Unmarshal(data, &a); err != nil {
			return nil, err
		}
		keys := make([][]byte, len(a.Keys))
		for i, key := range a.Keys {
			keys[i] = key
		}
		var values []*Bytes
		if batched, ok := backend.(BatchBackend); ok {
			var err error
			values, err = batched.GetMany(keys, a.MaxBytes)
			if err != nil {
				return nil, err
			}
		} else {
			var total uint64
			values = make([]*Bytes, 0, len(keys))
			for _, key := range keys {
				value, found, err := backend.Get(key)
				if err != nil {
					return nil, err
				}
				var item *Bytes
				var size uint64
				if found {
					b := Bytes(value)
					item = &b
					size = uint64(len(value))
				}
				if a.MaxBytes != nil && len(values) > 0 && size > *a.MaxBytes-total {
					break
				}
				total += size
				values = append(values, item)
				if a.MaxBytes != nil && total >= *a.MaxBytes {
					break
				}
			}
		}
		if len(values) > len(keys) || (a.MaxBytes == nil && len(values) != len(keys)) {
			return nil, invalidError("GetMany returned wrong number of values")
		}
		if len(keys) > 0 && a.MaxBytes != nil && len(values) == 0 {
			return nil, invalidError("GetMany returned no progress")
		}
		if a.MaxBytes != nil && len(values) > 1 {
			var total uint64
			for _, value := range values {
				if value != nil {
					size := uint64(len(*value))
					if size > *a.MaxBytes-total {
						return nil, invalidError("GetMany exceeded byte budget")
					}
					total += size
				}
			}
		}
		if values == nil {
			values = make([]*Bytes, 0)
		}
		return values, nil
	case "keys_page":
		var page struct {
			Prefix Bytes  `json:"prefix"`
			After  Bytes  `json:"after"`
			Limit  uint64 `json:"limit"`
		}
		if err := json.Unmarshal(data, &page); err != nil {
			return nil, err
		}
		var keys [][]byte
		var err error
		if paged, ok := backend.(PagedBackend); ok {
			keys, err = paged.KeysPage(page.Prefix, page.After, page.Limit)
		} else {
			keys, err = backend.Keys(page.Prefix)
			sort.Slice(keys, func(i, j int) bool { return bytes.Compare(keys[i], keys[j]) < 0 })
			filtered := make([][]byte, 0)
			for _, key := range keys {
				if page.After == nil || bytes.Compare(key, page.After) > 0 {
					if uint64(len(filtered)) >= page.Limit {
						break
					}
					filtered = append(filtered, key)
				}
			}
			keys = filtered
		}
		out := make([]Bytes, len(keys))
		for i, key := range keys {
			out[i] = key
		}
		return out, err
	case "keys":
		keys, err := backend.Keys(a.Prefix)
		out := make([]Bytes, len(keys))
		for i, k := range keys {
			out[i] = k
		}
		return out, err
	case "commit":
		return nil, backend.Commit(a.Records)
	case "import_bridge":
		return backend.ImportBridge(a.Header, a.Payload)
	case "release_ownership":
		return backend.ReleaseOwnership(a.Table, a.Key, a.Column)
	case "publish":
		return nil, backend.Publish(a.Records)
	case "snapshot":
		b, err := backend.Snapshot()
		return Bytes(b), err
	case "snapshot_with_progress":
		b, p, err := backend.SnapshotWithProgress()
		return []any{Bytes(b), nonNil(p)}, err
	case "create_snapshot":
		return backend.CreateSnapshot(a.Recipient, a.Config)
	case "acknowledge_snapshot":
		return nil, backend.AcknowledgeSnapshot(a.Recipient, a.Transfer, a.Digest)
	case "retention_checkpoint":
		return nil, backend.RetentionCheckpoint()
	case "prune_history":
		return backend.PruneHistory(a.Origin, a.Through)
	case "reclaim_retention":
		return nil, backend.ReclaimRetention()
	case "schema":
		b, err := backend.Schema()
		return Bytes(b), err
	case "apply":
		payloads := make([][]byte, len(a.Payloads))
		for i, b := range a.Payloads {
			payloads[i] = b
		}
		return backend.Apply(payloads, a.Records)
	case "observe":
		return nil, backend.Observe(a.Millis, a.Logical)
	case "accept_schema":
		return backend.AcceptSchema(a.Schema)
	}
	return nil, invalidError("unknown backend callback")
}

// BackendDefaults supplies Rust-equivalent defaults for optional snapshot hooks.
// Embed it only alongside an implementation of the required Backend methods.
type BackendDefaults struct{}

func (BackendDefaults) CreateSnapshot(NodeID, ReplicationConfig) (SnapshotManifest, error) {
	return SnapshotManifest{}, invalidError("durable snapshots unsupported by backend")
}
func (BackendDefaults) AcknowledgeSnapshot(NodeID, NodeID, Key) error {
	return invalidError("durable snapshots unsupported by backend")
}

// Progress returns durable per-origin watermarks using the Rust backend contract.
func (h *BackendHandle) Progress() ([]PeerProgress, error) {
	id, err := h.handle()
	if err != nil {
		return nil, err
	}
	var progress []PeerProgress
	err = invoke(0, "backend_progress", map[string]any{"backend": id}, &progress)
	return progress, err
}

// GetMany reads keys in input order; nil entries indicate missing records.
func (h *BackendHandle) GetMany(keys [][]byte) ([]*Bytes, error) { return h.getMany(keys, nil) }

// GetManyBounded reads a prefix, allowing one oversized first value for progress.
func (h *BackendHandle) GetManyBounded(keys [][]byte, maxBytes uint64) ([]*Bytes, error) {
	return h.getMany(keys, &maxBytes)
}
func (h *BackendHandle) getMany(keys [][]byte, maxBytes *uint64) ([]*Bytes, error) {
	id, err := h.handle()
	if err != nil {
		return nil, err
	}
	encoded := make([]Bytes, len(keys))
	for i, key := range keys {
		encoded[i] = key
	}
	var values []*Bytes
	err = invoke(0, "backend_get_many", map[string]any{"backend": id, "keys": encoded, "max_bytes": maxBytes}, &values)
	return values, err
}

// RunBlocking invokes the Rust scheduler/session utility for a synchronous callback.
// On ordinary Go calls it executes inline; it does not start a goroutine.
func RunBlocking(wait func() error) error {
	if wait == nil {
		return invalidError("nil blocking callback")
	}
	return callbackInvoke(0, "helper_run_blocking", nil, func(op uint32, _ uint64, _ []byte) ([]byte, error) {
		if op != 11 {
			return nil, invalidError("blocking callback operation")
		}
		return nil, wait()
	}, nil)
}
