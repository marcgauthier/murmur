package murmur

import "encoding/json"

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
	case "identity":
		db, node := backend.Identity()
		return [2]NodeID{db, node}, nil
	case "get":
		b, found, err := backend.Get(a.Key)
		if !found {
			return nil, err
		}
		return Bytes(b), err
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
