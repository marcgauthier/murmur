package murmur

import "encoding/json"

type SnapshotPin struct {
	Recipient NodeID          `json:"recipient"`
	Boundary  []NodeSequence  `json:"boundary"`
	Target    *[]NodeSequence `json:"target"`
	Expires   uint64          `json:"expires"`
	Digest    Key             `json:"digest"`
}

func SnapshotDeadline(config ReplicationConfig, now uint64) (uint64, error) {
	var deadline uint64
	err := invoke(0, "helper_snapshot", map[string]any{"method": "deadline", "config": config, "now": now}, &deadline)
	return deadline, err
}
func BuildSnapshot(identity [2]NodeID, recipient NodeID, data []byte, progress []PeerProgress, schema Key, config ReplicationConfig, now uint64) (SnapshotManifest, []Record, error) {
	var parts []json.RawMessage
	err := invoke(0, "helper_snapshot", map[string]any{"method": "build", "identity": identity, "recipient": recipient, "data": Bytes(data), "progress": nonNil(progress), "schema": schema, "config": config, "now": now}, &parts)
	if err != nil {
		return SnapshotManifest{}, nil, err
	}
	if len(parts) != 2 {
		return SnapshotManifest{}, nil, invalidError("snapshot result")
	}
	var m SnapshotManifest
	var records []Record
	if err = json.Unmarshal(parts[0], &m); err != nil {
		return m, nil, err
	}
	err = json.Unmarshal(parts[1], &records)
	return m, records, err
}
func snapshotCall(backend *BackendHandle, method string, args map[string]any, out any) error {
	id, err := backend.handle()
	if err != nil {
		return err
	}
	if args == nil {
		args = map[string]any{}
	}
	args["method"] = method
	args["backend"] = id
	return invoke(0, "helper_snapshot", args, out)
}
func (m SnapshotManifest) Verify(backend *BackendHandle, config ReplicationConfig, source NodeID, now uint64) error {
	return snapshotCall(backend, "verify", map[string]any{"manifest": m, "config": config, "source": source, "now": now}, nil)
}
func OutgoingSnapshot(backend *BackendHandle, recipient, transfer NodeID, now uint64) (*SnapshotManifest, error) {
	var m *SnapshotManifest
	err := snapshotCall(backend, "outgoing", map[string]any{"recipient": recipient, "transfer": transfer, "now": now}, &m)
	return m, err
}
func CancelSnapshot(backend *BackendHandle, recipient, transfer NodeID) error {
	return snapshotCall(backend, "cancel", map[string]any{"recipient": recipient, "transfer": transfer}, nil)
}
func IncomingSnapshot(backend *BackendHandle) (*SnapshotIncoming, error) {
	var v *SnapshotIncoming
	err := snapshotCall(backend, "incoming", nil, &v)
	return v, err
}
func AcceptSnapshot(backend *BackendHandle, config ReplicationConfig, source NodeID, manifest SnapshotManifest, now uint64) error {
	return snapshotCall(backend, "accept", map[string]any{"config": config, "source": source, "manifest": manifest, "now": now}, nil)
}
func ReceiveSnapshot(backend *BackendHandle, source, transfer NodeID, index uint32, data []byte, now uint64) error {
	return snapshotCall(backend, "receive", map[string]any{"source": source, "transfer": transfer, "index": index, "data": Bytes(data), "now": now}, nil)
}
func MissingSnapshot(backend *BackendHandle, incoming SnapshotIncoming) ([]uint32, error) {
	var v []uint32
	err := snapshotCall(backend, "missing", map[string]any{"incoming": incoming}, &v)
	return v, err
}
func FinishSnapshot(backend *BackendHandle, config ReplicationConfig, source, transfer NodeID, now uint64) (bool, error) {
	var b bool
	err := snapshotCall(backend, "finish", map[string]any{"config": config, "source": source, "transfer": transfer, "now": now}, &b)
	return b, err
}
func SnapshotPruneLimit(backend *BackendHandle, origin NodeID, now uint64) (uint64, error) {
	var n uint64
	err := snapshotCall(backend, "prune_limit", map[string]any{"origin": origin, "now": now}, &n)
	return n, err
}
func MaintainSnapshots(backend *BackendHandle, now uint64) error {
	return snapshotCall(backend, "maintain", map[string]any{"now": now}, nil)
}

// BuildSnapshotStream persists encoded Content pages in bounded native groups.
// The supplied Go pages are materialized; database-managed exports stream from Spool.
func BuildSnapshotStream(backend *BackendHandle, identity [2]NodeID, recipient NodeID, pages []Bytes, progress []PeerProgress, schema Key, config ReplicationConfig, now uint64) (SnapshotManifest, error) {
	var manifest SnapshotManifest
	err := snapshotCall(backend, "build_stream", map[string]any{"identity": identity, "recipient": recipient, "pages": nonNil(pages), "progress": nonNil(progress), "schema": schema, "config": config, "now": now}, &manifest)
	return manifest, err
}
