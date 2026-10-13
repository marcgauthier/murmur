package demo

import (
	"bytes"
	"encoding/json"
	"errors"
	murmur "github.com/marcgauthier/murmur/clients/go"
	"sort"
	"sync"
)

// RecordBackend is an ephemeral, record-only teaching backend. It owns no
// SQLite database. Managed Murmur databases still use their authoritative Spool.
// Persistence and SQL/CRDT application are deliberately unsupported here.
type RecordBackend struct {
	murmur.BackendDefaults
	mu             sync.Mutex
	publishMu      sync.Mutex
	Database, Node murmur.NodeID
	Config         murmur.ReplicationConfig
	Handle         *murmur.BackendHandle
	records        map[string][]byte
	schema         []byte
	millis         uint64
}

func NewBackend(database, node murmur.NodeID, config murmur.ReplicationConfig) (*RecordBackend, error) {
	schema, err := murmur.Encode("Schema", murmur.Schema{Tables: []murmur.Table{}, Indexes: []string{}})
	if err != nil {
		return nil, err
	}
	b := &RecordBackend{Database: database, Node: node, Config: config, records: map[string][]byte{}, schema: schema}
	b.Handle, err = murmur.RegisterBackend(b)
	if err != nil {
		return nil, err
	}
	if err = b.Handle.BindSigner(config); err != nil {
		b.Handle.Close()
		return nil, err
	}
	return b, nil
}
func (b *RecordBackend) Identity() (murmur.NodeID, murmur.NodeID) { return b.Database, b.Node }
func (b *RecordBackend) Get(key []byte) ([]byte, bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	v, ok := b.records[string(key)]
	return append([]byte{}, v...), ok, nil
}
func (b *RecordBackend) Keys(prefix []byte) ([][]byte, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	keys := [][]byte{}
	for k := range b.records {
		if bytes.HasPrefix([]byte(k), prefix) {
			keys = append(keys, []byte(k))
		}
	}
	sort.Slice(keys, func(i, j int) bool { return bytes.Compare(keys[i], keys[j]) < 0 })
	return keys, nil
}
func (b *RecordBackend) commitLocked(records []murmur.Record) {
	for _, r := range records {
		if r.Value == nil {
			delete(b.records, string(r.Key))
		} else {
			b.records[string(r.Key)] = append([]byte{}, (*r.Value)...)
		}
	}
}
func (b *RecordBackend) Commit(records []murmur.Record) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.commitLocked(records)
	return nil
}
func (b *RecordBackend) Publish(records []murmur.Record) error {
	b.publishMu.Lock()
	defer b.publishMu.Unlock()
	payload, err := murmur.Encode("Content", murmur.Content{Records: records, Origin: b.Node})
	if err != nil {
		return err
	}
	schema, err := b.Schema()
	if err != nil {
		return err
	}
	now, err := murmur.NowMillis()
	if err != nil {
		return err
	}
	logs, err := murmur.LocalRecords(b.Handle, payload, schema, now, 0, b.Config.SigningKey)
	if err != nil {
		return err
	}
	// LocalRecords signs the journal entry; a custom backend must also publish
	// its retained range so peers know history is available rather than asking
	// for a snapshot. Commit data, journal and range as one batch.
	sequenceKey, err := murmur.StorageKey(murmur.KeyRequest{Kind: "sequence", Origin: b.Node})
	if err != nil {
		return err
	}
	var sequence uint64
	for _, record := range logs {
		if bytes.Equal(record.Key, sequenceKey) && record.Value != nil {
			if err = murmur.Decode("U64", *record.Value, &sequence); err != nil {
				return err
			}
		}
	}
	retainedKey, err := murmur.StorageKey(murmur.KeyRequest{Kind: "retained", Origin: b.Node})
	if err != nil {
		return err
	}
	head := murmur.PeerProgress{Origin: b.Node, RetainedFrom: sequence}
	if previous, ok, err := b.Get(retainedKey); err != nil {
		return err
	} else if ok {
		if err = murmur.Decode("Progress", previous, &head); err != nil {
			return err
		}
	}
	head.Applied = sequence
	head.Observed = sequence
	head.RetainedThrough = sequence
	encoded, err := murmur.Encode("Progress", head)
	if err != nil {
		return err
	}
	logs = append(logs, murmur.PutRecord(retainedKey, encoded))
	return b.Commit(append(records, logs...))
}
func (b *RecordBackend) Schema() ([]byte, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]byte{}, b.schema...), nil
}
func (b *RecordBackend) AcceptSchema(schema []byte) (bool, error) {
	current, _ := b.Schema()
	if !bytes.Equal(current, schema) {
		return false, errors.New("example backend has a fixed record-only schema")
	}
	return true, nil
}
func (b *RecordBackend) Observe(millis uint64, logical uint32) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if millis > b.millis {
		b.millis = millis
	}
	return nil
}
func (b *RecordBackend) Snapshot() ([]byte, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	data := map[string][]byte{}
	for k, v := range b.records {
		if bytes.HasPrefix([]byte(k), []byte("demo/")) {
			data[k] = append([]byte{}, v...)
		}
	}
	return json.Marshal(data)
}
func (b *RecordBackend) SnapshotWithProgress() ([]byte, []murmur.PeerProgress, error) {
	data, err := b.Snapshot()
	return data, []murmur.PeerProgress{}, err
}
func (b *RecordBackend) Apply(payloads [][]byte, records []murmur.Record) (bool, error) {
	var application []murmur.Record
	for _, payload := range payloads {
		var content murmur.Content
		if err := murmur.Decode("Content", payload, &content); err == nil {
			if content.SQLite != nil {
				return false, errors.New("record-only example cannot apply SQLite changesets")
			}
			application = append(application, content.Records...)
			continue
		}
		var snapshot map[string][]byte
		if err := json.Unmarshal(payload, &snapshot); err != nil {
			return false, err
		}
		for k, v := range snapshot {
			if !bytes.HasPrefix([]byte(k), []byte("demo/")) {
				return false, errors.New("unexpected snapshot namespace")
			}
			application = append(application, murmur.PutRecord([]byte(k), v))
		}
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.commitLocked(append(application, records...))
	return true, nil
}
func (b *RecordBackend) ImportBridge(h murmur.BridgeHeader, payload []byte) (bool, error) {
	return b.Apply([][]byte{payload}, nil)
}
func (b *RecordBackend) ReleaseOwnership(string, []byte, *string) (bool, error) {
	return false, errors.New("ownership requires a managed database")
}
func (b *RecordBackend) RetentionCheckpoint() error {
	return errors.New("ephemeral example has no durable checkpoint")
}
func (b *RecordBackend) PruneHistory(murmur.NodeID, uint64) (uint64, error) {
	return 0, errors.New("ephemeral example must not prune durable history")
}
func (b *RecordBackend) ReclaimRetention() error {
	return errors.New("ephemeral example has no durable retention")
}
