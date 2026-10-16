package murmur

import (
	"encoding/json"
	"fmt"
)

// Message preserves Rust's externally tagged enum, including every protocol variant.
type Message struct {
	Kind string
	Body json.RawMessage
}

func NewMessage(kind string, body any) (Message, error) {
	if body == nil {
		return Message{Kind: kind}, nil
	}
	b, err := json.Marshal(body)
	return Message{Kind: kind, Body: b}, err
}
func (m Message) MarshalJSON() ([]byte, error) {
	if len(m.Body) == 0 {
		return json.Marshal(m.Kind)
	}
	return json.Marshal(map[string]json.RawMessage{m.Kind: m.Body})
}
func (m *Message) UnmarshalJSON(data []byte) error {
	var unit string
	if err := json.Unmarshal(data, &unit); err == nil {
		m.Kind = unit
		m.Body = nil
		return nil
	}
	var tagged map[string]json.RawMessage
	if err := json.Unmarshal(data, &tagged); err != nil {
		return err
	}
	if len(tagged) != 1 {
		return invalidError("protocol enum representation")
	}
	for kind, body := range tagged {
		m.Kind = kind
		m.Body = append(json.RawMessage{}, body...)
	}
	return nil
}
func (m Message) DecodeBody(out any) error { return decodeJSON(m.Body, out) }

// Encode/Decode delegate postcard encoding to Rust for the exported type named by typeName.
// Supported names are listed in ABI.md; these are not JSON bytes on the replication wire.
func Encode(typeName string, value any) ([]byte, error) {
	var bytes Bytes
	err := invoke(0, "codec_encode", map[string]any{"type": typeName, "value": value}, &bytes)
	return bytes, err
}
func Decode(typeName string, bytes []byte, out any) error {
	return invoke(0, "codec_decode", map[string]any{"type": typeName, "bytes": Bytes(bytes)}, out)
}
func PublicKey(seed Key) (Key, error) {
	var k Key
	err := invoke(0, "helper_public_key", map[string]any{"seed": seed}, &k)
	return k, err
}
func RecipientPublic(secret Key) (Key, error) {
	var k Key
	err := invoke(0, "helper_recipient_public", map[string]any{"secret": secret}, &k)
	return k, err
}
func CertificateNodeID(der []byte) (NodeID, error) {
	var id NodeID
	err := invoke(0, "helper_node_id", map[string]any{"der": Bytes(der)}, &id)
	return id, err
}
func NetworkAllowed(config ReplicationConfig, address string) (bool, error) {
	var b bool
	err := invoke(0, "helper_network_allowed", map[string]any{"config": config, "address": address}, &b)
	return b, err
}
func SchemaDigest(bytes []byte) (Key, error) {
	var k Key
	err := invoke(0, "helper_digest", map[string]any{"bytes": Bytes(bytes)}, &k)
	return k, err
}
func Hex(bytes []byte) (string, error) {
	var s string
	err := invoke(0, "helper_hex", map[string]any{"bytes": Bytes(bytes)}, &s)
	return s, err
}
func NowMillis() (uint64, error) {
	var n uint64
	err := invoke(0, "helper_now", nil, &n)
	return n, err
}

type ProtocolConstants struct {
	Version          uint16 `json:"version"`
	ALPN             Bytes  `json:"alpn"`
	FrameBytes       uint64 `json:"frame_bytes"`
	ChunkBytes       uint64 `json:"chunk_bytes"`
	CapSignedSchema  uint64 `json:"cap_signed_schema"`
	Capabilities     uint64 `json:"capabilities"`
	Required         uint64 `json:"required"`
	SignDomain       Bytes  `json:"sign_domain"`
	SchemaSignDomain Bytes  `json:"schema_sign_domain"`
	CoverageKey      Bytes  `json:"coverage_key"`
	SnapshotOut      Bytes  `json:"out"`
	SnapshotIn       Bytes  `json:"in"`
	SnapshotReceipts Bytes  `json:"receipts"`
	SnapshotPins     Bytes  `json:"pins"`
	BuildGraceMillis       uint64 `json:"build_grace_millis"`
	BuildStaleMillis       uint64 `json:"build_stale_millis"`
	BuildFreshMarginMillis uint64 `json:"build_fresh_margin_millis"`
}

func Constants() (ProtocolConstants, error) {
	var c ProtocolConstants
	err := invoke(0, "helper_constants", nil, &c)
	return c, err
}

type KeyRequest struct {
	Kind     string `json:"kind"`
	Origin   NodeID `json:"origin"`
	Sequence uint64 `json:"sequence"`
	Index    uint32 `json:"index"`
	Transfer NodeID `json:"transfer"`
	Prefix   Bytes  `json:"prefix"`
}

func StorageKey(request KeyRequest) ([]byte, error) {
	b, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}
	var args map[string]json.RawMessage
	if err = json.Unmarshal(b, &args); err != nil {
		return nil, err
	}
	values := map[string]any{}
	for k, v := range args {
		values[k] = v
	}
	var out Bytes
	err = invoke(0, "helper_key", values, &out)
	return out, err
}
func (t *OriginTransaction) Sign(key Key) error {
	return invoke(0, "helper_transaction", map[string]any{"method": "sign", "value": t, "key": key}, t)
}
func (t OriginTransaction) Verify(database NodeID, trusted []TrustEntry, max uint64) error {
	return invoke(0, "helper_transaction", map[string]any{"method": "verify", "value": t, "database": database, "trusted": nonNil(trusted), "max": max}, nil)
}
func (t OriginTransaction) VerifyHeader(database NodeID, trusted []TrustEntry) error {
	return invoke(0, "helper_transaction", map[string]any{"method": "verify_header", "value": t, "database": database, "trusted": nonNil(trusted)}, nil)
}
func (t OriginTransaction) Digest() (Key, error) {
	var k Key
	err := invoke(0, "helper_transaction", map[string]any{"method": "digest", "value": t}, &k)
	return k, err
}
func (t *SignedSchema) Sign(key Key) error {
	return invoke(0, "helper_signed_schema", map[string]any{"method": "sign", "value": t, "key": key}, t)
}
func (t SignedSchema) Verify(database, master NodeID, trusted []TrustEntry, max uint64) error {
	return invoke(0, "helper_signed_schema", map[string]any{"method": "verify", "value": t, "database": database, "master": master, "trusted": nonNil(trusted), "max": max}, nil)
}
func (t SignedSchema) Digest() (Key, error) {
	var k Key
	err := invoke(0, "helper_signed_schema", map[string]any{"method": "digest", "value": t}, &k)
	return k, err
}
func LocalRecords(backend *BackendHandle, payload, schema []byte, millis uint64, logical uint32, signing Key) ([]Record, error) {
	id, err := backend.handle()
	if err != nil {
		return nil, err
	}
	var records []Record
	err = invoke(0, "helper_local_records", map[string]any{"backend": id, "payload": Bytes(payload), "schema": Bytes(schema), "millis": millis, "logical": logical, "signing": signing}, &records)
	return records, err
}
func (n *NodeSequence) UnmarshalJSON(data []byte) error {
	var parts []json.RawMessage
	if err := json.Unmarshal(data, &parts); err != nil {
		return err
	}
	if len(parts) != 2 {
		return fmt.Errorf("node sequence tuple")
	}
	if err := json.Unmarshal(parts[0], &n.Node); err != nil {
		return err
	}
	return json.Unmarshal(parts[1], &n.Sequence)
}
func (n NodeSequence) MarshalJSON() ([]byte, error) { return json.Marshal([2]any{n.Node, n.Sequence}) }
