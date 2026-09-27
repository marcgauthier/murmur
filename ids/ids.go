// Package ids defines the 128-bit identity types used across the package.
//
// All identities are UUID-sized so they can be generated offline without
// coordination. The canonical text form is the standard UUID string.
package ids

import (
	"bytes"
	"crypto/rand"
	"encoding"
	"fmt"

	"github.com/google/uuid"
)

// NodeID identifies a cluster node. It is bound to the node's TLS
// certificate (URI SAN) so a peer cannot spoof another node's identity.
type NodeID [16]byte

// DBID identifies a database/cluster. Nodes only replicate with peers that
// share the same DBID.
type DBID [16]byte

// TxID identifies one committed application transaction. It provides
// idempotency for ambiguous commits and retries.
type TxID [16]byte

// RowID is the primary key of a replicated row.
type RowID [16]byte

var (
	_ encoding.TextMarshaler   = NodeID{}
	_ encoding.TextUnmarshaler = (*NodeID)(nil)
)

// NewNodeID returns a random NodeID.
func NewNodeID() NodeID { return NodeID(uuid.New()) }

// NewDBID returns a random DBID.
func NewDBID() DBID { return DBID(uuid.New()) }

// NewTxID returns a random TxID.
func NewTxID() TxID { return TxID(uuid.New()) }

// NewRowID returns a random RowID.
func NewRowID() RowID { return RowID(uuid.New()) }

// MustNodeID parses s or panics. Useful for static configuration.
func MustNodeID(s string) NodeID {
	id, err := ParseNodeID(s)
	if err != nil {
		panic(err)
	}
	return id
}

// ParseNodeID parses the canonical UUID text form.
func ParseNodeID(s string) (NodeID, error) {
	u, err := uuid.Parse(s)
	if err != nil {
		return NodeID{}, fmt.Errorf("invalid node id %q: %w", s, err)
	}
	return NodeID(u), nil
}

// ParseDBID parses the canonical UUID text form.
func ParseDBID(s string) (DBID, error) {
	u, err := uuid.Parse(s)
	if err != nil {
		return DBID{}, fmt.Errorf("invalid db id %q: %w", s, err)
	}
	return DBID(u), nil
}

func (id NodeID) String() string               { return uuid.UUID(id).String() }
func (id NodeID) IsZero() bool                 { return id == NodeID{} }
func (id NodeID) Compare(o NodeID) int         { return bytes.Compare(id[:], o[:]) }
func (id NodeID) MarshalText() ([]byte, error) { return []byte(id.String()), nil }
func (id *NodeID) UnmarshalText(b []byte) error {
	parsed, err := ParseNodeID(string(b))
	if err != nil {
		return err
	}
	*id = parsed
	return nil
}

func (id DBID) String() string     { return uuid.UUID(id).String() }
func (id DBID) IsZero() bool       { return id == DBID{} }
func (id DBID) Compare(o DBID) int { return bytes.Compare(id[:], o[:]) }

func (id TxID) String() string { return uuid.UUID(id).String() }
func (id TxID) IsZero() bool   { return id == TxID{} }

func (id RowID) String() string { return uuid.UUID(id).String() }
func (id RowID) IsZero() bool   { return id == RowID{} }

// Random16 fills a 16-byte array from crypto/rand.
func Random16() ([16]byte, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return b, err
	}
	return b, nil
}
