package state

import (
	"github.com/marcgauthier/murmur/codec"
	"github.com/marcgauthier/murmur/crdt"
	"github.com/marcgauthier/murmur/ids"
)

// RowKey identifies one durable row independently of its query materializer.
type RowKey struct {
	TableID uint32
	RowID   ids.RowID
}

// Reader exposes the authoritative row state needed to materialize query
// views. It deliberately lives with state rather than a particular query
// engine so durable state can be projected into RIME or another consumer.
type Reader interface {
	IterateTable(tableID uint32, fn func(*Row) error) error
	GetRow(table uint32, row ids.RowID) (map[uint32]codec.CellState, error)
	GetTombstone(table uint32, row ids.RowID) (crdt.Version, bool, error)
}
