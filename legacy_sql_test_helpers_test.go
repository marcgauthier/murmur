package murmur

import (
	"github.com/marcgauthier/murmur/codec"
	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/replication"
)

func remoteBatchCells(db *DB, origin ids.NodeID, seq, hlc uint64, table string, row ids.RowID, cells map[string]codec.Value) *codec.MutationBatch {
	tableSchema := db.reg.Table(table)
	if tableSchema == nil {
		return nil
	}
	columns := make(map[string]uint32, len(tableSchema.Columns))
	for _, column := range tableSchema.Columns {
		columns[column.Name] = column.ID
	}
	batch := &codec.MutationBatch{
		ProtocolVersion: replication.ProtocolVersion,
		TxID:            ids.NewTxID(),
		OriginNode:      origin,
		Sequence:        seq,
		HLC:             hlc,
		SchemaEpoch:     db.reg.Epoch,
	}
	for name, value := range cells {
		column, ok := columns[name]
		if !ok {
			return nil
		}
		batch.Mutations = append(batch.Mutations, codec.Mutation{TableID: tableSchema.ID, RowID: row, ColumnID: column, Value: value})
	}
	return batch
}
