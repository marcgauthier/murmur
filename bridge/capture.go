package bridge

import (
	"context"
	"fmt"

	db "github.com/marcgauthier/murmur"
	"github.com/marcgauthier/murmur/codec"
	"github.com/marcgauthier/murmur/ids"
)

// Capturer drains Low origin logs into the outbox, one transaction-linked
// batch per committed source transaction. Resume state lives in the outbox,
// so restarts continue exactly where capture stopped.
type Capturer struct {
	src      db.BridgeLogSource
	schema   db.BridgeSchemaResolver
	outbox   *Outbox
	maxBatch int
	maxBytes int
}

// NewCapturer binds a capture loop. maxBatchesPerOrigin and maxBytesPerScan
// bound each poll; non-positive values select 64 batches / 4 MiB.
// It initializes log GC protection floors for all known origins from outbox resume.
func NewCapturer(src db.BridgeLogSource, schema db.BridgeSchemaResolver, outbox *Outbox, maxBatchesPerOrigin, maxBytesPerScan int) (*Capturer, error) {
	if src == nil || schema == nil || outbox == nil {
		return nil, fmt.Errorf("bridge: capturer requires a log source, schema, and outbox")
	}
	if maxBatchesPerOrigin <= 0 {
		maxBatchesPerOrigin = 64
	}
	if maxBytesPerScan <= 0 {
		maxBytesPerScan = 4 << 20
	}
	c := &Capturer{src: src, schema: schema, outbox: outbox,
		maxBatch: maxBatchesPerOrigin, maxBytes: maxBytesPerScan}
	if origins, err := src.KnownOrigins(context.Background()); err == nil {
		for _, o := range origins {
			_ = src.ProtectResume(o, outbox.Resume(o))
		}
	}
	return c, nil
}

// CaptureOnce polls every known origin once and appends new transactions.
// It returns the appended event count. Uncaptured origin logs are protected from GC.
func (c *Capturer) CaptureOnce(ctx context.Context) (int, error) {
	origins, err := c.src.KnownOrigins(ctx)
	if err != nil {
		return 0, err
	}
	appended := 0
	for _, origin := range origins {
		if err := ctx.Err(); err != nil {
			return appended, err
		}
		from := c.outbox.Resume(origin) + 1
		if from == 0 {
			from = 1
		}
		_ = c.src.ProtectResume(origin, from-1)
		last, err := c.src.ScanLog(ctx, origin, from, c.maxBatch, c.maxBytes, func(mb *codec.MutationBatch) error {
			batches, err := c.convert(mb)
			if err != nil {
				return err
			}
			for _, batch := range batches {
				_, err = c.outbox.Append(batch, origin, mb.Sequence, mb.SchemaEpoch, mb.SchemaHash)
				if err != nil {
					return err
				}
				appended++
			}
			_ = c.src.ProtectResume(origin, mb.Sequence)
			return nil
		})
		if err != nil {
			return appended, err
		}
		if last > c.outbox.Resume(origin) {
			if err := c.outbox.AdvanceResume(origin, last); err != nil {
				return appended, err
			}
			_ = c.src.ProtectResume(origin, last)
		}
	}
	return appended, nil
}

// convert maps one committed source transaction to logical batches.
// Mutations collapse per row: any tombstone wins (the row is gone),
// otherwise puts merge with last-column-wins. Table/column IDs resolve to
// names; unknown IDs fail loudly. File metadata records split into their own
// batch so publishers can attach object sidecars per file batch.
func (c *Capturer) convert(mb *codec.MutationBatch) ([]Batch, error) {
	newBatch := func() Batch {
		return Batch{TxID: mb.TxID, Origin: mb.OriginNode, Sequence: mb.Sequence, HLC: mb.HLC}
	}
	rows := newBatch()
	files := newBatch()
	type rowKey struct {
		table uint32
		row   ids.RowID
	}
	puts := make(map[rowKey]map[uint32]codec.Value)
	tombs := make(map[rowKey]bool)
	var order []rowKey
	seen := make(map[rowKey]bool)
	for _, m := range mb.Mutations {
		if m.TableID == db.BridgePolicyTableID {
			continue
		}
		k := rowKey{table: m.TableID, row: m.RowID}
		if !seen[k] {
			seen[k] = true
			order = append(order, k)
		}
		if m.IsTombstone() {
			tombs[k] = true
			delete(puts, k)
			continue
		}
		if tombs[k] {
			continue
		}
		cols := puts[k]
		if cols == nil {
			cols = make(map[uint32]codec.Value)
			puts[k] = cols
		}
		cols[m.ColumnID] = m.Value
	}
	emit := func(rec Record, file bool) {
		if file {
			files.Records = append(files.Records, rec)
		} else {
			rows.Records = append(rows.Records, rec)
		}
	}
	for _, k := range order {
		table, err := c.resolveTable(k.table)
		if err != nil {
			return nil, err
		}
		file := table == db.BridgeFileTableName
		if tombs[k] {
			emit(Record{Table: table, Row: k.row, Op: RecordDelete}, file)
			continue
		}
		rec := Record{Table: table, Row: k.row, Op: RecordPut}
		for colID, v := range puts[k] {
			column, err := c.schema.ColumnName(k.table, colID)
			if err != nil {
				return nil, fmt.Errorf("bridge: capture: %w", err)
			}
			rec.Columns = append(rec.Columns, ColumnValue{Column: column, Value: v})
		}
		// Deterministic column order for stable bundles.
		for i := 1; i < len(rec.Columns); i++ {
			for j := i; j > 0 && rec.Columns[j].Column < rec.Columns[j-1].Column; j-- {
				rec.Columns[j], rec.Columns[j-1] = rec.Columns[j-1], rec.Columns[j]
			}
		}
		emit(rec, file)
	}
	var out []Batch
	if len(rows.Records) > 0 {
		out = append(out, rows)
	}
	if len(files.Records) > 0 {
		out = append(out, files)
	}
	return out, nil
}

func (c *Capturer) resolveTable(tableID uint32) (string, error) {
	table, err := c.schema.TableName(tableID)
	if err != nil {
		return "", fmt.Errorf("bridge: capture: %w", err)
	}
	if table == "" {
		return "", fmt.Errorf("bridge: capture: unknown table %d", tableID)
	}
	return table, nil
}
