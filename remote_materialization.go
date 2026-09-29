package replicateddb

import (
	"bytes"
	"sort"
	"time"

	"github.com/marcgauthier/spedsql/sqlengine"
	"github.com/marcgauthier/spedsql/state"
)

// queueRemoteLocked records final row identities, not copies of values. Later
// updates to the same row coalesce, and flush reads the authoritative state.
// applyMu must be held.
func (db *DB) queueRemoteLocked(winners []state.WinningChange, generation uint64, transactions int) error {
	if len(winners) > 0 && db.remoteRows == nil {
		db.remoteRows = make(map[sqlengine.RowKey]struct{})
	}
	for _, winner := range winners {
		if db.reg.TableByID(winner.TableID) != nil {
			db.remoteRows[sqlengine.RowKey{TableID: winner.TableID, RowID: winner.RowID}] = struct{}{}
		}
	}
	db.metrics.remoteApplyWinners.Add(uint64(len(winners)))
	if len(db.remoteRows) > 0 {
		db.remoteTxnCount += transactions
	}
	if db.remoteTxnCount >= db.cfg.QueryStore.RemoteApplyMaxTransactions {
		select {
		case db.remoteFlushWake <- struct{}{}:
		default:
		}
	}
	if len(db.remoteRows) == 0 {
		db.materializedGeneration.Store(generation)
		return nil
	}
	return nil
}

// flushRemoteLocked applies the current durable state of queued rows in one
// SQLite transaction. A failed apply rolls back and rebuilds from Pebble.
// applyMu must be held.
func (db *DB) flushRemoteLocked() error {
	if len(db.remoteRows) == 0 {
		return nil
	}
	rows := make([]sqlengine.RowKey, 0, len(db.remoteRows))
	for row := range db.remoteRows {
		rows = append(rows, row)
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].TableID != rows[j].TableID {
			return rows[i].TableID < rows[j].TableID
		}
		return bytes.Compare(rows[i].RowID[:], rows[j].RowID[:]) < 0
	})
	if err := db.engine.ApplyRows(db.shadowReader(), rows); err != nil {
		db.log.Warn("bulk remote materialization failed; rebuilding", "err", err.Error())
		return db.rebuildLocked()
	}
	gen, err := db.store.StateGeneration()
	if err != nil {
		return err
	}
	db.materializedGeneration.Store(gen)
	db.remoteRows = nil
	db.remoteTxnCount = 0
	if db.subMgr != nil {
		db.subMgr.notifyChange(false)
	}
	return nil
}

func (db *DB) remoteMaterializationLoop() {
	defer db.wg.Done()
	ticker := time.NewTicker(db.cfg.QueryStore.RemoteApplyInterval)
	defer ticker.Stop()
	for {
		select {
		case <-db.ctx.Done():
			return
		case <-ticker.C:
		case <-db.remoteFlushWake:
		}
		if db.ctx.Err() != nil {
			return
		}
		db.writeMu.Lock()
		db.applyMu.Lock()
		err := db.flushRemoteLocked()
		db.applyMu.Unlock()
		db.writeMu.Unlock()
		if err != nil {
			db.log.Error("remote materialization failed", "err", err.Error())
			db.setState(StateFailed)
			db.cancel()
			return
		}
	}
}
