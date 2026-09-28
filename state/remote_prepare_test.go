package state

import (
	"context"
	"errors"
	"testing"

	"github.com/nomadsql/replicateddb/codec"
	"github.com/nomadsql/replicateddb/ids"
)

func TestCommitRemotePreparedRecoveryIsAtomic(t *testing.T) {
	path := t.TempDir()
	node := ids.NewNodeID()
	origin := ids.NewNodeID()
	opt := Options{Limits: codec.DefaultLimits()}
	s, err := Open(path, node, ids.DBID{}, opt)
	if err != nil {
		t.Fatal(err)
	}
	batch := &codec.MutationBatch{
		ProtocolVersion: 1,
		TxID:            ids.NewTxID(),
		OriginNode:      origin,
		Sequence:        1,
		HLC:             77,
		SchemaEpoch:     1,
		Mutations:       []codec.Mutation{{TableID: 4, RowID: ids.NewRowID(), ColumnID: 2, Value: codec.Text("recovered")}},
	}
	batch.Mutations[0].RowID[0] = 9
	s.remotePrepareFault = func() error { return errors.New("injected interruption after prepare") }
	if _, err := s.CommitRemote(context.Background(), batch); err == nil {
		t.Fatal("expected interruption after durable prepare")
	}
	if wm, _ := s.ReceiveWatermark(origin); wm != 0 {
		t.Fatalf("pre-recovery watermark = %d", wm)
	}
	if gen, _ := s.StateGeneration(); gen != 0 {
		t.Fatalf("pre-recovery generation = %d", gen)
	}
	if _, err := s.getDirect(ReceiptKey(batch.TxID)); !isNotFound(err) {
		t.Fatalf("receipt visible before recovery: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s, err = Open(path, node, ids.DBID{}, opt)
	if err != nil {
		t.Fatalf("open and recover: %v", err)
	}
	defer s.Close()
	if wm, _ := s.ReceiveWatermark(origin); wm != 1 {
		t.Fatalf("recovered watermark = %d", wm)
	}
	if gen, _ := s.StateGeneration(); gen != 1 {
		t.Fatalf("recovered generation = %d", gen)
	}
	if got, err := s.getDirect(ReceiptKey(batch.TxID)); err != nil || len(got) != 24 {
		t.Fatalf("recovered receipt = %x, err=%v", got, err)
	}
	if got, err := s.getDirect(LogKey(origin, 1)); err != nil || len(got) == 0 {
		t.Fatalf("recovered log = %x, err=%v", got, err)
	}
	if _, err := s.getDirect(SysKey(sysRemotePrepare)); !isNotFound(err) {
		t.Fatalf("prepare record remains after recovery: %v", err)
	}
	cell, ok, err := s.GetCell(4, batch.Mutations[0].RowID, 2)
	if err != nil || !ok || cell.Value.S != "recovered" {
		t.Fatalf("recovered cell = %+v, present=%v, err=%v", cell, ok, err)
	}
	if hlc := s.ClockMax(); hlc < batch.HLC {
		t.Fatalf("recovered HLC = %d", hlc)
	}
	if _, err := s.CommitRemote(context.Background(), batch); err != nil {
		t.Fatalf("idempotent retry: %v", err)
	}
	if gen, _ := s.StateGeneration(); gen != 1 {
		t.Fatalf("retry changed generation to %d", gen)
	}
}
