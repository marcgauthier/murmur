package state

import (
	"bytes"
	"context"
	"testing"

	"github.com/marcgauthier/murmur/codec"
	"github.com/marcgauthier/murmur/ids"
)

func TestCompleteBridgeImportPreservesReceiptsAndProgress(t *testing.T) {
	s := openTestStore(t, ids.NewNodeID())
	batch := localBatch(s, s.ClockNow(), codec.Mutation{TableID: 1, RowID: ids.NewRowID(), ColumnID: 1, Value: codec.Text("row")})
	if _, err := s.CommitLocal(context.Background(), batch); err != nil {
		t.Fatal(err)
	}
	original, err := s.getDirect(ReceiptKey(batch.TxID))
	if err != nil {
		t.Fatal(err)
	}
	completion := ids.NewTxID()
	if err := s.CompleteBridgeImport("stream", 12, []ids.TxID{batch.TxID, completion, {}, completion}); err != nil {
		t.Fatal(err)
	}
	if err := s.CompleteBridgeImport("stream", 3, []ids.TxID{completion}); err != nil {
		t.Fatal(err)
	}
	got, err := s.getDirect(ReceiptKey(batch.TxID))
	if err != nil || !bytes.Equal(got, original) {
		t.Fatalf("authenticated receipt changed: %x -> %x, %v", original, got, err)
	}
	if seq, ok, err := s.BridgeStreamProgress("stream"); err != nil || !ok || seq != 12 {
		t.Fatalf("progress = %d, %v, %v", seq, ok, err)
	}
	if has, err := s.HasReceipt(completion); err != nil || !has {
		t.Fatalf("completion receipt = %v, %v", has, err)
	}
	if has, err := s.HasReceipt(ids.TxID{}); err != nil || has {
		t.Fatalf("zero receipt = %v, %v", has, err)
	}
}

func TestCompleteBridgeImportFailurePublishesNoReceipts(t *testing.T) {
	s := openTestStore(t, ids.NewNodeID())
	// A read/validation failure after staging receipts must abandon the
	// entire batch, rather than leave a durable prefix of completion IDs.
	if err := s.dbSet(BridgeProgressKey("stream"), []byte{1}, s.syncCommits); err != nil {
		t.Fatal(err)
	}
	receipts := []ids.TxID{ids.NewTxID(), ids.NewTxID()}
	if err := s.CompleteBridgeImport("stream", 2, receipts); err == nil {
		t.Fatal("corrupt progress accepted")
	}
	for _, id := range receipts {
		if has, err := s.HasReceipt(id); err != nil || has {
			t.Fatalf("receipt published after failure: %s = %v, %v", id, has, err)
		}
	}
	got, err := s.getDirect(BridgeProgressKey("stream"))
	if err != nil || !bytes.Equal(got, []byte{1}) {
		t.Fatalf("failed completion changed progress: %x, %v", got, err)
	}
}
