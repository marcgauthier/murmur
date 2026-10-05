package murmur

import (
	"context"
	"fmt"
	"github.com/marcgauthier/murmur/codec"
	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/internal/testidentity"
	"github.com/marcgauthier/murmur/schema"
	"math/big"
	"sync"
	"testing"
	"time"
)

func mergeTestConfig(t *testing.T) Config {
	t.Helper()
	node := ids.NewNodeID()
	return Config{Path: t.TempDir(), Encryption: EncryptionConfig{Key: make([]byte, 32), KeyID: "test"}, NodeID: node, DBID: ids.NewDBID(), OriginSigning: testidentity.Config(node), Schema: SchemaConfig{Version: 1, Tables: []schema.TableSchema{{ID: 1, Name: "items", PK: 1, Columns: []schema.ColumnSchema{{ID: 1, Name: "id", Type: schema.ColBlob}, {ID: 2, Name: "count", Type: schema.ColText, MergePolicy: schema.PN_COUNTER}, {ID: 3, Name: "tags", Type: schema.ColText, MergePolicy: schema.OR_SET}, {ID: 4, Name: "maxv", Type: schema.ColInteger, MergePolicy: schema.MAX}, {ID: 5, Name: "minv", Type: schema.ColReal, MergePolicy: schema.MIN}}}}}}
}
func TestMergePolicyTransaction(t *testing.T) {
	ctx := context.Background()
	cfg := mergeTestConfig(t)
	d, err := Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	row := ids.NewRowID()
	if _, err = d.ExecContext(ctx, "INSERT INTO items VALUES (?, '0', '[]', 4, 4)", row[:]); err != nil {
		t.Fatal(err)
	}
	tx, err := d.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = tx.CounterAdd(ctx, "items", "count", row, big.NewInt(5)); err != nil {
		t.Fatal(err)
	}
	if err = tx.CounterAdd(ctx, "items", "count", row, big.NewInt(7)); err != nil {
		t.Fatal(err)
	}
	if err = tx.CounterAdd(ctx, "items", "count", row, big.NewInt(-3)); err != nil {
		t.Fatal(err)
	}
	if err = tx.SetAdd(ctx, "items", "tags", row, SetString("red")); err != nil {
		t.Fatal(err)
	}
	if err = tx.SetAdd(ctx, "items", "tags", row, SetInt(1)); err != nil {
		t.Fatal(err)
	}
	if err = tx.SetRemove(ctx, "items", "tags", row, SetString("red")); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	var count, tags string
	var maxv int64
	var minv float64
	if err = d.QueryRowContext(ctx, "SELECT count,tags,maxv,minv FROM items").Scan(&count, &tags, &maxv, &minv); err != nil {
		t.Fatal(err)
	}
	if count != "9" || tags != `[{"type":"integer","value":"1"}]` {
		t.Fatalf("count=%s tags=%s", count, tags)
	}
	if _, err = d.ExecContext(ctx, "UPDATE items SET maxv=2,minv=8"); err != nil {
		t.Fatal(err)
	}
	if err = d.QueryRowContext(ctx, "SELECT maxv,minv FROM items").Scan(&maxv, &minv); err != nil {
		t.Fatal(err)
	}
	if maxv != 4 || minv != 4 {
		st, _ := d.store.GetRow(1, row)
		t.Logf("stored=%+v", st)
		t.Fatalf("extrema %d %f", maxv, minv)
	}
	if _, err = d.ExecContext(ctx, "UPDATE items SET count='200'"); err == nil {
		t.Fatal("accepted direct SQL counter write")
	}
	stats, err := d.MergePolicyStats(ctx)
	if err != nil || stats.ActorComponents != 2 || stats.SetAdditions != 2 || stats.SetRemovals != 1 || stats.MetadataBytes == 0 || stats.MergeAttempts == 0 {
		t.Fatalf("metadata stats: %+v %v", stats, err)
	}
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
	d, err = Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if err = d.QueryRowContext(ctx, "SELECT count,tags FROM items").Scan(&count, &tags); err != nil {
		t.Fatal(err)
	}
	if count != "9" {
		t.Fatal(count)
	}
}
func TestMergePolicyRemoteConvergence(t *testing.T) {
	ctx := context.Background()
	cfg := mergeTestConfig(t)
	a, err := Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	other := cfg
	other.Path = t.TempDir()
	other.NodeID = ids.NewNodeID()
	other.OriginSigning = testidentity.Config(other.NodeID)
	b, err := Open(ctx, other)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	row := ids.NewRowID()
	if _, err = a.ExecContext(ctx, "INSERT INTO items VALUES (?, '0', '[]', 4, 4)", row[:]); err != nil {
		t.Fatal(err)
	}
	forward := func(src, dst *DB, origin ids.NodeID, seq uint64) {
		t.Helper()
		_, err := src.ScanReplicationLog(ctx, origin, seq, 100, 1<<20, func(batch *codec.MutationBatch) error { return dst.ApplyRemote(ctx, batch) })
		if err != nil {
			t.Fatal(err)
		}
	}
	forward(a, b, cfg.NodeID, 1)
	for i, d := range []*DB{a, b} {
		tx, err := d.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err = tx.CounterAdd(ctx, "items", "count", row, big.NewInt(int64(i+1))); err != nil {
			t.Fatal(err)
		}
		if err = tx.SetAdd(ctx, "items", "tags", row, SetString([]string{"red", "blue"}[i])); err != nil {
			t.Fatal(err)
		}
		if err = tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	forward(a, b, cfg.NodeID, 2)
	forward(b, a, other.NodeID, 1)
	for _, d := range []*DB{a, b} {
		d.applyMu.Lock()
		err := d.flushRemoteLocked()
		d.applyMu.Unlock()
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, d := range []*DB{a, b} {
		var count, tags string
		if err = d.QueryRowContext(ctx, "SELECT count,tags FROM items").Scan(&count, &tags); err != nil {
			t.Fatal(err)
		}
		if count != "3" {
			st, _ := d.store.GetRow(1, row)
			t.Logf("stored=%+v", st)
			t.Fatal(count)
		}
		if tags != `[{"type":"string","value":"red"},{"type":"string","value":"blue"}]` && tags != `[{"type":"string","value":"blue"},{"type":"string","value":"red"}]` {
			t.Fatal(tags)
		}
	}
}

func TestMergePolicySnapshotsJoinHistory(t *testing.T) {
	for _, limit := range []uint64{8 << 20, 1} {
		t.Run(fmt.Sprint(limit), func(t *testing.T) {
			ctx := context.Background()
			cfg := mergeTestConfig(t)
			a, err := Open(ctx, cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer a.Close()
			row := ids.NewRowID()
			if _, err = a.ExecContext(ctx, "INSERT INTO items VALUES (?, '0','[]',4,4)", row[:]); err != nil {
				t.Fatal(err)
			}
			tx, _ := a.BeginTx(ctx, nil)
			_ = tx.CounterAdd(ctx, "items", "count", row, big.NewInt(20))
			_ = tx.SetAdd(ctx, "items", "tags", row, SetString("red"))
			if err = tx.Commit(); err != nil {
				t.Fatal(err)
			}
			tx, _ = a.BeginTx(ctx, nil)
			_ = tx.SetRemove(ctx, "items", "tags", row, SetString("red"))
			if err = tx.Commit(); err != nil {
				t.Fatal(err)
			}
			other := cfg
			other.Path = t.TempDir()
			other.NodeID = ids.NewNodeID()
			other.OriginSigning = testidentity.Config(other.NodeID)
			other.Replication.SnapshotAtomicMergeBytes = limit
			b, err := Open(ctx, other)
			if err != nil {
				t.Fatal(err)
			}
			defer b.Close()
			empty := ids.NewRowID()
			if _, err = a.ExecContext(ctx, "INSERT INTO items VALUES (?, '0','[]',0,0)", empty[:]); err != nil {
				t.Fatal(err)
			}
			index := uint64(0)
			err = a.store.ExportSnapshot(2, func(manifest *codec.SnapshotManifest, cells []codec.SnapshotCell, last bool) error {
				_, err := b.ApplySnapshotChunk(ctx, manifest, index, cells, last)
				index++
				return err
			})
			if err != nil {
				t.Fatal(err)
			}
			var count, tags string
			if err = b.QueryRowContext(ctx, "SELECT count,tags FROM items WHERE id=?", row[:]).Scan(&count, &tags); err != nil {
				t.Fatal(err)
			}
			if count != "20" || tags != "[]" {
				t.Fatalf("%s %s", count, tags)
			}
			if err = b.QueryRowContext(ctx, "SELECT count,tags FROM items WHERE id=?", empty[:]).Scan(&count, &tags); err != nil || count != "0" || tags != "[]" {
				t.Fatalf("neutral snapshot %s %s %v", count, tags, err)
			}
			records, _ := b.store.CRDTRecords(1, row, 3)
			if len(records) != 2 {
				t.Fatalf("lost removed tag history: %d", len(records))
			}
		})
	}
}

func TestMergePolicyBridgeOwnership(t *testing.T) {
	ctx := context.Background()
	cfg := mergeTestConfig(t)
	d, err := Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	row := ids.NewRowID()
	source := ids.NewDBID()
	origin := ids.NewNodeID()
	key := append([]byte{'p'}, source[:]...)
	key = append(key, origin[:]...)
	importCount := func(n int64, sequence uint64) {
		t.Helper()
		tx, err := d.BeginBridgeImportTx(ctx, ids.NewTxID(), source, "counter", ids.NewTxID(), sequence, sequence, false, nil)
		if err != nil {
			t.Fatal(err)
		}
		if sequence == 1 {
			_, err = tx.ExecContext(ctx, "INSERT INTO items VALUES (?, '0','[]',4,4)", row[:])
		} else {
			_, err = tx.ExecContext(ctx, "UPDATE items SET count='0' WHERE id=?", row[:])
		}
		if err != nil {
			t.Fatal(err)
		}
		if err = tx.ImportMergeColumn(ctx, "items", "count", row, schema.PN_COUNTER, codec.Null(), []codec.CRDTRecord{{Key: key, Data: big.NewInt(n).Bytes()}}); err != nil {
			t.Fatal(err)
		}
		if err = tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	check := func(want string) {
		t.Helper()
		var got string
		if err := d.QueryRowContext(ctx, "SELECT count FROM items").Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("got %s want %s", got, want)
		}
	}
	importCount(10, 1)
	check("10")
	tx, _ := d.BeginTx(ctx, nil)
	if err = tx.CounterAdd(ctx, "items", "count", row, big.NewInt(3)); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	check("13")
	// High causal branches and neutral columns survive snapshot rebuild.
	other := cfg
	other.Path = t.TempDir()
	other.NodeID = ids.NewNodeID()
	other.OriginSigning = testidentity.Config(other.NodeID)
	copy, err := Open(ctx, other)
	if err != nil {
		t.Fatal(err)
	}
	index := uint64(0)
	err = d.store.ExportSnapshot(2, func(manifest *codec.SnapshotManifest, cells []codec.SnapshotCell, last bool) error {
		_, err := copy.ApplySnapshotChunk(ctx, manifest, index, cells, last)
		index++
		return err
	})
	if err != nil {
		copy.Close()
		t.Fatal(err)
	}
	var snapshotCount string
	if err = copy.QueryRowContext(ctx, "SELECT count FROM items WHERE id=?", row[:]).Scan(&snapshotCount); err != nil || snapshotCount != "13" {
		copy.Close()
		t.Fatalf("High snapshot %s %v", snapshotCount, err)
	}
	if err = copy.ReleaseBridgeOwnership(ctx, "items", row, "count"); err != nil {
		copy.Close()
		t.Fatal(err)
	}
	if err = copy.QueryRowContext(ctx, "SELECT count FROM items WHERE id=?", row[:]).Scan(&snapshotCount); err != nil || snapshotCount != "10" {
		copy.Close()
		t.Fatalf("Low snapshot %s %v", snapshotCount, err)
	}
	if err = copy.Close(); err != nil {
		t.Fatal(err)
	}

	importCount(20, 2)
	check("13")
	if err = d.ReleaseBridgeOwnership(ctx, "items", row, "count"); err != nil {
		t.Fatal(err)
	}
	check("20")
	tx, _ = d.BeginTx(ctx, nil)
	if err = tx.CounterAdd(ctx, "items", "count", row, big.NewInt(1)); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	check("21")
}

func TestMergePolicyConcurrentGroupWriters(t *testing.T) {
	ctx := context.Background()
	cfg := mergeTestConfig(t)
	cfg.Durability.GroupCommit.MaxDelay = 10 * time.Millisecond
	d, err := Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	row := ids.NewRowID()
	if _, err = d.ExecContext(ctx, "INSERT INTO items VALUES (?, '0','[]',0,0)", row[:]); err != nil {
		t.Fatal(err)
	}
	const writers = 8
	const iterations = 12
	var wg sync.WaitGroup
	errors := make(chan error, writers)
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				tx, err := d.BeginTx(ctx, nil)
				if err != nil {
					errors <- err
					return
				}
				if err = tx.CounterAdd(ctx, "items", "count", row, big.NewInt(1)); err == nil {
					err = tx.SetAdd(ctx, "items", "tags", row, SetString(fmt.Sprintf("%d/%d", w, i)))
				}
				if err != nil {
					tx.Rollback()
					errors <- err
					return
				}
				if err = tx.Commit(); err != nil {
					errors <- err
					return
				}
			}
		}(w)
	}
	wg.Wait()
	close(errors)
	for err := range errors {
		t.Fatal(err)
	}
	tx, err := d.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	count, err := tx.CounterValue(ctx, "items", "count", row)
	if err != nil || count.Int64() != writers*iterations {
		t.Fatalf("count=%v err=%v", count, err)
	}
	tags, err := tx.SetValues(ctx, "items", "tags", row)
	if err != nil || len(tags) != writers*iterations {
		t.Fatalf("tags=%d err=%v", len(tags), err)
	}
}
