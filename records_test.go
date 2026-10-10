package murmur

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/marcgauthier/murmur/backup"
	"github.com/marcgauthier/murmur/codec"
	"github.com/marcgauthier/murmur/crdt"
	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/internal/testidentity"
	"github.com/marcgauthier/murmur/rime"
)

type facadeRecord struct {
	ID   ids.RowID `rime:"primary"`
	Name string
}

func TestTypedWriteCallbacksPrepareConcurrently(t *testing.T) {
	cfg := testConfig(t.TempDir())
	cfg.Schema.Tables = nil
	cfg.Tables = []TableDefinition{recordDefinition(t)}
	db, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	table, err := tableOf[facadeRecord](db, "records")
	if err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	results := make(chan error, 2)
	for i := 0; i < 2; i++ {
		i := i
		go func() {
			results <- db.WriteTxContext(context.Background(), func(tx *Tx) error {
				entered <- struct{}{}
				<-release
				return table.Insert(tx, &facadeRecord{ID: ids.NewRowID(), Name: fmt.Sprintf("parallel-%d", i)})
			})
		}()
	}
	bothEntered := true
	for i := 0; i < 2; i++ {
		select {
		case <-entered:
		case <-time.After(2 * time.Second):
			bothEntered = false
			i = 2
		}
	}
	close(release)
	for i := 0; i < 2; i++ {
		if err := <-results; err != nil {
			t.Errorf("typed write %d: %v", i, err)
		}
	}
	if !bothEntered {
		t.Fatal("typed transaction callbacks were serialized before writer admission")
	}
}

func TestTypedNodeLocalTableCommitsWithReplicatedRowsAndReopens(t *testing.T) {
	cfg := testConfig(t.TempDir())
	cfg.Schema.Tables = nil
	localDefinition, err := define[facadeRecord]("private_records", 72, RecordOptions{
		PrimaryField: "ID",
		FieldIDs:     map[string]uint32{"ID": 1, "Name": 2},
		Scope:        TableScopeNodeLocal,
	})
	if err != nil {
		t.Fatal(err)
	}
	cfg.Tables = []TableDefinition{recordDefinition(t), localDefinition}
	db, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("open with node-local table: %v", err)
	}
	replicated, err := tableOf[facadeRecord](db, "records")
	if err != nil {
		t.Fatal(err)
	}
	local, err := tableOf[facadeRecord](db, "private_records")
	if err != nil {
		t.Fatal(err)
	}
	replicatedRow := &facadeRecord{ID: ids.NewRowID(), Name: "cluster"}
	localRow := &facadeRecord{ID: ids.NewRowID(), Name: "node"}
	if err := db.WriteTxContext(context.Background(), func(tx *Tx) error {
		if err := replicated.Insert(tx, replicatedRow); err != nil {
			return err
		}
		return local.Insert(tx, localRow)
	}); err != nil {
		t.Fatalf("mixed typed write: %v", err)
	}
	if replicatedCells, err := db.store.GetRow(72, localRow.ID); err != nil || len(replicatedCells) != 0 {
		t.Fatalf("node-local row appeared in replicated cells: %v, %v", replicatedCells, err)
	}
	if cells, _, _, err := db.store.GetLocalRow(72, localRow.ID); err != nil || len(cells) == 0 {
		t.Fatalf("node-local durable row: cells=%v err=%v", cells, err)
	}
	seq, err := db.store.LocalSeq()
	if err != nil || seq != 1 {
		t.Fatalf("replicated sequence = %d, %v; want one mixed transaction", seq, err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("reopen node-local database: %v", err)
	}
	defer db.Close()
	replicated, err = tableOf[facadeRecord](db, "records")
	if err != nil {
		t.Fatal(err)
	}
	local, err = tableOf[facadeRecord](db, "private_records")
	if err != nil {
		t.Fatal(err)
	}
	gotReplicated, err := replicated.Get(replicatedRow.ID)
	if err != nil || gotReplicated == nil || gotReplicated.Name != replicatedRow.Name {
		t.Fatalf("reopened replicated row = %+v err=%v", gotReplicated, err)
	}
	gotLocal, err := local.Get(localRow.ID)
	if err != nil || gotLocal == nil || gotLocal.Name != localRow.Name {
		t.Fatalf("reopened local row = %+v err=%v", gotLocal, err)
	}
}

func TestTypedDatabaseCanUseOnlyNodeLocalTables(t *testing.T) {
	cfg := testConfig(t.TempDir())
	cfg.Schema.Tables = nil
	definition, err := define[facadeRecord]("private_records", 72, RecordOptions{
		PrimaryField: "ID",
		FieldIDs:     map[string]uint32{"ID": 1, "Name": 2},
		Scope:        TableScopeNodeLocal,
	})
	if err != nil {
		t.Fatal(err)
	}
	cfg.Tables = []TableDefinition{definition}
	db, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("open local-only database: %v", err)
	}
	defer db.Close()
	table, err := tableOf[facadeRecord](db, "private_records")
	if err != nil {
		t.Fatal(err)
	}
	row := &facadeRecord{ID: ids.NewRowID(), Name: "private"}
	if err := db.WriteTxContext(context.Background(), func(tx *Tx) error { return table.Insert(tx, row) }); err != nil {
		t.Fatalf("write local-only database: %v", err)
	}
	got, err := table.Get(row.ID)
	if err != nil || got == nil || got.Name != row.Name {
		t.Fatalf("read local-only record = %+v, %v", got, err)
	}
	manifestTables, err := db.SchemaTables()
	if err != nil || len(manifestTables) != 0 {
		t.Fatalf("local-only tables entered cluster schema manifest: %v, %v", manifestTables, err)
	}
}

func TestTypedEphemeralTableResetsOnOpenAndRejectsMixedTransactions(t *testing.T) {
	cfg := testConfig(t.TempDir())
	cfg.Schema.Tables = nil
	ephemeralDefinition, err := define[facadeRecord]("scratch_records", 73, RecordOptions{
		PrimaryField: "ID",
		FieldIDs:     map[string]uint32{"ID": 1, "Name": 2},
		Scope:        TableScopeEphemeral,
	})
	if err != nil {
		t.Fatal(err)
	}
	cfg.Tables = []TableDefinition{recordDefinition(t), ephemeralDefinition}
	db, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("open ephemeral table database: %v", err)
	}
	replicated, err := tableOf[facadeRecord](db, "records")
	if err != nil {
		t.Fatal(err)
	}
	ephemeral, err := tableOf[facadeRecord](db, "scratch_records")
	if err != nil {
		t.Fatal(err)
	}
	sub, err := ephemeral.Subscribe(context.Background(), recordSubscriptionOptions{})
	if err != nil {
		t.Fatalf("subscribe to ephemeral table: %v", err)
	}
	defer sub.Close()
	if initial := receiveRecordEvent(t, sub); initial.Type != EventInitial || len(initial.Rows) != 0 {
		t.Fatalf("ephemeral initial event = %+v", initial)
	}
	scratch := &facadeRecord{ID: ids.NewRowID(), Name: "temporary"}
	if err := db.WriteTxContext(context.Background(), func(tx *Tx) error { return ephemeral.Insert(tx, scratch) }); err != nil {
		t.Fatalf("ephemeral-only write: %v", err)
	}
	if got, err := ephemeral.Get(scratch.ID); err != nil || got == nil || got.Name != scratch.Name {
		t.Fatalf("ephemeral read = %+v, %v", got, err)
	}
	if event := receiveRecordEvent(t, sub); event.Type != EventUpdate || len(event.Changes) != 1 || event.Changes[0].Type != recordAdded || event.Changes[0].Key != scratch.ID {
		t.Fatalf("ephemeral subscription did not observe published insert: %+v", event)
	}
	if seq, err := db.store.LocalSeq(); err != nil || seq != 0 {
		t.Fatalf("ephemeral write advanced durable replication sequence: %d, %v", seq, err)
	}
	durable := &facadeRecord{ID: ids.NewRowID(), Name: "durable"}
	mixedErr := db.WriteTxContext(context.Background(), func(tx *Tx) error {
		if err := replicated.Insert(tx, durable); err != nil {
			return err
		}
		return ephemeral.Insert(tx, &facadeRecord{ID: ids.NewRowID(), Name: "mixed"})
	})
	if mixedErr == nil {
		t.Fatal("mixed durable/ephemeral transaction was accepted")
	}
	if got, err := replicated.Get(durable.ID); err == nil || got != nil {
		t.Fatalf("rejected mixed write changed durable table: %+v, %v", got, err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("reopen ephemeral table database: %v", err)
	}
	defer db.Close()
	ephemeral, err = tableOf[facadeRecord](db, "scratch_records")
	if err != nil {
		t.Fatal(err)
	}
	if got, err := ephemeral.Get(scratch.ID); err == nil || got != nil {
		t.Fatalf("ephemeral row survived reopen: %+v, %v", got, err)
	}
	tables, err := db.SchemaTables()
	if err != nil || len(tables) != 1 || tables[0].Name != "records" {
		t.Fatalf("node-local scope entered manifest: tables=%+v err=%v", tables, err)
	}
}

func TestCloseDrainsTypedWritePreparation(t *testing.T) {
	cfg := testConfig(t.TempDir())
	cfg.Schema.Tables = nil
	cfg.Tables = []TableDefinition{recordDefinition(t)}
	db, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	table, err := tableOf[facadeRecord](db, "records")
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	release := make(chan struct{})
	writeResult := make(chan error, 1)
	go func() {
		writeResult <- db.WriteTxContext(context.Background(), func(tx *Tx) error {
			close(started)
			<-release
			return table.Insert(tx, &facadeRecord{ID: ids.NewRowID(), Name: "close-drain"})
		})
	}()
	<-started
	closeResult := make(chan error, 1)
	go func() { closeResult <- db.Close() }()
	closedEarly := false
	var closeErr error
	select {
	case closeErr = <-closeResult:
		closedEarly = true
	case <-time.After(30 * time.Millisecond):
	}
	close(release)
	writeErr := <-writeResult
	if !closedEarly {
		closeErr = <-closeResult
	}
	if closeErr != nil {
		t.Fatal(closeErr)
	}
	if closedEarly {
		t.Fatal("Close returned before staged callback drained")
	}
	if !errors.Is(writeErr, ErrClosed) {
		t.Fatalf("write admitted after Close began: %v", writeErr)
	}
}

func TestConcurrentTypedWritesToSameRowConflict(t *testing.T) {
	cfg := testConfig(t.TempDir())
	cfg.Schema.Tables = nil
	cfg.Tables = []TableDefinition{recordDefinition(t)}
	db, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	table, err := tableOf[facadeRecord](db, "records")
	if err != nil {
		t.Fatal(err)
	}
	row := &facadeRecord{ID: ids.NewRowID(), Name: "base"}
	if err := db.WriteTxContext(context.Background(), func(tx *Tx) error {
		return table.Insert(tx, row)
	}); err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	results := make(chan error, 2)
	for i := 0; i < 2; i++ {
		i := i
		go func() {
			results <- db.WriteTxContext(context.Background(), func(tx *Tx) error {
				if err := table.Update(tx, row.ID, func(rec *facadeRecord) error {
					rec.Name = fmt.Sprintf("writer-%d", i)
					return nil
				}); err != nil {
					return err
				}
				entered <- struct{}{}
				<-release
				return nil
			})
		}()
	}
	for i := 0; i < 2; i++ {
		select {
		case <-entered:
		case <-time.After(2 * time.Second):
			close(release)
			<-results
			<-results
			t.Fatal("same-row callbacks did not stage concurrently")
		}
	}
	close(release)
	resultsByStatus := [2]int{}
	for i := 0; i < 2; i++ {
		err := <-results
		switch {
		case err == nil:
			resultsByStatus[0]++
		case errors.Is(err, rime.ErrConflict):
			resultsByStatus[1]++
		default:
			t.Fatalf("concurrent same-row write error = %v, want nil or ErrConflict", err)
		}
	}
	if resultsByStatus != [2]int{1, 1} {
		t.Fatalf("concurrent same-row results = success:%d conflict:%d, want one each", resultsByStatus[0], resultsByStatus[1])
	}
}

func TestTypedDurableCommitCrashBoundaries(t *testing.T) {
	cfg := testConfig(t.TempDir())
	cfg.Schema.Tables = nil
	localDefinition, err := define[facadeRecord]("private_records", 72, RecordOptions{
		PrimaryField: "ID", FieldIDs: map[string]uint32{"ID": 1, "Name": 2}, Scope: TableScopeNodeLocal,
	})
	if err != nil {
		t.Fatal(err)
	}
	cfg.Tables = []TableDefinition{recordDefinition(t), localDefinition}
	db, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	table, err := tableOf[facadeRecord](db, "records")
	if err != nil {
		t.Fatal(err)
	}
	localTable, err := tableOf[facadeRecord](db, "private_records")
	if err != nil {
		t.Fatal(err)
	}
	before := &facadeRecord{ID: ids.NewRowID(), Name: "before-durable"}
	beforeLocal := &facadeRecord{ID: ids.NewRowID(), Name: "before-durable-local"}
	db.crash = &crashHooks{beforeDurable: func() error { return errors.New("injected before durable commit") }}
	err = db.WriteTxContext(context.Background(), func(tx *Tx) error {
		if err := table.Insert(tx, before); err != nil {
			return err
		}
		return localTable.Insert(tx, beforeLocal)
	})
	if err == nil || errors.Is(err, ErrCommitOutcomeUncertain) {
		t.Fatalf("pre-durable fault = %v, want definite failure", err)
	}
	if _, getErr := table.Get(before.ID); !errors.Is(getErr, rime.ErrNotFound) {
		t.Fatalf("pre-durable row lookup = %v, want not found", getErr)
	}
	if _, getErr := localTable.Get(beforeLocal.ID); !errors.Is(getErr, rime.ErrNotFound) {
		t.Fatalf("pre-durable local row lookup = %v, want not found", getErr)
	}

	after := &facadeRecord{ID: ids.NewRowID(), Name: "after-durable"}
	afterLocal := &facadeRecord{ID: ids.NewRowID(), Name: "after-durable-local"}
	db.crash = &crashHooks{afterDurable: func() error { return errors.New("injected lost acknowledgement") }}
	err = db.WriteTxContext(context.Background(), func(tx *Tx) error {
		if err := table.Insert(tx, after); err != nil {
			return err
		}
		return localTable.Insert(tx, afterLocal)
	})
	var uncertain *CommitOutcomeUncertainError
	if !errors.As(err, &uncertain) || uncertain.TxID == (ids.TxID{}) {
		t.Fatalf("post-durable fault = %v, want uncertain outcome with transaction ID", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("reopen after lost acknowledgement: %v", err)
	}
	defer reopened.Close()
	committed, err := reopened.HasTransactionReceipt(uncertain.TxID)
	if err != nil || !committed {
		t.Fatalf("uncertain transaction receipt = %t, %v; want committed", committed, err)
	}
	table, err = tableOf[facadeRecord](reopened, "records")
	if err != nil {
		t.Fatal(err)
	}
	if got, err := table.Get(after.ID); err != nil || got.Name != after.Name {
		t.Fatalf("durable typed row after reopen = %+v, %v", got, err)
	}
	localTable, err = tableOf[facadeRecord](reopened, "private_records")
	if err != nil {
		t.Fatal(err)
	}
	if got, err := localTable.Get(afterLocal.ID); err != nil || got.Name != afterLocal.Name {
		t.Fatalf("durable node-local row after reopen = %+v, %v", got, err)
	}
}

func TestTypedProcessCrashAfterDurableCommit(t *testing.T) {
	const helperEnv = "MURMUR_TYPED_DURABLE_CRASH_HELPER"
	if os.Getenv(helperEnv) == "1" {
		path := os.Getenv("MURMUR_TYPED_CRASH_PATH")
		nodeHex := os.Getenv("MURMUR_TYPED_CRASH_NODE")
		nodeBytes, err := hex.DecodeString(nodeHex)
		if err != nil || len(nodeBytes) != len(ids.NodeID{}) {
			t.Fatalf("invalid helper node ID %q", nodeHex)
		}
		var node ids.NodeID
		copy(node[:], nodeBytes)
		cfg := testConfig(path)
		cfg.NodeID = node
		cfg.OriginSigning = testidentity.Config(node)
		cfg.Schema.Tables = nil
		cfg.Tables = []TableDefinition{recordDefinition(t)}
		db, err := Open(context.Background(), cfg)
		if err != nil {
			t.Fatalf("helper open: %v", err)
		}
		phase := os.Getenv("MURMUR_TYPED_CRASH_PHASE")
		exitCrash := func(code int) func() error {
			return func() error {
				// Deliberately bypass database close and test cleanup to model
				// process death at the selected durable commit boundary.
				os.Exit(code)
				return nil
			}
		}
		switch phase {
		case "before-durable":
			db.crash = &crashHooks{beforeDurable: exitCrash(85)}
		case "after-durable":
			db.crash = &crashHooks{afterDurable: exitCrash(86)}
		default:
			t.Fatalf("unexpected helper crash phase %q", phase)
		}
		table, err := tableOf[facadeRecord](db, "records")
		if err != nil {
			t.Fatalf("helper tableOf: %v", err)
		}
		rowID := ids.RowID{0x71, 0x72, 0x73}
		if err := db.WriteTxContext(context.Background(), func(tx *Tx) error {
			return table.Insert(tx, &facadeRecord{ID: rowID, Name: "process-crash-" + phase})
		}); err != nil {
			t.Fatalf("helper write returned before crash hook: %v", err)
		}
		t.Fatal("helper write returned without triggering the crash hook")
	}

	rowID := ids.RowID{0x71, 0x72, 0x73}
	for _, tc := range []struct {
		phase      string
		exitCode   int
		wantStored bool
	}{
		{phase: "before-durable", exitCode: 85},
		{phase: "after-durable", exitCode: 86, wantStored: true},
	} {
		t.Run(tc.phase, func(t *testing.T) {
			path := t.TempDir()
			node := NewNodeID()
			cmd := exec.Command(os.Args[0], "-test.run=^TestTypedProcessCrashAfterDurableCommit$", "-test.count=1")
			cmd.Env = append(os.Environ(),
				helperEnv+"=1",
				"MURMUR_TYPED_CRASH_PATH="+path,
				"MURMUR_TYPED_CRASH_NODE="+hex.EncodeToString(node[:]),
				"MURMUR_TYPED_CRASH_PHASE="+tc.phase,
			)
			output, err := cmd.CombinedOutput()
			var exitErr *exec.ExitError
			if !errors.As(err, &exitErr) || exitErr.ExitCode() != tc.exitCode {
				t.Fatalf("crash helper exit = %v, output %s; want exit %d", err, output, tc.exitCode)
			}

			cfg := testConfig(path)
			cfg.NodeID = node
			cfg.OriginSigning = testidentity.Config(node)
			cfg.Schema.Tables = nil
			cfg.Tables = []TableDefinition{recordDefinition(t)}
			db, err := Open(context.Background(), cfg)
			if err != nil {
				t.Fatalf("reopen after abrupt process death: %v; helper output: %s", err, output)
			}
			defer db.Close()
			table, err := tableOf[facadeRecord](db, "records")
			if err != nil {
				t.Fatal(err)
			}
			got, err := table.Get(rowID)
			if tc.wantStored {
				if err != nil || got.Name != "process-crash-"+tc.phase {
					t.Fatalf("typed record after crash recovery = %+v, %v; want durable row", got, err)
				}
			} else if !errors.Is(err, rime.ErrNotFound) {
				t.Fatalf("typed record after pre-durable crash = %+v, %v; want not found", got, err)
			}
		})
	}
}

func TestTypedSchemaMigrationProcessCrashBoundary(t *testing.T) {
	const helperEnv = "MURMUR_TYPED_SCHEMA_CRASH_HELPER"
	if os.Getenv(helperEnv) == "1" {
		path := os.Getenv("MURMUR_TYPED_SCHEMA_CRASH_PATH")
		nodeBytes, err := hex.DecodeString(os.Getenv("MURMUR_TYPED_SCHEMA_CRASH_NODE"))
		if err != nil || len(nodeBytes) != len(ids.NodeID{}) {
			t.Fatalf("invalid schema crash node id")
		}
		var node ids.NodeID
		copy(node[:], nodeBytes)
		cfg := testConfig(path)
		cfg.NodeID = node
		cfg.OriginSigning = testidentity.Config(node)
		cfg.Schema.Tables = nil
		cfg.Tables = []TableDefinition{recordDefinition(t)}
		db, err := Open(context.Background(), cfg)
		if err != nil {
			t.Fatalf("schema crash helper open: %v", err)
		}
		phase := os.Getenv("MURMUR_TYPED_SCHEMA_CRASH_PHASE")
		exitCrash := func(code int) func() error {
			return func() error {
				os.Exit(code)
				return nil
			}
		}
		switch phase {
		case "before-store":
			db.crash = &crashHooks{beforeSchemaStore: exitCrash(85)}
		case "after-store":
			db.crash = &crashHooks{afterSchemaStore: exitCrash(86)}
		default:
			t.Fatalf("unexpected schema crash phase %q", phase)
		}
		if err := db.MigrateRecords(context.Background(), []TableDefinition{recordDefinitionV2(t)}); err != nil {
			t.Fatalf("schema migration returned before process crash: %v", err)
		}
		t.Fatal("schema migration returned without triggering crash hook")
	}

	for _, tc := range []struct {
		phase    string
		exitCode int
		epoch    uint64
		v2       bool
	}{
		{phase: "before-store", exitCode: 85, epoch: 1},
		{phase: "after-store", exitCode: 86, epoch: 2, v2: true},
	} {
		t.Run(tc.phase, func(t *testing.T) {
			path := t.TempDir()
			node := NewNodeID()
			cfg := testConfig(path)
			cfg.NodeID = node
			cfg.OriginSigning = testidentity.Config(node)
			cfg.Schema.Tables = nil
			cfg.Tables = []TableDefinition{recordDefinition(t)}
			db, err := Open(context.Background(), cfg)
			if err != nil {
				t.Fatal(err)
			}
			base, err := tableOf[facadeRecord](db, "records")
			if err != nil {
				t.Fatal(err)
			}
			rows := make([]*facadeRecord, 8)
			for i := range rows {
				rows[i] = &facadeRecord{ID: ids.RowID{byte(i + 1), 0x52, 0x53}, Name: fmt.Sprintf("schema-crash-%d", i)}
			}
			if err := db.WriteTxContext(context.Background(), func(tx *Tx) error { return base.InsertMany(tx, rows) }); err != nil {
				t.Fatalf("seed typed rows: %v", err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}

			cmd := exec.Command(os.Args[0], "-test.run=^TestTypedSchemaMigrationProcessCrashBoundary$", "-test.count=1")
			cmd.Env = append(os.Environ(),
				helperEnv+"=1",
				"MURMUR_TYPED_SCHEMA_CRASH_PATH="+path,
				"MURMUR_TYPED_SCHEMA_CRASH_NODE="+hex.EncodeToString(node[:]),
				"MURMUR_TYPED_SCHEMA_CRASH_PHASE="+tc.phase,
			)
			if err := cmd.Run(); err == nil {
				t.Fatal("schema migration helper exited successfully; wanted injected process death")
			} else if exit, ok := err.(*exec.ExitError); !ok || exit.ExitCode() != tc.exitCode {
				t.Fatalf("schema migration helper error = %v, want exit %d", err, tc.exitCode)
			}

			if tc.v2 {
				cfg.Tables = []TableDefinition{recordDefinitionV2(t)}
			}
			reopened, err := Open(context.Background(), cfg)
			if err != nil {
				t.Fatalf("reopen after %s: %v", tc.phase, err)
			}
			defer reopened.Close()
			if got := reopened.Status().SchemaEpoch; got != tc.epoch {
				t.Fatalf("schema epoch after %s crash = %d, want %d", tc.phase, got, tc.epoch)
			}
			if tc.v2 {
				table, err := tableOf[facadeRecordV2](reopened, "records")
				if err != nil {
					t.Fatal(err)
				}
				for _, row := range rows {
					got, err := table.Get(row.ID)
					if err != nil || got.Name != row.Name || got.Secret != "" {
						t.Fatalf("row after durable schema crash = %+v, err=%v", got, err)
					}
				}
			} else {
				table, err := tableOf[facadeRecord](reopened, "records")
				if err != nil {
					t.Fatal(err)
				}
				for _, row := range rows {
					got, err := table.Get(row.ID)
					if err != nil || got.Name != row.Name {
						t.Fatalf("row before schema store crash = %+v, err=%v", got, err)
					}
				}
			}
		})
	}
}

type facadeRecordV2 struct {
	ID     ids.RowID `rime:"primary"`
	Name   string
	Secret string
}

type facadeNestedProfileV1 struct {
	Name string
}

type facadeNestedProfileV2 struct {
	Name   string
	Region string
}

type facadeNestedRecordV1 struct {
	ID      ids.RowID `rime:"primary"`
	Profile facadeNestedProfileV1
}

type facadeNestedRecordV2 struct {
	ID      ids.RowID `rime:"primary"`
	Profile facadeNestedProfileV2
}

type facadeCollectionItemV1 struct{ Name string }
type facadeCollectionItemV2 struct{ Name, Region string }
type facadeCollectionRecordV1 struct {
	ID    ids.RowID `rime:"primary"`
	Items []facadeCollectionItemV1
	ByKey map[string]facadeCollectionItemV1
}
type facadeCollectionRecordV2 struct {
	ID    ids.RowID `rime:"primary"`
	Items []facadeCollectionItemV2
	ByKey map[string]facadeCollectionItemV2
}

type facadeToken string

type facadeCustomRecord struct {
	ID    ids.RowID `rime:"primary"`
	Token facadeToken
}

var facadeCustomCloneCalls atomic.Int64
var facadeCustomEqualCalls atomic.Int64

func recordDefinition(t *testing.T) TableDefinition {
	t.Helper()
	definition, err := define[facadeRecord]("records", 71, RecordOptions{
		PrimaryField: "ID",
		FieldIDs:     map[string]uint32{"ID": 1, "Name": 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	return definition
}

func recordReferenceDefinition(t *testing.T) TableDefinition {
	t.Helper()
	definition, err := define[facadeRecord]("record_refs", 74, RecordOptions{
		PrimaryField: "ID",
		FieldIDs:     map[string]uint32{"ID": 1, "Name": 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	return definition
}

func recordDefinitionV2(t *testing.T) TableDefinition {
	t.Helper()
	definition, err := define[facadeRecordV2]("records", 71, RecordOptions{
		PrimaryField: "ID",
		FieldIDs:     map[string]uint32{"ID": 1, "Name": 2, "Secret": 3},
	})
	if err != nil {
		t.Fatal(err)
	}
	return definition
}

func nestedRecordDefinitionV1(t *testing.T) TableDefinition {
	t.Helper()
	d, err := define[facadeNestedRecordV1]("nested_records", 72, RecordOptions{
		PrimaryField: "ID",
		FieldIDs:     map[string]uint32{"ID": 1, "Profile": 2, "Profile.Name": 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func nestedRecordDefinitionV2(t *testing.T) TableDefinition {
	t.Helper()
	d, err := define[facadeNestedRecordV2]("nested_records", 72, RecordOptions{
		PrimaryField: "ID",
		FieldIDs:     map[string]uint32{"ID": 1, "Profile": 2, "Profile.Name": 1, "Profile.Region": 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func collectionRecordDefinitionV1(t *testing.T) TableDefinition {
	t.Helper()
	d, err := define[facadeCollectionRecordV1]("collection_records", 75, RecordOptions{
		PrimaryField: "ID",
		FieldIDs:     map[string]uint32{"ID": 1, "Items": 2, "Items[].Name": 1, "ByKey": 3, "ByKey{}.Name": 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func collectionRecordDefinitionV2(t *testing.T) TableDefinition {
	t.Helper()
	d, err := define[facadeCollectionRecordV2]("collection_records", 75, RecordOptions{
		PrimaryField: "ID",
		FieldIDs:     map[string]uint32{"ID": 1, "Items": 2, "Items[].Name": 1, "Items[].Region": 2, "ByKey": 3, "ByKey{}.Name": 1, "ByKey{}.Region": 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func customRecordDefinition(t *testing.T) TableDefinition {
	t.Helper()
	d, err := define[facadeCustomRecord]("custom_records", 73, RecordOptions{
		PrimaryField: "ID",
		FieldIDs:     map[string]uint32{"ID": 1, "Token": 2},
		Codecs: []RecordCodec{{
			ID: "example.token", Version: 1, Example: facadeToken(""),
			Encode: func(value any) ([]byte, error) { return []byte(value.(facadeToken)), nil },
			Decode: func(data []byte, target any) error {
				value, ok := target.(*facadeToken)
				if !ok {
					return errors.New("unexpected custom token decode target")
				}
				*value = facadeToken(data)
				return nil
			},
			Clone: func(value any) (any, error) {
				facadeCustomCloneCalls.Add(1)
				return facadeToken(value.(facadeToken)), nil
			},
			Equal: func(left, right any) bool {
				facadeCustomEqualCalls.Add(1)
				return left.(facadeToken) == right.(facadeToken)
			},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func TestTypedRecordFacadeDurableReopenAndSQLIsolation(t *testing.T) {
	cfg := testConfig(t.TempDir())
	cfg.Schema.Tables = nil
	cfg.Tables = []TableDefinition{recordDefinition(t), recordReferenceDefinition(t)}
	db, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	table, err := tableOf[facadeRecord](db, "records")
	if err != nil {
		t.Fatal(err)
	}
	refs, err := tableOf[facadeRecord](db, "record_refs")
	if err != nil {
		t.Fatal(err)
	}
	want := &facadeRecord{ID: ids.NewRowID(), Name: "durable"}
	unmatched := &facadeRecord{ID: ids.NewRowID(), Name: "unmatched"}
	ref := &facadeRecord{ID: ids.NewRowID(), Name: "durable"}
	db.subMgr.mu.RLock()
	initialCursor := db.subMgr.cursor
	db.subMgr.mu.RUnlock()
	if err := db.WriteTxContext(context.Background(), func(tx *Tx) error {
		if err := table.InsertMany(tx, []*facadeRecord{want, unmatched}); err != nil {
			return err
		}
		if err := refs.Insert(tx, ref); err != nil {
			return err
		}
		query, err := table.WhereTx(tx, fieldOf[facadeRecord, string](table, "Name").Eq("durable"))
		if err != nil {
			return err
		}
		count, err := query.Count()
		if err != nil {
			return err
		}
		if count != 1 {
			return errors.New("staged query did not include newly inserted record")
		}
		aggregate, err := query.Aggregate(rime.Count[facadeRecord]())
		if err != nil {
			return err
		}
		if aggregate[0] != 1 {
			return errors.New("typed aggregate did not include staged record")
		}
		compiled, err := table.CompileTx(tx, fieldOf[facadeRecord, string](table, "Name").Eq(rime.Param[string]()))
		if err != nil {
			return err
		}
		compiledCount, err := compiled.Count("durable")
		if err != nil || compiledCount != 1 {
			return errors.New("typed compiled query did not include staged record")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	db.subMgr.mu.RLock()
	committedCursor := db.subMgr.cursor
	db.subMgr.mu.RUnlock()
	if committedCursor <= initialCursor {
		t.Fatalf("typed durable commit did not notify observers: cursor %d -> %d", initialCursor, committedCursor)
	}
	if err := db.WriteTxContext(context.Background(), func(*Tx) error { return nil }); err != nil {
		t.Fatal(err)
	}
	db.subMgr.mu.RLock()
	noOpCursor := db.subMgr.cursor
	db.subMgr.mu.RUnlock()
	if noOpCursor != committedCursor {
		t.Fatalf("typed no-op transaction advanced observer cursor: %d -> %d", committedCursor, noOpCursor)
	}
	got, err := table.Get(want.ID)
	if err != nil || *got != *want {
		t.Fatalf("Get() = %+v, %v; want %+v", got, err, want)
	}
	query := table.Where(fieldOf[facadeRecord, string](table, "Name").Eq("durable"))
	rows, err := query.Limit(4).Find()
	if err != nil || len(rows) != 1 {
		t.Fatalf("typed query rows = %d, %v; want 1", len(rows), err)
	}
	aggregates, err := query.Aggregate(rime.Count[facadeRecord]())
	if err != nil || len(aggregates) != 1 || aggregates[0] != 1 {
		t.Fatalf("typed query aggregate = %v, %v; want [1]", aggregates, err)
	}
	compiled := table.Compile(fieldOf[facadeRecord, string](table, "Name").Eq(rime.Param[string]()))
	compiledRows, err := compiled.Find("durable")
	if err != nil || len(compiledRows) != 1 || compiledRows[0].ID != want.ID {
		t.Fatalf("typed compiled query rows = %+v, %v; want durable record", compiledRows, err)
	}
	if _, err := compiled.Find(42); err == nil {
		t.Fatal("typed compiled query accepted a parameter with the wrong type")
	}
	grouped, err := table.Where(fieldOf[facadeRecord, string](table, "Name").Eq("durable")).GroupBy(fieldOf[facadeRecord, string](table, "Name")).Aggregate(rime.Count[facadeRecord]())
	if err != nil || len(grouped) != 1 || len(grouped[0].Keys) != 1 || grouped[0].Keys[0] != "durable" || grouped[0].Values[0] != 1 {
		t.Fatalf("typed grouped aggregate = %+v, %v; want one durable row with count 1", grouped, err)
	}
	joinTx, err := db.readTxContext(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	joined, err := innerJoinReadTx(joinTx, table, fieldOf[facadeRecord, string](table, "Name"), refs, fieldOf[facadeRecord, string](refs, "Name"))
	if err != nil || len(joined) != 1 || joined[0].Left.Name != "durable" || joined[0].Right.Name != "durable" {
		t.Fatalf("typed read-snapshot inner join = %+v, %v; want one durable pair", joined, err)
	}
	joined[0].Left.Name = "caller mutation"
	if got, err := table.Get(want.ID); err != nil || got.Name != "durable" {
		t.Fatalf("join exposed a mutable published record: %+v, %v", got, err)
	}
	leftJoined, err := leftJoinReadTx(joinTx, table, fieldOf[facadeRecord, string](table, "Name"), refs, fieldOf[facadeRecord, string](refs, "Name"))
	if err != nil || len(leftJoined) != 2 {
		t.Fatalf("typed read-snapshot left join = %+v, %v; want two rows", leftJoined, err)
	}
	misses := 0
	for _, row := range leftJoined {
		if row.Left.Name == "unmatched" && row.Right == nil {
			misses++
		}
	}
	if misses != 1 {
		t.Fatalf("typed left join unmatched rows = %d, want 1", misses)
	}
	if err := joinTx.Close(); err != nil {
		t.Fatal(err)
	}
	sub, err := table.Subscribe(context.Background(), recordSubscriptionOptions{}, fieldOf[facadeRecord, string](table, "Name").IsNotNull())
	if err != nil {
		t.Fatal(err)
	}
	initialEvent := receiveRecordEvent(t, sub)
	if initialEvent.Type != EventInitial || len(initialEvent.Rows) != 2 {
		t.Fatalf("typed subscription initial event = %+v; want two records", initialEvent)
	}
	initialID := initialEvent.Rows[0].ID
	initialEvent.Rows[0].Name = "subscriber mutation"
	if observed, err := table.Get(initialID); err != nil || observed.Name == "subscriber mutation" {
		t.Fatalf("typed subscription exposed published record ownership: %+v, %v", observed, err)
	}
	if err := db.WriteTxContext(context.Background(), func(tx *Tx) error {
		return table.Update(tx, unmatched.ID, func(row *facadeRecord) error {
			row.Name = "changed"
			return nil
		})
	}); err != nil {
		t.Fatal(err)
	}
	updateEvent := receiveRecordEvent(t, sub)
	if updateEvent.Type != EventUpdate || len(updateEvent.Rows) != 2 {
		t.Fatalf("typed subscription update event = %+v; want two updated records", updateEvent)
	}
	if len(updateEvent.Changes) != 1 || updateEvent.Changes[0].Type != recordUpdated || updateEvent.Changes[0].Key != unmatched.ID || updateEvent.Changes[0].Row == nil || updateEvent.Changes[0].Row.Name != "changed" {
		t.Fatalf("typed subscription keyed diff = %+v; want one update for %s", updateEvent.Changes, unmatched.ID)
	}
	changedVisible := false
	for _, row := range updateEvent.Rows {
		changedVisible = changedVisible || row.Name == "changed"
	}
	if !changedVisible {
		t.Fatalf("typed subscription update omitted changed row: %+v", updateEvent.Rows)
	}
	if err := sub.Close(); err != nil {
		t.Fatal(err)
	}
	resumed, err := table.Subscribe(context.Background(), recordSubscriptionOptions{ResumeFrom: initialEvent.ResumeCursor}, fieldOf[facadeRecord, string](table, "Name").IsNotNull())
	if err != nil {
		t.Fatalf("resume typed subscription: %v", err)
	}
	resumedEvent := receiveRecordEventType(t, resumed, EventUpdate)
	if resumedEvent.Cursor != updateEvent.Cursor || len(resumedEvent.Rows) != 2 {
		t.Fatalf("resumed typed subscription event = %+v; want latest snapshot at cursor %d", resumedEvent, updateEvent.Cursor)
	}
	if err := resumed.Close(); err != nil {
		t.Fatal(err)
	}
	future := updateEvent.ResumeCursor
	future.Sequence++
	if _, err := table.Subscribe(context.Background(), recordSubscriptionOptions{ResumeFrom: future}); !errors.Is(err, ErrSubscriptionExpired) {
		t.Fatalf("future typed subscription cursor error = %v; want ErrSubscriptionExpired", err)
	}
	membership, err := table.Subscribe(context.Background(), recordSubscriptionOptions{}, fieldOf[facadeRecord, string](table, "Name").Eq("changed"))
	if err != nil {
		t.Fatal(err)
	}
	if initial := receiveRecordEvent(t, membership); len(initial.Rows) != 1 || initial.Rows[0].ID != unmatched.ID {
		t.Fatalf("typed membership initial rows = %+v", initial.Rows)
	}
	if err := db.WriteTxContext(context.Background(), func(tx *Tx) error {
		return table.Update(tx, unmatched.ID, func(row *facadeRecord) error {
			row.Name = "left filter"
			return nil
		})
	}); err != nil {
		t.Fatal(err)
	}
	removed := receiveRecordEvent(t, membership)
	if len(removed.Changes) != 1 || removed.Changes[0].Type != recordRemoved || removed.Changes[0].Key != unmatched.ID || removed.Changes[0].Row != nil {
		t.Fatalf("typed membership removal = %+v", removed.Changes)
	}
	if err := db.WriteTxContext(context.Background(), func(tx *Tx) error {
		return table.Update(tx, unmatched.ID, func(row *facadeRecord) error {
			row.Name = "changed"
			return nil
		})
	}); err != nil {
		t.Fatal(err)
	}
	added := receiveRecordEvent(t, membership)
	if len(added.Changes) != 1 || added.Changes[0].Type != recordAdded || added.Changes[0].Key != unmatched.ID || added.Changes[0].Row == nil {
		t.Fatalf("typed membership addition = %+v", added.Changes)
	}
	if err := membership.Close(); err != nil {
		t.Fatal(err)
	}
	resetSub, err := table.Subscribe(context.Background(), recordSubscriptionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	resetInitial := receiveRecordEvent(t, resetSub)
	oldGenerationCursor := resetInitial.ResumeCursor
	if err := db.MigrateRecords(context.Background(), []TableDefinition{recordDefinition(t), recordReferenceDefinition(t)}); err != nil {
		t.Fatalf("rebuild typed materializer: %v", err)
	}
	resetEvent := receiveRecordEventType(t, resetSub, EventReset)
	if !errors.Is(resetEvent.Err, ErrSubscriptionReset) {
		t.Fatalf("typed subscription rebuild event = %+v; want reset", resetEvent)
	}
	if err := resetSub.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := table.Subscribe(context.Background(), recordSubscriptionOptions{ResumeFrom: oldGenerationCursor}); !errors.Is(err, ErrSubscriptionExpired) {
		t.Fatalf("stale typed subscription generation error = %v; want ErrSubscriptionExpired", err)
	}
	overflowSub, err := table.Subscribe(context.Background(), recordSubscriptionOptions{BufferSize: 1}, fieldOf[facadeRecord, string](table, "Name").IsNotNull())
	if err != nil {
		t.Fatal(err)
	}
	overflowCursor := overflowSub.Cursor()
	if err := db.WriteTxContext(context.Background(), func(tx *Tx) error {
		return table.Update(tx, unmatched.ID, func(row *facadeRecord) error {
			row.Name = "overflow-reset"
			return nil
		})
	}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for overflowSub.Cursor() == overflowCursor && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if overflowSub.Cursor() == overflowCursor {
		t.Fatal("typed overflow subscription did not evaluate the committed change")
	}
	overflowReset := receiveRecordEventType(t, overflowSub, EventReset)
	if overflowReset.Type != EventReset || !errors.Is(overflowReset.Err, ErrSubscriptionReset) {
		t.Fatalf("typed subscription overflow event = %+v; want reset", overflowReset)
	}
	if err := overflowSub.Close(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := query.AggregateContext(ctx, rime.Count[facadeRecord]()); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled typed aggregate error = %v", err)
	}
	rows[0].Name = "caller mutation"
	got, err = table.Get(want.ID)
	if err != nil || got.Name != "durable" {
		t.Fatalf("query exposed a mutable published record: %+v, %v", got, err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	table, err = tableOf[facadeRecord](db, "records")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := table.Subscribe(context.Background(), recordSubscriptionOptions{ResumeFrom: updateEvent.ResumeCursor}); !errors.Is(err, ErrSubscriptionExpired) {
		t.Fatalf("cross-open typed subscription cursor error = %v; want ErrSubscriptionExpired", err)
	}
	got, err = table.Get(want.ID)
	if err != nil || *got != *want {
		t.Fatalf("reopened Get() = %+v, %v; want %+v", got, err, want)
	}
	readTx, err := db.readTxContext(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer readTx.Close()
	readSnapshot := readTx.Snapshot()
	readQuery, err := table.WhereReadTx(readTx, fieldOf[facadeRecord, string](table, "Name").Eq("durable"))
	if err != nil {
		t.Fatal(err)
	}
	if err := db.WriteTxContext(context.Background(), func(tx *Tx) error {
		return table.Update(tx, want.ID, func(record *facadeRecord) error {
			record.Name = "updated"
			return nil
		})
	}); err != nil {
		t.Fatal(err)
	}
	got, err = table.Get(want.ID)
	if err != nil || got.Name != "updated" {
		t.Fatalf("updated Get() = %+v, %v", got, err)
	}
	got, err = table.GetRead(readTx, want.ID)
	if err != nil || got.Name != "durable" {
		t.Fatalf("pinned GetRead() = %+v, %v; want original value", got, err)
	}
	rows, err = readQuery.Find()
	if err != nil || len(rows) != 1 || rows[0].Name != "durable" {
		t.Fatalf("pinned query = %+v, %v; want original value", rows, err)
	}
	oldRead, err := db.readAt(readSnapshot)
	if err != nil {
		t.Fatal(err)
	}
	defer oldRead.Close()
	got, err = table.GetRead(oldRead, want.ID)
	if err != nil || got.Name != "durable" {
		t.Fatalf("historical GetRead() = %+v, %v; want original value", got, err)
	}
}

func TestTypedSubscriptionSubscriberLimitAndRelease(t *testing.T) {
	cfg := testConfig(t.TempDir())
	cfg.Schema.Tables = nil
	cfg.Tables = []TableDefinition{recordDefinition(t)}
	cfg.Subscription.MaxSubscribers = 1
	db, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	table, err := tableOf[facadeRecord](db, "records")
	if err != nil {
		t.Fatal(err)
	}
	first, err := table.Subscribe(context.Background(), recordSubscriptionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := table.Subscribe(context.Background(), recordSubscriptionOptions{}); !errors.Is(err, ErrMaxSubscribersReached) {
		t.Fatalf("second typed subscription error = %v; want ErrMaxSubscribersReached", err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	second, err := table.Subscribe(context.Background(), recordSubscriptionOptions{})
	if err != nil {
		t.Fatalf("subscription after releasing capacity: %v", err)
	}
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestTypedExplicitTransactionCommitAndRollback(t *testing.T) {
	cfg := testConfig(t.TempDir())
	cfg.Schema.Tables = nil
	cfg.Tables = []TableDefinition{recordDefinition(t)}
	db, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	table, err := tableOf[facadeRecord](db, "records")
	if err != nil {
		t.Fatal(err)
	}
	want := &facadeRecord{ID: ids.NewRowID(), Name: "explicit-commit"}
	tx, err := db.BeginTx(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := table.Insert(tx, want); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	committedGeneration := db.Status().StateGeneration
	if err := tx.Commit(); err == nil {
		t.Fatal("second commit succeeded")
	}

	rolledBack := &facadeRecord{ID: ids.NewRowID(), Name: "explicit-rollback"}
	tx, err = db.BeginTx(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := table.Insert(tx, rolledBack); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if got := db.Status().StateGeneration; got != committedGeneration {
		t.Fatalf("typed rollback advanced durable generation: %d -> %d", committedGeneration, got)
	}
	if err := tx.Commit(); err == nil {
		t.Fatal("commit after rollback succeeded")
	}
	if _, err := table.Get(rolledBack.ID); !errors.Is(err, rime.ErrNotFound) {
		t.Fatalf("rolled-back row lookup error = %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	table, err = tableOf[facadeRecord](db, "records")
	if err != nil {
		t.Fatal(err)
	}
	got, err := table.Get(want.ID)
	if err != nil || got.Name != want.Name {
		t.Fatalf("committed row after reopen = %#v, %v", got, err)
	}
	if _, err := table.Get(rolledBack.ID); !errors.Is(err, rime.ErrNotFound) {
		t.Fatalf("rolled-back row after reopen error = %v", err)
	}
}

func TestTypedRecordRuntimeAdditiveSchemaMigration(t *testing.T) {
	cfg := testConfig(t.TempDir())
	cfg.Schema.Tables = nil
	cfg.Tables = []TableDefinition{recordDefinition(t)}
	db, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	first, err := tableOf[facadeRecord](db, "records")
	if err != nil {
		t.Fatal(err)
	}
	want := &facadeRecord{ID: ids.NewRowID(), Name: "before-migration"}
	if err := db.WriteTxContext(context.Background(), func(tx *Tx) error { return first.Insert(tx, want) }); err != nil {
		t.Fatal(err)
	}
	if err := db.MigrateRecords(context.Background(), []TableDefinition{recordDefinitionV2(t)}); err != nil {
		t.Fatalf("add typed field at runtime: %v", err)
	}
	if got := db.Status().SchemaEpoch; got != 2 {
		t.Fatalf("typed schema epoch = %d, want 2", got)
	}
	if err := db.MigrateRecords(context.Background(), []TableDefinition{recordDefinition(t)}); !errors.Is(err, ErrUnsupportedSchema) {
		t.Fatalf("dropping a typed field returned %v, want ErrUnsupportedSchema", err)
	}
	upgraded, err := tableOf[facadeRecordV2](db, "records")
	if err != nil {
		t.Fatal(err)
	}
	got, err := upgraded.Get(want.ID)
	if err != nil || got.Name != want.Name || got.Secret != "" {
		t.Fatalf("upgraded materializer row = %+v, %v; want existing name and zero-value new field", got, err)
	}
	if err := db.WriteTxContext(context.Background(), func(tx *Tx) error {
		return upgraded.Update(tx, want.ID, func(row *facadeRecordV2) error {
			row.Secret = "added-after-migration"
			return nil
		})
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	cfg.Tables = []TableDefinition{recordDefinitionV2(t)}
	db, err = Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("reopen upgraded typed schema: %v", err)
	}
	defer db.Close()
	upgraded, err = tableOf[facadeRecordV2](db, "records")
	if err != nil {
		t.Fatal(err)
	}
	got, err = upgraded.Get(want.ID)
	if err != nil || got.Name != want.Name || got.Secret != "added-after-migration" {
		t.Fatalf("reopened upgraded row = %+v, %v; want migrated field persisted", got, err)
	}
}

func receiveRecordEvent(t *testing.T, sub *recordSubscription[facadeRecord]) recordSubscriptionEvent[facadeRecord] {
	t.Helper()
	select {
	case event, ok := <-sub.Events():
		if !ok {
			t.Fatal("typed subscription closed before delivering event")
		}
		return event
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for typed subscription event")
		return recordSubscriptionEvent[facadeRecord]{}
	}
}

func receiveRecordEventType(t *testing.T, sub *recordSubscription[facadeRecord], want SubscriptionEventType) recordSubscriptionEvent[facadeRecord] {
	t.Helper()
	timer := time.NewTimer(3 * time.Second)
	defer timer.Stop()
	var last recordSubscriptionEvent[facadeRecord]
	for {
		select {
		case event, ok := <-sub.Events():
			if !ok {
				t.Fatalf("typed subscription closed before %q event; last=%+v", want, last)
			}
			last = event
			if event.Type == want {
				return event
			}
		case <-timer.C:
			t.Fatalf("timed out waiting for typed subscription %q event; last=%+v cursor=%d", want, last, sub.Cursor())
			return recordSubscriptionEvent[facadeRecord]{}
		}
	}
}

func TestDefineRejectsMissingRIMEPrimaryTag(t *testing.T) {
	type badRecord struct{ ID ids.RowID }
	if _, err := define[badRecord]("records", 72, RecordOptions{
		PrimaryField: "ID", FieldIDs: map[string]uint32{"ID": 1},
	}); err == nil {
		t.Fatal("definition without a RIME primary key tag was accepted")
	}
}

func TestTypedRecordRemoteApplyDoesNotEcho(t *testing.T) {
	definition := recordDefinition(t)
	sourceCfg := testConfig(t.TempDir())
	sourceCfg.Schema.Tables = nil
	sourceCfg.Tables = []TableDefinition{definition}
	source, err := Open(context.Background(), sourceCfg)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	sourceTable, err := tableOf[facadeRecord](source, "records")
	if err != nil {
		t.Fatal(err)
	}
	want := &facadeRecord{ID: ids.NewRowID(), Name: "from-peer"}
	if err := source.WriteTxContext(context.Background(), func(tx *Tx) error {
		return sourceTable.Insert(tx, want)
	}); err != nil {
		t.Fatal(err)
	}

	destCfg := testConfig(t.TempDir())
	destCfg.DBID = source.DBID()
	destCfg.Schema.Tables = nil
	destCfg.Tables = []TableDefinition{definition}
	dest, err := Open(context.Background(), destCfg)
	if err != nil {
		t.Fatal(err)
	}
	defer dest.Close()
	var batchApplied bool
	_, err = source.store.LogScan(source.cfg.NodeID, 1, 1, 1<<20, func(batch *codec.MutationBatch) error {
		if err := dest.ApplyRemote(context.Background(), batch); err != nil {
			return err
		}
		batchApplied = true
		return nil
	})
	if err != nil || !batchApplied {
		t.Fatalf("apply logged remote batch: applied=%v err=%v", batchApplied, err)
	}
	destTable, err := tableOf[facadeRecord](dest, "records")
	if err != nil {
		t.Fatal(err)
	}
	got, err := destTable.Get(want.ID)
	if err != nil || *got != *want {
		t.Fatalf("remote Get() = %+v, %v; want %+v", got, err, want)
	}
	localBatches := 0
	if _, err := dest.store.LogScan(dest.cfg.NodeID, 1, 10, 1<<20, func(*codec.MutationBatch) error {
		localBatches++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if localBatches != 0 {
		t.Fatalf("remote apply echoed into %d local Spool batches", localBatches)
	}
}

func TestTypedRemoteMaterializeFailureRetriesApply(t *testing.T) {
	definition := recordDefinition(t)
	sourceCfg := testConfig(t.TempDir())
	sourceCfg.Schema.Tables = nil
	sourceCfg.Tables = []TableDefinition{definition}
	source, err := Open(context.Background(), sourceCfg)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	sourceTable, err := tableOf[facadeRecord](source, "records")
	if err != nil {
		t.Fatal(err)
	}
	want := &facadeRecord{ID: ids.NewRowID(), Name: "recovered-remote"}
	if err := source.WriteTxContext(context.Background(), func(tx *Tx) error {
		return sourceTable.Insert(tx, want)
	}); err != nil {
		t.Fatal(err)
	}

	destCfg := testConfig(t.TempDir())
	destCfg.DBID = source.DBID()
	destCfg.Schema.Tables = nil
	destCfg.Tables = []TableDefinition{definition}
	dest, err := Open(context.Background(), destCfg)
	if err != nil {
		t.Fatal(err)
	}
	defer dest.Close()
	dest.crash = &crashHooks{remoteMaterialize: func() error { return errors.New("injected remote materialization failure") }}
	var batchApplied bool
	_, err = source.store.LogScan(source.cfg.NodeID, 1, 1, 1<<20, func(batch *codec.MutationBatch) error {
		if err := dest.ApplyRemote(context.Background(), batch); err != nil {
			return err
		}
		batchApplied = true
		return nil
	})
	if err != nil || !batchApplied {
		t.Fatalf("apply logged remote batch: applied=%v err=%v", batchApplied, err)
	}
	if status := dest.Status(); status.State != StateReady || status.StateGeneration != status.MaterializedGeneration {
		t.Fatalf("remote recovery status = %+v; want ready and converged", status)
	}
	destTable, err := tableOf[facadeRecord](dest, "records")
	if err != nil {
		t.Fatal(err)
	}
	if got, err := destTable.Get(want.ID); err != nil || *got != *want {
		t.Fatalf("recovered remote Get() = %+v, %v; want %+v", got, err, want)
	}
	localBatches := 0
	if _, err := dest.store.LogScan(dest.cfg.NodeID, 1, 10, 1<<20, func(*codec.MutationBatch) error {
		localBatches++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if localBatches != 0 {
		t.Fatalf("remote recovery echoed into %d local Spool batches", localBatches)
	}
}

func TestTypedLocalAndRemoteCommitsInterleaveAndRecover(t *testing.T) {
	const writes = 64
	definition := recordDefinition(t)

	sourceCfg := testConfig(t.TempDir())
	sourceCfg.Schema.Tables = nil
	sourceCfg.Tables = []TableDefinition{definition}
	source, err := Open(context.Background(), sourceCfg)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	sourceTable, err := tableOf[facadeRecord](source, "records")
	if err != nil {
		t.Fatal(err)
	}
	sharedRow := &facadeRecord{ID: ids.NewRowID(), Name: "shared-base"}
	if err := source.WriteTxContext(context.Background(), func(tx *Tx) error {
		return sourceTable.Insert(tx, sharedRow)
	}); err != nil {
		t.Fatalf("seed shared conflict row: %v", err)
	}
	remoteRows := make([]*facadeRecord, writes)
	for i := range remoteRows {
		remoteRows[i] = &facadeRecord{ID: ids.NewRowID(), Name: fmt.Sprintf("remote-%03d", i)}
		if err := source.WriteTxContext(context.Background(), func(tx *Tx) error {
			return sourceTable.Insert(tx, remoteRows[i])
		}); err != nil {
			t.Fatalf("source write %d: %v", i, err)
		}
	}
	if err := source.WriteTxContext(context.Background(), func(tx *Tx) error {
		return sourceTable.Update(tx, sharedRow.ID, func(row *facadeRecord) error {
			row.Name = "remote-shared"
			return nil
		})
	}); err != nil {
		t.Fatalf("update shared conflict row on source: %v", err)
	}
	remoteBatches := make([]*codec.MutationBatch, 0, writes+2)
	var remoteConflictBatch *codec.MutationBatch
	if _, err := source.store.LogScan(source.cfg.NodeID, 1, writes+2, 1<<20, func(batch *codec.MutationBatch) error {
		remoteBatches = append(remoteBatches, batch)
		for _, mutation := range batch.Mutations {
			if mutation.RowID == sharedRow.ID && mutation.ColumnID == 2 {
				remoteConflictBatch = batch
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("scan source batches: %v", err)
	}
	if len(remoteBatches) != writes+2 || remoteConflictBatch == nil {
		t.Fatalf("source batches = %d (conflict batch %v), want %d batches including shared-row update", len(remoteBatches), remoteConflictBatch != nil, writes+2)
	}

	destCfg := testConfig(t.TempDir())
	destCfg.DBID = source.DBID()
	destCfg.Schema.Tables = nil
	destCfg.Tables = []TableDefinition{definition}
	dest, err := Open(context.Background(), destCfg)
	if err != nil {
		t.Fatal(err)
	}
	destTable, err := tableOf[facadeRecord](dest, "records")
	if err != nil {
		t.Fatal(err)
	}
	if err := dest.ApplyRemote(context.Background(), remoteBatches[0]); err != nil {
		t.Fatalf("apply shared-row baseline: %v", err)
	}

	start := make(chan struct{})
	errCh := make(chan error, 2)
	var workers sync.WaitGroup
	workers.Add(2)
	go func() {
		defer workers.Done()
		<-start
		for i, batch := range remoteBatches[1:] {
			if err := dest.ApplyRemote(context.Background(), batch); err != nil {
				errCh <- fmt.Errorf("apply remote batch %d: %w", i+1, err)
				return
			}
		}
	}()
	localRows := make([]*facadeRecord, writes)
	go func() {
		defer workers.Done()
		<-start
		for i := range localRows {
			row := &facadeRecord{ID: ids.NewRowID(), Name: fmt.Sprintf("local-%03d", i)}
			if err := dest.WriteTxContext(context.Background(), func(tx *Tx) error {
				return destTable.Insert(tx, row)
			}); err != nil {
				errCh <- fmt.Errorf("local write %d: %w", i, err)
				return
			}
			localRows[i] = row
		}
		if err := dest.WriteTxContext(context.Background(), func(tx *Tx) error {
			return destTable.Update(tx, sharedRow.ID, func(row *facadeRecord) error {
				row.Name = "local-shared"
				return nil
			})
		}); err != nil {
			errCh <- fmt.Errorf("update shared conflict row locally: %w", err)
		}
	}()
	close(start)
	workers.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
	if t.Failed() {
		_ = dest.Close()
		t.FailNow()
	}

	allRows := append(append([]*facadeRecord(nil), remoteRows...), localRows...)
	for _, row := range allRows {
		got, err := destTable.Get(row.ID)
		if err != nil || *got != *row {
			t.Fatalf("interleaved row %q = %+v, %v; want %+v", row.Name, got, err, row)
		}
	}
	if count, err := destTable.Where().Count(); err != nil || count != writes*2+1 {
		t.Fatalf("interleaved record count = %d, %v; want %d", count, err, writes*2+1)
	}

	localSequences := 0
	var localConflictBatch *codec.MutationBatch
	if _, err := dest.store.LogScan(dest.cfg.NodeID, 1, writes+2, 1<<20, func(batch *codec.MutationBatch) error {
		localSequences++
		for _, mutation := range batch.Mutations {
			if mutation.RowID == sharedRow.ID && mutation.ColumnID == 2 {
				localConflictBatch = batch
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("scan destination local log: %v", err)
	}
	if localSequences != writes+1 || localConflictBatch == nil {
		t.Fatalf("destination local log contains %d batches (conflict batch %v), want %d local batches (remote apply must not echo)", localSequences, localConflictBatch != nil, writes+1)
	}
	sharedWinner := "local-shared"
	remoteVersion := crdt.Version{HLC: remoteConflictBatch.HLC, NodeID: remoteConflictBatch.OriginNode}
	localVersion := crdt.Version{HLC: localConflictBatch.HLC, NodeID: localConflictBatch.OriginNode}
	if crdt.CompareVersion(remoteVersion, localVersion) > 0 {
		sharedWinner = "remote-shared"
	}
	if got, err := destTable.Get(sharedRow.ID); err != nil || got.Name != sharedWinner {
		t.Fatalf("shared row after concurrent local/remote commits = %+v, %v; version winner is %q", got, err, sharedWinner)
	}
	if err := dest.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(context.Background(), destCfg)
	if err != nil {
		t.Fatalf("reopen interleaved database: %v", err)
	}
	defer reopened.Close()
	reopenedTable, err := tableOf[facadeRecord](reopened, "records")
	if err != nil {
		t.Fatal(err)
	}
	if count, err := reopenedTable.Where().Count(); err != nil || count != writes*2+1 {
		t.Fatalf("reopened interleaved record count = %d, %v; want %d", count, err, writes*2+1)
	}
	for _, row := range allRows {
		got, err := reopenedTable.Get(row.ID)
		if err != nil || *got != *row {
			t.Fatalf("reopened interleaved row %q = %+v, %v; want %+v", row.Name, got, err, row)
		}
	}
	if got, err := reopenedTable.Get(sharedRow.ID); err != nil || got.Name != sharedWinner {
		t.Fatalf("reopened shared row = %+v, %v; version winner is %q", got, err, sharedWinner)
	}
}

func TestTypedRecordSnapshotRebuildsMaterializer(t *testing.T) {
	ctx := context.Background()
	definition := recordDefinition(t)
	sourceCfg := testConfig(t.TempDir())
	sourceCfg.Schema.Tables = nil
	sourceCfg.Tables = []TableDefinition{definition}
	source, err := Open(ctx, sourceCfg)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	sourceTable, err := tableOf[facadeRecord](source, "records")
	if err != nil {
		t.Fatal(err)
	}
	want := &facadeRecord{ID: ids.NewRowID(), Name: "from-snapshot"}
	if err := source.WriteTxContext(ctx, func(tx *Tx) error {
		return sourceTable.Insert(tx, want)
	}); err != nil {
		t.Fatal(err)
	}

	destCfg := testConfig(t.TempDir())
	destCfg.DBID = source.DBID()
	destCfg.Schema.Tables = nil
	destCfg.Tables = []TableDefinition{definition}
	dest, err := Open(ctx, destCfg)
	if err != nil {
		t.Fatal(err)
	}
	defer dest.Close()
	destTable, err := tableOf[facadeRecord](dest, "records")
	if err != nil {
		t.Fatal(err)
	}
	oldRead, err := dest.readTxContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer oldRead.Close()
	oldSnapshot := oldRead.Snapshot()

	var manifest *codec.SnapshotManifest
	var chunks [][]codec.SnapshotCell
	if err := source.store.ExportSnapshot(1, func(m *codec.SnapshotManifest, cells []codec.SnapshotCell, _ bool) error {
		manifest = m
		chunks = append(chunks, append([]codec.SnapshotCell(nil), cells...))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if manifest == nil || len(chunks) == 0 {
		t.Fatal("snapshot export returned no manifest or chunks")
	}
	for i, chunk := range chunks {
		complete, err := dest.ApplySnapshotChunk(ctx, manifest, uint64(i), chunk, uint64(i+1) == manifest.ChunkCount)
		if err != nil {
			t.Fatalf("apply snapshot chunk %d: %v", i, err)
		}
		if complete != (uint64(i+1) == manifest.ChunkCount) {
			t.Fatalf("chunk %d completion = %v, want %v", i, complete, uint64(i+1) == manifest.ChunkCount)
		}
		if i+1 < len(chunks) {
			if _, err := destTable.Get(want.ID); !errors.Is(err, rime.ErrNotFound) {
				t.Fatalf("typed row visible before snapshot completion: err=%v", err)
			}
		}
	}
	got, err := destTable.Get(want.ID)
	if err != nil || *got != *want {
		t.Fatalf("snapshot typed Get() = %+v, %v; want %+v", got, err, want)
	}
	if _, err := destTable.GetRead(oldRead, want.ID); !errors.Is(err, rime.ErrNotFound) {
		t.Fatalf("pre-snapshot read transaction observed newly installed row: %v", err)
	}
	if _, err := dest.readAt(oldSnapshot); !errors.Is(err, rime.ErrSnapshotUnavailable) {
		t.Fatalf("readAt accepted a snapshot from a retired materializer generation: %v", err)
	}
	if got, want := dest.Status().MaterializedGeneration, func() uint64 {
		generation, err := dest.store.StateGeneration()
		if err != nil {
			t.Fatal(err)
		}
		return generation
	}(); got != want {
		t.Fatalf("materialized generation = %d, want state generation %d", got, want)
	}
}

func TestTypedRecordBackupRestoreWithoutSQLite(t *testing.T) {
	ctx := context.Background()
	cfg := testConfig(t.TempDir())
	cfg.Schema.Tables = nil
	localDefinition, err := define[facadeRecord]("private_records", 72, RecordOptions{
		PrimaryField: "ID", FieldIDs: map[string]uint32{"ID": 1, "Name": 2}, Scope: TableScopeNodeLocal,
	})
	if err != nil {
		t.Fatal(err)
	}
	cfg.Tables = []TableDefinition{recordDefinition(t), localDefinition}
	db, err := Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	dbID := db.DBID()
	table, err := tableOf[facadeRecord](db, "records")
	if err != nil {
		t.Fatal(err)
	}
	want := &facadeRecord{ID: ids.NewRowID(), Name: "backup-record"}
	localTable, err := tableOf[facadeRecord](db, "private_records")
	if err != nil {
		t.Fatal(err)
	}
	wantLocal := &facadeRecord{ID: ids.NewRowID(), Name: "backup-local-record"}
	if err := db.WriteTxContext(ctx, func(tx *Tx) error {
		if err := table.Insert(tx, want); err != nil {
			return err
		}
		return localTable.Insert(tx, wantLocal)
	}); err != nil {
		t.Fatal(err)
	}
	destination, err := backup.NewLocalDestination(filepath.Join(t.TempDir(), "backups"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Backup(ctx, backup.Config{Destination: destination}); err != nil {
		t.Fatal(err)
	}
	backups, err := destination.ListBackups(ctx, dbID.String())
	if err != nil || len(backups) != 1 {
		t.Fatalf("backup list = %d entries, %v", len(backups), err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	restoredPath := t.TempDir()
	freshNode := NewNodeID()
	if _, err := Restore(ctx, backup.RestoreConfig{
		Source: destination, BackupName: backups[0].Name, TargetPath: restoredPath,
		KeysPath: filepath.Join(restoredPath, "keys"), ExpectedDBID: dbID.String(),
		FreshNodeID: freshNode.String(),
	}); err != nil {
		t.Fatal(err)
	}
	restoredCfg := testConfig(restoredPath)
	restoredCfg.NodeID = freshNode
	restoredCfg.DBID = dbID
	restoredCfg.OriginSigning = testidentity.Config(freshNode)
	restoredCfg.Schema.Tables = nil
	restoredLocalDefinition, err := define[facadeRecord]("private_records", 72, RecordOptions{
		PrimaryField: "ID", FieldIDs: map[string]uint32{"ID": 1, "Name": 2}, Scope: TableScopeNodeLocal,
	})
	if err != nil {
		t.Fatal(err)
	}
	restoredCfg.Tables = []TableDefinition{recordDefinition(t), restoredLocalDefinition}
	restored, err := Open(ctx, restoredCfg)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	restoredTable, err := tableOf[facadeRecord](restored, "records")
	if err != nil {
		t.Fatal(err)
	}
	got, err := restoredTable.Get(want.ID)
	if err != nil || *got != *want {
		t.Fatalf("restored typed record = %+v, %v; want %+v", got, err, want)
	}
	restoredLocalTable, err := tableOf[facadeRecord](restored, "private_records")
	if err != nil {
		t.Fatal(err)
	}
	gotLocal, err := restoredLocalTable.Get(wantLocal.ID)
	if err != nil || *gotLocal != *wantLocal {
		t.Fatalf("restored typed node-local record = %+v, %v; want %+v", gotLocal, err, wantLocal)
	}
}

func TestOlderTypedWriterPreservesUnknownFields(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	newCfg := testConfig(dir)
	newCfg.Schema.Version = 2
	newCfg.Schema.Tables = nil
	newCfg.Tables = []TableDefinition{recordDefinitionV2(t)}
	newer, err := Open(ctx, newCfg)
	if err != nil {
		t.Fatal(err)
	}
	want := &facadeRecordV2{ID: ids.NewRowID(), Name: "before", Secret: "keep-me"}
	newTable, err := tableOf[facadeRecordV2](newer, "records")
	if err != nil {
		t.Fatal(err)
	}
	if err := newer.WriteTxContext(ctx, func(tx *Tx) error { return newTable.Insert(tx, want) }); err != nil {
		t.Fatal(err)
	}
	if err := newer.Close(); err != nil {
		t.Fatal(err)
	}

	oldCfg := testConfig(dir)
	oldCfg.DBID = newer.DBID()
	oldCfg.NodeID = newCfg.NodeID
	oldCfg.OriginSigning = newCfg.OriginSigning
	oldCfg.Schema.Tables = nil
	oldCfg.Tables = []TableDefinition{recordDefinition(t)}
	older, err := Open(ctx, oldCfg)
	if err != nil {
		t.Fatalf("open compatible older typed schema: %v", err)
	}
	oldTable, err := tableOf[facadeRecord](older, "records")
	if err != nil {
		t.Fatal(err)
	}
	if err := older.WriteTxContext(ctx, func(tx *Tx) error {
		return oldTable.Update(tx, want.ID, func(row *facadeRecord) error {
			row.Name = "after-old-writer"
			return nil
		})
	}); err != nil {
		t.Fatal(err)
	}
	if err := older.Close(); err != nil {
		t.Fatal(err)
	}

	newCfg.DBID = newer.DBID()
	newer, err = Open(ctx, newCfg)
	if err != nil {
		t.Fatal(err)
	}
	defer newer.Close()
	newTable, err = tableOf[facadeRecordV2](newer, "records")
	if err != nil {
		t.Fatal(err)
	}
	got, err := newTable.Get(want.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Name != "after-old-writer" || got.Secret != "keep-me" {
		t.Fatalf("newer record after old writer = %+v, want updated Name and retained Secret", got)
	}
}

func TestOlderTypedWriterPreservesUnknownNestedStructFields(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	newCfg := testConfig(dir)
	newCfg.Schema.Version = 2
	newCfg.Schema.Tables = nil
	newCfg.Tables = []TableDefinition{nestedRecordDefinitionV2(t)}
	newer, err := Open(ctx, newCfg)
	if err != nil {
		t.Fatal(err)
	}
	want := &facadeNestedRecordV2{ID: ids.NewRowID(), Profile: facadeNestedProfileV2{Name: "Ada", Region: "North"}}
	newTable, err := tableOf[facadeNestedRecordV2](newer, "nested_records")
	if err != nil {
		t.Fatal(err)
	}
	if err := newer.WriteTxContext(ctx, func(tx *Tx) error { return newTable.Insert(tx, want) }); err != nil {
		t.Fatal(err)
	}
	if err := newer.Close(); err != nil {
		t.Fatal(err)
	}

	oldCfg := testConfig(dir)
	oldCfg.DBID = newer.DBID()
	oldCfg.NodeID = newCfg.NodeID
	oldCfg.OriginSigning = newCfg.OriginSigning
	oldCfg.Schema.Tables = nil
	oldCfg.Tables = []TableDefinition{nestedRecordDefinitionV1(t)}
	older, err := Open(ctx, oldCfg)
	if err != nil {
		t.Fatalf("open compatible older nested schema: %v", err)
	}
	oldTable, err := tableOf[facadeNestedRecordV1](older, "nested_records")
	if err != nil {
		t.Fatal(err)
	}
	if err := older.WriteTxContext(ctx, func(tx *Tx) error {
		return oldTable.Update(tx, want.ID, func(row *facadeNestedRecordV1) error {
			row.Profile.Name = "Grace"
			return nil
		})
	}); err != nil {
		t.Fatal(err)
	}
	if err := older.Close(); err != nil {
		t.Fatal(err)
	}

	newer, err = Open(ctx, newCfg)
	if err != nil {
		t.Fatal(err)
	}
	defer newer.Close()
	newTable, err = tableOf[facadeNestedRecordV2](newer, "nested_records")
	if err != nil {
		t.Fatal(err)
	}
	got, err := newTable.Get(want.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Profile.Name != "Grace" || got.Profile.Region != "North" {
		t.Fatalf("newer nested record after old writer = %+v, want Name=Grace and Region=North", got)
	}
}

func TestOlderTypedWriterPreservesUnknownFieldsInsideCollections(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	newCfg := testConfig(dir)
	newCfg.Schema.Version = 2
	newCfg.Schema.Tables = nil
	newCfg.Tables = []TableDefinition{collectionRecordDefinitionV2(t)}
	newer, err := Open(ctx, newCfg)
	if err != nil {
		t.Fatal(err)
	}
	want := &facadeCollectionRecordV2{ID: ids.NewRowID(),
		Items: []facadeCollectionItemV2{{Name: "first", Region: "north"}, {Name: "second", Region: "south"}},
		ByKey: map[string]facadeCollectionItemV2{"a": {Name: "alpha", Region: "east"}, "z": {Name: "zeta", Region: "west"}},
	}
	newTable, err := tableOf[facadeCollectionRecordV2](newer, "collection_records")
	if err != nil {
		t.Fatal(err)
	}
	if err := newer.WriteTxContext(ctx, func(tx *Tx) error { return newTable.Insert(tx, want) }); err != nil {
		t.Fatal(err)
	}
	if err := newer.Close(); err != nil {
		t.Fatal(err)
	}

	oldCfg := testConfig(dir)
	oldCfg.DBID, oldCfg.NodeID, oldCfg.OriginSigning = newer.DBID(), newCfg.NodeID, newCfg.OriginSigning
	oldCfg.Schema.Tables = nil
	oldCfg.Tables = []TableDefinition{collectionRecordDefinitionV1(t)}
	older, err := Open(ctx, oldCfg)
	if err != nil {
		t.Fatalf("open compatible older collection schema: %v", err)
	}
	oldTable, err := tableOf[facadeCollectionRecordV1](older, "collection_records")
	if err != nil {
		t.Fatal(err)
	}
	if err := older.WriteTxContext(ctx, func(tx *Tx) error {
		return oldTable.Update(tx, want.ID, func(row *facadeCollectionRecordV1) error {
			row.Items[0].Name = "first edited"
			item := row.ByKey["z"]
			item.Name = "zeta edited"
			row.ByKey["z"] = item
			return nil
		})
	}); err != nil {
		t.Fatal(err)
	}
	if err := older.Close(); err != nil {
		t.Fatal(err)
	}

	newCfg.DBID = newer.DBID()
	newer, err = Open(ctx, newCfg)
	if err != nil {
		t.Fatal(err)
	}
	defer newer.Close()
	newTable, err = tableOf[facadeCollectionRecordV2](newer, "collection_records")
	if err != nil {
		t.Fatal(err)
	}
	got, err := newTable.Get(want.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Items[0].Name != "first edited" || got.Items[0].Region != "north" || got.Items[1].Region != "south" || got.ByKey["a"].Region != "east" || got.ByKey["z"].Name != "zeta edited" || got.ByKey["z"].Region != "west" {
		t.Fatalf("newer record after old collection writer = %+v, unknown collection fields were lost or misplaced", got)
	}
}

func TestTypedCustomRecordCodecPersistsAndReopens(t *testing.T) {
	ctx := context.Background()
	cfg := testConfig(t.TempDir())
	cfg.Schema.Tables = nil
	cfg.Tables = []TableDefinition{customRecordDefinition(t)}
	db, err := Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	facadeCustomCloneCalls.Store(0)
	facadeCustomEqualCalls.Store(0)
	want := &facadeCustomRecord{ID: ids.NewRowID(), Token: "stable-token"}
	table, err := tableOf[facadeCustomRecord](db, "custom_records")
	if err != nil {
		t.Fatal(err)
	}
	if err := db.WriteTxContext(ctx, func(tx *Tx) error { return table.Insert(tx, want) }); err != nil {
		t.Fatal(err)
	}
	if err := db.WriteTxContext(ctx, func(tx *Tx) error {
		return table.Update(tx, want.ID, func(row *facadeCustomRecord) error {
			row.Token = "updated-token"
			return nil
		})
	}); err != nil {
		t.Fatal(err)
	}
	want.Token = "updated-token"
	dbID := db.DBID()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	cfg.DBID = dbID
	db, err = Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	table, err = tableOf[facadeCustomRecord](db, "custom_records")
	if err != nil {
		t.Fatal(err)
	}
	got, err := table.Get(want.ID)
	if err != nil || *got != *want {
		t.Fatalf("custom typed record = %+v, %v; want %+v", got, err, want)
	}
	if facadeCustomCloneCalls.Load() == 0 || facadeCustomEqualCalls.Load() == 0 {
		t.Fatalf("custom codec hooks were not used: clone=%d equal=%d", facadeCustomCloneCalls.Load(), facadeCustomEqualCalls.Load())
	}
}
