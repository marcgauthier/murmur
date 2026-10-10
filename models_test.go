package murmur

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/q"
	"github.com/marcgauthier/murmur/rime"
)

type modelDevice struct {
	ID      ids.RowID `rime:"ID"`
	Name    string    `rime:"primary"`
	Site    string    `rime:"index"`
	Status  int       `rime:"ordered"`
	Online  bool
	Data    map[string][]byte
	Ignored chan int `rime:"-"`
}

type modelRandom struct {
	ID   ids.RowID `rime:"primary"`
	Name string
}

func modelTestConfig(t *testing.T, models ...any) Config {
	t.Helper()
	cfg := testConfig(t.TempDir())
	cfg.Tables = nil
	cfg.Models = models
	return cfg
}

func TestModelsCRUDPersistence(t *testing.T) {
	for _, typed := range []bool{false, true} {
		t.Run(map[bool]string{false: "runtime", true: "typed"}[typed], func(t *testing.T) {
			ctx := context.Background()
			var registration any = modelDevice{}
			if typed {
				def, err := Model[modelDevice]()
				if err != nil {
					t.Fatal(err)
				}
				registration = def
			}
			cfg := modelTestConfig(t, registration, modelRandom{})
			db, err := Open(ctx, cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = db.Close() }()
			device := modelDevice{Name: "router-01", Site: "OTT", Status: 3, Online: true, Data: map[string][]byte{"owned": {1, 2}}}
			if err = db.InsertItem(ctx, &device); err != nil {
				t.Fatal(err)
			}
			if device.ID.IsZero() || device.ID[6]>>4 != 5 {
				t.Fatalf("ID=%s, want v5", device.ID)
			}
			device.Data["owned"][0] = 99
			got := modelDevice{Name: device.Name}
			if err = db.GetItem(ctx, &got); err != nil {
				t.Fatal(err)
			}
			if got.ID != device.ID || got.Data["owned"][0] != 1 {
				t.Fatalf("read = %+v", got)
			}
			if err = db.InsertItem(ctx, &modelDevice{Name: device.Name}); !errors.Is(err, rime.ErrAlreadyExists) {
				t.Fatalf("duplicate=%v", err)
			}
			if err = db.InsertItem(ctx, &modelDevice{}); err == nil {
				t.Fatal("empty key accepted")
			}
			if err = db.UpdateFields(ctx, &got, map[string]any{"Status": 0, "Online": false}); err != nil {
				t.Fatal(err)
			}
			if err = db.UpdateFields(ctx, &got, map[string]any{"Name": "changed"}); err == nil {
				t.Fatal("key mutation accepted")
			}
			replacement := got
			replacement.Name = "changed"
			if err = db.SaveItem(ctx, &replacement); err == nil {
				t.Fatal("replacement changed key")
			}
			var rows []modelDevice
			query := db.Query(modelDevice{}, q.Eq("Site", "OTT"), q.Gte("Status", 0))
			if err = query.FindInto(&rows); err != nil || len(rows) != 1 {
				t.Fatalf("query=%+v, %v", rows, err)
			}
			plan, err := db.Query(modelDevice{}, q.Eq("Site", "OTT")).Explain()
			if err != nil || !strings.Contains(plan, "INDEX SEEK Site") {
				t.Fatalf("plan=%q, %v", plan, err)
			}
			rows[0].Data["owned"][0] = 44
			if typed {
				table, err := tableOf[modelDevice](db, "modelDevice")
				if err != nil {
					t.Fatal(err)
				}
				if err = db.Transaction(ctx, func(tx *Tx) error {
					return table.Update(tx, device.ID, func(d *modelDevice) error { d.Name = "illegal"; return nil })
				}); err == nil {
					t.Fatal("typed mutation changed business key")
				}
			} else if _, err = tableOf[modelDevice](db, "modelDevice"); err == nil {
				t.Fatal("runtime table unexpectedly has typed handle")
			}
			random := modelRandom{Name: "random"}
			if err = db.InsertItem(ctx, &random); err != nil || random.ID[6]>>4 != 4 {
				t.Fatalf("random=%s, %v", random.ID, err)
			}
			if err = db.Close(); err != nil {
				t.Fatal(err)
			}
			db, err = Open(ctx, cfg)
			if err != nil {
				t.Fatal(err)
			}
			loaded := modelDevice{Name: "router-01"}
			if err = db.GetItem(ctx, &loaded); err != nil || loaded.Status != 0 || loaded.Online || loaded.Data["owned"][0] != 1 {
				t.Fatalf("reopen=%+v, %v", loaded, err)
			}
			if err = db.DeleteItem(ctx, &modelDevice{Name: "router-01"}); err != nil {
				t.Fatal(err)
			}
			if err = db.GetItem(ctx, &loaded); !errors.Is(err, rime.ErrNotFound) {
				t.Fatalf("deleted=%v", err)
			}
		})
	}
}

func TestModelsBatchTransactions(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, modelTestConfig(t, modelDevice{}))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	devices := []modelDevice{{Name: "a", Site: "OTT"}, {Name: "b", Site: "OTT"}}
	if err = db.InsertMany(ctx, &devices); err != nil {
		t.Fatal(err)
	}
	for i := range devices {
		devices[i].Status = i + 1
	}
	if err = db.UpdateMany(ctx, devices); err != nil {
		t.Fatal(err)
	}
	abort := errors.New("rollback")
	rolled := modelDevice{Name: "rolled"}
	if err = db.Transaction(ctx, func(tx *Tx) error {
		if err := tx.InsertItem(&rolled); err != nil {
			return err
		}
		return abort
	}); !errors.Is(err, abort) {
		t.Fatal(err)
	}
	if rolled.ID.IsZero() {
		t.Fatal("generated ID lost on rollback")
	}
	if err = db.GetItem(ctx, &rolled); !errors.Is(err, rime.ErrNotFound) {
		t.Fatal(err)
	}
	// A failed batch which supersedes an earlier write must retain that write.
	err = db.Transaction(ctx, func(tx *Tx) error {
		if err := tx.UpdateFields(&devices[0], map[string]any{"Status": 7}); err != nil {
			return err
		}
		changed := devices[0]
		changed.Status = 9
		missing := modelDevice{Name: "missing", Status: 10}
		batchErr := tx.UpdateMany([]modelDevice{changed, missing})
		var detail *rime.BatchError
		if !errors.As(batchErr, &detail) || detail.Index != 1 {
			return errors.New("missing batch error")
		}
		got := modelDevice{Name: "a"}
		if err := tx.GetItem(&got); err != nil {
			return err
		}
		if got.Status != 7 {
			return errors.New("failed batch erased prior write")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	got := modelDevice{Name: "a"}
	if err = db.GetItem(ctx, &got); err != nil || got.Status != 7 {
		t.Fatalf("committed prior write=%+v, %v", got, err)
	}
	if err = db.InsertMany(ctx, []*modelDevice{{Name: "new"}, nil}); err == nil {
		t.Fatal("nil accepted")
	}
	if err = db.GetItem(ctx, &modelDevice{Name: "new"}); !errors.Is(err, rime.ErrNotFound) {
		t.Fatalf("partial batch=%v", err)
	}
	if err = db.DeleteMany(ctx, &devices); err != nil {
		t.Fatal(err)
	}
	count, err := db.Query(modelDevice{}).Count()
	if err != nil || count != 0 {
		t.Fatalf("count=%d, %v", count, err)
	}
}

func TestModelRegistrationValidation(t *testing.T) {
	type missing struct{ Name string }
	type untagged struct{ ID ids.RowID }
	type wrong struct {
		ID string `rime:"ID"`
	}
	type duplicate struct {
		ID ids.RowID `rime:"ID"`
		A  string    `rime:"primary"`
		B  string    `rime:"primary"`
	}
	type ignored struct {
		ID   ids.RowID `rime:"ID"`
		Name string    `rime:"primary" murmur:"-"`
	}
	for _, model := range []any{missing{}, untagged{}, wrong{}, duplicate{}, ignored{}, 1, nil} {
		if _, err := compileModels([]any{model}); err == nil {
			t.Fatalf("accepted %T", model)
		}
	}
}

func TestModelStableIdentitiesAndLegacyOverrides(t *testing.T) {
	type nestedA struct {
		Label  string
		Amount int
	}
	type nestedB struct {
		Amount int
		Label  string
	}
	type orderedA struct {
		ID    ids.RowID `rime:"ID"`
		Name  string    `rime:"primary"`
		Left  nestedA
		Right nestedA
	}
	type orderedB struct {
		Right nestedB
		Left  nestedB
		Name  string    `rime:"primary"`
		ID    ids.RowID `rime:"ID"`
	}
	a, err := Model[orderedA](ModelOptions{Name: "devices"})
	if err != nil {
		t.Fatal(err)
	}
	b, err := Model[orderedB](ModelOptions{Name: "devices"})
	if err != nil {
		t.Fatal(err)
	}
	if a.id != b.id || string(a.table.RecordDescriptor) != string(b.table.RecordDescriptor) {
		t.Fatal("field reordering changed durable identities")
	}
	first := orderedA{Name: "router-01"}
	if err = a.model.ensure(&first, true); err != nil {
		t.Fatal(err)
	}
	if first.ID.String() != "b15994d4-3d6e-524e-97ac-9cddfd0b8096" {
		t.Fatalf("UUIDv5 vector=%s", first.ID)
	}
	second := orderedB{Name: "router-01"}
	if err = b.model.ensure(&second, true); err != nil || first.ID != second.ID {
		t.Fatalf("reordered ID=%s, %v", second.ID, err)
	}
	ctx := context.Background()
	explicit := NewRowID()
	first.ID = explicit
	if err = a.model.ensure(&first, true); err != nil || first.ID != explicit {
		t.Fatal("explicit ID was replaced")
	}
	// An explicitly selected legacy identity can retain its original array type.
	type legacy struct {
		Key   [16]byte `rime:"primary"`
		Value string
	}
	definition, err := Model[legacy](ModelOptions{Name: "legacy", TableID: 56, RecordOptions: RecordOptions{PrimaryField: "Key", FieldIDs: map[string]uint32{"Key": 1, "Value": 2}}})
	if err != nil {
		t.Fatal(err)
	}
	db, err := Open(ctx, modelTestConfig(t, definition))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	row := legacy{Value: "legacy"}
	if err = db.InsertItem(ctx, &row); err != nil {
		t.Fatal(err)
	}
	if err = db.GetItem(ctx, &legacy{Key: row.Key}); err != nil {
		t.Fatal(err)
	}
}

type modelMigrationV1 struct {
	ID   ids.RowID `rime:"ID"`
	Name string    `rime:"primary"`
}
type modelMigrationV2 struct {
	ID    ids.RowID `rime:"ID"`
	Name  string    `rime:"primary"`
	Owner *string
}

func TestMigrateModelsPreservesUnknownFields(t *testing.T) {
	ctx := context.Background()
	old, err := Model[modelMigrationV1](ModelOptions{Name: "devices"})
	if err != nil {
		t.Fatal(err)
	}
	next, err := Model[modelMigrationV2](ModelOptions{Name: "devices"})
	if err != nil {
		t.Fatal(err)
	}
	cfg := modelTestConfig(t, old)
	db, err := Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	row := modelMigrationV1{Name: "router-01"}
	if err = db.InsertItem(ctx, &row); err != nil {
		t.Fatal(err)
	}
	// Opening with an addition requires an explicit migration.
	if err = db.MigrateModels(ctx, []any{next}); err != nil {
		t.Fatal(err)
	}
	if err = db.MigrateModels(ctx, []any{old}); !errors.Is(err, ErrUnsupportedSchema) {
		t.Fatalf("drop=%v", err)
	}
	owner := "operations"
	upgraded := modelMigrationV2{Name: row.Name, Owner: &owner}
	if err = db.SaveItem(ctx, &upgraded); err != nil {
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	// An older binding keeps the unknown new field during its own replacement.
	db, err = Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err = db.SaveItem(ctx, &row); err != nil {
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	cfg.Models = []any{next}
	db, err = Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	got := modelMigrationV2{Name: row.Name}
	if err = db.GetItem(ctx, &got); err != nil || got.Owner == nil || *got.Owner != owner {
		t.Fatalf("unknown retention=%+v, %v", got, err)
	}
}

func TestModelFailedReplacementRetainsEarlierWrite(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, modelTestConfig(t, modelDevice{}))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	original := modelDevice{Name: "stable"}
	err = db.Transaction(ctx, func(tx *Tx) error {
		if err := tx.InsertItem(&original); err != nil {
			return err
		}
		replacement := original
		replacement.Name = "changed"
		if err := tx.UpdateItem(&replacement); err == nil {
			return errors.New("immutable replacement accepted")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	got := modelDevice{Name: "stable"}
	if err = db.GetItem(ctx, &got); err != nil || got.ID != original.ID {
		t.Fatalf("earlier insert lost=%+v, %v", got, err)
	}
}

func TestModelReusedNestedFieldOverrides(t *testing.T) {
	type profile struct {
		Name string
		Rank int
	}
	type model struct {
		ID ids.RowID `rime:"ID"`
		A  profile
		B  profile
	}
	definition, err := Model[model](ModelOptions{RecordOptions: RecordOptions{FieldIDs: map[string]uint32{"B.Name": 19}}})
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range definition.model.record.Fields {
		if field.GoName == "A" || field.GoName == "B" {
			for _, nested := range field.Descriptor.Fields {
				if nested.GoName == "Name" && nested.ID != 19 {
					t.Fatalf("%s.Name=%d", field.GoName, nested.ID)
				}
			}
		}
	}
	_, err = Model[model](ModelOptions{RecordOptions: RecordOptions{FieldIDs: map[string]uint32{"A.Name": 18, "B.Name": 19}}})
	if !errors.Is(err, ErrUnsupportedSchema) {
		t.Fatalf("conflicting override=%v", err)
	}
}

func TestModelSnapshotBindingsSurviveMigration(t *testing.T) {
	ctx := context.Background()
	cfg := modelTestConfig(t, modelMigrationV1{})
	db, err := Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	row := modelMigrationV1{Name: "router-01"}
	if err = db.InsertItem(ctx, &row); err != nil {
		t.Fatal(err)
	}
	snapshot, err := db.readTxContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	query := db.Query(modelMigrationV1{}, q.Eq("Name", row.Name)).inRead(snapshot)
	stale := db.Query(modelMigrationV1{})
	next, err := Model[modelMigrationV2](ModelOptions{Name: "modelMigrationV1"})
	if err != nil {
		t.Fatal(err)
	}
	if err = db.MigrateModels(ctx, []any{next}); err != nil {
		t.Fatal(err)
	}
	got := modelMigrationV1{Name: row.Name}
	if err = snapshot.GetItem(&got); err != nil || got.ID != row.ID {
		t.Fatalf("snapshot read=%+v, %v", got, err)
	}
	var rows []modelMigrationV1
	if err = query.FindInto(&rows); err != nil || len(rows) != 1 {
		t.Fatalf("snapshot query=%+v, %v", rows, err)
	}
	if err = snapshot.Find(&rows); err != nil || len(rows) != 1 {
		t.Fatalf("snapshot inferred find=%v, %v", rows, err)
	}
	if err = snapshot.FindOne(&got, q.Eq("Name", row.Name)); err != nil {
		t.Fatal(err)
	}
	if n, err := snapshot.Count(modelMigrationV1{}); err != nil || n != 1 {
		t.Fatalf("snapshot count=%d, %v", n, err)
	}
	if found, err := snapshot.Exists(modelMigrationV1{}); err != nil || !found {
		t.Fatalf("snapshot exists=%v, %v", found, err)
	}
	if err = stale.FindInto(&rows); !errors.Is(err, ErrUnsupportedSchema) {
		t.Fatalf("stale query=%v", err)
	}
}

func TestModelCustomBusinessKeyCodec(t *testing.T) {
	type key string
	type model struct {
		ID  ids.RowID `rime:"ID"`
		Key key       `rime:"primary"`
	}
	definition, err := Model[model](ModelOptions{RecordOptions: RecordOptions{Codecs: []RecordCodec{{
		ID: "model-key", Version: 1, Example: key(""),
		Encode: func(value any) ([]byte, error) { return []byte(value.(key)), nil },
		Decode: func(data []byte, destination any) error { *destination.(*key) = key(data); return nil },
		Clone:  func(value any) (any, error) { return value, nil },
		Equal:  func(left, right any) bool { return left.(key) == right.(key) },
	}}}})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	db, err := Open(ctx, modelTestConfig(t, definition))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	first := model{Key: "business"}
	if err = db.InsertItem(ctx, &first); err != nil {
		t.Fatal(err)
	}
	got := model{Key: first.Key}
	if err = db.GetItem(ctx, &got); err != nil || got.ID != first.ID {
		t.Fatalf("custom key lookup=%+v, %v", got, err)
	}
	sameKey := model{ID: NewRowID(), Key: first.Key}
	if err = db.InsertItem(ctx, &sameKey); err != nil {
		t.Fatalf("explicit duplicate business key=%v", err)
	}
}
