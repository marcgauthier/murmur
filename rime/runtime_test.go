package rime

import (
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestRegisterTypeUsesIndexesSnapshotsAndBatchSavepoint(t *testing.T) {
	type runtimeRecord struct {
		ID       string  `rime:"ID"`
		Name     string  `rime:"primary"`
		Rank     int     `rime:"ordered"`
		Optional *string `rime:"default=ready"`
	}
	db := New()
	defer db.Close()
	table, err := RegisterType(db, reflect.TypeFor[runtimeRecord]())
	if err != nil {
		t.Fatal(err)
	}
	record := &runtimeRecord{ID: "one", Name: "first", Rank: 2}
	var carrier any = record
	if err = table.Insert(&carrier); err != nil {
		t.Fatal(err)
	}
	record.Name = "mutated caller"
	name := FieldOf[any, string](table, "Name")
	rank := OrderedFieldOf[int](table, "Rank")
	rows, err := table.Where(name.Eq("first"), rank.Ge(2)).Find()
	if err != nil || len(rows) != 1 {
		t.Fatalf("query=%+v, %v", rows, err)
	}
	stored := (*rows[0]).(*runtimeRecord)
	if stored.Optional == nil || *stored.Optional != "ready" {
		t.Fatalf("default=%+v", stored)
	}
	snapshot := db.ReadTx()
	defer snapshot.Close()
	err = db.WriteTx(func(tx *Tx) error {
		if err := table.In(tx).Update("one", func(value *any) error { (*value).(*runtimeRecord).Rank = 7; return nil }); err != nil {
			return err
		}
		failed := tx.Batch("update", 2, func(i int) error {
			if i == 1 {
				return errors.New("batch failure")
			}
			return table.In(tx).Update("one", func(value *any) error { (*value).(*runtimeRecord).Rank = 9; return nil })
		})
		var batch *BatchError
		if !errors.As(failed, &batch) || batch.Index != 1 {
			return errors.New("missing batch error")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	current, err := table.Get("one")
	if err != nil || (*current).(*runtimeRecord).Rank != 7 {
		t.Fatalf("current=%v, %v", current, err)
	}
	previous, err := table.In(snapshot).Get("one")
	if err != nil || (*previous).(*runtimeRecord).Rank != 2 {
		t.Fatalf("snapshot=%v, %v", previous, err)
	}
}

func TestRegisterTypeRejectsWrongNativeRecord(t *testing.T) {
	type runtimeRecord struct {
		ID string `rime:"primary"`
	}
	db := New()
	defer db.Close()
	table, err := RegisterType(db, reflect.TypeFor[runtimeRecord]())
	if err != nil {
		t.Fatal(err)
	}
	for _, record := range []any{42, &struct{ Other string }{}, runtimeRecord{ID: "value"}} {
		if err = table.Insert(&record); !errors.Is(err, ErrBadSchema) {
			t.Fatalf("accepted %T: %v", record, err)
		}
	}
	var record any = &runtimeRecord{ID: "right"}
	if err = table.Insert(&record); err != nil {
		t.Fatal(err)
	}
	if err = table.Update("right", func(carrier *any) error { *carrier = 42; return nil }); !errors.Is(err, ErrBadSchema) {
		t.Fatalf("type replacement=%v", err)
	}
}

func TestDynamicFieldsTimestampIndexesAndNamedValues(t *testing.T) {
	type namedString string
	type namedInteger int64
	type record struct {
		ID     string       `rime:"primary"`
		Name   namedString  `rime:"prefix,ordered"`
		Number namedInteger `rime:"ordered"`
		At     time.Time    `rime:"index,ordered"`
		Unique time.Time    `rime:"unique"`
	}
	db := New()
	defer db.Close()
	table, err := Register[record](db, WithCompound[record]("name_at", "Name", "At"))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	same := now.Round(0).In(time.FixedZone("other", 7200))
	row := record{ID: "one", Name: "router", Number: 1 << 60, At: now, Unique: now}
	if err = table.Insert(&row); err != nil {
		t.Fatal(err)
	}
	if err = table.Insert(&record{ID: "two", Name: "switch", Number: (1 << 60) + 1, At: now.Add(time.Second), Unique: same}); !errors.Is(err, ErrUnique) {
		t.Fatalf("unique timestamp=%v", err)
	}
	if err = table.Insert(&record{ID: "two", Name: "switch", Number: (1 << 60) + 1, At: now.Add(time.Second), Unique: now.Add(time.Second)}); err != nil {
		t.Fatal(err)
	}
	at, err := DynamicFieldOf(table, "At")
	if err != nil {
		t.Fatal(err)
	}
	equal, err := at.Compare("=", same)
	if err != nil {
		t.Fatal(err)
	}
	matches, err := table.Where(equal).Find()
	if err != nil || len(matches) != 1 || matches[0].ID != "one" {
		t.Fatalf("timestamp=%+v %v", matches, err)
	}
	typed := FieldOf[record, time.Time](table, "At")
	for _, predicate := range []Expr[record]{typed.Eq(same), typed.In(same)} {
		n, err := table.Where(predicate).Count()
		if err != nil || n != 1 {
			t.Fatalf("typed timestamp=%d %v", n, err)
		}
	}
	name, err := DynamicFieldOf(table, "Name")
	if err != nil {
		t.Fatal(err)
	}
	prefix, err := name.StringMatch("STARTS WITH", "rou")
	if err != nil {
		t.Fatal(err)
	}
	n, err := table.Where(prefix).Count()
	if err != nil || n != 1 {
		t.Fatalf("prefix=%d %v", n, err)
	}
	number, err := DynamicFieldOf(table, "Number")
	if err != nil {
		t.Fatal(err)
	}
	greater, err := number.Compare(">", namedInteger(1<<60))
	if err != nil {
		t.Fatal(err)
	}
	matches, err = table.Where(greater).Find()
	if err != nil || len(matches) != 1 || matches[0].ID != "two" {
		t.Fatalf("large integer=%+v %v", matches, err)
	}
	nameEqual, err := name.Compare("=", namedString("router"))
	if err != nil {
		t.Fatal(err)
	}
	n, err = table.Where(nameEqual, equal).Count()
	if err != nil || n != 1 {
		t.Fatalf("compound=%d %v", n, err)
	}
	// Dynamic constructors report schema/type errors rather than panic.
	if _, err = DynamicFieldOf[record](nil, "At"); !errors.Is(err, ErrBadSchema) {
		t.Fatal(err)
	}
	if _, err = DynamicFieldOf(table, "Missing"); !errors.Is(err, ErrBadSchema) {
		t.Fatal(err)
	}
	if _, err = number.Compare("=", int64(1)); !errors.Is(err, ErrBadSchema) {
		t.Fatal(err)
	}
	if _, err = number.StringMatch("LIKE", "%"); !errors.Is(err, ErrBadSchema) {
		t.Fatal(err)
	}
	if _, err = number.Compare("bad", namedInteger(1)); !errors.Is(err, ErrBadSchema) {
		t.Fatal(err)
	}
	// Updates remove the old normalized timestamp bucket, including unique claims.
	if err = table.Update("one", func(value *record) error {
		value.At = value.At.Add(2 * time.Second)
		value.Unique = value.Unique.Add(2 * time.Second)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	n, err = table.Where(equal).Count()
	if err != nil || n != 0 {
		t.Fatalf("stale timestamp=%d %v", n, err)
	}
	if err = table.Insert(&record{ID: "three", Unique: same}); err != nil {
		t.Fatalf("old unique claim retained: %v", err)
	}
}
