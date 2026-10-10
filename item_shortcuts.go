package murmur

import (
	"context"
	"fmt"
	"reflect"

	"github.com/marcgauthier/murmur/q"
	"github.com/marcgauthier/murmur/rime"
)

// Assignment describes one explicit partial update. Construct it with Set.
type Assignment struct {
	field string
	value any
}

// Set assigns a Go field, including zero and nil values.
func Set(field string, value any) Assignment { return Assignment{field: field, value: value} }

func assignmentFields(assignments []Assignment) (map[string]any, error) {
	fields := make(map[string]any, len(assignments))
	for _, a := range assignments {
		if a.field == "" {
			return nil, fmt.Errorf("murmur: assignment requires a field name")
		}
		if _, exists := fields[a.field]; exists {
			return nil, fmt.Errorf("murmur: duplicate assignment to %q", a.field)
		}
		fields[a.field] = a.value
	}
	return fields, nil
}

// Update partially updates an existing row using explicit assignments.
func (db *DB) Update(ctx context.Context, item any, assignments ...Assignment) error {
	fields, err := assignmentFields(assignments)
	if err != nil {
		return err
	}
	return db.UpdateFields(ctx, item, fields)
}

// Update stages explicit partial assignments in this transaction.
func (tx *Tx) Update(item any, assignments ...Assignment) error {
	fields, err := assignmentFields(assignments)
	if err != nil {
		return err
	}
	return tx.UpdateFields(item, fields)
}

func inferredItemModel(dest any, many bool) (any, error) {
	value := reflect.ValueOf(dest)
	if !value.IsValid() || value.Kind() != reflect.Pointer || value.IsNil() {
		return nil, fmt.Errorf("murmur: destination must be a non-nil pointer")
	}
	typ := value.Type().Elem()
	if many {
		if typ.Kind() != reflect.Slice {
			return nil, fmt.Errorf("murmur: Find requires *[]T or *[]*T")
		}
		typ = typ.Elem()
		if typ.Kind() == reflect.Pointer {
			typ = typ.Elem()
		}
	}
	if typ.Kind() != reflect.Struct {
		return nil, fmt.Errorf("murmur: destination must contain a registered struct")
	}
	return reflect.New(typ).Interface(), nil
}

// Find infers a model from *[]T or *[]*T and returns detached matching records.
func (db *DB) Find(ctx context.Context, dest any, filters ...q.Matcher) error {
	model, err := inferredItemModel(dest, true)
	if err != nil {
		return err
	}
	return db.Query(model, filters...).WithContext(ctx).FindInto(dest)
}

// FindOne infers a model from *T, returning ErrNotFound when no row matches.
func (db *DB) FindOne(ctx context.Context, dest any, filters ...q.Matcher) error {
	model, err := inferredItemModel(dest, false)
	if err != nil {
		return err
	}
	return db.Query(model, filters...).WithContext(ctx).FirstInto(dest)
}

// Count counts matches for a registered model exemplar.
func (db *DB) Count(ctx context.Context, model any, filters ...q.Matcher) (int, error) {
	return db.Query(model, filters...).WithContext(ctx).Count()
}

// Exists reports whether any row matches the filters.
func (db *DB) Exists(ctx context.Context, model any, filters ...q.Matcher) (bool, error) {
	return db.Query(model, filters...).WithContext(ctx).Exists()
}

func (tx *Tx) itemQuery(model any, filters []q.Matcher) *ItemQuery {
	if tx == nil || tx.db == nil || tx.inner == nil || tx.done {
		return &ItemQuery{err: rime.ErrTxClosed}
	}
	return tx.db.Query(model, filters...).In(tx)
}

// Find returns detached matches, including staged writes.
func (tx *Tx) Find(dest any, filters ...q.Matcher) error {
	model, err := inferredItemModel(dest, true)
	if err != nil {
		return err
	}
	return tx.itemQuery(model, filters).FindInto(dest)
}

// FindOne returns the first match, including staged writes.
func (tx *Tx) FindOne(dest any, filters ...q.Matcher) error {
	model, err := inferredItemModel(dest, false)
	if err != nil {
		return err
	}
	return tx.itemQuery(model, filters).FirstInto(dest)
}

// Count counts matches including staged writes.
func (tx *Tx) Count(model any, filters ...q.Matcher) (int, error) {
	return tx.itemQuery(model, filters).Count()
}

// Exists reports whether any row matches, including staged writes.
func (tx *Tx) Exists(model any, filters ...q.Matcher) (bool, error) {
	return tx.itemQuery(model, filters).Exists()
}

func (tx *recordReadTx) itemQuery(model any, filters []q.Matcher) *ItemQuery {
	iq := &ItemQuery{limit: -1}
	if tx == nil || tx.db == nil || tx.inner == nil || tx.done {
		iq.err = rime.ErrTxClosed
		return iq
	}
	iq.db, iq.read = tx.db, tx
	binding, err := resolveItemBindingFrom(model, tx.itemBindings, tx.itemAmbiguous)
	if err != nil {
		iq.err = err
		return iq
	}
	iq.binding = binding
	return iq.Where(filters...)
}

// Find returns detached records from this pinned snapshot's model bindings.
func (tx *recordReadTx) Find(dest any, filters ...q.Matcher) error {
	model, err := inferredItemModel(dest, true)
	if err != nil {
		return err
	}
	return tx.itemQuery(model, filters).FindInto(dest)
}

// FindOne returns the first match from this pinned snapshot.
func (tx *recordReadTx) FindOne(dest any, filters ...q.Matcher) error {
	model, err := inferredItemModel(dest, false)
	if err != nil {
		return err
	}
	return tx.itemQuery(model, filters).FirstInto(dest)
}

// Count counts matches in this pinned snapshot.
func (tx *recordReadTx) Count(model any, filters ...q.Matcher) (int, error) {
	return tx.itemQuery(model, filters).Count()
}

// Exists reports whether any snapshot row matches the filters.
func (tx *recordReadTx) Exists(model any, filters ...q.Matcher) (bool, error) {
	return tx.itemQuery(model, filters).Exists()
}
