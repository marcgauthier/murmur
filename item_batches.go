package murmur

import (
	"context"
	"fmt"
	"reflect"

	"github.com/marcgauthier/murmur/rime"
)

// Transaction runs one atomic managed transaction. Callback errors roll it back.
func (db *DB) Transaction(ctx context.Context, fn func(*Tx) error) error {
	if db == nil {
		return ErrClosed
	}
	return db.WriteTxContext(ctx, fn)
}

// UpdateItem replaces an existing row. SaveItem is the insert-or-replace form.
func (db *DB) UpdateItem(ctx context.Context, item any) error {
	if db == nil {
		return ErrClosed
	}
	return db.Transaction(ctx, func(tx *Tx) error { return tx.UpdateItem(item) })
}

// UpdateItem stages a full replacement and fails if the row is missing.
func (tx *Tx) UpdateItem(item any) error {
	if tx == nil || tx.db == nil || tx.inner == nil || tx.done {
		return rime.ErrTxClosed
	}
	binding, err := tx.db.resolveItemBinding(item)
	if err != nil {
		return err
	}
	key, err := binding.rowID(item)
	if err != nil {
		return err
	}
	value := reflect.ValueOf(item)
	if value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return fmt.Errorf("murmur: nil item")
		}
		value = value.Elem()
	}
	if value.Type() != binding.goType {
		return fmt.Errorf("murmur: expected %s, got %T", binding.goType, item)
	}
	return binding.ops.update(tx.db, tx, key, func(current any) error {
		reflect.ValueOf(current).Elem().Set(value)
		return nil
	})
}

// InsertMany inserts a homogeneous slice in one atomic transaction.
func (db *DB) InsertMany(ctx context.Context, items any) error {
	return db.itemBatch(ctx, "insert", items)
}

// UpdateMany replaces existing rows atomically; missing rows fail the batch.
func (db *DB) UpdateMany(ctx context.Context, items any) error {
	return db.itemBatch(ctx, "update", items)
}

// SaveMany inserts or replaces all rows atomically.
func (db *DB) SaveMany(ctx context.Context, items any) error { return db.itemBatch(ctx, "save", items) }

// DeleteMany deletes all referenced rows atomically.
func (db *DB) DeleteMany(ctx context.Context, items any) error {
	return db.itemBatch(ctx, "delete", items)
}

func (db *DB) itemBatch(ctx context.Context, operation string, items any) error {
	if db == nil {
		return ErrClosed
	}
	return db.Transaction(ctx, func(tx *Tx) error { return tx.itemBatch(operation, items) })
}

func (tx *Tx) InsertMany(items any) error { return tx.itemBatch("insert", items) }
func (tx *Tx) UpdateMany(items any) error { return tx.itemBatch("update", items) }
func (tx *Tx) SaveMany(items any) error   { return tx.itemBatch("save", items) }
func (tx *Tx) DeleteMany(items any) error { return tx.itemBatch("delete", items) }

func (tx *Tx) itemBatch(operation string, items any) error {
	if tx == nil || tx.db == nil || tx.inner == nil || tx.done {
		return rime.ErrTxClosed
	}
	value := reflect.ValueOf(items)
	if value.IsValid() && value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return fmt.Errorf("murmur: nil batch slice")
		}
		value = value.Elem()
	}
	if !value.IsValid() || value.Kind() != reflect.Slice {
		return fmt.Errorf("murmur: batch requires []T, []*T or a pointer to either")
	}
	typ := value.Type().Elem()
	if typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	if typ.Kind() != reflect.Struct {
		return fmt.Errorf("murmur: batch elements must be structs or struct pointers")
	}
	if _, err := tx.db.resolveItemBinding(reflect.New(typ).Interface()); err != nil {
		return err
	}
	return tx.inner.Batch(operation, value.Len(), func(i int) error {
		entry := value.Index(i)
		if entry.Kind() != reflect.Pointer {
			entry = entry.Addr()
		}
		if entry.IsNil() {
			return fmt.Errorf("murmur: nil batch item")
		}
		item := entry.Interface()
		switch operation {
		case "insert":
			return tx.InsertItem(item)
		case "update":
			return tx.UpdateItem(item)
		case "save":
			return tx.SaveItem(item)
		case "delete":
			return tx.DeleteItem(item)
		default:
			return fmt.Errorf("murmur: unknown batch operation %q", operation)
		}
	})
}
