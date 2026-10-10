package murmur

import (
	"context"
	"fmt"

	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/rime"
)

// This file implements item-level CRDT operations for merge-policy fields:
// PN_COUNTER increments, OR_SET membership changes, and MIN/MAX extrema.
// The item supplies the row identity; the engine enforces the exact field
// policy. Caller items are never updated in place; re-read with GetItem for
// the converged value.

// validateCRDTField checks that a field exists, is mutable, and uses a merge
// policy. The engine enforces the exact counter, set, or extrema policy.
func (b *itemBinding) validateCRDTField(field string) error {
	if _, ok := b.fields[field]; !ok {
		return fmt.Errorf("murmur: unknown field %q for %s", field, b.goType)
	}
	if field == b.primaryField || (b.model != nil && field == b.model.business) {
		return fmt.Errorf("murmur: primary field %q is immutable", field)
	}
	if !b.nonLWW[field] {
		return fmt.Errorf("murmur: field %q uses last-write-wins; use UpdateFields", field)
	}
	return nil
}

// CounterAdd atomically adds delta to an int64 PN_COUNTER field. A zero delta
// is a no-op. A missing row reports rime.ErrNotFound.
func (db *DB) CounterAdd(ctx context.Context, item any, field string, delta int64) error {
	if db == nil {
		return ErrClosed
	}
	binding, err := db.resolveItemBinding(item)
	if err != nil {
		return err
	}
	if err := binding.validateCRDTField(field); err != nil {
		return err
	}
	key, err := binding.rowID(item)
	if err != nil {
		return err
	}
	return db.WriteTxContext(ctx, func(tx *Tx) error {
		return binding.ops.counterAdd(db, tx, key, field, delta)
	})
}

// CounterAdd stages a counter increment in the transaction.
func (tx *Tx) CounterAdd(item any, field string, delta int64) error {
	if tx == nil || tx.db == nil {
		return rime.ErrTxClosed
	}
	binding, err := tx.db.resolveItemBinding(item)
	if err != nil {
		return err
	}
	if err := binding.validateCRDTField(field); err != nil {
		return err
	}
	key, err := binding.rowID(item)
	if err != nil {
		return err
	}
	return binding.ops.counterAdd(tx.db, tx, key, field, delta)
}

// SetAdd adds a string to a []string OR_SET field. Re-adding a present value
// is a no-op. A missing row reports rime.ErrNotFound.
func (db *DB) SetAdd(ctx context.Context, item any, field, value string) error {
	if db == nil {
		return ErrClosed
	}
	binding, err := db.resolveItemBinding(item)
	if err != nil {
		return err
	}
	if err := binding.validateCRDTField(field); err != nil {
		return err
	}
	key, err := binding.rowID(item)
	if err != nil {
		return err
	}
	return db.WriteTxContext(ctx, func(tx *Tx) error {
		return binding.ops.setAdd(db, tx, key, field, value)
	})
}

// SetAdd stages an OR_SET addition in the transaction.
func (tx *Tx) SetAdd(item any, field, value string) error {
	if tx == nil || tx.db == nil {
		return rime.ErrTxClosed
	}
	binding, err := tx.db.resolveItemBinding(item)
	if err != nil {
		return err
	}
	if err := binding.validateCRDTField(field); err != nil {
		return err
	}
	key, err := binding.rowID(item)
	if err != nil {
		return err
	}
	return binding.ops.setAdd(tx.db, tx, key, field, value)
}

// SetRemove removes a string from a []string OR_SET field. Removing an absent
// value is a no-op. A missing row reports rime.ErrNotFound.
func (db *DB) SetRemove(ctx context.Context, item any, field, value string) error {
	if db == nil {
		return ErrClosed
	}
	binding, err := db.resolveItemBinding(item)
	if err != nil {
		return err
	}
	if err := binding.validateCRDTField(field); err != nil {
		return err
	}
	key, err := binding.rowID(item)
	if err != nil {
		return err
	}
	return db.WriteTxContext(ctx, func(tx *Tx) error {
		return binding.ops.setRemove(db, tx, key, field, value)
	})
}

// SetRemove stages an OR_SET removal in the transaction.
func (tx *Tx) SetRemove(item any, field, value string) error {
	if tx == nil || tx.db == nil {
		return rime.ErrTxClosed
	}
	binding, err := tx.db.resolveItemBinding(item)
	if err != nil {
		return err
	}
	if err := binding.validateCRDTField(field); err != nil {
		return err
	}
	key, err := binding.rowID(item)
	if err != nil {
		return err
	}
	return binding.ops.setRemove(tx.db, tx, key, field, value)
}

// Max applies a numeric maximum to a MIN/MAX-policy field. Values coerce to
// the field type with the usual range checks. A missing row reports
// rime.ErrNotFound.
func (db *DB) Max(ctx context.Context, item any, field string, value any) error {
	if db == nil {
		return ErrClosed
	}
	binding, err := db.resolveItemBinding(item)
	if err != nil {
		return err
	}
	coerced, key, err := binding.extremaArg(item, field, value)
	if err != nil {
		return err
	}
	return db.WriteTxContext(ctx, func(tx *Tx) error {
		return binding.ops.extrema(db, tx, key, field, coerced, true)
	})
}

// Max stages a numeric maximum in the transaction.
func (tx *Tx) Max(item any, field string, value any) error {
	if tx == nil || tx.db == nil {
		return rime.ErrTxClosed
	}
	binding, err := tx.db.resolveItemBinding(item)
	if err != nil {
		return err
	}
	coerced, key, err := binding.extremaArg(item, field, value)
	if err != nil {
		return err
	}
	return binding.ops.extrema(tx.db, tx, key, field, coerced, true)
}

// Min applies a numeric minimum to a MIN/MAX-policy field. Values coerce to
// the field type with the usual range checks. A missing row reports
// rime.ErrNotFound.
func (db *DB) Min(ctx context.Context, item any, field string, value any) error {
	if db == nil {
		return ErrClosed
	}
	binding, err := db.resolveItemBinding(item)
	if err != nil {
		return err
	}
	coerced, key, err := binding.extremaArg(item, field, value)
	if err != nil {
		return err
	}
	return db.WriteTxContext(ctx, func(tx *Tx) error {
		return binding.ops.extrema(db, tx, key, field, coerced, false)
	})
}

// Min stages a numeric minimum in the transaction.
func (tx *Tx) Min(item any, field string, value any) error {
	if tx == nil || tx.db == nil {
		return rime.ErrTxClosed
	}
	binding, err := tx.db.resolveItemBinding(item)
	if err != nil {
		return err
	}
	coerced, key, err := binding.extremaArg(item, field, value)
	if err != nil {
		return err
	}
	return binding.ops.extrema(tx.db, tx, key, field, coerced, false)
}

// extremaArg validates an extrema field and coerces the candidate to the
// exact field type the engine requires.
func (b *itemBinding) extremaArg(item any, field string, value any) (any, ids.RowID, error) {
	if err := b.validateCRDTField(field); err != nil {
		return nil, ids.RowID{}, err
	}
	coerced, err := coerceItemValue(b.fields[field], value)
	if err != nil {
		return nil, ids.RowID{}, fmt.Errorf("murmur: field %q: %w", field, err)
	}
	key, err := b.rowID(item)
	if err != nil {
		return nil, ids.RowID{}, err
	}
	return coerced.Interface(), key, nil
}
