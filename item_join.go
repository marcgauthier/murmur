package murmur

import (
	"context"
	"fmt"
	"reflect"

	"github.com/marcgauthier/murmur/rime"
)

// This file implements item-level joins over one pinned read snapshot. Both
// sides are fetched through snapshot item queries and hash-joined in memory
// with the same semantics as the typed joins: left-scan order, duplicate
// keys fan out, and zero values match zero values.

// ItemJoinRow is one join output pair. Right is nil for unmatched LEFT JOIN
// rows. Left and Right hold detached *T pointers for their models.
type ItemJoinRow struct {
	Left, Right any
}

// InnerJoin hash-joins two models on equal key fields over one pinned
// snapshot, returning one row per matching pair. Both keys must share the
// same comparable Go type.
func (db *DB) InnerJoin(ctx context.Context, leftModel any, leftField string, rightModel any, rightField string) ([]ItemJoinRow, error) {
	return db.joinItems(ctx, leftModel, leftField, rightModel, rightField, false)
}

func (tx *recordReadTx) InnerJoin(leftModel any, leftField string, rightModel any, rightField string) ([]ItemJoinRow, error) {
	return tx.joinItems(leftModel, leftField, rightModel, rightField, false)
}

// LeftJoin keeps every left row, with a nil Right when nothing matches.
func (db *DB) LeftJoin(ctx context.Context, leftModel any, leftField string, rightModel any, rightField string) ([]ItemJoinRow, error) {
	return db.joinItems(ctx, leftModel, leftField, rightModel, rightField, true)
}

func (tx *recordReadTx) LeftJoin(leftModel any, leftField string, rightModel any, rightField string) ([]ItemJoinRow, error) {
	return tx.joinItems(leftModel, leftField, rightModel, rightField, true)
}

// joinItems pins one read snapshot and hash-joins both models inside it.
func (db *DB) joinItems(ctx context.Context, leftModel any, leftField string, rightModel any, rightField string, outer bool) ([]ItemJoinRow, error) {
	tx, err := db.readTxContext(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Close()
	return tx.joinItems(leftModel, leftField, rightModel, rightField, outer)
}

func (tx *recordReadTx) joinItems(leftModel any, leftField string, rightModel any, rightField string, outer bool) ([]ItemJoinRow, error) {
	if tx == nil || tx.db == nil || tx.inner == nil || tx.done {
		return nil, rime.ErrTxClosed
	}
	leftBinding, err := resolveItemBindingFrom(leftModel, tx.itemBindings, tx.itemAmbiguous)
	if err != nil {
		return nil, err
	}
	rightBinding, err := resolveItemBindingFrom(rightModel, tx.itemBindings, tx.itemAmbiguous)
	if err != nil {
		return nil, err
	}
	leftType, ok := leftBinding.fields[leftField]
	if !ok {
		return nil, fmt.Errorf("murmur: unknown field %q for %s", leftField, leftBinding.goType)
	}
	rightType, ok := rightBinding.fields[rightField]
	if !ok {
		return nil, fmt.Errorf("murmur: unknown field %q for %s", rightField, rightBinding.goType)
	}
	if leftType != rightType {
		return nil, fmt.Errorf("murmur: join keys must share the same Go type, got %s and %s", leftType, rightType)
	}
	if !itemComparableType(leftType) {
		return nil, fmt.Errorf("murmur: join key %q has type %s, which item queries cannot compare", leftField, leftType)
	}
	leftRows, err := tx.joinSide(leftModel, leftBinding)
	if err != nil {
		return nil, err
	}
	rightRows, err := tx.joinSide(rightModel, rightBinding)
	if err != nil {
		return nil, err
	}
	probe := make(map[any][]int, rightRows.Len())
	for i := 0; i < rightRows.Len(); i++ {
		key := rightRows.Index(i).FieldByName(rightField).Interface()
		probe[key] = append(probe[key], i)
	}
	out := make([]ItemJoinRow, 0, leftRows.Len())
	for i := 0; i < leftRows.Len(); i++ {
		if i%64 == 0 && tx.ctx != nil {
			if err := tx.ctx.Err(); err != nil {
				return nil, err
			}
		}
		left := leftRows.Index(i)
		key := left.FieldByName(leftField).Interface()
		matched := false
		for _, j := range probe[key] {
			matched = true
			out = append(out, ItemJoinRow{Left: left.Addr().Interface(), Right: rightRows.Index(j).Addr().Interface()})
		}
		if outer && !matched {
			out = append(out, ItemJoinRow{Left: left.Addr().Interface()})
		}
	}
	return out, nil
}

// joinSide fetches every row of one model from the pinned snapshot.
func (tx *recordReadTx) joinSide(model any, binding *itemBinding) (reflect.Value, error) {
	dest := reflect.New(reflect.SliceOf(binding.goType))
	if err := tx.itemQuery(model, nil).FindInto(dest.Interface()); err != nil {
		return reflect.Value{}, err
	}
	return dest.Elem(), nil
}
