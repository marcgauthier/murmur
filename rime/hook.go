package rime

// Transactional hooks operate directly on native pointers with no
// serialization. Before hooks run while the change is still private and may
// reject it with an error. After hooks run after the commit lock is released
// with the committed change. Hooks must be fast and must not open write
// transactions (BeforeCommit runs under the commit lock).

// BeforeInsert registers a hook called with the private copy before insert.
// Return an error to reject the operation.
func (t *Table[T]) BeforeInsert(fn func(*T) error) *Table[T] {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.beforeInsert = append(t.beforeInsert, fn)
	t.hasBeforeOp.Store(true)
	return t
}

// AfterInsert registers a hook called with the committed record.
func (t *Table[T]) AfterInsert(fn func(*T)) *Table[T] {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.afterInsert = append(t.afterInsert, fn)
	t.hasAfter.Store(true)
	return t
}

// BeforeUpdate registers a hook called with (old, new) before update.
func (t *Table[T]) BeforeUpdate(fn func(old, newv *T) error) *Table[T] {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.beforeUpdate = append(t.beforeUpdate, fn)
	t.hasBeforeOp.Store(true)
	return t
}

// AfterUpdate registers a hook called with (old, new) after commit.
func (t *Table[T]) AfterUpdate(fn func(old, newv *T)) *Table[T] {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.afterUpdate = append(t.afterUpdate, fn)
	t.hasAfter.Store(true)
	return t
}

// BeforeDelete registers a hook called with the record before delete.
func (t *Table[T]) BeforeDelete(fn func(*T) error) *Table[T] {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.beforeDelete = append(t.beforeDelete, fn)
	t.hasBeforeOp.Store(true)
	return t
}

// AfterDelete registers a hook called with the removed record after commit.
func (t *Table[T]) AfterDelete(fn func(*T)) *Table[T] {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.afterDelete = append(t.afterDelete, fn)
	t.hasAfter.Store(true)
	return t
}

// BeforeCommit registers a hook run once per committing transaction that
// touches this table. It executes under the commit lock.
func (t *Table[T]) BeforeCommit(fn func(*Tx) error) *Table[T] {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.beforeCommit = append(t.beforeCommit, fn)
	t.hasBeforeCommit.Store(true)
	return t
}

// AfterCommit registers a hook run with the commit ID after commit.
func (t *Table[T]) AfterCommit(fn func(TxID)) *Table[T] {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.afterCommit = append(t.afterCommit, fn)
	t.hasAfter.Store(true)
	return t
}

// AfterSave registers a hook receiving every committed Change for this table.
func (t *Table[T]) AfterSave(fn func(Change[T])) *Table[T] {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.afterSave = append(t.afterSave, fn)
	t.hasAfter.Store(true)
	return t
}
