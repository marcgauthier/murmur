package rime

// Constraints execute before a transaction publishes; failure aborts the
// whole transaction. Supported constraints:
//
//	Primary key  exactly one `primary` field; enforced by key identity.
//	Unique       `unique` tag (or primary); enforced atomically at commit.
//	Not Null     `notnull` tag; absent pointers/Optional values rejected.
//	Check        WithCheck funcs; run for every new/updated record.
//	Foreign Key  WithForeignKey or `fk=Table.Field`; enforced when enabled
//	             (DB option WithForeignKeys or per-table WithTableForeignKeys).
//	Default      `default=X` tag; applied to absent optional fields on insert/save.
//
// Check is a named CHECK-constraint function for documentation purposes;
// WithCheck accepts plain funcs as well.
type Check[T any] func(*T) error

// ForeignKey describes a local -> remote table/field reference.
type ForeignKey struct {
	Local    string
	RefTable string
	RefField string
}

// AddCheck registers an additional CHECK constraint after registration.
func (t *Table[T]) AddCheck(fn func(*T) error) *Table[T] {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.checks = append(t.checks, fn)
	t.recomputeValidate()
	return t
}

// AddForeignKey registers an additional foreign key after registration.
func (t *Table[T]) AddForeignKey(local, refTable, refField string) *Table[T] {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.fks = append(t.fks, fkDef{local: local, refTable: refTable, refField: refField})
	t.recomputeValidate()
	return t
}

// SetForeignKeys enables or disables FK enforcement for this table,
// overriding the database default.
func (t *Table[T]) SetForeignKeys(enforce bool) *Table[T] {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.fkOn = &enforce
	t.recomputeValidate()
	return t
}

// ForeignKeys lists the registered foreign keys.
func (t *Table[T]) ForeignKeys() []ForeignKey {
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := make([]ForeignKey, 0, len(t.fks))
	for _, fk := range t.fks {
		out = append(out, ForeignKey{Local: fk.local, RefTable: fk.refTable, RefField: fk.refField})
	}
	for _, fm := range t.sch.fields {
		if fm.fkTable != "" {
			out = append(out, ForeignKey{Local: fm.name, RefTable: fm.fkTable, RefField: fm.fkField})
		}
	}
	return out
}
