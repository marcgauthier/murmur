package rime

import (
	"reflect"
	"sort"
	"time"
)

// compoundIdx is a multi-field equality index over native comparable values.
// Levels nest per field (level i maps field-i values to level i+1); leaves
// hold record-key sets. No string encoding is involved on the hot path.
type compoundIdx struct {
	fields []string
	root   *compoundLevel
	total  int64 // total key references, maintained incrementally
}

type compoundLevel struct {
	kids    map[any]*compoundLevel
	keys    map[any]struct{} // non-nil only at leaf depth
	parent  *compoundLevel
	edge    any
	keyHint int
}

// comparableIndexType reports whether a field type is usable as a native
// index key (map keys panic on slices, maps, and funcs).
func comparableIndexType(t reflect.Type) bool {
	switch t.Kind() {
	case reflect.Slice, reflect.Map, reflect.Func, reflect.Struct:
		if t == reflect.TypeOf(UUID{}) || t == reflect.TypeFor[time.Time]() {
			return true
		}
		if t.Kind() == reflect.Struct {
			// Named struct kinds beyond UUID may hold slices; reject.
			return false
		}
		return false
	case reflect.Array:
		return t.Elem().Kind() == reflect.Uint8
	case reflect.Ptr, reflect.Interface, reflect.UnsafePointer:
		return false
	}
	return true
}

// compoundVals extracts the ordered native key values for c.
func compoundVals(c *compoundIdx, vals map[string]any) []any {
	parts := make([]any, len(c.fields))
	for i, f := range c.fields {
		parts[i] = vals[f]
	}
	return parts
}

func (c *compoundIdx) add(vals []any, key any) {
	n := c.root
	for _, v := range vals {
		if n.kids == nil {
			n.kids = map[any]*compoundLevel{}
		}
		kid, ok := n.kids[v]
		if !ok {
			kid = &compoundLevel{parent: n, edge: v}
			n.kids[v] = kid
		}
		n = kid
	}
	if n.keys == nil {
		n.keys = map[any]struct{}{}
	}
	if _, dup := n.keys[key]; !dup {
		n.keys[key] = struct{}{}
		c.total++
	}
}

func (l *compoundLevel) reserveKeys(total int) {
	if total <= l.keyHint {
		return
	}
	m := make(map[any]struct{}, total)
	for k := range l.keys {
		m[k] = struct{}{}
	}
	l.keys, l.keyHint = m, total
}

// prepareAdd attaches path-only levels and reserves leaf key storage before
// durability. Empty leaves contain no record keys, so Abort can prune them
// without changing query results.
func (c *compoundIdx) prepareAdd(vals []any, reserve int) {
	if reserve < 1 {
		reserve = 1
	}
	n := c.root
	for _, v := range vals {
		kid := n.kids[v]
		if kid == nil {
			if n.kids == nil {
				n.kids = make(map[any]*compoundLevel)
			}
			kid = &compoundLevel{parent: n, edge: v}
			n.kids[v] = kid
		}
		n = kid
	}
	n.reserveKeys(len(n.keys) + reserve)
}

func (c *compoundIdx) addPrepared(vals []any, key any) {
	n := c.root
	for _, v := range vals {
		kid := n.kids[v]
		if kid == nil {
			panic("rime: missing prepared compound path")
		}
		n = kid
	}
	if _, dup := n.keys[key]; !dup {
		n.keys[key] = struct{}{}
		c.total++
	}
}

func (c *compoundIdx) removeNoPrune(vals []any, key any) {
	n := c.root
	for _, v := range vals {
		n = n.kids[v]
		if n == nil {
			return
		}
	}
	if _, ok := n.keys[key]; !ok {
		return
	}
	delete(n.keys, key)
	c.total--
}

func (c *compoundIdx) prune(vals []any) {
	n := c.root
	for _, v := range vals {
		n = n.kids[v]
		if n == nil {
			return
		}
	}
	for n != c.root && len(n.keys) == 0 && len(n.kids) == 0 {
		parent := n.parent
		delete(parent.kids, n.edge)
		n = parent
	}
}

func (c *compoundIdx) remove(vals []any, key any) {
	n := c.root
	for _, v := range vals {
		kid, ok := n.kids[v]
		if !ok {
			return
		}
		n = kid
	}
	if _, ok := n.keys[key]; !ok {
		return
	}
	delete(n.keys, key)
	c.total--
	// Prune empty branches bottom-up.
	for n != c.root && len(n.keys) == 0 && len(n.kids) == 0 {
		parent := n.parent
		delete(parent.kids, n.edge)
		n = parent
	}
}

// lookupInto fills out with keys matching vals.
func (c *compoundIdx) lookupInto(out map[any]struct{}, vals []any) map[any]struct{} {
	n := c.root
	for _, v := range vals {
		kid, ok := n.kids[v]
		if !ok {
			return out
		}
		n = kid
	}
	for k := range n.keys {
		out[k] = struct{}{}
	}
	return out
}

func (c *compoundIdx) lookup(vals []any) map[any]struct{} {
	return c.lookupInto(map[any]struct{}{}, vals)
}

// dropKey removes key from every leaf, pruning empty branches. It reports
// whether anything was removed.
func (c *compoundIdx) dropKey(key any) bool {
	removed := false
	var walk func(l *compoundLevel) bool
	walk = func(l *compoundLevel) bool {
		if _, ok := l.keys[key]; ok {
			delete(l.keys, key)
			removed = true
		}
		for v, k := range l.kids {
			if walk(k) {
				delete(l.kids, v)
			}
		}
		return len(l.keys) == 0 && len(l.kids) == 0
	}
	walk(c.root)
	return removed
}

// recount recomputes the total reference count (GC safety net only).
func (c *compoundIdx) recount() int64 {
	var n int64
	var walk func(l *compoundLevel)
	walk = func(l *compoundLevel) {
		n += int64(len(l.keys))
		for _, k := range l.kids {
			walk(k)
		}
	}
	walk(c.root)
	return n
}

// comboCount counts leaf combinations holding at least one key.
func (c *compoundIdx) comboCount() int64 {
	var n int64
	var walk func(l *compoundLevel)
	walk = func(l *compoundLevel) {
		if len(l.keys) > 0 {
			n++
		}
		for _, k := range l.kids {
			walk(k)
		}
	}
	walk(c.root)
	return n
}

// compoundLookup encodes equality values for fields and probes the index.
// It reports false when any field lacks an equality value.
func (ix *indexSet) compoundLookup(name string, eq map[string]any) (map[any]struct{}, bool) {
	c, ok := ix.comp[name]
	if !ok {
		return nil, false
	}
	vals := make([]any, len(c.fields))
	for i, f := range c.fields {
		v, ok := eq[f]
		if !ok {
			return nil, false
		}
		vals[i] = normalizeIndexValue(v)
	}
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	return c.lookup(vals), true
}

// compoundChoices returns compound indexes whose fields are all covered by
// equality predicates, ordered by field count (most specific first).
func (ix *indexSet) compoundChoices(eq map[string]any) []string {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	var out []string
	for _, name := range ix.compoundOrder {
		c := ix.comp[name]
		cover := true
		for _, f := range c.fields {
			if _, ok := eq[f]; !ok {
				cover = false
				break
			}
		}
		if cover {
			out = append(out, name)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return len(ix.comp[out[i]].fields) > len(ix.comp[out[j]].fields)
	})
	return out
}
