package rime

import (
	"bytes"
	"cmp"
	"fmt"
	"reflect"
	"sync"
)

// indexSet is the type-erased index bundle for one table. Record values are
// exchanged as any; per-field comparators are compiled at registration.
// All methods are safe for concurrent use.
//
// hashBucket is one hash-index entry: a single key inline until a second
// key promotes it to a set. Buckets are stored by value, so single-key
// values (the common case for low-cardinality fields) cost no map
// allocation at all. Promotion is one-way; empty buckets are deleted.
type hashBucket struct {
	single  any
	set     map[any]struct{}
	setHint int
}

const uniqueMapInitialHint = 8

// hashAdd indexes key under v, promoting single buckets to sets. It reports
// whether a new reference was added.
func hashAdd[K comparable](m map[K]hashBucket, v K, key any) bool {
	b, ok := m[v]
	switch {
	case !ok:
		m[v] = hashBucket{single: key}
		return true
	case b.set != nil:
		if _, present := b.set[key]; !present {
			b.set[key] = struct{}{}
			if len(b.set) > b.setHint {
				b.setHint = len(b.set)
				m[v] = b
			}
			return true
		}
		return false
	case b.single == key:
		return false
	default:
		s := make(map[any]struct{}, 2)
		s[b.single] = struct{}{}
		s[key] = struct{}{}
		m[v] = hashBucket{set: s, setHint: 2}
		return true
	}
}

type preparedHashIndexes struct {
	seeds []func()
}

type hashPrepareKey struct{ n int }

func reserveEmptyHashSet(count int) map[any]struct{} {
	set := make(map[any]struct{}, count)
	for i := 0; i < count; i++ {
		set[hashPrepareKey{i}] = struct{}{}
	}
	clear(set)
	return set
}

// reserveHashValues moves outer-map and collision-set growth into managed
// preparation. Any seed bucket it returns has an empty set and is installed
// only as publication begins, while index readers still observe the same
// record set as before the commit.
func reserveHashValues[K comparable](ix *indexSet, hm map[string]map[K]hashBucket, field string, adds map[K]int, seeds *[]func()) {
	m := hm[field]
	hint := ix.hashHints[field]
	missing := 0
	for value := range adds {
		if _, ok := m[value]; !ok {
			missing++
		}
	}
	need := len(m) + missing
	if need > hint {
		if hint < uniqueMapInitialHint {
			hint = uniqueMapInitialHint
		}
		for hint < need {
			hint *= 2
		}
		replacement := make(map[K]hashBucket, hint)
		for value, bucket := range m {
			replacement[value] = bucket
		}
		m = replacement
		hm[field] = m
		ix.hashHints[field] = hint
	}
	// Grow empty-map storage during preparation too. Go may defer allocating
	// map groups until the first insert even when make received a capacity.
	// These placeholder entries are hidden by ix.mu and removed before readers
	// can observe the map; the new backing groups remain available at install.
	inserted := make([]K, 0, missing)
	for value := range adds {
		if _, ok := m[value]; !ok {
			m[value] = hashBucket{}
			inserted = append(inserted, value)
		}
	}
	if len(m) == len(inserted) {
		clear(m)
	} else {
		for _, value := range inserted {
			delete(m, value)
		}
	}
	for value, count := range adds {
		bucket, ok := m[value]
		if !ok {
			if count >= 2 {
				set := reserveEmptyHashSet(count)
				v, prepared := value, hashBucket{set: set, setHint: count}
				*seeds = append(*seeds, func() { m[v] = prepared })
			}
			continue
		}
		if bucket.set != nil {
			wanted := len(bucket.set) + count
			if wanted > bucket.setHint {
				capacity := bucket.setHint
				if capacity < 2 {
					capacity = 2
				}
				for capacity < wanted {
					capacity *= 2
				}
				set := make(map[any]struct{}, capacity)
				for key := range bucket.set {
					set[key] = struct{}{}
				}
				bucket.set, bucket.setHint = set, capacity
				m[value] = bucket
			}
			continue
		}
		if count > 0 {
			capacity := 1 + count
			set := make(map[any]struct{}, capacity)
			set[bucket.single] = struct{}{}
			bucket.single, bucket.set, bucket.setHint = nil, set, capacity
			m[value] = bucket
		}
	}
}

// hashRemove drops key from v's bucket, deleting emptied buckets. It reports
// whether a reference was removed.
func hashRemove[K comparable](m map[K]hashBucket, v K, key any) bool {
	b, ok := m[v]
	if !ok {
		return false
	}
	if b.set != nil {
		if _, present := b.set[key]; !present {
			return false
		}
		delete(b.set, key)
		if len(b.set) == 0 {
			delete(m, v)
		}
		return true
	}
	if b.single == key {
		delete(m, v)
		return true
	}
	return false
}

// hashMaintain syncs one kind-family of hash indexes for a commit. Keys come
// from native extractors with no interface boxing.
func hashMaintain[T any, K comparable](ix *indexSet, t *Table[T], hm map[string]map[K]hashBucket, ex map[string]func(*T) K, oldHead, next *T, key any) {
	for field, m := range hm {
		e := ex[field]
		if oldHead != nil && next != nil && t.equals[field](oldHead, next) {
			continue
		}
		if oldHead != nil {
			if hashRemove(m, e(oldHead), key) {
				ix.hashRefs[field]--
			}
		}
		if next != nil {
			if hashAdd(m, e(next), key) {
				ix.hashRefs[field]++
			}
		}
	}
}

func hashMaintainPrepared[K comparable](ix *indexSet, hm map[string]map[K]hashBucket, effect hashIndexEffect, key any) {
	m := hm[effect.field]
	if effect.hasOld {
		if hashRemove(m, effect.old.(K), key) {
			ix.hashRefs[effect.field]--
		}
	}
	if effect.hasNew {
		if hashAdd(m, effect.new.(K), key) {
			ix.hashRefs[effect.field]++
		}
	}
}

// Query-side normalizers convert already-boxed predicate values to storage
// keys. Mismatched types report ok=false (a lookup miss), never panic.
func normStr(v any) (string, bool) {
	if s, ok := v.(string); ok {
		return s, true
	}
	if rv := reflect.ValueOf(v); rv.IsValid() && rv.Kind() == reflect.String {
		return rv.String(), true
	}
	return "", false
}

func normI64(v any) (int64, bool) {
	switch n := v.(type) {
	case int:
		return int64(n), true
	case int64:
		return n, true
	case int32:
		return int64(n), true
	case int16:
		return int64(n), true
	case int8:
		return int64(n), true
	}
	if rv := reflect.ValueOf(v); rv.IsValid() {
		switch rv.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			return rv.Int(), true
		}
	}
	return 0, false
}

func normU64(v any) (uint64, bool) {
	switch n := v.(type) {
	case uint64:
		return n, true
	case uint:
		return uint64(n), true
	case uint32:
		return uint64(n), true
	case uint16:
		return uint64(n), true
	case uint8:
		return uint64(n), true
	}
	if rv := reflect.ValueOf(v); rv.IsValid() {
		switch rv.Kind() {
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			return rv.Uint(), true
		}
	}
	return 0, false
}

func normF64(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	}
	if rv := reflect.ValueOf(v); rv.IsValid() {
		switch rv.Kind() {
		case reflect.Float32, reflect.Float64:
			return rv.Float(), true
		}
	}
	return 0, false
}

func normBool(v any) (bool, bool) {
	if b, ok := v.(bool); ok {
		return b, true
	}
	if rv := reflect.ValueOf(v); rv.IsValid() && rv.Kind() == reflect.Bool {
		return rv.Bool(), true
	}
	return false, false
}

// orderedMaintain syncs one ordered index for a commit through native keys.
// Non-orderable kinds (UUID) keep the boxed path.
func orderedMaintain[T any](t *Table[T], field string, o *orderedIdx, oldHead, next *T, key any) {
	hasOld, hasNew := oldHead != nil, next != nil
	switch o.kind {
	case kString:
		var oldV, newV string
		if hasOld {
			oldV = t.keyStr[field](oldHead)
		}
		if hasNew {
			newV = t.keyStr[field](next)
		}
		orderedApply(o, key, hasOld, hasNew, oldV, newV, unboxStr)
	case kInt64:
		var oldV, newV int64
		if hasOld {
			oldV = t.keyI64[field](oldHead)
		}
		if hasNew {
			newV = t.keyI64[field](next)
		}
		orderedApply(o, key, hasOld, hasNew, oldV, newV, unboxI64)
	case kUint64:
		var oldV, newV uint64
		if hasOld {
			oldV = t.keyU64[field](oldHead)
		}
		if hasNew {
			newV = t.keyU64[field](next)
		}
		orderedApply(o, key, hasOld, hasNew, oldV, newV, unboxU64)
	case kFloat64:
		var oldV, newV float64
		if hasOld {
			oldV = t.keyF64[field](oldHead)
		}
		if hasNew {
			newV = t.keyF64[field](next)
		}
		orderedApply(o, key, hasOld, hasNew, oldV, newV, unboxF64)
	default:
		var old, value any
		if hasOld {
			old = t.gets[field](oldHead)
		}
		if hasNew {
			value = t.gets[field](next)
		}
		if hasOld && hasNew {
			o.replace(old, value, key)
		} else {
			if hasOld {
				o.remove(old, key)
			}
			if hasNew {
				o.add(value, key)
			}
		}
	}
}

func orderedApply[K cmp.Ordered](o *orderedIdx, key any, hasOld, hasNew bool, oldV, newV K, unbox func(any) K) {
	switch {
	case hasOld && hasNew:
		orderedReplace(o, key, oldV, newV, unbox)
	case hasOld:
		orderedRemove(o, key, oldV, unbox)
	default:
		orderedAdd(o, key, newV, unbox)
	}
}

// uniqueOwner reports the live owner of a unique value, normalizing the
// already-boxed predicate to storage keys. The caller holds the index lock.
func (ix *indexSet) uniqueOwner(field string, val any) (any, bool) {
	switch ix.uniqueKinds[field] {
	case kString:
		if k, ok := normStr(val); ok {
			owner, ok := ix.uniqueStr[field][k]
			return owner, ok
		}
	case kInt64:
		if k, ok := normI64(val); ok {
			owner, ok := ix.uniqueI64[field][k]
			return owner, ok
		}
	case kUint64:
		if k, ok := normU64(val); ok {
			owner, ok := ix.uniqueU64[field][k]
			return owner, ok
		}
	case kFloat64:
		if k, ok := normF64(val); ok {
			owner, ok := ix.uniqueF64[field][k]
			return owner, ok
		}
	case kBool:
		if k, ok := normBool(val); ok {
			owner, ok := ix.uniqueBool[field][k]
			return owner, ok
		}
	default:
		if _, ok := ix.uniqueKinds[field]; ok {
			owner, ok := ix.uniqueAny[field][val]
			return owner, ok
		}
	}
	return nil, false
}

// hashBucketFor fetches one hash bucket by normalized key. The caller holds
// the index lock.
func (ix *indexSet) hashBucketFor(field string, val any) (hashBucket, bool) {
	switch ix.hashKinds[field] {
	case kString:
		if k, ok := normStr(val); ok {
			b, ok := ix.hashStr[field][k]
			return b, ok
		}
	case kInt64:
		if k, ok := normI64(val); ok {
			b, ok := ix.hashI64[field][k]
			return b, ok
		}
	case kUint64:
		if k, ok := normU64(val); ok {
			b, ok := ix.hashU64[field][k]
			return b, ok
		}
	case kFloat64:
		if k, ok := normF64(val); ok {
			b, ok := ix.hashF64[field][k]
			return b, ok
		}
	case kBool:
		if k, ok := normBool(val); ok {
			b, ok := ix.hashBool[field][k]
			return b, ok
		}
	default:
		if _, ok := ix.hashKinds[field]; ok {
			b, ok := ix.hashAny[field][val]
			return b, ok
		}
	}
	return hashBucket{}, false
}

// uniqueInstall stores one unique claim by normalized key. The caller holds
// the index lock; a nil owner deletes the entry. Claim keys always carry the
// field's type (they come from gets), so normalization cannot fail; a failure
// panics rather than silently dropping unique enforcement.
func (ix *indexSet) uniqueInstall(field string, val, owner any) {
	switch ix.uniqueKinds[field] {
	case kString:
		k, ok := normStr(val)
		if !ok {
			panic(fmt.Sprintf("rime: cannot normalize unique value for %s", field))
		}
		ix.uniqueInstallStr(field, k, owner)
	case kInt64:
		k, ok := normI64(val)
		if !ok {
			panic(fmt.Sprintf("rime: cannot normalize unique value for %s", field))
		}
		ix.uniqueInstallI64(field, k, owner)
	case kUint64:
		k, ok := normU64(val)
		if !ok {
			panic(fmt.Sprintf("rime: cannot normalize unique value for %s", field))
		}
		ix.uniqueInstallU64(field, k, owner)
	case kFloat64:
		k, ok := normF64(val)
		if !ok {
			panic(fmt.Sprintf("rime: cannot normalize unique value for %s", field))
		}
		ix.uniqueInstallF64(field, k, owner)
	case kBool:
		k, ok := normBool(val)
		if !ok {
			panic(fmt.Sprintf("rime: cannot normalize unique value for %s", field))
		}
		ix.uniqueInstallBool(field, k, owner)
	default:
		if _, ok := ix.uniqueKinds[field]; ok {
			if owner == nil {
				delete(ix.uniqueAny[field], val)
			} else {
				ix.uniqueAny[field][val] = owner
			}
		}
	}
}

func (ix *indexSet) uniqueInstallStr(field, k string, owner any) {
	if owner == nil {
		delete(ix.uniqueStr[field], k)
	} else {
		ix.uniqueStr[field][k] = owner
	}
}

func (ix *indexSet) uniqueInstallI64(field string, k int64, owner any) {
	if owner == nil {
		delete(ix.uniqueI64[field], k)
	} else {
		ix.uniqueI64[field][k] = owner
	}
}

func (ix *indexSet) uniqueInstallU64(field string, k uint64, owner any) {
	if owner == nil {
		delete(ix.uniqueU64[field], k)
	} else {
		ix.uniqueU64[field][k] = owner
	}
}

func (ix *indexSet) uniqueInstallF64(field string, k float64, owner any) {
	if owner == nil {
		delete(ix.uniqueF64[field], k)
	} else {
		ix.uniqueF64[field][k] = owner
	}
}

func (ix *indexSet) uniqueInstallBool(field string, k bool, owner any) {
	if owner == nil {
		delete(ix.uniqueBool[field], k)
	} else {
		ix.uniqueBool[field][k] = owner
	}
}

// refreshUniqueHintsLocked records actual occupancy after a non-managed write.
// The caller holds ix.mu for writing.
func (ix *indexSet) refreshUniqueHintsLocked() {
	for _, field := range ix.uniqueFields {
		var n int
		switch ix.uniqueKinds[field] {
		case kString:
			n = len(ix.uniqueStr[field])
		case kInt64:
			n = len(ix.uniqueI64[field])
		case kUint64:
			n = len(ix.uniqueU64[field])
		case kFloat64:
			n = len(ix.uniqueF64[field])
		case kBool:
			n = len(ix.uniqueBool[field])
		default:
			n = len(ix.uniqueAny[field])
		}
		if n > ix.uniqueHints[field] {
			ix.uniqueHints[field] = n
		}
	}
}

type indexSet struct {
	mu sync.RWMutex

	// Kind-specialized hash (field -> value -> keys) and unique
	// (field -> value -> key) maps. Native keys hash and compare without
	// interface boxing; kAny fields keep map[any] storage.
	hashStr   map[string]map[string]hashBucket
	hashI64   map[string]map[int64]hashBucket
	hashU64   map[string]map[uint64]hashBucket
	hashF64   map[string]map[float64]hashBucket
	hashBool  map[string]map[bool]hashBucket
	hashAny   map[string]map[any]hashBucket
	hashHints map[string]int

	uniqueStr  map[string]map[string]any
	uniqueI64  map[string]map[int64]any
	uniqueU64  map[string]map[uint64]any
	uniqueF64  map[string]map[float64]any
	uniqueBool map[string]map[bool]any
	uniqueAny  map[string]map[any]any
	// uniqueHints records the guaranteed entry budget used when each unique
	// map was created. Managed publication replaces a map only when a final
	// transaction state crosses this tracked growth threshold.
	uniqueHints map[string]int

	// hashKinds/uniqueKinds map indexed fields to their storage kind for
	// query-time dispatch. Maintenance loops range the kind maps directly.
	hashKinds   map[string]keyKind
	uniqueKinds map[string]keyKind

	// hashRefs counts total key references per hash field, maintained
	// incrementally so selectivity estimates stay O(1).
	hashRefs map[string]int64

	ordered map[string]*orderedIdx
	prefix  map[string]*trieIdx
	comp    map[string]*compoundIdx

	// kind metadata for stats/planner
	hashFields    []string
	uniqueFields  []string
	orderedFields []string
	prefixFields  []string
	compoundOrder []string
}

func buildIndexes(sch *Schema) (*indexSet, error) {
	ix := &indexSet{
		hashStr:     map[string]map[string]hashBucket{},
		hashI64:     map[string]map[int64]hashBucket{},
		hashU64:     map[string]map[uint64]hashBucket{},
		hashF64:     map[string]map[float64]hashBucket{},
		hashBool:    map[string]map[bool]hashBucket{},
		hashAny:     map[string]map[any]hashBucket{},
		hashHints:   map[string]int{},
		uniqueStr:   map[string]map[string]any{},
		uniqueI64:   map[string]map[int64]any{},
		uniqueU64:   map[string]map[uint64]any{},
		uniqueF64:   map[string]map[float64]any{},
		uniqueBool:  map[string]map[bool]any{},
		uniqueAny:   map[string]map[any]any{},
		uniqueHints: map[string]int{},
		hashKinds:   map[string]keyKind{},
		uniqueKinds: map[string]keyKind{},
		hashRefs:    make(map[string]int64, len(sch.fields)),
		ordered:     map[string]*orderedIdx{},
		prefix:      map[string]*trieIdx{},
		comp:        map[string]*compoundIdx{},
	}
	pk := sch.fields[sch.primary].name
	for _, fm := range sch.fields {
		kind := keyKindOf(fm.typ)
		if fm.primary || fm.unique {
			// Unique lookups resolve through the unique maps alone; a
			// parallel hash bucket would double storage and maintenance
			// for every unique value, so none is built.
			ix.uniqueKinds[fm.name] = kind
			ix.uniqueHints[fm.name] = uniqueMapInitialHint
			switch kind {
			case kString:
				ix.uniqueStr[fm.name] = make(map[string]any, uniqueMapInitialHint)
			case kInt64:
				ix.uniqueI64[fm.name] = make(map[int64]any, uniqueMapInitialHint)
			case kUint64:
				ix.uniqueU64[fm.name] = make(map[uint64]any, uniqueMapInitialHint)
			case kFloat64:
				ix.uniqueF64[fm.name] = make(map[float64]any, uniqueMapInitialHint)
			case kBool:
				ix.uniqueBool[fm.name] = make(map[bool]any, uniqueMapInitialHint)
			default:
				ix.uniqueAny[fm.name] = make(map[any]any, uniqueMapInitialHint)
			}
			ix.uniqueFields = append(ix.uniqueFields, fm.name)
		} else if fm.hash {
			ix.hashKinds[fm.name] = kind
			ix.hashHints[fm.name] = uniqueMapInitialHint
			// Seed the counter key at schema build time so the first managed
			// publication never allocates while recording index references.
			ix.hashRefs[fm.name] = 0
			switch kind {
			case kString:
				ix.hashStr[fm.name] = make(map[string]hashBucket, uniqueMapInitialHint)
			case kInt64:
				ix.hashI64[fm.name] = make(map[int64]hashBucket, uniqueMapInitialHint)
			case kUint64:
				ix.hashU64[fm.name] = make(map[uint64]hashBucket, uniqueMapInitialHint)
			case kFloat64:
				ix.hashF64[fm.name] = make(map[float64]hashBucket, uniqueMapInitialHint)
			case kBool:
				ix.hashBool[fm.name] = make(map[bool]hashBucket, uniqueMapInitialHint)
			default:
				ix.hashAny[fm.name] = make(map[any]hashBucket, uniqueMapInitialHint)
			}
			ix.hashFields = append(ix.hashFields, fm.name)
		}
		if fm.ordered {
			c, err := comparatorFor(fm.typ)
			if err != nil {
				return nil, err
			}
			o := newOrderedIdx(c)
			o.kind = kind
			ix.ordered[fm.name] = o
			ix.orderedFields = append(ix.orderedFields, fm.name)
		}
		if fm.prefix && fm.typ.Kind() == reflect.String {
			ix.prefix[fm.name] = newTrieIdx()
			ix.prefixFields = append(ix.prefixFields, fm.name)
		}
	}
	for _, cd := range sch.compound {
		for _, f := range cd.fields {
			fi, ok := sch.byName[f]
			if !ok {
				continue
			}
			if !comparableIndexType(sch.fields[fi].typ) {
				return nil, fmt.Errorf("%w: compound index %s field %s has non-comparable type %s",
					ErrBadSchema, cd.name, f, sch.fields[fi].typ)
			}
		}
		ix.comp[cd.name] = &compoundIdx{fields: append([]string(nil), cd.fields...), root: &compoundLevel{}}
		ix.compoundOrder = append(ix.compoundOrder, cd.name)
	}
	_ = pk
	return ix, nil
}

// onCommit maintains mutable indexes under the table index lock. Unchanged
// fields retain their entries. Unique claims publish once from the final
// transaction state, rather than exposing temporary claims between operations.
func onCommit[T any](t *Table[T], oldHead *T, p *pendingWrite, directUnique bool) {
	ix := t.idx
	ix.mu.Lock()
	defer ix.mu.Unlock()
	var next *T
	if !p.del {
		next = p.val.(*T)
	}
	// Length guards skip empty kind-families without paying for a map
	// iteration on every committed row.
	if effects, ok := p.preparedIndexEffects.(managedIndexEffects); ok {
		for _, effect := range effects.hash {
			switch effect.kind {
			case kString:
				hashMaintainPrepared(ix, ix.hashStr, effect, p.key)
			case kInt64:
				hashMaintainPrepared(ix, ix.hashI64, effect, p.key)
			case kUint64:
				hashMaintainPrepared(ix, ix.hashU64, effect, p.key)
			case kFloat64:
				hashMaintainPrepared(ix, ix.hashF64, effect, p.key)
			case kBool:
				hashMaintainPrepared(ix, ix.hashBool, effect, p.key)
			default:
				hashMaintainPrepared(ix, ix.hashAny, effect, p.key)
			}
		}
	} else {
		if len(ix.hashStr) > 0 {
			hashMaintain(ix, t, ix.hashStr, t.keyStr, oldHead, next, p.key)
		}
		if len(ix.hashI64) > 0 {
			hashMaintain(ix, t, ix.hashI64, t.keyI64, oldHead, next, p.key)
		}
		if len(ix.hashU64) > 0 {
			hashMaintain(ix, t, ix.hashU64, t.keyU64, oldHead, next, p.key)
		}
		if len(ix.hashF64) > 0 {
			hashMaintain(ix, t, ix.hashF64, t.keyF64, oldHead, next, p.key)
		}
		if len(ix.hashBool) > 0 {
			hashMaintain(ix, t, ix.hashBool, t.keyBool, oldHead, next, p.key)
		}
		if len(ix.hashAny) > 0 {
			hashMaintain(ix, t, ix.hashAny, t.gets, oldHead, next, p.key)
		}
	}
	if len(ix.ordered) > 0 {
		if effects, ok := p.preparedIndexEffects.(managedIndexEffects); ok {
			for _, effect := range effects.ordered {
				switch {
				case effect.hasOld && effect.hasNew:
					effect.index.replacePrepared(effect.old, effect.new, p.key, effect.node, effect.item)
				case effect.hasOld:
					effect.index.remove(effect.old, p.key)
				case effect.hasNew:
					effect.index.addPrepared(effect.new, p.key, effect.node, effect.item)
				}
			}
		} else {
			for field, o := range ix.ordered {
				if oldHead != nil && next != nil && t.equals[field](oldHead, next) {
					continue
				}
				orderedMaintain(t, field, o, oldHead, next, p.key)
			}
		}
	}
	if len(ix.prefix) > 0 {
		if effects, ok := p.preparedIndexEffects.(managedIndexEffects); ok {
			for _, effect := range effects.prefix {
				if effect.hasOld {
					effect.index.removeNoPrune(effect.old, p.key)
				}
				if effect.hasNew {
					effect.index.addPrepared(effect.new, p.key)
				}
			}
		} else {
			for field, trie := range ix.prefix {
				if oldHead != nil && next != nil && t.equals[field](oldHead, next) {
					continue
				}
				ex := t.keyStr[field]
				if oldHead != nil {
					trie.remove(ex(oldHead), p.key)
				}
				if next != nil {
					trie.add(ex(next), p.key)
				}
			}
		}
	}
	if len(ix.comp) > 0 {
		if effects, ok := p.preparedIndexEffects.(managedIndexEffects); ok {
			for _, effect := range effects.compound {
				if effect.oldValues != nil {
					effect.index.removeNoPrune(effect.oldValues, p.key)
				}
				if effect.newValues != nil {
					effect.index.addPrepared(effect.newValues, p.key)
				}
			}
		} else {
			for _, compound := range ix.comp {
				if oldHead != nil && next != nil {
					unchanged := true
					for _, field := range compound.fields {
						if !t.equals[field](oldHead, next) {
							unchanged = false
							break
						}
					}
					if unchanged {
						continue
					}
				}
				values := make([]any, len(compound.fields))
				var old []any
				if oldHead != nil {
					old = make([]any, len(values))
					for i, field := range compound.fields {
						old[i] = t.gets[field](oldHead)
					}
				}
				if next != nil {
					for i, field := range compound.fields {
						values[i] = t.gets[field](next)
					}
				}
				if oldHead != nil {
					compound.remove(old, p.key)
				}
				if next != nil {
					compound.add(values, p.key)
				}
			}
		}
	}
	if directUnique {
		t.installSingleUnique(p)
		ix.refreshUniqueHintsLocked()
	}
}

// publishUniqueClaims installs the validated final-state overlay once all
// versions have published. Intermediate mutations cannot erase another key's
// unique entry even when a temporary value is reused inside a transaction.
func (t *Table[T]) publishUniqueClaims(claims map[string]map[any]any) {
	t.idx.mu.Lock()
	defer t.idx.mu.Unlock()
	for field, values := range claims {
		for value, owner := range values {
			t.idx.uniqueInstall(field, value, owner)
		}
	}
	t.idx.refreshUniqueHintsLocked()
}

// checkUnique enforces unique constraints for one record, honoring
// same-commit tentative claims (recorded so later ops in the tx see them).
func idxCheckUnique[T any](ix *indexSet, t *Table[T], key any, rec *T, st *commitState) error {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	for _, field := range ix.uniqueFields {
		// Claims keys stay field-typed (uniform with releasePending); the
		// live check uses the native key with no boxing.
		v := t.gets[field](rec)
		fc := st.fieldClaims(t, field)
		owner, claimed := fc[v]
		if !claimed {
			owner, _ = ix.uniqueOwner(field, v)
		}
		if owner != nil && owner != key {
			return &ConstraintError{Table: t.name, Field: field, Value: v, Err: ErrUnique}
		}
		fc[v] = key
	}
	return nil
}

// candidatesInto fills out with keys where field == val. out should be empty;
// it is returned for chaining. Scoped callers pass pooled maps.
func (ix *indexSet) candidatesInto(out map[any]struct{}, field string, val any) map[any]struct{} {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	if _, ok := ix.uniqueKinds[field]; ok {
		if k, ok := ix.uniqueOwner(field, val); ok {
			out[k] = struct{}{}
		}
		return out
	}
	if b, ok := ix.hashBucketFor(field, val); ok {
		if b.set != nil {
			for k := range b.set {
				out[k] = struct{}{}
			}
		} else {
			out[b.single] = struct{}{}
		}
	}
	return out
}

// eqEstimate returns the average matches per equality value for an indexed
// field (1 for unique), used to pick the most selective index before probing.
func (ix *indexSet) eqEstimate(field string) (float64, bool) {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	if _, ok := ix.uniqueKinds[field]; ok {
		return 1, true
	}
	if _, ok := ix.hashKinds[field]; ok {
		n := ix.hashLen(field)
		if n == 0 {
			return 0, true
		}
		return float64(ix.hashRefs[field]) / float64(n), true
	}
	return 0, false
}

// hashLen reports the distinct-value count of one hash index.
func (ix *indexSet) hashLen(field string) int {
	switch ix.hashKinds[field] {
	case kString:
		return len(ix.hashStr[field])
	case kInt64:
		return len(ix.hashI64[field])
	case kUint64:
		return len(ix.hashU64[field])
	case kFloat64:
		return len(ix.hashF64[field])
	case kBool:
		return len(ix.hashBool[field])
	default:
		return len(ix.hashAny[field])
	}
}

// uniqueLen reports the entry count of one unique index.
func (ix *indexSet) uniqueLen(field string) int {
	switch ix.uniqueKinds[field] {
	case kString:
		return len(ix.uniqueStr[field])
	case kInt64:
		return len(ix.uniqueI64[field])
	case kUint64:
		return len(ix.uniqueU64[field])
	case kFloat64:
		return len(ix.uniqueF64[field])
	case kBool:
		return len(ix.uniqueBool[field])
	default:
		return len(ix.uniqueAny[field])
	}
}

// candidates returns keys with field == val via hash/unique index.
func (ix *indexSet) candidates(field string, val any) map[any]struct{} {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	if _, ok := ix.uniqueKinds[field]; ok {
		if k, ok := ix.uniqueOwner(field, val); ok {
			return map[any]struct{}{k: {}}
		}
		return map[any]struct{}{}
	}
	if _, ok := ix.hashKinds[field]; ok {
		if b, ok := ix.hashBucketFor(field, val); ok {
			if b.set != nil {
				out := make(map[any]struct{}, len(b.set))
				for k := range b.set {
					out[k] = struct{}{}
				}
				return out
			}
			return map[any]struct{}{b.single: {}}
		}
		return map[any]struct{}{}
	}
	return nil
}

// uniqueHas reports a unique/primary value existence check used by FK lookups.
func idxUniqueHas[T any](ix *indexSet, t *Table[T], field string, val any, snap TxID) bool {
	scope := t.db.newScope()
	defer scope.release()
	for k := range ix.candidatesInto(scope.cmap(), field, val) {
		s := t.shardFor(k)
		s.mu.RLock()
		c, ok := s.rows[k]
		s.mu.RUnlock()
		if !ok {
			continue
		}
		if v, ok := c.visible(snap); ok && !v.tomb {
			return true
		}
	}
	return false
}

// indexStats returns entry counts and approximate cardinalities.
func (ix *indexSet) indexStats() (entries int64, perIndex map[string]IndexStat) {
	perIndex = map[string]IndexStat{}
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	for f := range ix.uniqueKinds {
		n := int64(ix.uniqueLen(f))
		entries += n
		perIndex[f] = IndexStat{Name: f, Kind: "unique", Entries: n, Cardinality: n, Selectivity: selectivity(n, n)}
	}
	for f := range ix.hashKinds {
		n := ix.hashRefs[f]
		entries += n
		card := int64(ix.hashLen(f))
		perIndex[f] = IndexStat{Name: f, Kind: "hash", Entries: n,
			Cardinality: card, Selectivity: selectivity(card, n)}
	}
	for f, o := range ix.ordered {
		n := int64(o.len())
		entries += n
		perIndex[f] = IndexStat{Name: f, Kind: "ordered", Entries: n, Cardinality: n, Selectivity: selectivity(n, n)}
	}
	for f, tr := range ix.prefix {
		n := int64(tr.len())
		entries += n
		perIndex[f] = IndexStat{Name: f, Kind: "prefix", Entries: n, Cardinality: n, Selectivity: selectivity(n, n)}
	}
	for name, c := range ix.comp {
		entries += c.total
		combos := c.comboCount()
		perIndex[name] = IndexStat{Name: name, Kind: "compound", Entries: c.total,
			Cardinality: combos, Selectivity: selectivity(combos, c.total)}
	}
	return entries, perIndex
}

// selectivity is the distinct fraction driving index choice.
func selectivity(distinct, entries int64) float64 {
	if entries == 0 {
		return 0
	}
	return float64(distinct) / float64(entries)
}

// comparatorFor compiles an any-domain comparator for an ordered field type.
func comparatorFor(typ reflect.Type) (func(a, b any) int, error) {
	switch typ.Kind() {
	case reflect.String:
		return func(a, b any) int { return cmp.Compare(a.(string), b.(string)) }, nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return func(a, b any) int { return cmp.Compare(toInt64(a), toInt64(b)) }, nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return func(a, b any) int { return cmp.Compare(toUint64(a), toUint64(b)) }, nil
	case reflect.Float32, reflect.Float64:
		return func(a, b any) int { return cmp.Compare(toFloat64(a), toFloat64(b)) }, nil
	}
	if typ == reflect.TypeOf(UUID{}) {
		return func(a, b any) int {
			au, aok := a.(UUID)
			bu, bok := b.(UUID)
			if !aok || !bok {
				return 0
			}
			return bytes.Compare(au[:], bu[:])
		}, nil
	}
	return nil, &ConstraintError{Field: typ.String(), Err: ErrBadSchema}
}

func toInt64(v any) int64 {
	switch n := v.(type) {
	case int:
		return int64(n)
	case int64:
		return n
	case int32:
		return int64(n)
	case int16:
		return int64(n)
	case int8:
		return int64(n)
	}
	return reflect.ValueOf(v).Convert(reflect.TypeOf(int64(0))).Interface().(int64)
}

func toUint64(v any) uint64 {
	switch n := v.(type) {
	case uint64:
		return n
	case uint:
		return uint64(n)
	case uint32:
		return uint64(n)
	case uint16:
		return uint64(n)
	case uint8:
		return uint64(n)
	}
	return reflect.ValueOf(v).Convert(reflect.TypeOf(uint64(0))).Interface().(uint64)
}

func toFloat64(v any) float64 {
	switch n := v.(type) {
	case float64:
		return n
	case float32:
		return float64(n)
	}
	return reflect.ValueOf(v).Convert(reflect.TypeOf(float64(0))).Interface().(float64)
}
