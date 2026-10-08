package recordcodec

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

const maxDescriptorNodes = 4096

type descriptorField struct {
	id    uint32
	merge byte
	child uint32
}
type descriptorNode struct {
	kind                Kind
	length              uint32
	element, key, value uint32
	codecVersion        uint16
	codecID             string
	fields              []descriptorField
}
type wireDescriptor struct {
	table, primary uint32
	nodes          []descriptorNode
}

// UnionDescriptors returns the canonical additive union of two RSD1 schemas.
// Existing field IDs must retain their exact type, codec and merge semantics.
func UnionDescriptors(left, right []byte) ([]byte, error) {
	a, e := parseDescriptor(left)
	if e != nil {
		return nil, e
	}
	b, e := parseDescriptor(right)
	if e != nil {
		return nil, e
	}
	if a.table != b.table || a.primary != b.primary {
		return nil, fmt.Errorf("%w: table or primary key changed", ErrDescriptor)
	}
	leftNodes, rightNodes := a.toGraph(), b.toGraph()
	memo := map[nodePair]*Descriptor{}
	var union func(*Descriptor, *Descriptor) (*Descriptor, error)
	union = func(x, y *Descriptor) (*Descriptor, error) {
		if x == nil || y == nil {
			if x == nil && y == nil {
				return nil, nil
			}
			return nil, fmt.Errorf("%w: descriptor shape changed", ErrDescriptor)
		}
		pair := nodePair{x, y}
		if found := memo[pair]; found != nil {
			return found, nil
		}
		if x.Kind != y.Kind || x.Length != y.Length || x.CodecID != y.CodecID || x.CodecVersion != y.CodecVersion {
			return nil, fmt.Errorf("%w: field type or codec changed", ErrDescriptor)
		}
		out := &Descriptor{Kind: x.Kind, Length: x.Length, CodecID: x.CodecID, CodecVersion: x.CodecVersion}
		memo[pair] = out
		var err error
		out.Element, err = union(x.Element, y.Element)
		if err != nil {
			return nil, err
		}
		out.Key, err = union(x.Key, y.Key)
		if err != nil {
			return nil, err
		}
		out.Value, err = union(x.Value, y.Value)
		if err != nil {
			return nil, err
		}
		if x.Kind == KindStruct {
			byID := map[uint32]Field{}
			for _, f := range x.Fields {
				byID[f.ID] = f
			}
			for _, f := range y.Fields {
				if old, ok := byID[f.ID]; ok {
					if mergeCode(old.Merge) != mergeCode(f.Merge) {
						return nil, fmt.Errorf("%w: merge policy changed for field %d", ErrDescriptor, f.ID)
					}
					d, e := union(old.Descriptor, f.Descriptor)
					if e != nil {
						return nil, e
					}
					old.Descriptor = d
					byID[f.ID] = old
				} else {
					byID[f.ID] = Field{ID: f.ID, Merge: f.Merge, Descriptor: cloneDescriptor(f.Descriptor, map[*Descriptor]*Descriptor{})}
				}
			}
			for _, f := range byID {
				out.Fields = append(out.Fields, f)
			}
		} else if len(x.Fields) != 0 || len(y.Fields) != 0 {
			return nil, fmt.Errorf("%w: fields on non-struct node", ErrDescriptor)
		}
		return out, nil
	}
	root, err := union(leftNodes, rightNodes)
	if err != nil {
		return nil, err
	}
	return MarshalDescriptor(&Schema{TableID: a.table, PrimaryID: a.primary, Record: root})
}

// DescriptorSuperset reports whether next is an additive extension of base.
func DescriptorSuperset(base, next []byte) bool {
	if len(base) == 0 || len(next) == 0 {
		return bytes.Equal(base, next)
	}
	merged, e := UnionDescriptors(base, next)
	return e == nil && bytes.Equal(merged, next)
}

// DescriptorSupersetPreservingNestedUnknown reports whether next adds only
// fields that an older runtime can retain while rewriting known record values.
// RGV1 unknown-field metadata qualifies nested additions by collection indexes
// and canonical map keys.
func DescriptorSupersetPreservingNestedUnknown(base, next []byte) bool {
	if !DescriptorSuperset(base, next) {
		return false
	}
	a, err := parseDescriptor(base)
	if err != nil {
		return false
	}
	b, err := parseDescriptor(next)
	if err != nil {
		return false
	}
	left, right := a.toGraph(), b.toGraph()
	type visit struct {
		left, right *Descriptor
		collection  bool
	}
	seen := make(map[visit]bool)
	var safe func(*Descriptor, *Descriptor, bool) bool
	safe = func(x, y *Descriptor, collection bool) bool {
		if x == nil || y == nil {
			return x == nil && y == nil
		}
		key := visit{x, y, collection}
		if seen[key] {
			return true
		}
		seen[key] = true
		if x.Kind != y.Kind {
			return false
		}
		if x.Kind == KindStruct {
			old := make(map[uint32]Field, len(x.Fields))
			for _, field := range x.Fields {
				old[field.ID] = field
			}
			for _, field := range y.Fields {
				previous, ok := old[field.ID]
				if !ok {
					continue
				}
				if !safe(previous.Descriptor, field.Descriptor, collection) {
					return false
				}
			}
			return true
		}
		inside := collection || x.Kind == KindArray || x.Kind == KindSlice || x.Kind == KindMap
		return safe(x.Element, y.Element, inside) && safe(x.Key, y.Key, inside) && safe(x.Value, y.Value, inside)
	}
	return safe(left, right, false)
}

// ValidateDescriptor checks an untrusted canonical RSD1 schema payload.
func ValidateDescriptor(src []byte) error { _, err := parseDescriptor(src); return err }

type nodePair struct{ x, y *Descriptor }

func cloneDescriptor(d *Descriptor, m map[*Descriptor]*Descriptor) *Descriptor {
	if d == nil {
		return nil
	}
	if v := m[d]; v != nil {
		return v
	}
	v := &Descriptor{Kind: d.Kind, Length: d.Length, CodecID: d.CodecID, CodecVersion: d.CodecVersion}
	m[d] = v
	v.Element = cloneDescriptor(d.Element, m)
	v.Key = cloneDescriptor(d.Key, m)
	v.Value = cloneDescriptor(d.Value, m)
	for _, f := range d.Fields {
		f.Descriptor = cloneDescriptor(f.Descriptor, m)
		v.Fields = append(v.Fields, f)
	}
	return v
}

func parseDescriptor(src []byte) (wireDescriptor, error) {
	var out wireDescriptor
	if len(src) < 16 || len(src) > 1<<20 || !bytes.Equal(src[:4], []byte{'R', 'S', 'D', 1}) {
		return out, fmt.Errorf("%w: invalid RSD1 descriptor", ErrDescriptor)
	}
	out.table = binary.BigEndian.Uint32(src[4:8])
	out.primary = binary.BigEndian.Uint32(src[8:12])
	n := binary.BigEndian.Uint32(src[12:16])
	if out.table == 0 || out.primary == 0 || n == 0 || n > maxDescriptorNodes {
		return out, fmt.Errorf("%w: invalid descriptor header", ErrDescriptor)
	}
	off := 16
	read32 := func() (uint32, error) {
		if len(src)-off < 4 {
			return 0, ErrDescriptor
		}
		v := binary.BigEndian.Uint32(src[off:])
		off += 4
		return v, nil
	}
	for i := uint32(0); i < n; i++ {
		if len(src)-off < 1+24 {
			return out, fmt.Errorf("%w: truncated node", ErrDescriptor)
		}
		d := descriptorNode{kind: Kind(src[off])}
		off++
		if d.kind < KindBool || d.kind > KindCustom {
			return out, fmt.Errorf("%w: unknown descriptor kind", ErrDescriptor)
		}
		var e error
		if d.length, e = read32(); e != nil {
			return out, e
		}
		if d.element, e = read32(); e != nil {
			return out, e
		}
		if d.key, e = read32(); e != nil {
			return out, e
		}
		if d.value, e = read32(); e != nil {
			return out, e
		}
		d.codecVersion = binary.BigEndian.Uint16(src[off:])
		idLen := int(binary.BigEndian.Uint16(src[off+2:]))
		off += 4
		if len(src)-off < idLen+4 {
			return out, fmt.Errorf("%w: truncated codec/fields", ErrDescriptor)
		}
		d.codecID = string(src[off : off+idLen])
		off += idLen
		nf := binary.BigEndian.Uint32(src[off:])
		off += 4
		if nf > maxDescriptorNodes || uint64(nf)*9 > uint64(len(src)-off) {
			return out, fmt.Errorf("%w: field limit/truncation", ErrDescriptor)
		}
		if d.kind != KindStruct && nf != 0 {
			return out, fmt.Errorf("%w: fields on non-struct node", ErrDescriptor)
		}
		if d.kind == KindCustom {
			if d.codecID == "" || d.codecVersion == 0 {
				return out, fmt.Errorf("%w: incomplete custom codec identity", ErrDescriptor)
			}
		} else if d.codecID != "" || d.codecVersion != 0 {
			return out, fmt.Errorf("%w: codec identity on built-in type", ErrDescriptor)
		}
		var last uint32
		for j := uint32(0); j < nf; j++ {
			f := descriptorField{id: binary.BigEndian.Uint32(src[off:]), merge: src[off+4], child: binary.BigEndian.Uint32(src[off+5:])}
			off += 9
			if f.id == 0 || f.id <= last || f.merge < 1 || f.merge > 5 || f.child == ^uint32(0) {
				return out, fmt.Errorf("%w: invalid field metadata", ErrDescriptor)
			}
			last = f.id
			d.fields = append(d.fields, f)
		}
		out.nodes = append(out.nodes, d)
	}
	if off != len(src) {
		return out, fmt.Errorf("%w: trailing descriptor bytes", ErrDescriptor)
	}
	valid := func(ref uint32) bool { return ref == ^uint32(0) || ref < n }
	none := ^uint32(0)
	for _, d := range out.nodes {
		for _, ref := range []uint32{d.element, d.key, d.value} {
			if !valid(ref) {
				return out, ErrDescriptor
			}
		}
		for _, f := range d.fields {
			if !valid(f.child) {
				return out, ErrDescriptor
			}
		}
		switch d.kind {
		case KindOptional, KindSlice:
			if d.element == none || d.key != none || d.value != none {
				return out, fmt.Errorf("%w: invalid element references", ErrDescriptor)
			}
		case KindArray:
			if d.length == 0 || d.key != none || d.value != none {
				return out, fmt.Errorf("%w: invalid array descriptor", ErrDescriptor)
			}
		case KindMap:
			if d.key == none || d.value == none || d.element != none {
				return out, fmt.Errorf("%w: invalid map descriptor", ErrDescriptor)
			}
		case KindStruct:
			if d.element != none || d.key != none || d.value != none {
				return out, fmt.Errorf("%w: invalid struct references", ErrDescriptor)
			}
		case KindBool, KindInt, KindUint, KindFloat32, KindFloat64, KindString, KindBytes, KindInstant, KindCustom:
			if d.element != none || d.key != none || d.value != none {
				return out, fmt.Errorf("%w: unexpected child references", ErrDescriptor)
			}
		}
	}
	if out.nodes[0].kind != KindStruct {
		return out, fmt.Errorf("%w: record root must be struct", ErrDescriptor)
	}
	primaryFound := false
	for _, f := range out.nodes[0].fields {
		if f.id == out.primary {
			if f.child == none {
				return out, fmt.Errorf("%w: primary field has no descriptor", ErrDescriptor)
			}
			child := out.nodes[f.child]
			if child.kind != KindArray || child.length != 16 || child.element != none || f.merge != 1 {
				return out, fmt.Errorf("%w: primary field must be an immutable 16-byte ID", ErrDescriptor)
			}
			primaryFound = true
		}
	}
	if !primaryFound {
		return out, fmt.Errorf("%w: primary field is absent", ErrDescriptor)
	}
	return out, nil
}

func (w wireDescriptor) toGraph() *Descriptor {
	nodes := make([]*Descriptor, len(w.nodes))
	for i, n := range w.nodes {
		nodes[i] = &Descriptor{Kind: n.kind, Length: int(n.length), CodecVersion: n.codecVersion, CodecID: n.codecID}
	}
	ref := func(i uint32) *Descriptor {
		if i == ^uint32(0) {
			return nil
		}
		return nodes[i]
	}
	for i, n := range w.nodes {
		d := nodes[i]
		d.Element = ref(n.element)
		d.Key = ref(n.key)
		d.Value = ref(n.value)
		for _, f := range n.fields {
			d.Fields = append(d.Fields, Field{ID: f.id, Merge: mergePolicy(f.merge), Descriptor: ref(f.child)})
		}
	}
	return nodes[0]
}
func mergePolicy(c byte) MergePolicy {
	switch c {
	case 1:
		return MergeLWW
	case 2:
		return MergeMin
	case 3:
		return MergeMax
	case 4:
		return MergeCounter
	case 5:
		return MergeORSet
	default:
		return ""
	}
}
