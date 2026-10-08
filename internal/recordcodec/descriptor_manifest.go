package recordcodec

import (
	"encoding/binary"
	"fmt"
	"sort"
)

// MarshalDescriptor returns the stable RSD1 descriptor payload for a compiled
// record. Go names and declaration order are deliberately excluded.
func MarshalDescriptor(s *Schema) ([]byte, error) {
	if s == nil || s.Record == nil || s.TableID == 0 || s.PrimaryID == 0 {
		return nil, fmt.Errorf("%w: compiled schema required", ErrDescriptor)
	}
	type wireField struct {
		id    uint32
		merge byte
		child uint32
	}
	type wireNode struct {
		descriptor          *Descriptor
		element, key, value uint32
		fields              []wireField
	}
	nodes := []wireNode{}
	active := map[*Descriptor]uint32{}
	var visit func(*Descriptor) (uint32, error)
	visit = func(d *Descriptor) (uint32, error) {
		if d == nil {
			return ^uint32(0), nil
		}
		if i, ok := active[d]; ok {
			return i, nil
		}
		if len(nodes) >= maxDescriptorNodes {
			return 0, fmt.Errorf("%w: descriptor node limit", ErrDescriptor)
		}
		i := uint32(len(nodes))
		active[d] = i
		nodes = append(nodes, wireNode{descriptor: d})
		e, e1 := visit(d.Element)
		if e1 != nil {
			return 0, e1
		}
		k, e1 := visit(d.Key)
		if e1 != nil {
			return 0, e1
		}
		v, e1 := visit(d.Value)
		if e1 != nil {
			return 0, e1
		}
		fields := append([]Field(nil), d.Fields...)
		sort.Slice(fields, func(i, j int) bool { return fields[i].ID < fields[j].ID })
		wireFields := make([]wireField, 0, len(fields))
		for _, f := range fields {
			child, err := visit(f.Descriptor)
			if err != nil {
				return 0, err
			}
			wireFields = append(wireFields, wireField{f.ID, mergeCode(f.Merge), child})
		}
		nodes[i].element, nodes[i].key, nodes[i].value = e, k, v
		nodes[i].fields = wireFields
		delete(active, d)
		return i, nil
	}
	if _, err := visit(s.Record); err != nil {
		return nil, err
	}
	b := []byte{'R', 'S', 'D', 1}
	b = binary.BigEndian.AppendUint32(b, s.TableID)
	b = binary.BigEndian.AppendUint32(b, s.PrimaryID)
	b = binary.BigEndian.AppendUint32(b, uint32(len(nodes)))
	for _, node := range nodes {
		d := node.descriptor
		b = append(b, byte(d.Kind))
		b = binary.BigEndian.AppendUint32(b, uint32(d.Length))
		b = binary.BigEndian.AppendUint32(b, node.element)
		b = binary.BigEndian.AppendUint32(b, node.key)
		b = binary.BigEndian.AppendUint32(b, node.value)
		b = binary.BigEndian.AppendUint16(b, d.CodecVersion)
		b = binary.BigEndian.AppendUint16(b, uint16(len(d.CodecID)))
		b = append(b, d.CodecID...)
		b = binary.BigEndian.AppendUint32(b, uint32(len(node.fields)))
		for _, f := range node.fields {
			b = binary.BigEndian.AppendUint32(b, f.id)
			b = append(b, f.merge)
			b = binary.BigEndian.AppendUint32(b, f.child)
		}
	}
	return b, nil
}

func mergeCode(p MergePolicy) byte {
	switch p {
	case MergeLWW:
		return 1
	case MergeMin:
		return 2
	case MergeMax:
		return 3
	case MergeCounter:
		return 4
	case MergeORSet:
		return 5
	default:
		return 0
	}
}
