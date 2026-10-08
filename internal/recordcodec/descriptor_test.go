package recordcodec

import (
	"bytes"
	"encoding/binary"
	"reflect"
	"testing"
	"time"

	"github.com/marcgauthier/murmur/rime"
)

type descriptorProfile struct{ Name string }

type descriptorRecord struct {
	ID      [16]byte
	Count   rime.Optional[int]
	Profile descriptorProfile
	Tags    []string
	Blob    []byte
	Labels  map[string][]int
	Ignored func() `murmur:"-"`
}

func TestCanonicalRecordRoundTripAndUnknownFieldPreservation(t *testing.T) {
	ids := map[string]uint32{"ID": 1, "Count": 2, "Profile": 3, "Profile.Name": 8, "Tags": 4, "Blob": 5, "Labels": 6}
	s, err := Compile(reflect.TypeFor[descriptorRecord](), CompileOptions{TableID: 9, PrimaryField: "ID", FieldIDs: ids})
	if err != nil {
		t.Fatal(err)
	}
	desc1, err := MarshalDescriptor(s)
	if err != nil {
		t.Fatal(err)
	}
	idsReordered := map[string]uint32{"Labels": 6, "Blob": 5, "Tags": 4, "Profile.Name": 8, "Profile": 3, "Count": 2, "ID": 1}
	s2, err := Compile(reflect.TypeFor[descriptorRecord](), CompileOptions{TableID: 9, PrimaryField: "ID", FieldIDs: idsReordered})
	if err != nil {
		t.Fatal(err)
	}
	desc2, err := MarshalDescriptor(s2)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(desc1, desc2) {
		t.Fatal("descriptor depends on option map iteration order")
	}
	var row descriptorRecord
	row.ID[0] = 7
	row.Count = rime.Some(0)
	row.Profile.Name = "Ada"
	row.Tags = []string{}
	row.Blob = []byte{}
	row.Labels = map[string][]int{"z": {2}, "a": {1}}
	unknown := []UnknownField{{ID: 99, Payload: []byte{0x80, 0x01}}}
	b1, err := Encode(s, row, unknown, nil, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	b2, err := Encode(s, row, unknown, nil, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(b1, b2) {
		t.Fatal("map encoding is not deterministic")
	}
	decoded, extra, err := Decode(s, b1, nil, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	got := decoded.(descriptorRecord)
	if !got.Count.IsPresent() || got.Count.Value != 0 || got.Tags == nil || got.Blob == nil || got.Profile.Name != "Ada" || len(got.Labels) != 2 {
		t.Fatalf("round trip lost values: %+v", got)
	}
	if len(extra) != 1 || extra[0].ID != 99 || !bytes.Equal(extra[0].Payload, unknown[0].Payload) {
		t.Fatalf("unknown fields: %+v", extra)
	}
	b3, err := Encode(s, got, extra, nil, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(b1, b3) {
		t.Fatalf("unknown field round trip changed canonical bytes\n% x\n% x", b1, b3)
	}
}

func TestNestedUnknownFieldsSurviveDecodeEditEncode(t *testing.T) {
	type oldProfile struct{ Name string }
	type oldRow struct {
		ID      [16]byte
		Profile oldProfile
	}
	type newProfile struct {
		Name     string
		Nickname string
	}
	type newRow struct {
		ID      [16]byte
		Profile newProfile
	}
	old, err := Compile(reflect.TypeFor[oldRow](), CompileOptions{TableID: 1, PrimaryField: "ID", FieldIDs: map[string]uint32{"ID": 1, "Profile": 3, "Profile.Name": 7}})
	if err != nil {
		t.Fatal(err)
	}
	newer, err := Compile(reflect.TypeFor[newRow](), CompileOptions{TableID: 1, PrimaryField: "ID", FieldIDs: map[string]uint32{"ID": 1, "Profile": 3, "Profile.Name": 7, "Profile.Nickname": 11}})
	if err != nil {
		t.Fatal(err)
	}
	original := newRow{Profile: newProfile{Name: "Ada", Nickname: "Countess"}}
	wire, err := Encode(newer, original, nil, nil, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	decoded, unknown, err := Decode(old, wire, nil, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	if len(unknown) != 1 || unknown[0].ID != 11 || !equalIDs(unknown[0].Path, []uint32{3}) {
		t.Fatalf("nested unknown metadata: %+v", unknown)
	}
	row := decoded.(oldRow)
	row.Profile.Name = "Ada Lovelace"
	encoded, err := Encode(old, row, unknown, nil, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	encodedAgain, err := Encode(old, row, unknown, nil, Limits{})
	if err != nil || !bytes.Equal(encoded, encodedAgain) {
		t.Fatalf("nested encoding not deterministic: %v", err)
	}
	got, extra, err := Decode(newer, encoded, nil, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	newGot := got.(newRow)
	if newGot.Profile.Name != "Ada Lovelace" || newGot.Profile.Nickname != "Countess" || len(extra) != 0 {
		t.Fatalf("edit dropped nested extension: %+v extra=%+v", newGot, extra)
	}
}

func TestUnknownFieldsInsideCollectionsSurviveDecodeEditEncode(t *testing.T) {
	type oldItem struct{ Name string }
	type newItem struct {
		Name  string
		Extra string
	}
	type oldRow struct {
		ID   [16]byte
		List []oldItem
		Map  map[string]oldItem
	}
	type newRow struct {
		ID   [16]byte
		List []newItem
		Map  map[string]newItem
	}
	ids := map[string]uint32{"ID": 1, "List": 2, "List[].Name": 3, "Map": 4, "Map{}.Name": 5}
	old, err := Compile(reflect.TypeFor[oldRow](), CompileOptions{TableID: 1, PrimaryField: "ID", FieldIDs: ids})
	if err != nil {
		t.Fatal(err)
	}
	newIDs := map[string]uint32{"ID": 1, "List": 2, "List[].Name": 3, "List[].Extra": 6, "Map": 4, "Map{}.Name": 5, "Map{}.Extra": 7}
	newer, err := Compile(reflect.TypeFor[newRow](), CompileOptions{TableID: 1, PrimaryField: "ID", FieldIDs: newIDs})
	if err != nil {
		t.Fatal(err)
	}
	oldDesc, _ := MarshalDescriptor(old)
	newDesc, _ := MarshalDescriptor(newer)
	if !DescriptorSupersetPreservingNestedUnknown(oldDesc, newDesc) {
		t.Fatal("additions within collection elements should be compatible")
	}
	want := newRow{
		List: []newItem{{Name: "first", Extra: "list-one"}, {Name: "second", Extra: "list-two"}},
		Map:  map[string]newItem{"a": {Name: "alpha", Extra: "map-a"}, "z": {Name: "zeta", Extra: "map-z"}},
	}
	wire, err := Encode(newer, want, nil, nil, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	decoded, unknown, err := Decode(old, wire, nil, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	if len(unknown) != 4 {
		t.Fatalf("unknown collection fields=%+v, want four", unknown)
	}
	row := decoded.(oldRow)
	row.List[0].Name = "first edited"
	row.Map["z"] = oldItem{Name: "zeta edited"}
	encoded, err := Encode(old, row, unknown, nil, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	got, remaining, err := Decode(newer, encoded, nil, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	if len(remaining) != 0 {
		t.Fatalf("unknown fields remain after newer decode: %+v", remaining)
	}
	result := got.(newRow)
	if result.List[0].Extra != "list-one" || result.List[1].Extra != "list-two" || result.Map["a"].Extra != "map-a" || result.Map["z"].Extra != "map-z" {
		t.Fatalf("collection unknown values moved or were dropped: %+v", result)
	}
	if result.List[0].Name != "first edited" || result.Map["z"].Name != "zeta edited" {
		t.Fatalf("known edits were lost: %+v", result)
	}
}

func FuzzNestedUnknownFieldRetention(f *testing.F) {
	type profile struct{ Name string }
	type row struct {
		ID      [16]byte
		Profile profile
	}
	s, err := Compile(reflect.TypeFor[row](), CompileOptions{TableID: 1, PrimaryField: "ID", FieldIDs: map[string]uint32{"ID": 1, "Profile": 3, "Profile.Name": 7}})
	if err != nil {
		f.Fatal(err)
	}
	f.Add([]byte("opaque"))
	f.Add([]byte{})
	f.Add([]byte{0, 255, 1})
	f.Fuzz(func(t *testing.T, payload []byte) {
		if len(payload) > 4096 {
			t.Skip()
		}
		wire, e := Encode(s, row{Profile: profile{Name: "before"}}, []UnknownField{{ID: 99, Payload: payload, Path: []uint32{3}}}, nil, Limits{MaxBytes: 8192})
		if e != nil {
			t.Fatal(e)
		}
		decoded, unknown, e := Decode(s, wire, nil, Limits{MaxBytes: 8192, MaxElements: 64, MaxDepth: 8})
		if e != nil {
			t.Fatal(e)
		}
		got := decoded.(row)
		got.Profile.Name = "edited"
		reencoded, e := Encode(s, got, unknown, nil, Limits{MaxBytes: 8192})
		if e != nil {
			t.Fatal(e)
		}
		check, rest, e := Decode(s, reencoded, nil, Limits{MaxBytes: 8192, MaxElements: 64, MaxDepth: 8})
		if e != nil {
			t.Fatal(e)
		}
		if check.(row).Profile.Name != "edited" || len(rest) != 1 || rest[0].ID != 99 || !bytes.Equal(rest[0].Payload, payload) || !equalIDs(rest[0].Path, []uint32{3}) {
			t.Fatal("nested unknown field changed across edit")
		}
	})
}

func FuzzCloneRecordOwnership(f *testing.F) {
	type profile struct{ Name string }
	type row struct {
		ID      [16]byte
		Profile profile
		Tags    []string
		Blob    []byte
		Labels  map[string][]int
	}
	s, err := Compile(reflect.TypeFor[row](), CompileOptions{TableID: 1, PrimaryField: "ID", FieldIDs: map[string]uint32{
		"ID": 1, "Profile": 2, "Profile.Name": 3, "Tags": 4, "Blob": 5, "Labels": 6,
	}})
	if err != nil {
		f.Fatal(err)
	}
	f.Add([]byte("nested-value"))
	f.Add([]byte{})
	f.Add([]byte{0, 255, 1})
	f.Fuzz(func(t *testing.T, payload []byte) {
		if len(payload) > 4096 {
			t.Skip()
		}
		original := row{
			Profile: profile{Name: string(payload)},
			Tags:    []string{string(payload)},
			Blob:    append([]byte(nil), payload...),
			Labels:  map[string][]int{string(payload): {len(payload)}},
		}
		clonedValue, err := CloneRecord(s, original, nil)
		if err != nil {
			t.Fatal(err)
		}
		cloned := clonedValue.(row)
		if !reflect.DeepEqual(original, cloned) {
			t.Fatal("clone changed record values")
		}
		cloned.Profile.Name = "changed"
		cloned.Tags[0] = "changed"
		if len(cloned.Blob) == 0 {
			cloned.Blob = append(cloned.Blob, 1)
		} else {
			cloned.Blob[0] ^= 0xff
		}
		cloned.Labels[string(payload)][0]++
		if original.Profile.Name != string(payload) || original.Tags[0] != string(payload) || !bytes.Equal(original.Blob, payload) || original.Labels[string(payload)][0] != len(payload) {
			t.Fatal("clone mutation changed the source record")
		}
	})
}

func TestDecodeRejectsMalformedAndOversizedValues(t *testing.T) {
	type row struct {
		ID   [16]byte
		When time.Time
	}
	s, err := Compile(reflect.TypeFor[row](), CompileOptions{TableID: 1, PrimaryField: "ID", FieldIDs: map[string]uint32{"ID": 1, "When": 2}})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = Decode(s, []byte("RGV\x01\x80"), nil, Limits{}); err == nil {
		t.Fatal("malformed count accepted")
	}
	encoded, err := Encode(s, row{}, nil, nil, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = Decode(s, encoded, nil, Limits{MaxBytes: 4}); err == nil {
		t.Fatal("oversized value accepted")
	}
}

func TestDecodeAbsentOptionalFields(t *testing.T) {
	type row struct {
		ID   [16]byte
		Name *string
		Note rime.Optional[string]
	}
	s, err := Compile(reflect.TypeFor[row](), CompileOptions{
		TableID: 1, PrimaryField: "ID", FieldIDs: map[string]uint32{"ID": 1, "Name": 2, "Note": 3},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, fieldID := range []uint32{2, 3} {
		encoded, err := EncodeField(s, fieldID, row{}, nil, Limits{})
		if err != nil {
			t.Fatalf("encode absent field %d: %v", fieldID, err)
		}
		got, _, err := DecodeFieldWithUnknown(s, fieldID, encoded, nil, Limits{})
		if err != nil {
			t.Fatalf("decode absent field %d: %v", fieldID, err)
		}
		switch value := got.(type) {
		case *string:
			if value != nil {
				t.Fatalf("decoded absent pointer = %q", *value)
			}
		case rime.Optional[string]:
			if value.Present {
				t.Fatalf("decoded absent Optional has value %q", value.Value)
			}
		default:
			t.Fatalf("decoded absent field %d has type %T", fieldID, got)
		}
	}
}

func TestDecodeNilBytesField(t *testing.T) {
	type row struct {
		ID   [16]byte
		Data []byte
	}
	s, err := Compile(reflect.TypeFor[row](), CompileOptions{
		TableID: 1, PrimaryField: "ID", FieldIDs: map[string]uint32{"ID": 1, "Data": 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := EncodeField(s, 2, row{}, nil, Limits{})
	if err != nil {
		t.Fatalf("encode nil bytes: %v", err)
	}
	got, _, err := DecodeFieldWithUnknown(s, 2, encoded, nil, Limits{})
	if err != nil {
		t.Fatalf("decode nil bytes: %v", err)
	}
	if value, ok := got.([]byte); !ok || value != nil {
		t.Fatalf("decoded nil bytes = %#v (%T)", got, got)
	}
}

func TestCodecCustomAndCycles(t *testing.T) {
	type custom struct{ N uint16 }
	type row struct {
		ID [16]byte
		V  custom
	}
	reg := NewCodecRegistry()
	err := reg.RegisterCodec("test.uint16", 1, custom{}, func(v any) ([]byte, error) { return binary.LittleEndian.AppendUint16(nil, v.(custom).N), nil }, func(p []byte, out any) error {
		if len(p) != 2 {
			return ErrCodec
		}
		out.(*custom).N = binary.LittleEndian.Uint16(p)
		return nil
	}, func(v any) (any, error) { return v, nil }, func(a, b any) bool { return a == b })
	if err != nil {
		t.Fatal(err)
	}
	s, err := Compile(reflect.TypeFor[row](), CompileOptions{TableID: 1, PrimaryField: "ID", FieldIDs: map[string]uint32{"ID": 1, "V": 2}, Codecs: reg})
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := Encode(s, row{V: custom{N: 42}}, nil, reg, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	decoded, _, err := Decode(s, encoded, reg, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	if decoded.(row).V.N != 42 {
		t.Fatal("custom value did not round trip")
	}
	type node struct {
		ID   [16]byte
		Next *node
	}
	cycleSchema, err := Compile(reflect.TypeFor[node](), CompileOptions{TableID: 2, PrimaryField: "ID", FieldIDs: map[string]uint32{"ID": 1, "Next": 2}})
	if err != nil {
		t.Fatal(err)
	}
	n := &node{}
	n.Next = n
	if _, err = Encode(cycleSchema, n, nil, nil, Limits{}); err == nil {
		t.Fatal("cyclic pointer graph accepted")
	}
}

func TestRichDescriptorAdditiveUnion(t *testing.T) {
	type base struct {
		ID   [16]byte
		Name string
	}
	type withAge struct {
		ID   [16]byte
		Name string
		Age  rime.Optional[int]
	}
	type withCity struct {
		ID   [16]byte
		Name string
		City rime.Optional[string]
	}
	compile := func(rt reflect.Type, ids map[string]uint32) []byte {
		t.Helper()
		s, e := Compile(rt, CompileOptions{TableID: 7, PrimaryField: "ID", FieldIDs: ids})
		if e != nil {
			t.Fatal(e)
		}
		b, e := MarshalDescriptor(s)
		if e != nil {
			t.Fatal(e)
		}
		return b
	}
	b := compile(reflect.TypeFor[base](), map[string]uint32{"ID": 1, "Name": 2})
	a := compile(reflect.TypeFor[withAge](), map[string]uint32{"ID": 1, "Name": 2, "Age": 10})
	c := compile(reflect.TypeFor[withCity](), map[string]uint32{"ID": 1, "Name": 2, "City": 11})
	if !DescriptorSuperset(b, a) || DescriptorSuperset(a, b) {
		t.Fatal("additive descriptor relation incorrect")
	}
	union, err := UnionDescriptors(a, c)
	if err != nil {
		t.Fatal(err)
	}
	if DescriptorSuperset(a, union) == false || DescriptorSuperset(c, union) == false {
		t.Fatal("merged descriptor omitted a concurrent addition")
	}
	type changed struct {
		ID   [16]byte
		Name int
	}
	d := compile(reflect.TypeFor[changed](), map[string]uint32{"ID": 1, "Name": 2})
	if _, err = UnionDescriptors(b, d); err == nil {
		t.Fatal("type replacement accepted")
	}
}

func TestDescriptorSubsetRetentionAllowsCollectionExtensions(t *testing.T) {
	type profileV1 struct{ Name string }
	type profileV2 struct {
		Name   string
		Region string
	}
	type nestedV1 struct {
		ID      [16]byte
		Profile profileV1
	}
	type nestedV2 struct {
		ID      [16]byte
		Profile profileV2
	}
	type listV1 struct {
		ID       [16]byte
		Profiles []profileV1
	}
	type listV2 struct {
		ID       [16]byte
		Profiles []profileV2
	}
	compile := func(rt reflect.Type, ids map[string]uint32) []byte {
		t.Helper()
		s, err := Compile(rt, CompileOptions{TableID: 10, PrimaryField: "ID", FieldIDs: ids})
		if err != nil {
			t.Fatal(err)
		}
		encoded, err := MarshalDescriptor(s)
		if err != nil {
			t.Fatal(err)
		}
		return encoded
	}
	nestedIDsV1 := map[string]uint32{"ID": 1, "Profile": 2, "Profile.Name": 1}
	nestedIDsV2 := map[string]uint32{"ID": 1, "Profile": 2, "Profile.Name": 1, "Profile.Region": 2}
	nestedOld := compile(reflect.TypeFor[nestedV1](), nestedIDsV1)
	nestedNew := compile(reflect.TypeFor[nestedV2](), nestedIDsV2)
	if !DescriptorSupersetPreservingNestedUnknown(nestedOld, nestedNew) {
		t.Fatal("direct nested struct addition should be preservable")
	}
	listIDsV1 := map[string]uint32{"ID": 1, "Profiles": 2, "Profiles[].Name": 1}
	listIDsV2 := map[string]uint32{"ID": 1, "Profiles": 2, "Profiles[].Name": 1, "Profiles[].Region": 2}
	listOld := compile(reflect.TypeFor[listV1](), listIDsV1)
	listNew := compile(reflect.TypeFor[listV2](), listIDsV2)
	if !DescriptorSuperset(listOld, listNew) || !DescriptorSupersetPreservingNestedUnknown(listOld, listNew) {
		t.Fatal("collection element extension with indexed unknown paths rejected")
	}
}

func FuzzDecodeNeverPanics(f *testing.F) {
	type row struct {
		ID    [16]byte
		Count int64
	}
	s, err := Compile(reflect.TypeFor[row](), CompileOptions{TableID: 1, PrimaryField: "ID", FieldIDs: map[string]uint32{"ID": 1, "Count": 2}})
	if err != nil {
		f.Fatal(err)
	}
	f.Add([]byte("RGV\x01\x00"))
	f.Add([]byte("RGV\x01\x02\x01\x10"))
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _, _ = Decode(s, data, nil, Limits{MaxBytes: 1 << 20, MaxElements: 1024, MaxDepth: 16})
	})
}

func TestCompileRecursiveDescriptor(t *testing.T) {
	ids := map[string]uint32{
		"ID": 11, "Count": 12, "Profile": 13, "Profile.Name": 21,
		"Tags": 14, "Blob": 15, "Labels": 16,
	}
	s, err := Compile(reflect.TypeFor[descriptorRecord](), CompileOptions{TableID: 9, PrimaryField: "ID", FieldIDs: ids})
	if err != nil {
		t.Fatal(err)
	}
	if s.TableID != 9 || s.PrimaryID != 11 || len(s.Fields) != 6 {
		t.Fatalf("schema header/fields: %+v", s)
	}
	count, ok := s.FieldByName("Count")
	if !ok || count.Descriptor.Kind != KindOptional || count.Descriptor.Element.Kind != KindInt {
		t.Fatalf("optional descriptor: %+v", count)
	}
	profile, ok := s.FieldByID(13)
	if !ok || profile.Descriptor.Kind != KindStruct || profile.Descriptor.Fields[0].ID != 21 {
		t.Fatalf("nested descriptor: %+v", profile)
	}
	labels, ok := s.FieldByName("Labels")
	if !ok || labels.Descriptor.Kind != KindMap || labels.Descriptor.Key.Kind != KindString || labels.Descriptor.Value.Kind != KindSlice {
		t.Fatalf("map descriptor: %+v", labels)
	}
}

type recursiveRecord struct {
	ID   [16]byte
	Next *recursiveRecord
}

func TestCompileRecursiveTypeGraph(t *testing.T) {
	s, err := Compile(reflect.TypeFor[recursiveRecord](), CompileOptions{TableID: 1, PrimaryField: "ID", FieldIDs: map[string]uint32{"ID": 1, "Next": 2}})
	if err != nil {
		t.Fatal(err)
	}
	next := s.Fields[1].Descriptor
	if next.Kind != KindOptional || next.Element != s.Record {
		t.Fatal("recursive descriptor did not preserve type reference")
	}
}

func TestCompileRejectsMissingIDsAndUnsupportedMapKeys(t *testing.T) {
	type missingID struct {
		ID [16]byte
		X  int
	}
	if _, err := Compile(reflect.TypeFor[missingID](), CompileOptions{TableID: 1, PrimaryField: "ID", FieldIDs: map[string]uint32{"ID": 1}}); err == nil {
		t.Fatal("missing field ID accepted")
	}
	type floatMap struct {
		ID [16]byte
		M  map[float64]string
	}
	if _, err := Compile(reflect.TypeFor[floatMap](), CompileOptions{TableID: 1, PrimaryField: "ID", FieldIDs: map[string]uint32{"ID": 1, "M": 2}}); err == nil {
		t.Fatal("float map key accepted")
	}
}

func TestCompileRejectsMergeTypeMismatch(t *testing.T) {
	type row struct {
		ID   [16]byte
		Name string
	}
	_, err := Compile(reflect.TypeFor[row](), CompileOptions{TableID: 1, PrimaryField: "ID", FieldIDs: map[string]uint32{"ID": 1, "Name": 2}, MergePolicies: map[string]MergePolicy{"Name": MergeMin}})
	if err == nil {
		t.Fatal("minimum merge on a string field accepted")
	}
}

func TestCompileUsesRegisteredCustomIdentity(t *testing.T) {
	type customValue struct{ N int }
	type row struct {
		ID [16]byte
		V  customValue
	}
	registry := NewCodecRegistry()
	if err := registry.Register("example.custom", 2, customValue{}); err != nil {
		t.Fatal(err)
	}
	s, err := Compile(reflect.TypeFor[row](), CompileOptions{TableID: 1, PrimaryField: "ID", FieldIDs: map[string]uint32{"ID": 1, "V": 2}, Codecs: registry})
	if err != nil {
		t.Fatal(err)
	}
	if f, _ := s.FieldByName("V"); f.Descriptor.Kind != KindCustom || f.Descriptor.CodecID != "example.custom" || f.Descriptor.CodecVersion != 2 {
		t.Fatalf("custom descriptor: %+v", f.Descriptor)
	}
}
