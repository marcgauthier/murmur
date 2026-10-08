package rime

import "testing"

func TestCloneRecordOwnsNestedMutableValues(t *testing.T) {
	type nested struct{ Values []string }
	type record struct {
		Nested *nested
		Items  map[string][]int
	}
	original := &record{Nested: &nested{Values: []string{"before"}}, Items: map[string][]int{"x": {1}}}
	clone := CloneRecord(original)
	clone.Nested.Values[0] = "after"
	clone.Items["x"][0] = 2
	if original.Nested.Values[0] != "before" || original.Items["x"][0] != 1 {
		t.Fatal("clone shares nested mutable state with source")
	}
	if CloneRecord[record](nil) != nil {
		t.Fatal("nil clone is not nil")
	}
}
