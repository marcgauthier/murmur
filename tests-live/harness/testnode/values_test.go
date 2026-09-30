package main

import (
	"reflect"
	"testing"
)

func TestValuesRoundTrip(t *testing.T) {
	rows := [][]any{
		{nil, int64(-42), 3.5, "hi", []byte{0x00, 0xff}, true},
		{int(7), int32(-8), uint16(9), "x"},
	}
	enc, err := MarshalRows(rows)
	if err != nil {
		t.Fatalf("MarshalRows: %v", err)
	}
	if len(enc) != 2 || len(enc[0]) != 6 || len(enc[1]) != 4 {
		t.Fatalf("unexpected shape: %#v", enc)
	}
	if enc[0][0] != Null || enc[0][1].I != -42 || enc[0][3].S != "hi" || !enc[0][5].Bool {
		t.Fatalf("unexpected encoding: %#v", enc[0])
	}
	if enc[0][4].Type != "blob" || enc[0][4].B == "" {
		t.Fatalf("blob not base64-encoded: %#v", enc[0][4])
	}
	back, err := ValuesToAny(enc[0])
	if err != nil {
		t.Fatalf("ValuesToAny: %v", err)
	}
	if !reflect.DeepEqual(back, rows[0]) {
		t.Fatalf("round trip mismatch: %#v vs %#v", back, rows[0])
	}
}

func TestValuesRejectsUnsupported(t *testing.T) {
	if _, err := ToValue(struct{}{}); err == nil {
		t.Fatal("expected error for unsupported type")
	}
	if _, err := DecodeJSONArgs("not-json"); err == nil {
		t.Fatal("expected error for invalid args JSON")
	}
	if _, err := (Value{Type: "blob", B: "%%%"}).Any(); err == nil {
		t.Fatal("expected error for bad blob encoding")
	}
	if _, err := (Value{Type: "nope"}).Any(); err == nil {
		t.Fatal("expected error for unknown type")
	}
	if _, err := uintValue(1 << 63); err == nil {
		t.Fatal("expected error for uint64 overflow")
	}
}
