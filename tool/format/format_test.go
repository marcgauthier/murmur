package format

import (
	"bytes"
	"errors"
	"strings"
	"testing"
)

func TestCSV(t *testing.T) {
	var buf bytes.Buffer
	headers := []string{"col1", "col2"}
	rows := [][]string{
		{"a", "b"},
		{"c", "d"},
	}

	if err := RenderCSV(&buf, headers, rows); err != nil {
		t.Fatalf("RenderCSV failed: %v", err)
	}

	res := buf.String()
	if !strings.Contains(res, "col1,col2") || !strings.Contains(res, "a,b") {
		t.Errorf("unexpected CSV output: %s", res)
	}

	// Empty headers
	buf.Reset()
	if err := RenderCSV(&buf, nil, rows); err != nil {
		t.Fatalf("RenderCSV failed: %v", err)
	}
	if strings.Contains(buf.String(), "col1") {
		t.Errorf("unexpected headers in headerless CSV: %s", buf.String())
	}
}

func TestJSON(t *testing.T) {
	var buf bytes.Buffer
	val := map[string]int{"one": 1, "two": 2}

	if err := RenderJSON(&buf, val, true); err != nil {
		t.Fatalf("RenderJSON pretty failed: %v", err)
	}
	if !strings.Contains(buf.String(), "{\n  \"one\": 1") {
		t.Errorf("unexpected pretty JSON: %s", buf.String())
	}

	buf.Reset()
	if err := RenderJSON(&buf, val, false); err != nil {
		t.Fatalf("RenderJSON compact failed: %v", err)
	}
	if !strings.Contains(buf.String(), `{"one":1,"two":2}`) {
		t.Errorf("unexpected compact JSON: %s", buf.String())
	}

	// Error JSON
	buf.Reset()
	RenderJSONError(&buf, errors.New("sample error"))
	if !strings.Contains(buf.String(), `"message": "sample error"`) {
		t.Errorf("unexpected JSON error format: %s", buf.String())
	}
}

func TestTable(t *testing.T) {
	var buf bytes.Buffer

	// Empty table
	RenderTable(&buf, nil, nil, false)
	if buf.Len() != 0 {
		t.Errorf("expected empty buffer for empty headers, got: %s", buf.String())
	}

	// ASCII Table
	headers := []string{"Key", "Value"}
	rows := [][]string{
		{"foo", "bar"},
		{"longer_key", "val"},
	}
	RenderTable(&buf, headers, rows, false)
	if !strings.Contains(buf.String(), "+------------+-------+") {
		t.Errorf("unexpected ASCII border: %s", buf.String())
	}

	// Markdown Table with short row
	buf.Reset()
	RenderTable(&buf, headers, [][]string{{"only_one"}}, true)
	if !strings.Contains(buf.String(), "| Key") || !strings.Contains(buf.String(), "| only_one") {
		t.Errorf("unexpected Markdown table: %s", buf.String())
	}

	// RenderKV
	buf.Reset()
	pairs := [][2]string{
		{"Alpha", "1"},
		{"", ""},
		{"BetaKey", "2"},
	}
	RenderKV(&buf, pairs)
	res := buf.String()
	if !strings.Contains(res, "Alpha   : 1") || !strings.Contains(res, "BetaKey : 2") {
		t.Errorf("unexpected KV output: %s", res)
	}
}
