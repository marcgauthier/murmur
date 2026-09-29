//go:build modernc || sqlite_preupdate_hook

package sqlengine

import (
	"testing"

	"github.com/marcgauthier/spedsql/codec"
)

// TestAnyRowToValues pins the driver-row conversion used by the capture
// backends, including the unsupported-type rejection.
func TestAnyRowToValues(t *testing.T) {
	vals, err := anyRowToValues([]any{nil, int64(-7), 3.5, "txt", []byte("b"), true, false})
	if err != nil {
		t.Fatal(err)
	}
	if len(vals) != 7 {
		t.Fatalf("len = %d, want 7", len(vals))
	}
	if vals[0].Type != codec.TypeNull || vals[1].I != -7 || vals[2].F != 3.5 ||
		vals[3].S != "txt" || string(vals[4].B) != "b" || vals[5].I != 1 || vals[6].I != 0 {
		t.Fatalf("converted = %+v", vals)
	}
	if _, err := anyRowToValues([]any{struct{}{}}); err == nil {
		t.Fatal("anyRowToValues(struct) = nil, want error")
	}
}
