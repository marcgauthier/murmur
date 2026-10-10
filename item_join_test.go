package murmur

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/rime"
)

type joinAuthor struct {
	ID   ids.RowID `rime:"primary"`
	Name string
}

type joinBook struct {
	ID       ids.RowID `rime:"primary"`
	Title    string
	Author   string
	AuthorID ids.RowID
	Tags     []string
}

func joinDefinitions(t testing.TB) []TableDefinition {
	t.Helper()
	authors, err := Model[joinAuthor]()
	if err != nil {
		t.Fatal(err)
	}
	books, err := Model[joinBook]()
	if err != nil {
		t.Fatal(err)
	}
	return []TableDefinition{authors, books}
}

func seedJoinRows(t testing.TB, db *DB) {
	t.Helper()
	ctx := context.Background()
	ann, bob, cid := ids.NewRowID(), ids.NewRowID(), ids.NewRowID()
	for _, author := range []joinAuthor{
		{ID: ann, Name: "ann"},
		{ID: bob, Name: "bob"},
		{ID: cid, Name: "cid"},
	} {
		if err := db.InsertItem(ctx, &author); err != nil {
			t.Fatalf("InsertItem author: %v", err)
		}
	}
	for _, book := range []joinBook{
		{ID: ids.NewRowID(), Title: "a1", Author: "ann", AuthorID: ann},
		{ID: ids.NewRowID(), Title: "a2", Author: "ann", AuthorID: ann},
		{ID: ids.NewRowID(), Title: "b1", Author: "bob", AuthorID: bob},
	} {
		if err := db.InsertItem(ctx, &book); err != nil {
			t.Fatalf("InsertItem book: %v", err)
		}
	}
}

func joinPairs(t testing.TB, rows []ItemJoinRow) map[string]string {
	t.Helper()
	pairs := make(map[string]string, len(rows))
	for _, row := range rows {
		author, ok := row.Left.(*joinAuthor)
		if !ok {
			t.Fatalf("Left = %T, want *joinAuthor", row.Left)
		}
		book, ok := row.Right.(*joinBook)
		if !ok {
			t.Fatalf("Right = %T, want *joinBook", row.Right)
		}
		pairs[author.Name+"/"+book.Title] = ""
	}
	out := make(map[string]string, len(pairs))
	for pair := range pairs {
		out[pair] = ""
	}
	return out
}

func TestItemDBJoinLive(t *testing.T) {
	db := openItemTestDB(t, joinDefinitions(t)...)
	seedJoinRows(t, db)
	ctx := context.Background()

	inner, err := db.InnerJoin(ctx, joinAuthor{}, "Name", joinBook{}, "Author")
	if err != nil {
		t.Fatalf("DB InnerJoin: %v", err)
	}
	pairs := joinPairs(t, inner)
	for _, want := range []string{"ann/a1", "ann/a2", "bob/b1"} {
		if _, ok := pairs[want]; !ok {
			t.Fatalf("DB InnerJoin pairs = %v, missing %s", pairs, want)
		}
	}
	if len(pairs) != 3 {
		t.Fatalf("DB InnerJoin pairs = %v, want 3", pairs)
	}
	left, err := db.LeftJoin(ctx, joinAuthor{}, "Name", joinBook{}, "Author")
	if err != nil {
		t.Fatalf("DB LeftJoin: %v", err)
	}
	if len(left) != 4 {
		t.Fatalf("DB LeftJoin rows = %d, want 4", len(left))
	}
	matched := 0
	for _, row := range left {
		if row.Right != nil {
			matched++
		}
	}
	if matched != 3 {
		t.Fatalf("DB LeftJoin matched = %d, want 3", matched)
	}
}

func TestItemJoinLive(t *testing.T) {
	db := openItemTestDB(t, joinDefinitions(t)...)
	seedJoinRows(t, db)

	rtx, err := db.readTxContext(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer rtx.Close()

	inner, err := rtx.InnerJoin(joinAuthor{}, "Name", joinBook{}, "Author")
	if err != nil {
		t.Fatalf("InnerJoin: %v", err)
	}
	pairs := joinPairs(t, inner)
	for _, want := range []string{"ann/a1", "ann/a2", "bob/b1"} {
		if _, ok := pairs[want]; !ok {
			t.Fatalf("InnerJoin pairs = %v, missing %s", pairs, want)
		}
	}
	if len(pairs) != 3 {
		t.Fatalf("InnerJoin pairs = %v, want 3", pairs)
	}
	// Identity keys join too.
	byID, err := rtx.InnerJoin(joinAuthor{}, "ID", joinBook{}, "AuthorID")
	if err != nil {
		t.Fatalf("InnerJoin IDs: %v", err)
	}
	if len(joinPairs(t, byID)) != 3 {
		t.Fatalf("InnerJoin IDs returned %d rows, want 3", len(byID))
	}

	outer, err := rtx.LeftJoin(joinAuthor{}, "Name", joinBook{}, "Author")
	if err != nil {
		t.Fatalf("LeftJoin: %v", err)
	}
	if len(outer) != 4 {
		t.Fatalf("LeftJoin returned %d rows, want 4", len(outer))
	}
	unmatched := 0
	for _, row := range outer {
		if row.Right == nil {
			unmatched++
			if row.Left.(*joinAuthor).Name != "cid" {
				t.Fatalf("unmatched Left = %+v, want cid", row.Left)
			}
		}
	}
	if unmatched != 1 {
		t.Fatalf("LeftJoin unmatched = %d, want 1", unmatched)
	}
}

func TestItemJoinEmptyLive(t *testing.T) {
	db := openItemTestDB(t, joinDefinitions(t)...)

	rtx, err := db.readTxContext(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer rtx.Close()

	inner, err := rtx.InnerJoin(joinAuthor{}, "Name", joinBook{}, "Author")
	if err != nil {
		t.Fatalf("InnerJoin: %v", err)
	}
	if len(inner) != 0 {
		t.Fatalf("InnerJoin empty = %d rows, want 0", len(inner))
	}
	outer, err := rtx.LeftJoin(joinAuthor{}, "Name", joinBook{}, "Author")
	if err != nil {
		t.Fatalf("LeftJoin: %v", err)
	}
	if len(outer) != 0 {
		t.Fatalf("LeftJoin empty = %d rows, want 0", len(outer))
	}
}

func TestItemJoinErrorsLive(t *testing.T) {
	db := openItemTestDB(t, joinDefinitions(t)...)
	seedJoinRows(t, db)

	rtx, err := db.readTxContext(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer rtx.Close()

	if _, err := rtx.InnerJoin(joinAuthor{}, "Nope", joinBook{}, "Author"); err == nil || !strings.Contains(err.Error(), `unknown field "Nope"`) {
		t.Fatalf("unknown left field = %v, want field error", err)
	}
	if _, err := rtx.InnerJoin(joinAuthor{}, "Name", joinBook{}, "Nope"); err == nil || !strings.Contains(err.Error(), `unknown field "Nope"`) {
		t.Fatalf("unknown right field = %v, want field error", err)
	}
	if _, err := rtx.InnerJoin(joinAuthor{}, "Name", joinBook{}, "AuthorID"); err == nil || !strings.Contains(err.Error(), "same Go type") {
		t.Fatalf("mismatched keys = %v, want type error", err)
	}
	if _, err := rtx.InnerJoin(joinBook{}, "Tags", joinBook{}, "Tags"); err == nil || !strings.Contains(err.Error(), "cannot compare") {
		t.Fatalf("slice keys = %v, want comparability error", err)
	}
	if _, err := rtx.InnerJoin(joinAuthor{}, "Name", 42, "Author"); err == nil {
		t.Fatal("unregistered model succeeded")
	}

	closed, err := db.readTxContext(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := closed.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := closed.InnerJoin(joinAuthor{}, "Name", joinBook{}, "Author"); !errors.Is(err, rime.ErrTxClosed) {
		t.Fatalf("closed snapshot = %v, want ErrTxClosed", err)
	}
	var nilTx *recordReadTx
	if _, err := nilTx.LeftJoin(joinAuthor{}, "Name", joinBook{}, "Author"); !errors.Is(err, rime.ErrTxClosed) {
		t.Fatalf("nil snapshot = %v, want ErrTxClosed", err)
	}
}
