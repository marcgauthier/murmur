package main

import (
	"context"
	"errors"
	"fmt"
	"math"

	murmur "github.com/marcgauthier/murmur/clients/go"
	"github.com/marcgauthier/murmur/clients/go/examples/internal/demo"
)

func run(ctx context.Context, e *demo.Env) error {
	db, err := e.Open("db", demo.Options())
	if err != nil {
		return err
	}
	if err = db.Migrate(murmur.NewMigration("CREATE TABLE docs(id INTEGER PRIMARY KEY,title TEXT,n INTEGER,r REAL,b BLOB,optional TEXT)")); err != nil {
		return err
	}
	err = db.Transaction(func(s *murmur.Sql) error {
		stmt, err := s.Prepare("INSERT INTO docs VALUES(?,?,?,?,?,?)")
		if err != nil {
			return err
		}
		defer stmt.Close()
		result, err := stmt.Exec(1, "Hello\x00Murmur", int64(math.MaxInt64), math.Inf(1), []byte{}, nil)
		if err == nil {
			e.Print("Inserted %d row; last row ID %d", result.RowsAffected, result.LastInsertRowID)
		}
		return err
	})
	if err != nil {
		return err
	}
	rollback := errors.New("cancel this transaction")
	err = db.Transaction(func(s *murmur.Sql) error {
		if _, err := s.Exec("DELETE FROM docs"); err != nil {
			return err
		}
		return rollback
	})
	if !errors.Is(err, rollback) {
		return fmt.Errorf("rollback: %w", err)
	}
	err = db.Read(func(s *murmur.Sql) error {
		rows, err := s.Query("SELECT title,n,r,b,optional FROM docs ORDER BY id")
		if err != nil {
			return err
		}
		defer rows.Close()
		if !rows.Next() {
			return fmt.Errorf("missing row: %v", rows.Err())
		}
		var title string
		var n int64
		var real float64
		var blob []byte
		var nullable murmur.Value
		if err = rows.Scan(&title, &n, &real, &blob, &nullable); err != nil {
			return err
		}
		if err = demo.Require(title == "Hello\x00Murmur" && n == math.MaxInt64 && math.IsInf(real, 1) && blob != nil && nullable.Kind == murmur.NullKind, "typed values changed"); err != nil {
			return err
		}
		e.Print("Read int64, infinity, NUL text, empty BLOB, and NULL; rollback preserved the row")
		return rows.Err()
	})
	if err != nil {
		return err
	}
	clone, err := db.Clone()
	if err != nil {
		return err
	}
	if err = clone.Release(); err != nil {
		return err
	}
	if err = db.Flush(); err != nil {
		return err
	}
	if err = db.Close(); err != nil {
		return err
	}
	reopened, err := e.Open("db", murmur.DefaultOptions())
	if err != nil {
		return err
	}
	n, err := demo.Int(reopened, "SELECT count(*) FROM docs")
	if err != nil {
		return err
	}
	e.Print("Reopened durable database: %d row", n)
	return demo.Require(n == 1, "recovery lost row")
}
func main() { demo.Main("basic", run) }
