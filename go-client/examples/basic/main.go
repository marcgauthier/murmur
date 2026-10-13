// Run with: ./build.sh run ./examples/basic /path/to/new/database
package main

import (
	"fmt"
	"log"
	"os"

	murmur "github.com/marcgauthier/murmur/go-client"
)

func run() error {
	if len(os.Args) != 2 {
		return fmt.Errorf("usage: basic /path/to/new/database")
	}
	// Demonstration key only. Supply a secret 32-byte key in applications.
	key := make([]byte, 32)
	for i := range key {
		key[i] = 7
	}
	opts := murmur.DefaultOptions()
	opts.Mode = murmur.CreateNew
	db, err := murmur.Open(os.Args[1], key, opts)
	if err != nil {
		return err
	}
	defer db.Close()
	if err = db.Migrate(murmur.NewMigration("CREATE TABLE docs(id INTEGER PRIMARY KEY,title TEXT)")); err != nil {
		return err
	}
	if _, err = db.Exec("INSERT INTO docs VALUES(?,?)", 1, "Hello Murmur"); err != nil {
		return err
	}
	if err = db.SetSearchIndexes([]murmur.SearchIndex{murmur.FTS5Index("docs_fts", "docs", "title")}); err != nil {
		return err
	}
	if err = db.RefreshIndexes(); err != nil {
		return err
	}
	err = db.Read(func(sql *murmur.Sql) error {
		var title string
		if err := sql.QueryRow("SELECT title FROM docs_fts WHERE docs_fts MATCH ?", "hello").Scan(&title); err != nil {
			return err
		}
		fmt.Println(title)
		return nil
	})
	if err != nil {
		return err
	}
	if err = db.Flush(); err != nil {
		return err
	}
	return db.Close()
}
func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}
