package gormsync_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	murmur "github.com/marcgauthier/murmur/gormmurmur"
	"github.com/marcgauthier/murmur/tests-live/gormharness"
)

type Author struct {
	murmur.Model
	Name   string
	Age    int
	Active bool
	Books  []Book `gorm:"foreignKey:AuthorID"`
}

type Book struct {
	murmur.Model
	Title    string
	AuthorID murmur.ID
	Author   Author `gorm:"foreignKey:AuthorID"`
}

// TestGormSyncConverges mirrors three-node-sync through GORM: three
// encrypted, mutually authenticated nodes commit associated rows
// concurrently via the dialect, and every node must converge on
// identical content with working cross-node associations.
func TestGormSyncConverges(t *testing.T) {
	cluster := gormharness.NewCluster(t, "gorm-sync", 3, &Author{}, &Book{})

	const authorsPerNode = 10
	var wg sync.WaitGroup
	for i := range cluster.Nodes {
		wg.Add(1)
		go func(nodeIdx int) {
			defer wg.Done()
			gdb := cluster.Nodes[nodeIdx].GDB
			for row := 0; row < authorsPerNode; row++ {
				a := Author{
					Name:   fmt.Sprintf("node-%d-author-%02d", nodeIdx, row),
					Age:    nodeIdx*100 + row,
					Active: row%2 == 0,
					Books: []Book{
						{Title: fmt.Sprintf("node-%d-row-%02d-a", nodeIdx, row)},
						{Title: fmt.Sprintf("node-%d-row-%02d-b", nodeIdx, row)},
					},
				}
				if err := gdb.Create(&a).Error; err != nil {
					t.Errorf("node %d create: %v", nodeIdx, err)
					return
				}
			}
		}(i)
	}
	wg.Wait()

	cluster.WaitConverged(&Author{}, 3*authorsPerNode, "name", 60*time.Second)
	cluster.WaitConverged(&Book{}, 3*authorsPerNode*2, "title", 60*time.Second)

	// Associations resolve for rows this node never wrote.
	var author Author
	if err := cluster.Nodes[2].GDB.Preload("Books").
		First(&author, "name = ?", "node-0-author-03").Error; err != nil {
		t.Fatalf("cross-node preload: %v", err)
	}
	if len(author.Books) != 2 {
		t.Fatalf("preloaded %d books, want 2", len(author.Books))
	}
	var book Book
	if err := cluster.Nodes[0].GDB.Preload("Author").
		First(&book, "title = ?", "node-1-row-07-b").Error; err != nil {
		t.Fatalf("cross-node belongs-to: %v", err)
	}
	if book.Author.Name != "node-1-author-07" {
		t.Fatalf("belongs-to author = %q", book.Author.Name)
	}
	t.Logf("three GORM nodes converged with working associations")
}
