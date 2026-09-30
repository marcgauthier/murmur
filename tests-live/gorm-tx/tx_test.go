package gormtx_test

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	murmur "github.com/marcgauthier/murmur/gormmurmur"
	"github.com/marcgauthier/murmur/tests-live/gormharness"
	"gorm.io/gorm"
)

type Widget struct {
	murmur.Model
	Name string
}

// TestGormTransactionsReplicateAtomically proves GORM transactions
// map to real engine transactions end to end: commits converge
// fully on the peer, while failed and rolled-back transactions leave
// zero rows behind locally and never expose partial state remotely
// (autocommit-per-statement would leak exactly that).
func TestGormTransactionsReplicateAtomically(t *testing.T) {
	cluster := gormharness.NewCluster(t, "gorm-tx", 2, &Widget{})

	// Commit path: one transaction, five rows, full convergence.
	err := cluster.Nodes[0].GDB.Transaction(func(tx *gorm.DB) error {
		for i := 0; i < 5; i++ {
			if err := tx.Create(&Widget{Name: fmt.Sprintf("kept-%d", i)}).Error; err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	cluster.WaitConverged(&Widget{}, 5, "name", 30*time.Second)

	// Error midway: the second insert collides with the first, so the
	// whole transaction must vanish everywhere.
	dup := murmur.NewID()
	err = cluster.Nodes[0].GDB.Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(&Widget{Model: murmur.Model{ID: dup}, Name: "partial"}).Error; err != nil {
			return err
		}
		return tx.Create(&Widget{Model: murmur.Model{ID: dup}, Name: "collision"}).Error
	})
	if err == nil {
		t.Fatal("duplicate-key transaction succeeded, want rollback")
	}
	assertNeverExceeds(t, cluster, 0, 5, 3*time.Second)
	assertNeverExceeds(t, cluster, 1, 5, 3*time.Second)

	// Explicit rollback: same guarantee, both nodes.
	boom := errors.New("boom")
	err = cluster.Nodes[1].GDB.Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(&Widget{Name: "doomed"}).Error; err != nil {
			return err
		}
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("rollback err = %v, want boom", err)
	}
	assertNeverExceeds(t, cluster, 0, 5, 3*time.Second)
	assertNeverExceeds(t, cluster, 1, 5, 3*time.Second)

	// Concurrent commits from both nodes converge to the union.
	var wg sync.WaitGroup
	for i := range cluster.Nodes {
		wg.Add(1)
		go func(nodeIdx int) {
			defer wg.Done()
			err := cluster.Nodes[nodeIdx].GDB.Transaction(func(tx *gorm.DB) error {
				for j := 0; j < 5; j++ {
					name := fmt.Sprintf("n%d-%d", nodeIdx, j)
					if err := tx.Create(&Widget{Name: name}).Error; err != nil {
						return err
					}
				}
				return nil
			})
			if err != nil {
				t.Errorf("node %d concurrent commit: %v", nodeIdx, err)
			}
		}(i)
	}
	wg.Wait()
	cluster.WaitConverged(&Widget{}, 15, "name", 60*time.Second)
}

// assertNeverExceeds samples the row count for the whole duration:
// any partial replication would show up as a transient excess.
func assertNeverExceeds(t *testing.T, cluster *gormharness.Cluster, idx int, baseline int64, d time.Duration) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if n := cluster.Count(idx, &Widget{}); n != baseline {
			t.Fatalf("node %d count = %d, want steady %d (partial state leaked)", idx, n, baseline)
		}
		time.Sleep(50 * time.Millisecond)
	}
}
