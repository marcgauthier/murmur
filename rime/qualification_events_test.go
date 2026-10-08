package rime_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/marcgauthier/murmur/rime"
)

// Isolate fatal runtime errors and deadlocks so a broken engine cannot strand
// the parent suite. The child inherits race instrumentation from go test -race.
func TestQualificationEvents(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestQualificationEventWorker$", "-test.timeout=15s")
	cmd.Env = append(os.Environ(), "RIME_EVENT_WORKER=1")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("event worker: %v\n%s", err, out)
	}
}

func TestQualificationEventWorker(t *testing.T) {
	if os.Getenv("RIME_EVENT_WORKER") != "1" {
		t.Skip("subprocess only")
	}
	for _, drop := range []bool{false, true} {
		t.Run(fmt.Sprintf("drop=%t", drop), func(t *testing.T) {
			db, tab := openDevices(t, rime.WithEventQueueSize(1), rime.WithEventDropOldest(drop))
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			defer once.Do(func() { close(release) })
			var mu sync.Mutex
			var delivered []string
			tab.OnInserted(func(d *Device) {
				if d.ID == "first" {
					close(entered)
					<-release
				}
				mu.Lock()
				delivered = append(delivered, d.ID)
				mu.Unlock()
			})
			mustSave(t, tab, Device{ID: "first", Hostname: "first"})
			<-entered
			mustSave(t, tab, Device{ID: "second", Hostname: "second"})
			if drop {
				mustSave(t, tab, Device{ID: "third", Hostname: "third"})
				once.Do(func() { close(release) })
				db.Close()
				mu.Lock()
				got := append([]string(nil), delivered...)
				mu.Unlock()
				if !reflect.DeepEqual(got, []string{"first", "third"}) {
					t.Fatalf("drop oldest: %v", got)
				}
				return
			}
			published := make(chan struct{})
			commitDone := make(chan error, 1)
			tab.AfterSave(func(ch rime.Change[Device]) {
				if ch.New.ID == "third" {
					close(published)
				}
			})
			go func() { commitDone <- tab.Upsert(&Device{ID: "third", Hostname: "third"}) }()
			<-published
			select {
			case err := <-commitDone:
				t.Fatalf("full queue did not block: %v", err)
			default:
			}
			closeDone := make(chan struct{})
			go func() { db.Close(); close(closeDone) }()
			once.Do(func() { close(release) })
			if err := <-commitDone; err != nil {
				t.Fatal(err)
			}
			<-closeDone
			mu.Lock()
			got := append([]string(nil), delivered...)
			mu.Unlock()
			// Close may win against the blocked event producer after publication.
			if !reflect.DeepEqual(got, []string{"first", "second"}) && !reflect.DeepEqual(got, []string{"first", "second", "third"}) {
				t.Fatalf("shutdown delivery: %v", got)
			}
		})
	}
	for iteration := 0; iteration < 50; iteration++ {
		db, tab := openDevices(t, rime.WithEventQueueSize(2), rime.WithEventDropOldest(iteration%2 == 0))
		tab.OnCommitted(func(rime.Change[Device]) {})
		start := make(chan struct{})
		errs := make(chan error, 8)
		var wg sync.WaitGroup
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				<-start
				errs <- tab.Upsert(&Device{ID: fmt.Sprint(i), Hostname: fmt.Sprint(i)})
			}(i)
		}
		close(start)
		db.Close()
		wg.Wait()
		close(errs)
		for err := range errs {
			if err != nil && !errors.Is(err, rime.ErrDBClosed) {
				t.Fatal(err)
			}
		}
	}
}

func TestQualificationStaleWriter(t *testing.T) {
	db, tab := openDevices(t)
	defer db.Close()
	mustSave(t, tab, Device{ID: "a", Hostname: "a", Status: 1})
	entered, release := make(chan struct{}), make(chan struct{})
	result := make(chan error, 1)
	go func() {
		result <- db.WriteTx(func(tx *rime.Tx) error {
			if _, err := tab.In(tx).Get("a"); err != nil {
				return err
			}
			close(entered)
			<-release
			return tab.In(tx).Update("a", func(d *Device) error { d.Status++; return nil })
		})
	}()
	<-entered
	if err := tab.Update("a", func(d *Device) error { d.Status = 100; return nil }); err != nil {
		t.Fatal(err)
	}
	close(release)
	if err := <-result; !errors.Is(err, rime.ErrConflict) {
		t.Fatalf("stale writer overwrote winner: %v", err)
	}
	row, err := tab.Get("a")
	if err != nil || row.Status != 100 {
		t.Fatalf("lost update: %+v %v", row, err)
	}
}
