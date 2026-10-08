package bridge

import (
	"context"
	"math/rand"
	"reflect"
	"testing"

	db "github.com/marcgauthier/murmur"
	"github.com/marcgauthier/murmur/codec"
	"github.com/marcgauthier/murmur/ids"
)

// TestBridgeOwnershipPermutationConvergence proves that for any randomized delivery schedule
// of Low stream updates, High node field takeovers, duplicate replay bundles, and explicit ownership releases:
// 1. High ownership holds are never corrupted or overwritten by Low imports.
// 2. Unheld fields consistently apply Low imports.
// 3. Releasing ownership cleanly restores Low authority.
// 4. Duplicate replays are strictly idempotent across all field policies.
func TestBridgeOwnershipPermutationConvergence(t *testing.T) {
	ctx := context.Background()

	type replicaOutcome struct {
		name       string
		score      int64
		fieldOwner uint8
	}

	runSchedule := func(seed int64) replicaOutcome {
		high := openTypedContactDBAt(t, t.TempDir(), db.NewNodeID())
		defer high.Close()

		signer, recipient, trust := inboxKeys(t, "owned-stream")
		inboxDir := t.TempDir()
		inbox, err := OpenInbox(inboxDir, trust, Limits{}.withDefaults())
		if err != nil {
			t.Fatal(err)
		}
		importer, err := NewImporter(high)
		if err != nil {
			t.Fatal(err)
		}

		row := ids.NewRowID()

		// Prepare 4 Low bundles as Artifacts
		bundles := make([]Artifact, 4)
		bundles[0] = sealTypedContactsForInbox(t, signer, recipient, "owned-stream", 1, []Batch{putBatch(1, row,
			ColumnValue{Column: "name", Value: codec.Text("low-name-1")},
			ColumnValue{Column: "score", Value: codec.Int(10)},
		)})
		bundles[1] = sealTypedContactsForInbox(t, signer, recipient, "owned-stream", 2, []Batch{putBatch(2, row,
			ColumnValue{Column: "name", Value: codec.Text("low-name-2")},
			ColumnValue{Column: "score", Value: codec.Int(20)},
		)})
		bundles[2] = sealTypedContactsForInbox(t, signer, recipient, "owned-stream", 3, []Batch{putBatch(3, row,
			ColumnValue{Column: "name", Value: codec.Text("low-name-3")},
			ColumnValue{Column: "score", Value: codec.Int(30)},
		)})
		bundles[3] = sealTypedContactsForInbox(t, signer, recipient, "owned-stream", 4, []Batch{putBatch(4, row,
			ColumnValue{Column: "name", Value: codec.Text("low-name-4")},
			ColumnValue{Column: "score", Value: codec.Int(40)},
		)})

		// Step 1: Import bundle 0 (establishing Low baseline)
		if err := inbox.Receive(bundles[0]); err != nil {
			t.Fatal(err)
		}
		if _, err := importer.Drain(ctx, inbox); err != nil {
			t.Fatal(err)
		}

		// Step 2: High takes ownership of "name"
		typedContactSetName(t, high, row, "high-authority-name")

		// Step 3: Deliver bundles 1 and 2 with potential duplicate replays
		rng := rand.New(rand.NewSource(seed))
		schedule := []int{1, 2}
		if rng.Float32() < 0.5 {
			// Replay bundle 0 or 1
			schedule = append(schedule, rng.Intn(2))
		}
		for _, bIdx := range schedule {
			if err := inbox.Receive(bundles[bIdx]); err != nil {
				t.Fatalf("seed %d: inbox.Receive failed: %v", seed, err)
			}
			if _, err := importer.Drain(ctx, inbox); err != nil {
				t.Fatal(err)
			}
		}

		// Verify "name" is still High and "score" was updated by Low (score = 30)
		mid, found := typedContactValue(t, high, row)
		if !found || mid.Name == nil || mid.Score == nil {
			t.Fatal("typed row missing during ownership check")
		}
		midName, midScore := *mid.Name, *mid.Score
		if midName != "high-authority-name" || midScore != 30 {
			t.Fatalf("seed %d: high ownership violated mid-run: name=%q, score=%d", seed, midName, midScore)
		}

		// Step 4: High explicitly releases ownership of "name"
		if err := high.ReleaseBridgeOwnership(ctx, "contacts", row, "Name"); err != nil {
			t.Fatal(err)
		}

		// Step 5: Deliver bundle 3 (which updates name and score)
		if err := inbox.Receive(bundles[3]); err != nil {
			t.Fatal(err)
		}
		if _, err := importer.Drain(ctx, inbox); err != nil {
			t.Fatal(err)
		}

		// Step 6: Replay random previous bundles (0..3) to verify idempotence after release
		for i := 0; i < 3; i++ {
			prevIdx := rng.Intn(4)
			_ = inbox.Receive(bundles[prevIdx])
			if _, err := importer.Drain(ctx, inbox); err != nil {
				t.Fatal(err)
			}
		}

		final, found := typedContactValue(t, high, row)
		if !found || final.Name == nil || final.Score == nil {
			t.Fatal("typed row missing after ownership schedule")
		}
		finalName, finalScore := *final.Name, *final.Score
		fieldPolicy, ok, err := high.BridgeFieldProvenance("contacts", row, "Name")
		if err != nil || !ok {
			t.Fatalf("seed %d: failed reading provenance: %v", seed, err)
		}

		return replicaOutcome{
			name:       finalName,
			score:      finalScore,
			fieldOwner: uint8(fieldPolicy.Owner),
		}
	}

	var canonical replicaOutcome
	for trial := 0; trial < 10; trial++ {
		seed := int64(100 + trial*17)
		res := runSchedule(seed)
		if trial == 0 {
			canonical = res
		} else if !reflect.DeepEqual(canonical, res) {
			t.Fatalf("trial %d (seed %d) diverged: %+v != %+v", trial, seed, res, canonical)
		}

		// Assert expected final state
		if res.name != "low-name-4" {
			t.Fatalf("trial %d: expected name 'low-name-4' after release, got %q", trial, res.name)
		}
		if res.score != 40 {
			t.Fatalf("trial %d: expected score 40, got %d", trial, res.score)
		}
		if res.fieldOwner != uint8(db.BridgeOwnerLow) {
			t.Fatalf("trial %d: expected fieldOwner BridgeOwnerLow, got %v", trial, res.fieldOwner)
		}
	}
}
