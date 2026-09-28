package transport

import (
	"errors"
	"testing"

	"github.com/nomadsql/replicateddb/ids"
)

func TestPoolValidation(t *testing.T) {
	// Invalid: Fanout + MaxConcurrentRepairs > MaxReplicationSessions
	_, err := NewPool(PoolOptions{
		MaxConnections:         32,
		ReservedMembership:     8,
		MaxReplicationSessions: 4,
		Fanout:                 3,
		MaxConcurrentRepairs:   2,
	})
	if err == nil {
		t.Fatalf("expected error for Fanout + Repairs > MaxSessions")
	}

	// Invalid: MaxConnections < MaxReplicationSessions + ReservedMembership
	_, err = NewPool(PoolOptions{
		MaxConnections:         15,
		ReservedMembership:     8,
		MaxReplicationSessions: 8,
		Fanout:                 3,
		MaxConcurrentRepairs:   1,
	})
	if err == nil {
		t.Fatalf("expected error for MaxConnections < MaxReplicationSessions + ReservedMembership")
	}

	// Valid
	pool, err := NewPool(PoolOptions{
		MaxConnections:         32,
		ReservedMembership:     8,
		MaxReplicationSessions: 8,
		Fanout:                 3,
		MaxConcurrentRepairs:   1,
	})
	if err != nil {
		t.Fatalf("unexpected error creating pool: %v", err)
	}
	defer pool.Close()

	st := pool.Stats()
	if st.MaxConnections != 32 || st.MaxSessions != 8 {
		t.Fatalf("unexpected stats: %+v", st)
	}
}

func TestPoolInboundAdmission(t *testing.T) {
	pool, err := NewPool(PoolOptions{
		MaxConnections:         16,
		ReservedMembership:     8,
		MaxReplicationSessions: 4,
		Fanout:                 2,
		MaxConcurrentRepairs:   1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	n1 := ids.NewNodeID()
	n2 := ids.NewNodeID()
	n3 := ids.NewNodeID()
	n4 := ids.NewNodeID()
	n5 := ids.NewNodeID()

	// Admit up to unreserved / total capacity
	if err := pool.AdmitInbound(n1, PurposeSelectedTarget); err != nil {
		t.Fatalf("admit n1: %v", err)
	}
	if err := pool.AdmitInbound(n2, PurposeSelectedTarget); err != nil {
		t.Fatalf("admit n2: %v", err)
	}
	if err := pool.AdmitInbound(n3, PurposeRepair); err != nil {
		t.Fatalf("admit n3: %v", err)
	}
	if err := pool.AdmitInbound(n4, PurposeInboundReplication); err != nil {
		t.Fatalf("admit n4: %v", err)
	}

	// 5th session must be rejected (MaxReplicationSessions = 4)
	if err := pool.AdmitInbound(n5, PurposeInboundReplication); !errors.Is(err, ErrSessionCapacityExhausted) {
		t.Fatalf("expected ErrSessionCapacityExhausted, got %v", err)
	}
}

func TestPoolPurposeReservationsAreHardCaps(t *testing.T) {
	pool, err := NewPool(PoolOptions{
		MaxConnections:         16,
		ReservedMembership:     8,
		MaxReplicationSessions: 6,
		Fanout:                 3,
		MaxConcurrentRepairs:   2,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	for i := 0; i < 3; i++ {
		if err := pool.AdmitInbound(ids.NewNodeID(), PurposeSelectedTarget); err != nil {
			t.Fatalf("selected target %d: %v", i, err)
		}
	}
	if err := pool.AdmitInbound(ids.NewNodeID(), PurposeSelectedTarget); !errors.Is(err, ErrSessionCapacityExhausted) {
		t.Fatalf("fourth selected target should respect fanout=3, got %v", err)
	}
	for i := 0; i < 2; i++ {
		if err := pool.AdmitInbound(ids.NewNodeID(), PurposeRepair); err != nil {
			t.Fatalf("repair %d: %v", i, err)
		}
	}
	if err := pool.AdmitInbound(ids.NewNodeID(), PurposeRepair); !errors.Is(err, ErrSessionCapacityExhausted) {
		t.Fatalf("third repair should respect max repairs=2, got %v", err)
	}
	if got := pool.Stats(); got.SelectedTargets != 3 || got.ActiveRepairs != 2 {
		t.Fatalf("reserved counts = %+v, want 3 targets and 2 repairs", got)
	}
}

func TestPoolMembershipAdmissionSurvivesBulkSessionSaturation(t *testing.T) {
	pool, err := NewPool(PoolOptions{
		MaxConnections: 16, ReservedMembership: 8,
		MaxReplicationSessions: 8, Fanout: 3, MaxConcurrentRepairs: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	for i := 0; i < 3; i++ {
		if err := pool.AdmitInbound(ids.NewNodeID(), PurposeSelectedTarget); err != nil {
			t.Fatalf("target %d: %v", i, err)
		}
	}
	if err := pool.AdmitInbound(ids.NewNodeID(), PurposeRepair); err != nil {
		t.Fatalf("repair: %v", err)
	}
	for i := 0; i < 4; i++ {
		if err := pool.AdmitInbound(ids.NewNodeID(), PurposeInboundReplication); err != nil {
			t.Fatalf("bulk session %d: %v", i, err)
		}
	}
	if got := pool.Stats(); got.TotalSessions != 8 {
		t.Fatalf("replication sessions = %d, want saturated 8", got.TotalSessions)
	}
	if err := pool.AdmitInbound(ids.NewNodeID(), PurposeMembership); err != nil {
		t.Fatalf("membership admission blocked by bulk sessions: %v", err)
	}
	if got := pool.Stats(); got.TotalSessions != 8 {
		t.Fatalf("membership consumed replication slot: %d", got.TotalSessions-8)
	}
}
