package state

import (
	"testing"

	"github.com/marcgauthier/murmur/codec"
	"github.com/marcgauthier/murmur/ids"
)

func TestMemberAdmissionLifecycle(t *testing.T) {
	s := openTestStore(t, ids.NewNodeID())
	peer := ids.NewNodeID()

	rec, err := s.GetMember(peer)
	if err != nil || rec.Status != MemberUnknown {
		t.Fatalf("fresh member = %+v, %v", rec, err)
	}

	admitted, err := s.EnsureMemberAdmitted(peer, 1000, 5000)
	if err != nil || !admitted {
		t.Fatalf("admit = %v, %v", admitted, err)
	}
	rec, err = s.GetMember(peer)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Status != MemberActive || rec.FirstAdmittedAt != 1000 || rec.RetentionDeadline != 6000 {
		t.Fatalf("record = %+v", rec)
	}
	if !rec.Gating(5999) || rec.Gating(6000) {
		t.Fatalf("gating boundary wrong: %+v", rec)
	}

	// Second admission is a no-op: handshakes cannot extend obligations.
	admitted, err = s.EnsureMemberAdmitted(peer, 2000, 5000)
	if err != nil || admitted {
		t.Fatalf("re-admit = %v, %v", admitted, err)
	}
	rec, _ = s.GetMember(peer)
	if rec.FirstAdmittedAt != 1000 || rec.RetentionDeadline != 6000 {
		t.Fatalf("record moved = %+v", rec)
	}
}

func TestMemberAckRenewal(t *testing.T) {
	s := openTestStore(t, ids.NewNodeID())
	peer, origin := ids.NewNodeID(), ids.NewNodeID()

	if _, err := s.EnsureMemberAdmitted(peer, 1000, 5000); err != nil {
		t.Fatal(err)
	}
	advanced, err := s.AdvanceMemberAck(peer, origin, 3, 2000, 5000)
	if err != nil || !advanced {
		t.Fatalf("advance = %v, %v", advanced, err)
	}
	rec, _ := s.GetMember(peer)
	if rec.LastProgressAt != 2000 || rec.RetentionDeadline != 7000 {
		t.Fatalf("record = %+v", rec)
	}
	if ack, _ := s.PeerAck(peer, origin); ack != 3 {
		t.Fatalf("ack = %d", ack)
	}

	// Repeated and stale acks renew nothing.
	for _, seq := range []uint64{3, 2} {
		advanced, err := s.AdvanceMemberAck(peer, origin, seq, 3000, 5000)
		if err != nil || advanced {
			t.Fatalf("seq %d: advanced = %v, %v", seq, advanced, err)
		}
	}
	rec, _ = s.GetMember(peer)
	if rec.LastProgressAt != 2000 || rec.RetentionDeadline != 7000 {
		t.Fatalf("record moved on stale ack = %+v", rec)
	}

	// Higher progress renews from the advance time.
	if advanced, err := s.AdvanceMemberAck(peer, origin, 4, 6500, 5000); err != nil || !advanced {
		t.Fatalf("advance = %v, %v", advanced, err)
	}
	rec, _ = s.GetMember(peer)
	if rec.LastProgressAt != 6500 || rec.RetentionDeadline != 11500 {
		t.Fatalf("record = %+v", rec)
	}
}

func TestMemberRetireAndReadmit(t *testing.T) {
	s := openTestStore(t, ids.NewNodeID())
	peer, origin := ids.NewNodeID(), ids.NewNodeID()

	if _, err := s.EnsureMemberAdmitted(peer, 1000, 5000); err != nil {
		t.Fatal(err)
	}
	if _, err := s.AdvanceMemberAck(peer, origin, 9, 2000, 5000); err != nil {
		t.Fatal(err)
	}
	if err := s.RetireMember(peer); err != nil {
		t.Fatal(err)
	}
	rec, _ := s.GetMember(peer)
	if rec.Status != MemberRetired || rec.Gating(2000) {
		t.Fatalf("retired record = %+v", rec)
	}

	// Handshake traffic cannot resurrect a retired member.
	if admitted, err := s.EnsureMemberAdmitted(peer, 3000, 5000); err != nil || admitted {
		t.Fatalf("re-admit retired = %v, %v", admitted, err)
	}
	// A racing ack still records progress but renews nothing.
	if advanced, err := s.AdvanceMemberAck(peer, origin, 10, 3000, 5000); err != nil || !advanced {
		t.Fatalf("retired advance = %v, %v", advanced, err)
	}
	rec, _ = s.GetMember(peer)
	if rec.Status != MemberRetired || rec.RetentionDeadline != 7000 {
		t.Fatalf("retired record moved = %+v", rec)
	}

	// Readmission clears the record; the next handshake starts fresh.
	if err := s.ReadmitMember(peer); err != nil {
		t.Fatal(err)
	}
	if rec, _ := s.GetMember(peer); rec.Status != MemberUnknown {
		t.Fatalf("readmitted = %+v", rec)
	}
	if admitted, err := s.EnsureMemberAdmitted(peer, 4000, 5000); err != nil || !admitted {
		t.Fatalf("admit after readmit = %v, %v", admitted, err)
	}
	rec, _ = s.GetMember(peer)
	if rec.FirstAdmittedAt != 4000 || rec.RetentionDeadline != 9000 {
		t.Fatalf("record = %+v", rec)
	}
}

func TestMemberListMergesExclusions(t *testing.T) {
	s := openTestStore(t, ids.NewNodeID())
	active, retired, excludedOnly := ids.NewNodeID(), ids.NewNodeID(), ids.NewNodeID()

	if _, err := s.EnsureMemberAdmitted(active, 1000, 5000); err != nil {
		t.Fatal(err)
	}
	if _, err := s.EnsureMemberAdmitted(retired, 1000, 5000); err != nil {
		t.Fatal(err)
	}
	if err := s.RetireMember(retired); err != nil {
		t.Fatal(err)
	}
	if err := s.SetPeerExcluded(excludedOnly, true); err != nil {
		t.Fatal(err)
	}
	if err := s.SetPeerExcluded(active, true); err != nil {
		t.Fatal(err)
	}

	members, err := s.ListMembers()
	if err != nil {
		t.Fatal(err)
	}
	byID := make(map[ids.NodeID]MemberRecord)
	for _, mb := range members {
		byID[mb.NodeID] = mb
	}
	if len(byID) != 3 {
		t.Fatalf("members = %d, want 3", len(byID))
	}
	if r := byID[active]; r.Status != MemberActive || !r.Excluded || r.Gating(2000) {
		t.Fatalf("active+excluded = %+v", r)
	}
	if r := byID[retired]; r.Status != MemberRetired || r.Excluded || r.Gating(2000) {
		t.Fatalf("retired = %+v", r)
	}
	if r := byID[excludedOnly]; r.Status != MemberRetired || !r.Excluded {
		t.Fatalf("excluded-only = %+v", r)
	}
}

func TestMemberPersistenceAcrossReopen(t *testing.T) {
	path := t.TempDir()
	node := ids.NewNodeID()
	s, err := Open(path, node, ids.DBID{}, Options{Limits: codec.DefaultLimits()})
	if err != nil {
		t.Fatal(err)
	}
	peer, origin := ids.NewNodeID(), ids.NewNodeID()
	if _, err := s.EnsureMemberAdmitted(peer, 1000, 5000); err != nil {
		t.Fatal(err)
	}
	if _, err := s.AdvanceMemberAck(peer, origin, 7, 2000, 5000); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := Open(path, node, ids.DBID{}, Options{Limits: codec.DefaultLimits()})
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	rec, err := s2.GetMember(peer)
	if err != nil {
		t.Fatal(err)
	}
	// Restart preserves the remaining obligation; nothing is renewed.
	if rec.Status != MemberActive || rec.FirstAdmittedAt != 1000 ||
		rec.LastProgressAt != 2000 || rec.RetentionDeadline != 7000 {
		t.Fatalf("reopened = %+v", rec)
	}
	if ack, _ := s2.PeerAck(peer, origin); ack != 7 {
		t.Fatalf("reopened ack = %d", ack)
	}
	advancers, err := s2.PeersWithAcks()
	if err != nil || len(advancers) != 1 || advancers[0] != peer {
		t.Fatalf("peers with acks = %v, %v", advancers, err)
	}
}
