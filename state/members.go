package state

import (
	"fmt"

	"github.com/cockroachdb/pebble/v2"

	"github.com/marcgauthier/murmur/ids"
)

// Member admission, acknowledgement-progress retention deadlines, and
// retirement (architecture/snapshots-backup-and-restore.md section 36).
//
// A member is admitted on first successful authenticated handshake. The
// record carries the first-admission time, the last durable ack-progress
// time, and a retention deadline. The deadline starts at admission plus the
// offline retention window and is renewed only when a committed replication
// acknowledgement advances the member's durable per-origin progress;
// connection activity and repeated unchanged acknowledgements do not renew
// it. Log GC gates on these persisted deadlines, never on volatile session
// state, so restart preserves the remaining obligation instead of granting
// a new window.
//
// Retirement is explicit and persists alongside the exclusion flag owned by
// RemovePeer: a retired member holds no retention obligation and cannot be
// re-admitted by handshake traffic. AddPeer clears both and the next
// successful authentication starts a fresh admission.

// MemberStatus is the persisted membership state of one node.
type MemberStatus uint8

const (
	// MemberUnknown means no admission record exists.
	MemberUnknown MemberStatus = iota
	// MemberActive holds a retention obligation until its deadline.
	MemberActive
	// MemberRetired was explicitly retired; it holds no obligation.
	MemberRetired
)

// MemberRecord is one node's persisted membership state. Times are unix
// millis; zero means never.
type MemberRecord struct {
	NodeID            ids.NodeID
	Status            MemberStatus
	FirstAdmittedAt   int64
	LastProgressAt    int64
	RetentionDeadline int64
	// Excluded mirrors the persistent exclusion flag: excluded members
	// hold no obligation and refuse sessions.
	Excluded bool
}

// Gating reports whether the member's retention obligation is live at now.
func (r MemberRecord) Gating(nowMillis int64) bool {
	return r.Status == MemberActive && !r.Excluded && r.RetentionDeadline > nowMillis
}

const memberRecordLen = 1 + 8*3

func encodeMemberRecord(status MemberStatus, admitted, progress, deadline int64) []byte {
	b := make([]byte, 0, memberRecordLen)
	b = append(b, byte(status))
	b = append(b, encodeU64(uint64(admitted))...)
	b = append(b, encodeU64(uint64(progress))...)
	return append(b, encodeU64(uint64(deadline))...)
}

func decodeMemberRecord(raw []byte) (MemberStatus, int64, int64, int64, error) {
	if len(raw) != memberRecordLen {
		return MemberUnknown, 0, 0, 0, fmt.Errorf("state: corrupt member record (len %d)", len(raw))
	}
	status := MemberStatus(raw[0])
	if status != MemberActive && status != MemberRetired {
		return MemberUnknown, 0, 0, 0, fmt.Errorf("state: corrupt member status %d", raw[0])
	}
	admitted, ok1 := decodeU64(raw[1:9])
	progress, ok2 := decodeU64(raw[9:17])
	deadline, ok3 := decodeU64(raw[17:25])
	if !ok1 || !ok2 || !ok3 {
		return MemberUnknown, 0, 0, 0, fmt.Errorf("state: corrupt member record")
	}
	return status, int64(admitted), int64(progress), int64(deadline), nil
}

// GetMember returns the node's membership record, or a zero
// MemberUnknown record when none exists.
func (s *Store) GetMember(node ids.NodeID) (MemberRecord, error) {
	s.gate.RLock()
	defer s.gate.RUnlock()
	rec := MemberRecord{NodeID: node, Status: MemberUnknown}
	raw, err := s.getDirect(MemberKey(node))
	if err != nil {
		if !isNotFound(err) {
			return rec, err
		}
	} else if status, admitted, progress, deadline, derr := decodeMemberRecord(raw); derr != nil {
		return rec, derr
	} else {
		rec.Status, rec.FirstAdmittedAt, rec.LastProgressAt, rec.RetentionDeadline =
			status, admitted, progress, deadline
	}
	excluded, err := s.isPeerExcludedLocked(PeerExcludedKey(node))
	if err != nil {
		return rec, err
	}
	rec.Excluded = excluded
	return rec, nil
}

func (s *Store) isPeerExcludedLocked(key []byte) (bool, error) {
	if err := s.failedErr(); err != nil {
		return false, err
	}
	_, closer, err := s.db.Get(key)
	if err != nil {
		if isNotFound(err) {
			return false, nil
		}
		return false, err
	}
	_ = closer.Close()
	return true, nil
}

// EnsureMemberAdmitted records first admission when no record exists,
// setting the retention deadline to nowMillis+retentionMillis. It reports
// whether a new admission was written. Retired members are never
// re-admitted (AddPeer must readmit first); active members are untouched,
// so handshake traffic cannot extend an existing obligation.
func (s *Store) EnsureMemberAdmitted(node ids.NodeID, nowMillis int64, retentionMillis int64) (bool, error) {
	s.gate.RLock()
	defer s.gate.RUnlock()
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	raw, err := s.getDirect(MemberKey(node))
	if err == nil {
		status, _, _, _, derr := decodeMemberRecord(raw)
		if derr != nil {
			return false, derr
		}
		if status == MemberRetired {
			return false, nil
		}
		return false, nil
	}
	if !isNotFound(err) {
		return false, err
	}
	b := s.db.NewBatch()
	defer b.Close()
	if err := b.Set(MemberKey(node), encodeMemberRecord(MemberActive, nowMillis, 0, nowMillis+retentionMillis), nil); err != nil {
		return false, err
	}
	if err := s.commitBatch(b, s.writeOpts); err != nil {
		return false, err
	}
	return true, nil
}

// AdvanceMemberAck records a durable per-origin acknowledgement, renewing
// the member's retention deadline only when seq advances the persisted
// progress. Repeated or stale acknowledgements change nothing. Members
// without a record (pre-upgrade durable progress) are admitted first so a
// restart never silently drops a live obligation.
func (s *Store) AdvanceMemberAck(node, origin ids.NodeID, seq uint64, nowMillis int64, retentionMillis int64) (bool, error) {
	s.gate.RLock()
	defer s.gate.RUnlock()
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	raw, err := s.getDirect(PeerAckKey(node, origin))
	if err == nil {
		cur, ok := decodeU64(raw)
		if !ok {
			return false, fmt.Errorf("state: corrupt peer ack")
		}
		if cur >= seq {
			return false, nil
		}
	} else if !isNotFound(err) {
		return false, err
	}
	mraw, err := s.getDirect(MemberKey(node))
	if err != nil && !isNotFound(err) {
		return false, err
	}
	b := s.db.NewBatch()
	defer b.Close()
	if err := b.Set(PeerAckKey(node, origin), encodeU64(seq), nil); err != nil {
		return false, err
	}
	if isNotFound(err) {
		// Legacy durable progress without an admission record: admit
		// now so the obligation is explicit and restart-stable.
		if err := b.Set(MemberKey(node), encodeMemberRecord(MemberActive, nowMillis, nowMillis, nowMillis+retentionMillis), nil); err != nil {
			return false, err
		}
	} else if status, admitted, _, _, derr := decodeMemberRecord(mraw); derr != nil {
		return false, derr
	} else if status == MemberActive {
		if err := b.Set(MemberKey(node), encodeMemberRecord(MemberActive, admitted, nowMillis, nowMillis+retentionMillis), nil); err != nil {
			return false, err
		}
	}
	// Retired records keep their timestamps: progress on a retired member
	// (a racing acknowledgement) must not resurrect its obligation.
	if err := s.commitBatch(b, s.writeOpts); err != nil {
		return false, err
	}
	return true, nil
}

// RetireMember marks the member retired, releasing its retention
// obligation. Timestamps are kept for audit. Use ReadmitMember to return
// the node to the admission cycle; handshake traffic alone cannot.
func (s *Store) RetireMember(node ids.NodeID) error {
	s.gate.RLock()
	defer s.gate.RUnlock()
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	raw, err := s.getDirect(MemberKey(node))
	admitted, progress, deadline := int64(0), int64(0), int64(0)
	if err == nil {
		_, admitted, progress, deadline, err = decodeMemberRecord(raw)
		if err != nil {
			return err
		}
	} else if !isNotFound(err) {
		return err
	}
	b := s.db.NewBatch()
	defer b.Close()
	if err := b.Set(MemberKey(node), encodeMemberRecord(MemberRetired, admitted, progress, deadline), nil); err != nil {
		return err
	}
	return s.commitBatch(b, pebble.Sync)
}

// ReadmitMember deletes the admission record so the next successful
// authentication starts a fresh obligation. It does not touch the
// exclusion flag (owned by RemovePeer/AddPeer exclusion handling).
func (s *Store) ReadmitMember(node ids.NodeID) error {
	s.gate.RLock()
	defer s.gate.RUnlock()
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	b := s.db.NewBatch()
	defer b.Close()
	if err := b.Delete(MemberKey(node), nil); err != nil {
		return err
	}
	return s.commitBatch(b, pebble.Sync)
}

// ListMembers returns every admission record merged with the persistent
// exclusion set. Excluded nodes without a record surface as retired and
// excluded so GC treats all exclusions uniformly.
func (s *Store) ListMembers() ([]MemberRecord, error) {
	s.gate.RLock()
	defer s.gate.RUnlock()
	if err := s.failedErr(); err != nil {
		return nil, err
	}
	byID := make(map[ids.NodeID]*MemberRecord)
	collect := func(prefix []byte, parse func(k []byte) (ids.NodeID, bool)) error {
		iter, err := s.db.NewIter(&pebble.IterOptions{
			LowerBound: prefix,
			UpperBound: prefixEnd(prefix),
		})
		if err != nil {
			return err
		}
		defer iter.Close()
		for iter.First(); iter.Valid(); iter.Next() {
			id, ok := parse(iter.Key())
			if !ok {
				continue
			}
			if _, seen := byID[id]; !seen {
				byID[id] = &MemberRecord{NodeID: id, Status: MemberUnknown}
			}
			if len(prefix) > 0 && prefix[0] == prefixMember {
				status, admitted, progress, deadline, err := decodeMemberRecord(iter.Value())
				if err != nil {
					return err
				}
				r := byID[id]
				r.Status, r.FirstAdmittedAt, r.LastProgressAt, r.RetentionDeadline =
					status, admitted, progress, deadline
			} else {
				byID[id].Excluded = true
			}
		}
		return iter.Error()
	}
	if err := collect(MemberPrefix(), ParseMemberKey); err != nil {
		return nil, err
	}
	if err := collect(PeerExcludedPrefix(), ParsePeerExcludedKey); err != nil {
		return nil, err
	}
	out := make([]MemberRecord, 0, len(byID))
	for _, r := range byID {
		if r.Status == MemberUnknown && r.Excluded {
			r.Status = MemberRetired
		}
		out = append(out, *r)
	}
	return out, nil
}

// PeersWithAcks lists distinct peers holding any durable per-origin
// acknowledgement. GC uses it to grant pre-upgrade progress a first
// explicit retention window instead of silently dropping its obligation.
func (s *Store) PeersWithAcks() ([]ids.NodeID, error) {
	s.gate.RLock()
	defer s.gate.RUnlock()
	if err := s.failedErr(); err != nil {
		return nil, err
	}
	prefix := PeerAckPrefix()
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: prefixEnd(prefix),
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	var out []ids.NodeID
	seen := make(map[ids.NodeID]bool)
	for iter.First(); iter.Valid(); iter.Next() {
		peer, _, ok := ParsePeerAckKey(iter.Key())
		if !ok || seen[peer] {
			continue
		}
		seen[peer] = true
		out = append(out, peer)
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}
	return out, nil
}
