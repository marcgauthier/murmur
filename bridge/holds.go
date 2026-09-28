package bridge

import (
	"encoding/hex"
	"fmt"
	"sort"
)

// SchemaHold records one bundle held for a compatible High schema. Holds
// are durable, ordered, and retried automatically: they never advance
// progress, never partially apply, and never trigger DDL — only an
// administrator's local migration releases them.
type SchemaHold struct {
	Kind          string
	Reason        string
	Resolution    string
	BundleID      string
	First         uint64
	Last          uint64
	RequiredEpoch uint64
	RequiredHash  string
	Missing       []string
}

// HoldError reports schema incompatibility with the missing objects.
type HoldError struct {
	Missing []string
}

func (e *HoldError) Error() string {
	return "bridge: waiting-schema: missing " + joinStrings(e.Missing)
}

func joinStrings(ss []string) string {
	out := ""
	for i, s := range ss {
		if i > 0 {
			out += ", "
		}
		out += s
	}
	return out
}

// Hold parks the staged bundle starting at first in waiting-schema state.
// Later sequences stay staged behind it; bytes stay in staging for retry.
func (in *Inbox) Hold(stream string, first uint64, bundleID string, last, epoch uint64, hash [32]byte, missing []string) error {
	in.mu.Lock()
	defer in.mu.Unlock()
	st, ok := in.streams[stream]
	if !ok {
		return fmt.Errorf("bridge: unknown stream %q", stream)
	}
	if _, ok := st.Staged[first]; !ok {
		return fmt.Errorf("bridge: stream %q seq %d is not staged", stream, first)
	}
	st.Holds[first] = &SchemaHold{
		Kind:     "schema",
		BundleID: bundleID, First: first, Last: last,
		RequiredEpoch: epoch, RequiredHash: hex.EncodeToString(hash[:]),
		Missing: append([]string(nil), missing...),
	}
	return in.saveLocked()
}

// HoldPolicy parks an import until an explicit High ownership decision is
// recorded. Unlike schema holds it is never released by schema rechecking.
func (in *Inbox) HoldPolicy(stream string, first uint64, bundleID string, last uint64, reason string) error {
	in.mu.Lock()
	defer in.mu.Unlock()
	st, ok := in.streams[stream]
	if !ok {
		return fmt.Errorf("bridge: unknown stream %q", stream)
	}
	if _, ok := st.Staged[first]; !ok {
		return fmt.Errorf("bridge: stream %q seq %d is not staged", stream, first)
	}
	st.Holds[first] = &SchemaHold{Kind: "policy", Reason: reason, BundleID: bundleID, First: first, Last: last}
	return in.saveLocked()
}

// ResolvePolicyHold records the High administrator's durable decision for a
// held Low delete. The next Drain performs the import and advances progress.
func (in *Inbox) ResolvePolicyHold(stream string, first uint64, resolution string) error {
	if resolution != ResolutionKeepHigh && resolution != ResolutionAcceptLowDelete {
		return fmt.Errorf("bridge: invalid policy resolution %q", resolution)
	}
	in.mu.Lock()
	defer in.mu.Unlock()
	st, ok := in.streams[stream]
	if !ok || st.Holds[first] == nil || st.Holds[first].Kind != "policy" {
		return fmt.Errorf("bridge: no policy hold for stream %q sequence %d", stream, first)
	}
	st.Holds[first].Resolution = resolution
	return in.saveLocked()
}

// PolicyResolution returns a persisted decision for one held bundle.
func (in *Inbox) PolicyResolution(stream string, first uint64) string {
	in.mu.Lock()
	defer in.mu.Unlock()
	if st := in.streams[stream]; st != nil && st.Holds[first] != nil && st.Holds[first].Kind == "policy" {
		return st.Holds[first].Resolution
	}
	return ""
}

const (
	ResolutionKeepHigh        = "keep-high"
	ResolutionAcceptLowDelete = "accept-low-delete"
)

// RecheckHolds re-validates held heads in stream order, releasing newly
// compatible bundles for import. check typically replays the importer's
// schema gate; a nil error releases the hold. Later holds stay until the
// head clears, preserving order. It returns released bundle count.
func (in *Inbox) RecheckHolds(check func(bundle *Bundle) error) (int, error) {
	in.mu.Lock()
	defer in.mu.Unlock()
	names := make([]string, 0, len(in.streams))
	for name := range in.streams {
		names = append(names, name)
	}
	sort.Strings(names)
	released := 0
	for _, name := range names {
		st := in.streams[name]
		for {
			want := st.Applied + 1
			hold, ok := st.Holds[want]
			if !ok {
				break
			}
			if hold.Kind == "policy" {
				break
			}
			sb, ok := st.Staged[want]
			if !ok {
				break
			}
			bundle, err := in.openStagedLocked(sb.file)
			if err != nil {
				return released, err
			}
			if bundle.Manifest.BundleID.String() != hold.BundleID {
				return released, fmt.Errorf("bridge: held bundle identity changed for stream %q seq %d", name, want)
			}
			if err := check(bundle); err != nil {
				break // still incompatible; ordered retry stops here
			}
			delete(st.Holds, want)
			released++
		}
	}
	if released > 0 {
		if err := in.saveLocked(); err != nil {
			return released, err
		}
	}
	return released, nil
}

// isHeldLocked reports whether seq starts a held bundle.
func (in *Inbox) isHeldLocked(st *streamState, seq uint64) bool {
	hold, ok := st.Holds[seq]
	return ok && !(hold.Kind == "policy" && hold.Resolution != "")
}
