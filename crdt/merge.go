package crdt

// Merge describes the outcome of merging one cell or tombstone against the
// stored winner.
type Merge int

const (
	// MergeKeep means the stored version wins; the incoming write is dropped.
	MergeKeep Merge = iota
	// MergeTake means the incoming version wins and must be stored.
	MergeTake
	// MergeEqual means both versions are identical; nothing changes.
	MergeEqual
)

// MergeCell compares an incoming cell write against the stored version.
func MergeCell(incoming, stored Version, storedPresent bool) Merge {
	if !storedPresent {
		return MergeTake
	}
	switch CompareVersion(incoming, stored) {
	case 0:
		return MergeEqual
	case 1:
		return MergeTake
	default:
		return MergeKeep
	}
}

// Visible reports whether a row with the given newest-cell version and
// tombstone is query-visible. A row is visible when it has at least one
// cell and no tombstone strictly newer than its newest cell (LWW delete;
// later updates resurrect the row).
func Visible(hasCells bool, newestCell Version, tomb TombstoneState) bool {
	if !hasCells {
		return false
	}
	if !tomb.Present {
		return true
	}
	return CompareVersion(tomb.Version, newestCell) <= 0
}

// TombstoneState is the stored row-tombstone version, if any.
type TombstoneState struct {
	Present bool
	Version Version
}
