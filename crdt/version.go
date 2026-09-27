// Package crdt implements per-cell last-writer-wins conflict resolution.
//
// The conflict version is (HLC, origin NodeID). The replication sequence
// (origin NodeID + monotonic sequence) is only a transport watermark and is
// never used for conflict decisions.
package crdt

import (
	"github.com/nomadsql/replicateddb/ids"
)

// Version is the conflict version of one cell or row tombstone.
type Version struct {
	HLC    uint64
	NodeID ids.NodeID
}

// CompareVersion orders versions: higher HLC wins; on equal HLC the
// lexicographically greater NodeID wins. All nodes implement this exact
// comparison so merges are deterministic regardless of arrival order.
func CompareVersion(a, b Version) int {
	switch {
	case a.HLC < b.HLC:
		return -1
	case a.HLC > b.HLC:
		return 1
	default:
		return a.NodeID.Compare(b.NodeID)
	}
}

// Wins reports whether incoming supersedes current (strictly greater).
// Equal versions mean the same logical write; no replacement is needed.
func Wins(incoming, current Version) bool {
	return CompareVersion(incoming, current) > 0
}

// Zero is the version older than any real write.
var Zero = Version{}
