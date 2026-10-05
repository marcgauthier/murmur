package murmur

import (
	"sync"
	"time"

	"github.com/marcgauthier/murmur/codec"
)

// groupMember is one SQL-committed transaction waiting for its shared
// durable commit.
type groupMember struct {
	batch     *codec.MutationBatch
	mutations []codec.Mutation
	// genBefore/seqBefore are the state generation and local sequence
	// observed before SQL commit; the leader compares them against the
	// pre-group values to detect interleaved remote commits that require
	// a materializer repair (same probe as the non-group path).
	genBefore uint64
	seqBefore uint64
	start     time.Time
	resCh     chan groupMemberResult
}

// groupMemberResult is one member's acknowledged outcome.
type groupMemberResult struct {
	applied bool
	gen     uint64
	err     error
}

// pendingGroup collects members until the window expires or a size cap is
// reached. Members is append-only until closed is closed.
type pendingGroup struct {
	members []*groupMember
	bytes   int64
	closed  chan struct{}
	timer   *time.Timer
}

// groupTicket is the enqueue receipt: the leader waits for the group to
// close and then commits it, while followers wait for their own result.
type groupTicket struct {
	group    *pendingGroup
	member   *groupMember
	isLeader bool
}

// groupCommitter batches SQL-committed transactions so concurrent writers
// share one Pebble batch with one fsync. ACKs still follow the shared
// fsync, preserving synchronous durability.
//
// Ordering: callers must enqueue under writeMu so group order matches
// SQL-commit order (batches carry strictly increasing HLCs in that order).
// Enqueue never blocks; waiting happens after the caller releases writeMu,
// which is what lets the next transaction's SQL overlap this fsync.
type groupCommitter struct {
	maxDelay time.Duration
	maxTx    int
	maxBytes int64
	commit   func(members []*groupMember) []groupMemberResult

	mu      sync.Mutex
	current *pendingGroup
}

func newGroupCommitter(cfg GroupCommitConfig, commit func(members []*groupMember) []groupMemberResult) *groupCommitter {
	return &groupCommitter{
		maxDelay: cfg.MaxDelay,
		maxTx:    cfg.MaxTransactions,
		maxBytes: cfg.MaxBytes,
		commit:   commit,
	}
}

// enqueue adds m to the open group, opening one (as leader) when none is
// open. It never blocks.
func (c *groupCommitter) enqueue(m *groupMember) groupTicket {
	c.mu.Lock()
	defer c.mu.Unlock()
	g := c.current
	isLeader := false
	if g == nil {
		g = &pendingGroup{closed: make(chan struct{})}
		g.timer = time.AfterFunc(c.maxDelay, func() { c.closeGroup(g) })
		c.current = g
		isLeader = true
	}
	g.members = append(g.members, m)
	g.bytes += int64(codec.EncodedBatchSize(m.batch))
	if len(g.members) >= c.maxTx || g.bytes >= c.maxBytes {
		c.closeGroupLocked(g)
	}
	return groupTicket{group: g, member: m, isLeader: isLeader}
}

// await resolves t: the leader waits for the group to close and commits
// it, then every member (leader included) receives its own result.
func (c *groupCommitter) await(t groupTicket) groupMemberResult {
	if !t.isLeader {
		return <-t.member.resCh
	}
	<-t.group.closed
	results := c.commit(t.group.members)
	for i, m := range t.group.members[1:] {
		m.resCh <- results[i+1]
	}
	return results[0]
}

func (c *groupCommitter) closeGroup(g *pendingGroup) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closeGroupLocked(g)
}

func (c *groupCommitter) closeGroupLocked(g *pendingGroup) {
	if c.current != g {
		return
	}
	c.current = nil
	if g.timer != nil {
		g.timer.Stop()
	}
	close(g.closed)
}
