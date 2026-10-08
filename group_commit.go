package murmur

import (
	"sync"
	"time"

	"github.com/marcgauthier/murmur/codec"
	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/internal/rimeadapter"
	"github.com/marcgauthier/murmur/rime"
	"github.com/marcgauthier/murmur/schema"
)

type groupRowKey struct {
	table uint32
	row   ids.RowID
}

// groupMember is one staged managed transaction waiting for its shared
// durable commit.
type groupMember struct {
	batch  *codec.MutationBatch
	start  time.Time
	resCh  chan groupMemberResult
	record *rimeadapter.GroupCandidate
	rows   map[groupRowKey]bool // true when every write to the row is commutative
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
	members   []*groupMember
	bytes     int64
	closed    chan struct{}
	preceding chan struct{}
	done      chan struct{}
	timer     *time.Timer
}

// groupTicket is the enqueue receipt: the leader waits for the group to
// close and then commits it, while followers wait for their own result.
type groupTicket struct {
	group    *pendingGroup
	member   *groupMember
	isLeader bool
}

// groupCommitter batches staged transactions so concurrent writers
// share one Spool commit with one fsync. ACKs still follow the shared
// fsync, preserving synchronous durability.
//
// Ordering: callers enqueue under writeMu so group order matches assigned
// Spool submission order (batches carry increasing HLCs in that order).
// Enqueue never blocks; waiting happens after the caller releases writeMu,
// which lets the next transaction stage and queue while this fsync runs.
type groupCommitter struct {
	maxDelay time.Duration
	maxTx    int
	maxBytes int64
	commit   func(members []*groupMember) []groupMemberResult

	mu         sync.Mutex
	current    *pendingGroup
	tail       chan struct{}
	activeRows map[groupRowKey]activeGroupRow
}

type activeGroupRow struct {
	refs      int
	mergeOnly bool
}

func newGroupCommitter(cfg GroupCommitConfig, commit func(members []*groupMember) []groupMemberResult) *groupCommitter {
	complete := make(chan struct{})
	close(complete)
	return &groupCommitter{
		maxDelay:   cfg.MaxDelay,
		maxTx:      cfg.MaxTransactions,
		maxBytes:   cfg.MaxBytes,
		commit:     commit,
		tail:       complete,
		activeRows: make(map[groupRowKey]activeGroupRow),
	}
}

// enqueue adds m to the open group, opening one (as leader) when none is
// open. It never blocks.
func (c *groupCommitter) enqueue(m *groupMember) (groupTicket, error) {
	c.mu.Lock()
	m.rows = groupMemberRows(m.batch)
	for key := range m.rows {
		// RIME candidates carry a staged MVCC base. If another admitted write
		// touches the same row, wait for it to publish and let the caller retry
		// against the new row version before any durable commit is submitted.
		if _, ok := c.activeRows[key]; ok {
			wait := c.tail
			c.mu.Unlock()
			<-wait
			return groupTicket{}, rime.ErrConflict
		}
	}
	g := c.current
	isLeader := false
	if g == nil {
		g = &pendingGroup{
			closed:    make(chan struct{}),
			preceding: c.tail,
			done:      make(chan struct{}),
		}
		c.tail = g.done
		g.timer = time.AfterFunc(c.maxDelay, func() { c.closeGroup(g) })
		c.current = g
		isLeader = true
	}
	g.members = append(g.members, m)
	for key, mergeOnly := range m.rows {
		active := c.activeRows[key]
		active.refs++
		if active.refs == 1 {
			active.mergeOnly = mergeOnly
		} else {
			active.mergeOnly = active.mergeOnly && mergeOnly
		}
		c.activeRows[key] = active
	}
	g.bytes += int64(codec.EncodedBatchSize(m.batch))
	if len(g.members) >= c.maxTx || g.bytes >= c.maxBytes {
		c.closeGroupLocked(g)
	}
	c.mu.Unlock()
	return groupTicket{group: g, member: m, isLeader: isLeader}, nil
}

// await resolves t: the leader waits for the group to close and commits
// it, then every member (leader included) receives its own result.
func (c *groupCommitter) await(t groupTicket) groupMemberResult {
	if !t.isLeader {
		return <-t.member.resCh
	}
	<-t.group.closed
	<-t.group.preceding
	results := c.commit(t.group.members)
	c.releaseRows(t.group)
	close(t.group.done)
	for i, m := range t.group.members[1:] {
		m.resCh <- results[i+1]
	}
	return results[0]
}

func groupMemberRows(batch *codec.MutationBatch) map[groupRowKey]bool {
	rows := make(map[groupRowKey]bool)
	if batch == nil {
		return rows
	}
	for _, mutation := range batch.Mutations {
		key := groupRowKey{table: mutation.TableID, row: mutation.RowID}
		mergeOnly, exists := rows[key]
		if !exists {
			mergeOnly = true
		}
		if mutation.Policy == schema.LWW {
			mergeOnly = false
		}
		rows[key] = mergeOnly
	}
	return rows
}

func (c *groupCommitter) releaseRows(group *pendingGroup) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, member := range group.members {
		for key := range member.rows {
			active := c.activeRows[key]
			active.refs--
			if active.refs <= 0 {
				delete(c.activeRows, key)
			} else {
				c.activeRows[key] = active
			}
		}
	}
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

// flush closes the current group so shutdown can drain admitted members
// without waiting for the configured batching delay.
func (c *groupCommitter) flush() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.current != nil {
		c.closeGroupLocked(c.current)
	}
}
