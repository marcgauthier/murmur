package crdt

import (
	"sync"
	"time"
)

// HLC layout: 64 bits = physical millis in the high 48 bits, logical
// counter in the low 16 bits. Millis needs ~39 bits until year 3000+,
// leaving headroom; 16 counter bits allow 65536 writes per millisecond
// per node before the clock must wait for the wall clock to advance.
const (
	logicalBits = 16
	logicalMask = (1 << logicalBits) - 1
)

// WallMillis extracts the physical millisecond component of an HLC timestamp.
func WallMillis(hlc uint64) int64 { return int64(hlc >> logicalBits) }

// Clock is a hybrid logical clock. The zero value is usable; call Observe
// with the persisted value after a restart so time cannot move backwards.
type Clock struct {
	mu   sync.Mutex
	last uint64
	now  func() int64 // physical millis; overridden in tests
}

func physMillis() int64 { return time.Now().UnixMilli() }

// Now returns the next HLC timestamp for a local write (HLC send rule).
func (c *Clock) Now() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	pt := c.phys()
	if pt < 0 {
		pt = 0
	}
	l := int64(c.last >> logicalBits)
	switch {
	case l > pt:
		c.last++ // logical clock ahead of wall clock; keep counting
		if c.last&logicalMask == 0 {
			// Counter exhausted: advance the logical physical component.
			// The clock may drift ahead of the wall under extreme write
			// rates; monotonicity is preserved.
			c.last = uint64(l+1) << logicalBits
		}
	case l == pt:
		c.last++
		if c.last&logicalMask == 0 {
			c.last = uint64(l+1) << logicalBits
		}
	default: // wall clock advanced past the logical clock
		c.last = uint64(pt) << logicalBits
	}
	return c.last
}

// Observe incorporates a remote HLC timestamp so future local writes are
// strictly greater (HLC receive rule).
func (c *Clock) Observe(remote uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	pt := c.phys()
	if pt < 0 {
		pt = 0
	}
	l := int64(c.last >> logicalBits)
	cm := c.last & logicalMask
	lm := int64(remote >> logicalBits)
	cmRemote := remote & logicalMask

	lNew := max3(pt, l, lm)
	var cNew uint64
	switch {
	case lNew == l && lNew == lm:
		cNew = max2(cm, cmRemote) + 1
	case lNew == l:
		cNew = cm + 1
	case lNew == lm:
		cNew = cmRemote + 1
	default: // wall clock ahead of both
		cNew = 0
	}
	if cNew > logicalMask {
		lNew++
		cNew = 0
	}
	c.last = uint64(lNew)<<logicalBits | cNew
}

// Max returns the highest timestamp issued or observed so far.
func (c *Clock) Max() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.last
}

// Restore sets the clock floor from durable state. Timestamps at or below
// the restored value will never be issued again.
func (c *Clock) Restore(v uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if v > c.last {
		c.last = v
	}
}

func (c *Clock) phys() int64 {
	if c.now != nil {
		return c.now()
	}
	return physMillis()
}

func max2(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

func max3(a, b, c int64) int64 {
	m := a
	if b > m {
		m = b
	}
	if c > m {
		m = c
	}
	return m
}
