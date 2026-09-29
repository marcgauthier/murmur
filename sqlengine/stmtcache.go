package sqlengine

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"sync/atomic"

	lru "github.com/hashicorp/golang-lru/v2"
)

// stmtCache is a small LRU of connection-bound prepared statements.
type stmtCache struct {
	mu    sync.Mutex
	cache *lru.Cache[string, *sql.Stmt]
	// hits/misses count cache lookups for sizing diagnostics. A lookup
	// that finds a cached statement (including the concurrent-prepare
	// collapse below) is a hit; any other lookup is a miss.
	hits   atomic.Uint64
	misses atomic.Uint64
}

func newStmtCache(entries int) *stmtCache {
	c, err := lru.NewWithEvict[string, *sql.Stmt](entries, func(_ string, s *sql.Stmt) {
		_ = s.Close()
	})
	if err != nil {
		// Only fails for non-positive size; fall back to a minimal cache.
		c, _ = lru.NewWithEvict[string, *sql.Stmt](1, func(_ string, s *sql.Stmt) {
			_ = s.Close()
		})
	}
	return &stmtCache{cache: c}
}

// preparer is satisfied by *sql.DB (pool-level statements shared across
// connections) and *sql.Conn (connection-bound statements).
type preparer interface {
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
}

func (c *stmtCache) prepare(ctx context.Context, p preparer, query string) (*sql.Stmt, error) {
	c.mu.Lock()
	if s, ok := c.cache.Get(query); ok {
		c.hits.Add(1)
		c.mu.Unlock()
		return s, nil
	}
	c.misses.Add(1)
	c.mu.Unlock()
	s, err := p.PrepareContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("sqlengine: prepare: %w", err)
	}
	c.mu.Lock()
	// Another goroutine may have prepared concurrently; prefer the cached one.
	if old, ok := c.cache.Get(query); ok {
		c.hits.Add(1)
		c.mu.Unlock()
		_ = s.Close()
		return old, nil
	}
	c.cache.Add(query, s)
	c.mu.Unlock()
	return s, nil
}

// invalidate closes and drops all cached statements (schema change).
func (c *stmtCache) invalidate() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, k := range c.cache.Keys() {
		if s, ok := c.cache.Get(k); ok {
			_ = s.Close()
		}
	}
	c.cache.Purge()
}

func (c *stmtCache) close() { c.invalidate() }

// Len returns the cached statement count (diagnostics).
func (c *stmtCache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cache.Len()
}

// Stats returns cumulative lookup hits and misses (sizing diagnostics).
func (c *stmtCache) Stats() (hits, misses uint64) {
	return c.hits.Load(), c.misses.Load()
}
