// Background mesh fetch for missing file objects.
//
// A node learns file metadata through normal replication, but object bytes
// stay on the uploader until fetched. The fetch worker periodically scans
// visible metadata for objects absent locally and pulls them from the
// statically configured fetch peers, trying sources in turn until one serves
// bytes that verify against the replicated digest and length.
//
// Transfers stage into per-source files under <Path>/files/staging and
// publish atomically through objectstore InstallVerified: a crash or restart
// resumes from the staged prefix and never exposes partial objects. Staging
// is per-source because container bytes differ across uploads of identical
// content (random container nonce); mixing sources would only fail install
// and restart from zero.
package replicateddb

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/marcgauthier/spedsql/filefetch"
	"github.com/marcgauthier/spedsql/ids"
	"github.com/marcgauthier/spedsql/objectstore"
	"github.com/marcgauthier/spedsql/state"
)

// fetchState is the worker-side fetch subsystem state.
type fetchState struct {
	server  *filefetch.Server
	client  *filefetch.Client
	staging string

	sem chan struct{}

	trigger chan struct{}

	mu     sync.Mutex
	active map[objectstore.Digest]int

	pending   atomic.Int64
	inFlight  atomic.Int64
	completed atomic.Uint64
	failed    atomic.Uint64
	bytesIn   atomic.Uint64

	errMu   sync.Mutex
	lastErr string
}

// FileFetchStats reports fetch worker progress.
type FileFetchStats struct {
	// Pending is the missing-object count from the last scan.
	Pending int64
	// InFlight is the current active fetch count.
	InFlight     int64
	Completed    uint64
	Failed       uint64
	BytesFetched uint64
	// LastError is the most recent fetch failure, if any.
	LastError string
	// Sources is the configured static source count (excluding self).
	Sources int
	// Serving reports whether this node serves fetches to peers.
	Serving bool
}

// FileFetchStats returns a fetch progress snapshot.
func (db *DB) FileFetchStats() FileFetchStats {
	st := FileFetchStats{}
	if db.files == nil || db.files.fetch == nil {
		return st
	}
	fs := db.files
	st.Pending = fs.fetch.pending.Load()
	st.InFlight = fs.fetch.inFlight.Load()
	st.Completed = fs.fetch.completed.Load()
	st.Failed = fs.fetch.failed.Load()
	st.BytesFetched = fs.fetch.bytesIn.Load()
	fs.fetch.errMu.Lock()
	st.LastError = fs.fetch.lastErr
	fs.fetch.errMu.Unlock()
	st.Sources = len(fs.sources())
	st.Serving = fs.fetch.server != nil
	return st
}

// sources lists fetch peers excluding the local node.
func (fs *fileStore) sources() []Peer {
	var out []Peer
	for _, p := range fs.db.cfg.Files.FetchPeers {
		if p.NodeID == fs.db.cfg.NodeID {
			continue
		}
		out = append(out, p)
	}
	return out
}

// triggerFetchScan wakes the background worker early. It never blocks.
func (fs *fileStore) triggerFetchScan() {
	if fs == nil || fs.fetch == nil {
		return
	}
	select {
	case fs.fetch.trigger <- struct{}{}:
	default:
	}
}

// FetchFile pulls the named file's object bytes from the fetch peers,
// trying sources in turn until the staged bytes verify against the
// replicated metadata. It returns nil when the bytes are already local.
func (db *DB) FetchFile(ctx context.Context, name string) error {
	fs, err := db.filesForRead()
	if err != nil {
		return err
	}
	if name == "" {
		return fmt.Errorf("replicateddb: file name is required")
	}
	st, err := fs.status(fileRowID(name))
	if err != nil {
		return err
	}
	if !st.Exists || st.Deleted {
		return fmt.Errorf("%w: no such file %q", ErrFileUnavailable, name)
	}
	if st.Available {
		return nil
	}
	sources := fs.sources()
	if len(sources) == 0 {
		return fmt.Errorf("replicateddb: fetch %q: no fetch sources configured", name)
	}
	timeout := db.cfg.Files.FetchTimeout
	if timeout <= 0 {
		timeout = 5 * time.Minute
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	select {
	case fs.fetch.sem <- struct{}{}:
		defer func() { <-fs.fetch.sem }()
	case <-ctx.Done():
		return ctx.Err()
	}
	fs.fetch.inFlight.Add(1)
	defer fs.fetch.inFlight.Add(-1)
	fs.markActive(st.Digest, true)
	defer fs.markActive(st.Digest, false)

	// Rotate the starting source across attempts so concurrent fetchers
	// spread load instead of hammering the first peer.
	start := int(fs.fetch.completed.Load()+fs.fetch.failed.Load()) % len(sources)
	var lastErr error
	for i := range sources {
		src := sources[(start+i)%len(sources)]
		if err := fs.fetchFrom(ctx, src, st); err != nil {
			lastErr = err
			fs.noteFetchErr(fmt.Sprintf("%s from %s: %v", name, src.NodeID, err))
			continue
		}
		fs.fetch.completed.Add(1)
		fs.fetch.bytesIn.Add(uint64(st.Size))
		return nil
	}
	fs.fetch.failed.Add(1)
	if lastErr == nil {
		lastErr = fmt.Errorf("no source served the object")
	}
	return fmt.Errorf("replicateddb: fetch %q: %w", name, lastErr)
}

// maxContainerFor bounds the container length for a plaintext size: header,
// per-chunk size/authenticator overhead, and trailer, with slack.
func maxContainerFor(plainLen int64) int64 {
	chunks := (plainLen + int64(objectstore.ChunkSize) - 1) / int64(objectstore.ChunkSize)
	return plainLen + chunks*32 + 512
}

// fetchFrom attempts one source's addresses in order.
func (fs *fileStore) fetchFrom(ctx context.Context, src Peer, st FileStatus) error {
	var lastErr error
	for _, addr := range src.Addrs {
		if err := fs.fetchAddr(ctx, addr, src.NodeID, st); err != nil {
			lastErr = err
			continue
		}
		return nil
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("no addresses")
	}
	return lastErr
}

// stagePath names the per-source staging file for a digest.
func (fs *fileStore) stagePath(digest objectstore.Digest, src ids.NodeID) string {
	suffix := strings.ReplaceAll(src.String(), "/", "_")
	return filepath.Join(fs.fetch.staging, digest.String()+"-"+suffix+".part")
}

// fetchAddr streams one object's container from one address into staging,
// resuming from any staged prefix, and installs it on completion.
func (fs *fileStore) fetchAddr(ctx context.Context, addr string, src ids.NodeID, st FileStatus) error {
	stage := fs.stagePath(st.Digest, src)
	var staged int64
	if fi, err := os.Stat(stage); err == nil {
		staged = fi.Size()
		if staged > maxContainerFor(st.Size) {
			// Poisoned staging can never verify; restart from zero.
			_ = os.Remove(stage)
			staged = 0
		}
	}
	if over, err := fs.stagingOverBudget(int64(0)); err != nil {
		return err
	} else if over {
		return fmt.Errorf("staging exceeds MaxStagingBytes; run FilesGC")
	}
	stream, err := fs.fetch.client.Fetch(ctx, addr, src, fs.db.store.DBID(), st.Digest, uint64(staged))
	if errors.Is(err, filefetch.ErrBadOffset) {
		// The source holds a different container generation; restart.
		_ = os.Remove(stage)
		staged = 0
		stream, err = fs.fetch.client.Fetch(ctx, addr, src, fs.db.store.DBID(), st.Digest, 0)
	}
	if err != nil {
		return err
	}
	defer stream.Close()
	declared := stream.ContainerLen()
	if declared > uint64(maxContainerFor(st.Size)) {
		return fmt.Errorf("peer declares %d container bytes for a %d-byte file", declared, st.Size)
	}
	if declared < uint64(staged) {
		_ = os.Remove(stage)
		return fmt.Errorf("peer container shrank; staging discarded")
	}
	f, err := os.OpenFile(stage, os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	if _, err := f.Seek(staged, io.SeekStart); err != nil {
		_ = f.Close()
		return err
	}
	buf := make([]byte, 32<<10)
	n, copyErr := io.CopyBuffer(f, io.LimitReader(stream, int64(declared)-staged), buf)
	if copyErr != nil {
		_ = f.Close()
		return copyErr
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	_ = f.Close()
	if staged+n != int64(declared) {
		// Truncated transfer: keep the prefix for resume, fail this round.
		return fmt.Errorf("truncated transfer: %d of %d bytes", staged+n, declared)
	}
	if _, err := fs.objects.InstallVerified(ctx, stage, st.Digest, st.Size); err != nil {
		// Staged bytes do not verify (mixed generations or a corrupt
		// source): discard so the next attempt starts clean.
		_ = os.Remove(stage)
		return err
	}
	return nil
}

// stagingOverBudget reports whether the staging directory already exceeds
// the configured cap (plus the in-progress file's declared remainder).
func (fs *fileStore) stagingOverBudget(add int64) (bool, error) {
	cap := fs.db.cfg.Files.MaxStagingBytes
	if cap <= 0 {
		return false, nil
	}
	entries, err := os.ReadDir(fs.fetch.staging)
	if err != nil {
		return false, err
	}
	var total int64
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		fi, err := e.Info()
		if err != nil {
			continue
		}
		total += fi.Size()
		if total+add > cap {
			return true, nil
		}
	}
	return total+add > cap, nil
}

func (fs *fileStore) markActive(digest objectstore.Digest, on bool) {
	fs.fetch.mu.Lock()
	defer fs.fetch.mu.Unlock()
	if on {
		fs.fetch.active[digest]++
	} else {
		if fs.fetch.active[digest] <= 1 {
			delete(fs.fetch.active, digest)
		} else {
			fs.fetch.active[digest]--
		}
	}
}

func (fs *fileStore) noteFetchErr(msg string) {
	fs.fetch.errMu.Lock()
	defer fs.fetch.errMu.Unlock()
	fs.fetch.lastErr = msg
}

// fetchLoop scans for missing objects on an interval (and when triggered)
// and fetches each with bounded concurrency.
func (db *DB) fetchLoop() {
	defer db.wg.Done()
	fs := db.files
	interval := db.cfg.Files.FetchInterval
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	// Repair first: a metadata-only restore (or any restart with pending
	// objects) schedules recovery immediately instead of waiting a tick.
	db.fetchScan()
	for {
		select {
		case <-db.ctx.Done():
			return
		case <-fs.fetch.trigger:
		case <-ticker.C:
		}
		db.fetchScan()
	}
}

// fetchScan queues one fetch per missing object.
func (db *DB) fetchScan() {
	fs, err := db.filesForRead()
	if err != nil {
		return
	}
	var missing []FileStatus
	if err := db.store.IterateTable(fs.ids.table, func(r *state.Row) error {
		if db.ctx.Err() != nil {
			return db.ctx.Err()
		}
		eff, visible, err := effectiveFileVisible(db, r.Cells, r.Tomb.Version, r.Tomb.Present)
		if err != nil {
			return err
		}
		if !visible {
			return nil
		}
		name, digest, size, err := fs.decode(eff)
		if err != nil {
			return err
		}
		if fs.objects.Has(digest) {
			return nil
		}
		missing = append(missing, FileStatus{Name: name, Digest: digest, Size: size})
		return nil
	}); err != nil {
		db.log.Warn("replicateddb: fetch scan failed", "err", err.Error())
		return
	}
	fs.fetch.pending.Store(int64(len(missing)))
	var wg sync.WaitGroup
	for _, m := range missing {
		if db.ctx.Err() != nil {
			break
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer fs.fetch.pending.Add(-1)
			_ = db.FetchFile(db.ctx, m.Name)
		}()
	}
	wg.Wait()
}
