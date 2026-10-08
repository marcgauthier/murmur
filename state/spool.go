package state

import (
	"context"

	"github.com/marcgauthier/murmur/spool"
)

// HoldCommits excludes state commits until the returned release function
// runs. Reads proceed; only the writer coordinator is held. Backup uses
// it to capture a checkpoint plus identity/schema metadata from one cut,
// then releases before archive streaming. Lock order is gate -> writeMu,
// matching every other path.
func (s *Store) HoldCommits() (release func()) {
	s.gate.RLock()
	s.writeMu.Lock()
	return func() {
		s.writeMu.Unlock()
		s.gate.RUnlock()
	}
}

// SpoolCheckpoint captures a point-in-time copy into destDir and returns
// the handle owning its retention registrations. The caller must Release
// the handle after streaming; the destination directory always belongs
// to the caller. Unlike Checkpoint, the handle stays registered so
// reclamation cannot prune checkpoint members mid-backup.
func (s *Store) SpoolCheckpoint(ctx context.Context, destDir string) (*spool.Checkpoint, error) {
	s.gate.RLock()
	defer s.gate.RUnlock()
	if err := s.failedErr(); err != nil {
		return nil, err
	}
	cp, err := s.spool.Checkpoint(ctx, destDir)
	if err != nil {
		return nil, s.noteTerminal("checkpoint", err)
	}
	return cp, nil
}

// RotateDataKey starts a new data-key generation. Blocks written
// afterwards use the fresh key; existing blocks keep theirs until
// compaction or RewriteDataKeys re-encrypts them. Online: the Spool
// maintenance gate pauses writers briefly, readers never block.
func (s *Store) RotateDataKey() error {
	s.gate.RLock()
	defer s.gate.RUnlock()
	if err := s.failedErr(); err != nil {
		return err
	}
	if err := s.spool.RotateKey(); err != nil {
		return s.noteTerminal("rotate data key", err)
	}
	return nil
}

// RotateWrappingKey re-protects the key envelope under new wrapping
// material without re-encrypting any data blocks. The id names the new
// material for provider selection on the next Open.
func (s *Store) RotateWrappingKey(key []byte, id string) error {
	s.gate.RLock()
	defer s.gate.RUnlock()
	if err := s.failedErr(); err != nil {
		return err
	}
	if err := s.spool.RotateWrappingKey(key, id); err != nil {
		return s.noteTerminal("rotate wrapping key", err)
	}
	return nil
}

// RewriteDataKeys re-encrypts every segment under the current data key.
// It runs online through Spool's maintenance gate and resumes after a
// crash from its intent file.
func (s *Store) RewriteDataKeys() error {
	s.gate.RLock()
	defer s.gate.RUnlock()
	if err := s.failedErr(); err != nil {
		return err
	}
	if err := s.spool.RewriteDataKeys(); err != nil {
		return s.noteTerminal("rewrite data keys", err)
	}
	return nil
}

// KeyInventory snapshots non-secret key and maintenance metadata for
// diagnostics. It touches memory only.
func (s *Store) KeyInventory() spool.KeyInventory {
	s.gate.RLock()
	defer s.gate.RUnlock()
	return s.spool.KeyInventory()
}

// KeyReferences scans authoritative segments and reports per-key live
// references. It costs about as much as a startup load; diagnostics
// call it, not the write path.
func (s *Store) KeyReferences() ([]spool.KeyReference, error) {
	s.gate.RLock()
	defer s.gate.RUnlock()
	if err := s.failedErr(); err != nil {
		return nil, err
	}
	return s.spool.KeyReferences()
}
