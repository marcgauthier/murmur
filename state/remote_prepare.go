package state

import (
	"context"
	"fmt"

	"github.com/marcgauthier/spedsql/codec"
)

// recoverPreparedRemote finishes the one remote transaction whose durable
// prepare record survived a process interruption. Open does not expose the
// Store until this succeeds, so neither partial state nor a stale materializer
// can be observed by callers.
func (s *Store) recoverPreparedRemote() error {
	s.gate.RLock()
	defer s.gate.RUnlock()
	raw, err := s.getDirect(SysKey(sysRemotePrepare))
	if isNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	batch, rest, err := codec.DecodeBatch(raw, s.limits)
	if err != nil {
		return fmt.Errorf("decode prepare record: %w", err)
	}
	if len(rest) != 0 || len(batch.Mutations) == 0 {
		return fmt.Errorf("invalid remote prepare record")
	}
	if _, err := s.CommitRemote(context.Background(), batch); err != nil {
		return err
	}
	return nil
}
