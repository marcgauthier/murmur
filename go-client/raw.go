package murmur

import "sync/atomic"

// RawConnection provides unmanaged SQLite/session helpers, without persistence or replication.
type RawConnection struct{ object }
type Session struct {
	connection *RawConnection
	id         uint64
	closed     atomic.Bool
}

func OpenInMemory() (*RawConnection, error) {
	var id uint64
	if err := invoke(0, "raw_open", nil, &id); err != nil {
		return nil, err
	}
	c := &RawConnection{}
	c.set(id)
	return c, nil
}
func (c *RawConnection) Run(fn func(*Sql) error) error { return runSQL(&c.object, "read", fn) }
func (c *RawConnection) Close() error                  { return c.object.close("close") }
func StartSession(c *RawConnection) (*Session, error) {
	var id uint64
	if err := c.call("session_start", nil, &id); err != nil {
		return nil, err
	}
	return &Session{connection: c, id: id}, nil
}
func (s *Session) call(op string, args map[string]any, out any) error {
	if s.closed.Load() {
		return ErrClosed
	}
	if args == nil {
		args = map[string]any{}
	}
	args["session"] = s.id
	return s.connection.call(op, args, out)
}
func (s *Session) Changeset() ([]byte, error) {
	var b Bytes
	err := s.call("session_changeset", nil, &b)
	return b, err
}
func (s *Session) Patchset() ([]byte, error) {
	var b Bytes
	err := s.call("session_patchset", nil, &b)
	return b, err
}
func (s *Session) Attach(table *string) error {
	return s.call("session_attach", map[string]any{"table": table}, nil)
}
func (s *Session) SetEnabled(enabled bool) error {
	return s.call("session_enable", map[string]any{"enabled": enabled}, nil)
}
func (s *Session) IsEmpty() (bool, error) {
	var b bool
	err := s.call("session_empty", nil, &b)
	return b, err
}
func (s *Session) Close() error {
	if s.closed.Load() {
		return nil
	}
	err := s.call("session_close", nil, nil)
	if err == nil {
		s.closed.Store(true)
	}
	return err
}
