package murmur

import "time"

type WriterScheduler struct{ object }
type Ticket struct{ object }

func NewWriterScheduler(config SchedulerConfig) (*WriterScheduler, error) {
	var id uint64
	if err := invoke(0, "scheduler_new", map[string]any{"config": config}, &id); err != nil {
		return nil, err
	}
	s := &WriterScheduler{}
	s.set(id)
	return s, nil
}
func (d *DB) Scheduler() (*WriterScheduler, error) {
	var id uint64
	if err := d.call("scheduler", nil, &id); err != nil {
		return nil, err
	}
	s := &WriterScheduler{}
	s.set(id)
	return s, nil
}
func (s *WriterScheduler) Snapshot() (SchedulerSnapshot, error) {
	var v SchedulerSnapshot
	err := s.call("snapshot", nil, &v)
	return v, err
}
func (s *WriterScheduler) Active() (bool, error) {
	var b bool
	err := s.call("active", nil, &b)
	return b, err
}
func (s *WriterScheduler) Admit(class WriterClass) (*Ticket, error) {
	return s.admit("admit", class, 0)
}
func (s *WriterScheduler) AdmitTimeout(class WriterClass, timeout time.Duration) (*Ticket, error) {
	return s.admit("admit_timeout", class, timeout)
}
func (s *WriterScheduler) admit(op string, class WriterClass, timeout time.Duration) (*Ticket, error) {
	if timeout < 0 {
		return nil, invalidError("negative timeout")
	}
	var id uint64
	if err := s.call(op, map[string]any{"class": class, "timeout": timeout}, &id); err != nil {
		return nil, err
	}
	t := &Ticket{}
	t.set(id)
	return t, nil
}
func (s *WriterScheduler) Close() error   { return s.object.close("close") }
func (s *WriterScheduler) Release() error { return s.object.close("release") }
func (t *Ticket) Release() error          { return t.object.close("release") }
func (t *Ticket) Class() (WriterClass, error) {
	var c WriterClass
	err := t.call("class", nil, &c)
	return c, err
}
func (t *Ticket) Dual() (bool, error) { var b bool; err := t.call("dual", nil, &b); return b, err }

// GrantedAt uses the native monotonic elapsed duration to construct a Go monotonic timestamp.
func (t *Ticket) GrantedAt() (time.Time, error) {
	var elapsed time.Duration
	err := t.call("granted_elapsed", nil, &elapsed)
	return time.Now().Add(-elapsed), err
}
func (c SchedulerConfig) Validate() error {
	s, err := NewWriterScheduler(c)
	if err != nil {
		return err
	}
	return s.Close()
}
