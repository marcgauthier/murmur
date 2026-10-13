package murmur

type Replication struct{ object }

func (d *DB) Replication() (*Replication, error) {
	var id *uint64
	if err := d.call("replication", nil, &id); err != nil {
		return nil, err
	}
	if id == nil {
		return nil, nil
	}
	r := &Replication{}
	r.set(*id)
	return r, nil
}
func (r *Replication) Status() (ReplicationStatus, error) {
	var s ReplicationStatus
	err := r.call("status", nil, &s)
	return s, err
}
func (r *Replication) AddPeer(address string) error {
	return r.call("add_peer", map[string]any{"address": address}, nil)
}
func (r *Replication) RemovePeer(node NodeID) error {
	return r.call("remove_peer", map[string]any{"node": node}, nil)
}
func (r *Replication) ForceSync() error { return r.call("force_sync", nil, nil) }
func (r *Replication) RetirePeer(node NodeID) error {
	return r.call("retire_peer", map[string]any{"node": node}, nil)
}
func (r *Replication) RetentionStatus() (RetentionStatus, error) {
	var s RetentionStatus
	err := r.call("retention_status", nil, &s)
	return s, err
}
func (r *Replication) CollectRetention() (uint64, error) {
	var n uint64
	err := r.call("collect_retention", nil, &n)
	return n, err
}
func (r *Replication) Close() error   { return r.object.close("close") }
func (r *Replication) Release() error { return r.object.close("release") }
func (r *Replication) Files() (*Objects, error) {
	var id *uint64
	if err := r.call("files", nil, &id); err != nil {
		return nil, err
	}
	if id == nil {
		return nil, nil
	}
	o := &Objects{}
	o.set(*id)
	return o, nil
}
func (r *Replication) Bridge() (*Bridge, error) {
	var id *uint64
	if err := r.call("bridge", nil, &id); err != nil {
		return nil, err
	}
	if id == nil {
		return nil, nil
	}
	b := &Bridge{}
	b.set(*id)
	return b, nil
}
func StartReplication(config ReplicationConfig, backend *BackendHandle) (*Replication, error) {
	id, err := backend.handle()
	if err != nil {
		return nil, err
	}
	var result uint64
	if err = invoke(0, "replication_start", map[string]any{"config": config, "backend": id}, &result); err != nil {
		return nil, err
	}
	r := &Replication{}
	r.set(result)
	return r, nil
}
