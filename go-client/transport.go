package murmur

type Endpoint struct{ object }
type Connection struct{ object }
type SendStream struct{ object }
type ReceiveStream struct{ object }

func NewEndpoint(config ReplicationConfig, node NodeID) (*Endpoint, error) {
	var id uint64
	if err := invoke(0, "transport_endpoint", map[string]any{"config": config, "node": node}, &id); err != nil {
		return nil, err
	}
	e := &Endpoint{}
	e.set(id)
	return e, nil
}
func (e *Endpoint) Address() (string, error) {
	var a string
	err := e.call("address", nil, &a)
	return a, err
}
func (e *Endpoint) Connect(address string) (*Connection, error) {
	var id uint64
	if err := e.call("connect", map[string]any{"address": address}, &id); err != nil {
		return nil, err
	}
	c := &Connection{}
	c.set(id)
	return c, nil
}
func (e *Endpoint) Accept() (*Connection, error) {
	var id uint64
	if err := e.call("accept", nil, &id); err != nil {
		return nil, err
	}
	c := &Connection{}
	c.set(id)
	return c, nil
}
func (e *Endpoint) Close() error    { return e.object.close("close") }
func (e *Endpoint) WaitIdle() error { return e.call("wait_idle", nil, nil) }
func (c *Connection) Address() (string, error) {
	var a string
	err := c.call("address", nil, &a)
	return a, err
}
func (c *Connection) PeerID(config ReplicationConfig) (NodeID, error) {
	var id NodeID
	err := c.call("peer_id", map[string]any{"config": config}, &id)
	return id, err
}
func (c *Connection) OpenBi() (*SendStream, *ReceiveStream, error)   { return c.bi("open_bi") }
func (c *Connection) AcceptBi() (*SendStream, *ReceiveStream, error) { return c.bi("accept_bi") }
func (c *Connection) bi(op string) (*SendStream, *ReceiveStream, error) {
	var ids [2]uint64
	if err := c.call(op, nil, &ids); err != nil {
		return nil, nil, err
	}
	s := &SendStream{}
	s.set(ids[0])
	r := &ReceiveStream{}
	r.set(ids[1])
	return s, r, nil
}
func (c *Connection) Close() error { return c.object.close("close") }
func (s *SendStream) WriteMessage(message Message) error {
	return s.call("write_message", map[string]any{"message": message}, nil)
}
func (s *SendStream) Finish() error { return s.call("finish", nil, nil) }
func (s *SendStream) Close() error  { return s.object.close("close") }
func (r *ReceiveStream) ReadMessage() (Message, error) {
	var m Message
	err := r.call("read_message", nil, &m)
	return m, err
}

// Close cancels a pending ReadMessage and releases the native stream.
func (r *ReceiveStream) Close() error { return r.object.close("close") }
