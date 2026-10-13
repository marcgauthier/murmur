package murmur

import "encoding/json"

type CounterComponent struct {
	Database NodeID
	Node     NodeID
	Positive uint64
	Negative uint64
}

func (c CounterComponent) MarshalJSON() ([]byte, error) {
	return json.Marshal([4]any{c.Database, c.Node, c.Positive, c.Negative})
}
func (c *CounterComponent) UnmarshalJSON(b []byte) error {
	var a []json.RawMessage
	if err := json.Unmarshal(b, &a); err != nil {
		return err
	}
	if len(a) != 4 {
		return invalidError("counter component")
	}
	for i, dest := range []any{&c.Database, &c.Node, &c.Positive, &c.Negative} {
		if err := json.Unmarshal(a[i], dest); err != nil {
			return err
		}
	}
	return nil
}

type Counter struct {
	Components []CounterComponent `json:"components"`
}

func (c *Counter) Add(database, node NodeID, delta int64) error {
	return invoke(0, "helper_policy", map[string]any{"kind": "Counter", "method": "add", "components": nonNil(c.Components), "database": database, "node": node, "delta": delta}, &c.Components)
}
func (c *Counter) Join(other Counter) error {
	return invoke(0, "helper_policy", map[string]any{"kind": "Counter", "method": "join", "components": nonNil(c.Components), "other": nonNil(other.Components)}, &c.Components)
}
func (c Counter) Value() (int64, error) {
	var n int64
	err := invoke(0, "helper_policy", map[string]any{"kind": "Counter", "method": "value", "components": nonNil(c.Components)}, &n)
	return n, err
}

type Tag struct {
	Database    NodeID `json:"database"`
	Node        NodeID `json:"node"`
	Transaction NodeID `json:"transaction"`
	Operation   uint32 `json:"operation"`
}
type SetAddition struct {
	Value Bytes
	Tags  []Tag
}

func (a SetAddition) MarshalJSON() ([]byte, error) {
	return json.Marshal([2]any{a.Value, nonNil(a.Tags)})
}
func (a *SetAddition) UnmarshalJSON(b []byte) error {
	var p []json.RawMessage
	if err := json.Unmarshal(b, &p); err != nil {
		return err
	}
	if len(p) != 2 {
		return invalidError("set addition")
	}
	if err := json.Unmarshal(p[0], &a.Value); err != nil {
		return err
	}
	return json.Unmarshal(p[1], &a.Tags)
}

type Set struct {
	Additions []SetAddition `json:"additions"`
	Removals  []Tag         `json:"removals"`
}

func (s Set) state() Set {
	s.Additions = nonNil(s.Additions)
	s.Removals = nonNil(s.Removals)
	return s
}
func (s *Set) Add(value []byte, tag Tag) error {
	return invoke(0, "helper_policy", map[string]any{"kind": "Set", "method": "add", "state": s.state(), "value": Bytes(value), "tag": tag}, s)
}
func (s *Set) Remove(value []byte) error {
	return invoke(0, "helper_policy", map[string]any{"kind": "Set", "method": "remove", "state": s.state(), "value": Bytes(value)}, s)
}
func (s *Set) Join(other Set) error {
	return invoke(0, "helper_policy", map[string]any{"kind": "Set", "method": "join", "state": s.state(), "other": other.state()}, s)
}
func (s Set) Values() ([][]byte, error) {
	var values []Bytes
	err := invoke(0, "helper_policy", map[string]any{"kind": "Set", "method": "values", "state": s.state()}, &values)
	out := make([][]byte, len(values))
	for i, v := range values {
		out[i] = v
	}
	return out, err
}

type Causal struct {
	Counter *Counter
	Set     *Set
}

func (c *Causal) Join(other Causal) error {
	if c.Counter != nil && other.Counter != nil && c.Set == nil && other.Set == nil {
		return c.Counter.Join(*other.Counter)
	}
	if c.Set != nil && other.Set != nil && c.Counter == nil && other.Counter == nil {
		return c.Set.Join(*other.Set)
	}
	return invalidError("merge policy changed")
}

func (c Counter) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Components []CounterComponent `json:"components"`
	}{nonNil(c.Components)})
}
func (s Set) MarshalJSON() ([]byte, error) { type plain Set; return json.Marshal(plain(s.state())) }
