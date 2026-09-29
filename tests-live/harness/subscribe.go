package harness

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// StreamValue mirrors one service JSON value in subscription events.
type StreamValue struct {
	Type string  `json:"t"`
	I    int64   `json:"i"`
	F    float64 `json:"f"`
	S    string  `json:"s"`
	B    string  `json:"b"`
	Bool bool    `json:"v"`
}

// StreamEvent is one decoded server-sent subscription event.
type StreamEvent struct {
	Type    string          `json:"type"`
	Cursor  uint64          `json:"cursor"`
	Columns []string        `json:"columns"`
	Rows    [][]StreamValue `json:"rows"`
	Error   string          `json:"error"`
}

// Subscription is a live SSE subscription against one node.
type Subscription struct {
	events chan StreamEvent
	cancel context.CancelFunc
}

// Events delivers decoded subscription events until Close.
func (s *Subscription) Events() <-chan StreamEvent { return s.events }

// Close terminates the subscription.
func (s *Subscription) Close() { s.cancel() }

// Subscribe opens a server-sent-events subscription on one node. The
// daemon streams the same wire format as the TLS service handler.
func (c *Cluster) Subscribe(idx int, query string) (*Subscription, error) {
	node := c.Nodes[idx]
	u := fmt.Sprintf("https://%s/v1/subscribe?%s", node.APIAddr,
		url.Values{"query": {query}}.Encode())
	ctx, cancel := context.WithCancel(context.Background())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		cancel()
		return nil, err
	}
	req.Header.Set("Accept", "text/event-stream")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		cancel()
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		cancel()
		return nil, fmt.Errorf("node %s subscribe failed (%d)", node.Label, resp.StatusCode)
	}
	sub := &Subscription{events: make(chan StreamEvent, 16), cancel: cancel}
	go func() {
		defer close(sub.events)
		defer resp.Body.Close()
		sc := bufio.NewScanner(resp.Body)
		sc.Buffer(make([]byte, 64<<10), 4<<20)
		for sc.Scan() {
			line := sc.Text()
			if !strings.HasPrefix(line, "data: ") {
				continue
			}
			var ev StreamEvent
			if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &ev); err != nil {
				return
			}
			select {
			case sub.events <- ev:
			case <-ctx.Done():
				return
			}
		}
	}()
	return sub, nil
}

// NextStreamEvent returns the next subscription event or fails on timeout.
func NextStreamEvent(t interface {
	Helper()
	Fatalf(string, ...any)
}, sub *Subscription, timeout time.Duration,
) StreamEvent {
	t.Helper()
	select {
	case ev, ok := <-sub.Events():
		if !ok {
			t.Fatalf("subscription closed")
		}
		return ev
	case <-time.After(timeout):
		t.Fatalf("timed out waiting for subscription event")
	}
	return StreamEvent{}
}
