package service

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Client is the remote SDK for the service adapter. It consumes the HTTP
// interface; it does not replace the embedded Go API.
type Client struct {
	base   string
	token  string
	client *http.Client
}

// NewClient builds a client for baseURL (e.g. https://host:port) with the
// shared Bearer token. tlsCfg may carry the server CA (or test roots); nil
// uses the host roots. Plain-http URLs are rejected: the adapter is
// TLS-only.
func NewClient(baseURL string, token []byte, tlsCfg *tls.Config) (*Client, error) {
	u, err := url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("service: invalid base URL: %w", err)
	}
	if u.Scheme != "https" {
		return nil, fmt.Errorf("service: base URL must use https")
	}
	if len(token) == 0 {
		return nil, fmt.Errorf("service: Bearer [REDACTED] is required")
	}
	transport, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		return nil, fmt.Errorf("service: unexpected default transport")
	}
	transport = transport.Clone()
	transport.TLSClientConfig = tlsCfg
	return &Client{
		base:   strings.TrimSuffix(baseURL, "/"),
		token:  string(token),
		client: &http.Client{Transport: transport, Timeout: 30 * time.Second},
	}, nil
}

func (c *Client) post(ctx context.Context, path string, body, out any) error {
	raw, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+path, bytes.NewReader(raw))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.token)
	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return responseError(resp)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func responseError(resp *http.Response) error {
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
	var envelope map[string]string
	if err := json.Unmarshal(raw, &envelope); err == nil {
		if msg, ok := envelope["error"]; ok {
			return fmt.Errorf("service: %s (http %d)", msg, resp.StatusCode)
		}
	}
	return fmt.Errorf("service: unexpected status %d", resp.StatusCode)
}

// Status fetches the server status snapshot.
func (c *Client) Status(ctx context.Context) (*StatusDTO, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+"/v1/status", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, responseError(resp)
	}
	var out StatusDTO
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	return &out, nil
}

// Query runs a read-only statement and returns decoded rows.
func (c *Client) Query(ctx context.Context, query string, args ...any) ([]string, [][]any, error) {
	vals := make([]Value, len(args))
	for i, a := range args {
		v, err := ToValue(a)
		if err != nil {
			return nil, nil, err
		}
		vals[i] = v
	}
	var out queryResponse
	if err := c.post(ctx, "/v1/query", sqlRequest{Query: query, Args: vals}, &out); err != nil {
		return nil, nil, err
	}
	rows := make([][]any, len(out.Rows))
	for i, r := range out.Rows {
		dec, err := ValuesToAny(r)
		if err != nil {
			return nil, nil, err
		}
		rows[i] = dec
	}
	return out.Columns, rows, nil
}

// ExecResult reports an exec outcome.
type ExecResult struct {
	LastInsertID int64
	RowsAffected int64
}

// Exec runs a statement through the server's implicit-transaction path.
func (c *Client) Exec(ctx context.Context, query string, args ...any) (*ExecResult, error) {
	vals := make([]Value, len(args))
	for i, a := range args {
		v, err := ToValue(a)
		if err != nil {
			return nil, err
		}
		vals[i] = v
	}
	var out execResponse
	if err := c.post(ctx, "/v1/exec", sqlRequest{Query: query, Args: vals}, &out); err != nil {
		return nil, err
	}
	return &ExecResult{LastInsertID: out.LastInsertID, RowsAffected: out.RowsAffected}, nil
}

// Event is one decoded subscription event.
type Event struct {
	Type    string
	Cursor  uint64
	Columns []string
	Rows    [][]any
}

// SubscribeOptions mirrors the server subscription parameters.
type SubscribeOptions struct {
	Args          []any
	ResumeCursor  uint64
	EmitUnchanged bool
}

// Subscribe opens a server-sent-events subscription. Events arrive on the
// returned channel until ctx ends or the server terminates the stream, at
// which point the channel closes. A server-side error arrives as a Go error
// from Err (exactly one terminal value; nil means clean end-of-stream).
func (c *Client) Subscribe(ctx context.Context, query string, opts SubscribeOptions) (<-chan Event, <-chan error) {
	events := make(chan Event, 16)
	errCh := make(chan error, 1)
	go func() {
		defer close(events)
		defer close(errCh)
		u, err := url.Parse(c.base + "/v1/subscribe")
		if err != nil {
			errCh <- err
			return
		}
		q := u.Query()
		q.Set("query", query)
		if len(opts.Args) > 0 {
			vals := make([]Value, len(opts.Args))
			for i, a := range opts.Args {
				v, err := ToValue(a)
				if err != nil {
					errCh <- err
					return
				}
				vals[i] = v
			}
			raw, err := json.Marshal(vals)
			if err != nil {
				errCh <- err
				return
			}
			q.Set("args", string(raw))
		}
		if opts.ResumeCursor != 0 {
			q.Set("resume", strconv.FormatUint(opts.ResumeCursor, 10))
		}
		if opts.EmitUnchanged {
			q.Set("emit_unchanged", "true")
		}
		u.RawQuery = q.Encode()
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
		if err != nil {
			errCh <- err
			return
		}
		req.Header.Set("Authorization", "Bearer "+c.token)
		req.Header.Set("Accept", "text/event-stream")
		resp, err := c.client.Do(req)
		if err != nil {
			errCh <- err
			return
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			errCh <- responseError(resp)
			return
		}
		sc := bufio.NewScanner(resp.Body)
		sc.Buffer(make([]byte, 64<<10), 4<<20)
		for sc.Scan() {
			line := sc.Text()
			if !strings.HasPrefix(line, "data: ") {
				continue
			}
			var ev streamEvent
			if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &ev); err != nil {
				errCh <- fmt.Errorf("service: decode event: %w", err)
				return
			}
			if ev.Type == "error" || ev.Error != "" {
				msg := ev.Error
				if msg == "" {
					msg = "subscription error"
				}
				errCh <- fmt.Errorf("service: %s", msg)
				return
			}
			rows := make([][]any, len(ev.Rows))
			for i, r := range ev.Rows {
				dec, err := ValuesToAny(r)
				if err != nil {
					errCh <- err
					return
				}
				rows[i] = dec
			}
			select {
			case events <- Event{Type: ev.Type, Cursor: ev.Cursor, Columns: ev.Columns, Rows: rows}:
			case <-ctx.Done():
				errCh <- ctx.Err()
				return
			}
		}
		if err := sc.Err(); err != nil && ctx.Err() == nil {
			errCh <- fmt.Errorf("service: stream read: %w", err)
			return
		}
		errCh <- nil
	}()
	return events, errCh
}
