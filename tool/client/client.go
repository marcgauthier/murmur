package client

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/marcgauthier/murmur/schema"
)

// Client interacts with a running Murmur node's HTTP / mTLS API.
type Client struct {
	baseURL    string
	httpClient *http.Client
}

// ClientOptions configures the remote connection.
type ClientOptions struct {
	BaseURL  string
	CertFile string
	KeyFile  string
	CAFile   string
	Insecure bool
	Timeout  time.Duration
}

// NewClient creates a new Murmur API client.
func NewClient(opts ClientOptions) (*Client, error) {
	baseURL := strings.TrimRight(opts.BaseURL, "/")
	if !strings.HasPrefix(baseURL, "http://") && !strings.HasPrefix(baseURL, "https://") {
		baseURL = "https://" + baseURL
	}

	tlsConfig := &tls.Config{
		MinVersion:         tls.VersionTLS13,
		InsecureSkipVerify: opts.Insecure,
	}

	if opts.CAFile != "" {
		caCert, err := os.ReadFile(opts.CAFile)
		if err != nil {
			return nil, fmt.Errorf("read CA cert: %w", err)
		}
		caCertPool := x509.NewCertPool()
		if !caCertPool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("failed to parse CA certificate from %s", opts.CAFile)
		}
		tlsConfig.RootCAs = caCertPool
	}

	if opts.CertFile != "" && opts.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(opts.CertFile, opts.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("load client key pair: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	timeout := opts.Timeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}

	transport := &http.Transport{
		TLSClientConfig: tlsConfig,
	}

	return &Client{
		baseURL: baseURL,
		httpClient: &http.Client{
			Transport: transport,
			Timeout:   timeout,
		},
	}, nil
}

// Status retrieves node status diagnostics.
func (c *Client) Status(ctx context.Context) (map[string]any, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/v1/status", nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("status request failed (HTTP %d): %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var result map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode status response: %w", err)
	}
	return result, nil
}

// AdminStatus retrieves detailed admin node diagnostics.
func (c *Client) AdminStatus(ctx context.Context) (map[string]any, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/v1/admin/status", nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		// Fallback to /v1/status if /v1/admin/status is not available
		return c.Status(ctx)
	}

	var result map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode admin status response: %w", err)
	}
	return result, nil
}

// TriggerGC asks the node to collect replication history permitted by its
// persisted peer acknowledgements and retention settings.
func (c *Client) TriggerGC(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/v1/admin/gc", nil)
	if err != nil {
		return err
	}
	// Collection can drain a large retained log in multiple bounded passes;
	// retain caller cancellation while allowing longer maintenance than the
	// short default timeout used by diagnostic reads.
	gcClient := *c.httpClient
	gcClient.Timeout = 5 * time.Minute
	resp, err := gcClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("garbage-collection request failed (HTTP %d): %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return nil
}

// SchemaTables retrieves the durable schema manifest from a running node.
func (c *Client) SchemaTables(ctx context.Context) ([]schema.TableSchema, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/v1/schema", nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("schema request failed (HTTP %d): %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var tables []schema.TableSchema
	if err := json.NewDecoder(resp.Body).Decode(&tables); err != nil {
		return nil, fmt.Errorf("decode schema response: %w", err)
	}
	return tables, nil
}

// DebugPeers queries debug peers list.
func (c *Client) DebugPeers(ctx context.Context) ([]map[string]any, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/v1/debug/peers", nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("debug peers failed (HTTP %d): %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var peers []map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&peers); err != nil {
		return nil, fmt.Errorf("decode peers response: %w", err)
	}
	return peers, nil
}
