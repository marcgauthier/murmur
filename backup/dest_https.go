package backup

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// HTTPSDestination implements Destination for generic HTTPS and S3-compatible endpoints.
type HTTPSDestination struct {
	// BaseURL is the target HTTPS root or bucket URL (e.g. https://s3.us-east-1.amazonaws.com/my-bucket/backups).
	BaseURL string
	// AuthBearer is an optional Authorization: Bearer <token>.
	AuthBearer string
	// Headers specifies additional HTTP headers (e.g. X-API-Key, S3 headers).
	Headers map[string]string
	// Client is the underlying HTTP client.
	Client *http.Client
}

// HTTPSOptions configures an HTTPS destination.
type HTTPSOptions struct {
	BaseURL     string
	AuthBearer  string
	Headers     map[string]string
	TLSConfig   *tls.Config
	Timeout     time.Duration
}

// NewHTTPSDestination creates a new HTTPS destination.
func NewHTTPSDestination(opt HTTPSOptions) (*HTTPSDestination, error) {
	if opt.BaseURL == "" {
		return nil, fmt.Errorf("backup: https BaseURL cannot be empty")
	}
	u, err := url.Parse(opt.BaseURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") {
		return nil, fmt.Errorf("backup: invalid https url %q", opt.BaseURL)
	}

	timeout := opt.Timeout
	if timeout == 0 {
		timeout = 30 * time.Minute // Large backups may stream for tens of minutes
	}

	transport := http.DefaultTransport.(*http.Transport).Clone()
	if opt.TLSConfig != nil {
		transport.TLSClientConfig = opt.TLSConfig
	}

	return &HTTPSDestination{
		BaseURL:    strings.TrimRight(opt.BaseURL, "/"),
		AuthBearer: opt.AuthBearer,
		Headers:    opt.Headers,
		Client: &http.Client{
			Transport: transport,
			Timeout:   timeout,
		},
	}, nil
}

func (h *HTTPSDestination) Type() string { return "https" }

func (h *HTTPSDestination) itemURL(name string) string {
	return h.BaseURL + "/" + url.PathEscape(name)
}

// WriteBackup streams the backup archive to the remote HTTPS endpoint via HTTP PUT.
func (h *HTTPSDestination) WriteBackup(ctx context.Context, name string, r io.Reader, sizeHint int64) error {
	destURL := h.itemURL(name)
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, destURL, r)
	if err != nil {
		return fmt.Errorf("backup: create https request: %w", err)
	}

	if sizeHint > 0 {
		req.ContentLength = sizeHint
	}
	req.Header.Set("Content-Type", "application/gzip")
	if h.AuthBearer != "" {
		req.Header.Set("Authorization", "Bearer "+h.AuthBearer)
	}
	for k, v := range h.Headers {
		req.Header.Set(k, v)
	}

	resp, err := h.Client.Do(req)
	if err != nil {
		return fmt.Errorf("backup: https upload to %s: %w", destURL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		bodySnippet, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
			return fmt.Errorf("%w: %s (status %d)", ErrUnauthenticated, string(bodySnippet), resp.StatusCode)
		}
		return fmt.Errorf("backup: upload failed with HTTP %d: %s", resp.StatusCode, string(bodySnippet))
	}
	return nil
}

// ReadBackup opens an HTTP GET stream from the remote HTTPS endpoint.
func (h *HTTPSDestination) ReadBackup(ctx context.Context, name string) (io.ReadCloser, error) {
	destURL := h.itemURL(name)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, destURL, nil)
	if err != nil {
		return nil, fmt.Errorf("backup: create https get request: %w", err)
	}

	if h.AuthBearer != "" {
		req.Header.Set("Authorization", "Bearer "+h.AuthBearer)
	}
	for k, v := range h.Headers {
		req.Header.Set(k, v)
	}

	resp, err := h.Client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("backup: download from %s: %w", destURL, err)
	}

	if resp.StatusCode == http.StatusNotFound {
		resp.Body.Close()
		return nil, fmt.Errorf("%w: %s", ErrDestinationNotFound, name)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		bodySnippet, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		resp.Body.Close()
		return nil, fmt.Errorf("backup: download failed with HTTP %d: %s", resp.StatusCode, string(bodySnippet))
	}

	return resp.Body, nil
}

// ListBackups queries the endpoint index (JSON array of BackupInfo) or lists backups matching dbID.
func (h *HTTPSDestination) ListBackups(ctx context.Context, dbID string) ([]BackupInfo, error) {
	reqURL := h.BaseURL
	if dbID != "" {
		reqURL += "?dbid=" + url.QueryEscape(dbID)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	if h.AuthBearer != "" {
		req.Header.Set("Authorization", "Bearer "+h.AuthBearer)
	}

	resp, err := h.Client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("backup: list backups: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("backup: list returned HTTP %d", resp.StatusCode)
	}

	var items []BackupInfo
	if err := json.NewDecoder(resp.Body).Decode(&items); err != nil {
		return nil, fmt.Errorf("backup: decode backup list: %w", err)
	}
	return items, nil
}

// DeleteBackup issues an HTTP DELETE request to delete a backup.
func (h *HTTPSDestination) DeleteBackup(ctx context.Context, name string) error {
	destURL := h.itemURL(name)
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, destURL, nil)
	if err != nil {
		return err
	}
	if h.AuthBearer != "" {
		req.Header.Set("Authorization", "Bearer "+h.AuthBearer)
	}
	for k, v := range h.Headers {
		req.Header.Set(k, v)
	}

	resp, err := h.Client.Do(req)
	if err != nil {
		return fmt.Errorf("backup: delete %s: %w", destURL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusNotFound {
		return fmt.Errorf("backup: delete returned HTTP %d", resp.StatusCode)
	}
	return nil
}
