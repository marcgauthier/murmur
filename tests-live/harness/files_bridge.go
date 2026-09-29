package harness

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"
)

// FilesOptions enables replicated files on every cluster node. The object
// key is shared by the cluster's mesh; different clusters (Low/High) must
// use different keys.
type FilesOptions struct {
	ObjectKeyHex    string
	FetchIntervalMs int64
	MaxFileBytes    int64
}

// BridgeOptions configures bridge endpoints in the cluster (Low exporters
// or High importers) on the indexed nodes. Key files come from
// GenerateBridgeKeys.
type BridgeOptions struct {
	Role             string // "low-exporter" or "high-importer"
	Stream           string
	NodeIndex        int
	NodeIndices      []int
	StagingDir       string
	SignerKeyFile    string
	RecipientPubFile string
	RecipientKeyFile string
	SignerPubFile    string
	SignerPubFiles   []string
}

// BridgeNodes returns the configured bridge node indices.
func (b *BridgeOptions) BridgeNodes() []int {
	if len(b.NodeIndices) > 0 {
		return b.NodeIndices
	}
	return []int{b.NodeIndex}
}

// BridgeKeyFiles locates generated bridge key material.
type BridgeKeyFiles struct {
	Dir              string
	SignerKeyFile    string
	SignerPubFile    string
	RecipientKeyFile string
	RecipientPubFile string
}

// GenerateBridgeKeys creates a signer identity and a recipient identity,
// writing private and public parts as raw binary files.
func GenerateBridgeKeys(t *testing.T, dir string) BridgeKeyFiles {
	t.Helper()
	keys, err := generateBridgeKeys(dir)
	if err != nil {
		t.Fatalf("generate bridge keys: %v", err)
	}
	return keys
}

// FileStatus mirrors the daemon /v1/files/status response.
type FileStatus struct {
	Name      string `json:"name"`
	Digest    string `json:"digest"`
	Size      int64  `json:"size"`
	Chunks    uint64 `json:"chunks"`
	Exists    bool   `json:"exists"`
	Deleted   bool   `json:"deleted"`
	Available bool   `json:"available"`
}

// UploadFile posts bytes under name to one node.
func (c *Cluster) UploadFile(idx int, name string, data []byte) (FileStatus, error) {
	var st FileStatus
	node := c.Nodes[idx]
	payload, _ := json.Marshal(map[string]any{
		"name":        name,
		"data_base64": base64.StdEncoding.EncodeToString(data),
	})
	resp, err := http.Post(fmt.Sprintf("https://%s/v1/files/upload", node.APIAddr), "application/json", bytes.NewReader(payload))
	if err != nil {
		return st, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return st, fmt.Errorf("node %s upload failed (%d): %s", node.Label, resp.StatusCode, string(b))
	}
	if err := json.NewDecoder(resp.Body).Decode(&st); err != nil {
		return st, err
	}
	return st, nil
}

// DownloadFile fetches verified file bytes from one node.
func (c *Cluster) DownloadFile(idx int, name string) ([]byte, error) {
	node := c.Nodes[idx]
	resp, err := http.Get(fmt.Sprintf("https://%s/v1/files/download?name=%s", node.APIAddr, name))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("node %s download failed (%d): %s", node.Label, resp.StatusCode, string(b))
	}
	return io.ReadAll(io.LimitReader(resp.Body, 1<<30))
}

// StatFile reports replicated metadata plus local availability.
func (c *Cluster) StatFile(idx int, name string) (FileStatus, error) {
	var st FileStatus
	node := c.Nodes[idx]
	resp, err := http.Get(fmt.Sprintf("https://%s/v1/files/status?name=%s", node.APIAddr, name))
	if err != nil {
		return st, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return st, fmt.Errorf("node %s status failed (%d): %s", node.Label, resp.StatusCode, string(b))
	}
	if err := json.NewDecoder(resp.Body).Decode(&st); err != nil {
		return st, err
	}
	return st, nil
}

// SearchFiles lists files by prefix or substring.
func (c *Cluster) SearchFiles(idx int, prefix, substr string) ([]FileStatus, error) {
	node := c.Nodes[idx]
	url := fmt.Sprintf("https://%s/v1/files/search?prefix=%s&substr=%s", node.APIAddr, prefix, substr)
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("node %s search failed (%d): %s", node.Label, resp.StatusCode, string(b))
	}
	var res struct {
		Files []FileStatus `json:"files"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return nil, err
	}
	return res.Files, nil
}

// DeleteFile replicates a file tombstone from one node.
func (c *Cluster) DeleteFile(idx int, name string) error {
	node := c.Nodes[idx]
	req, err := http.NewRequest(http.MethodDelete, fmt.Sprintf("https://%s/v1/files/delete?name=%s", node.APIAddr, name), nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("node %s delete failed (%d): %s", node.Label, resp.StatusCode, string(b))
	}
	return nil
}

// FetchFile triggers one on-demand mesh fetch on a node.
func (c *Cluster) FetchFile(idx int, name string) (FileStatus, error) {
	var st FileStatus
	node := c.Nodes[idx]
	payload, _ := json.Marshal(map[string]string{"name": name})
	resp, err := http.Post(fmt.Sprintf("https://%s/v1/files/fetch", node.APIAddr), "application/json", bytes.NewReader(payload))
	if err != nil {
		return st, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return st, fmt.Errorf("node %s fetch failed (%d): %s", node.Label, resp.StatusCode, string(b))
	}
	if err := json.NewDecoder(resp.Body).Decode(&st); err != nil {
		return st, err
	}
	return st, nil
}

// FetchStats mirrors the daemon /v1/files/fetch-stats response.
type FetchStats struct {
	Pending      int64  `json:"pending"`
	InFlight     int64  `json:"in_flight"`
	Completed    uint64 `json:"completed"`
	Failed       uint64 `json:"failed"`
	BytesFetched uint64 `json:"bytes_fetched"`
	LastError    string `json:"last_error"`
}

// FetchStats returns one node's file fetch counters.
func (c *Cluster) FetchStats(idx int) (FetchStats, error) {
	var st FetchStats
	node := c.Nodes[idx]
	resp, err := http.Get(fmt.Sprintf("https://%s/v1/files/fetch-stats", node.APIAddr))
	if err != nil {
		return st, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return st, fmt.Errorf("node %s fetch-stats failed (%d): %s", node.Label, resp.StatusCode, string(b))
	}
	if err := json.NewDecoder(resp.Body).Decode(&st); err != nil {
		return st, err
	}
	return st, nil
}

// WaitFileAvailable polls until a file's availability matches (or times out).
func (c *Cluster) WaitFileAvailable(idx int, name string, wantAvailable bool, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		st, err := c.StatFile(idx, name)
		if err != nil {
			return err
		}
		if st.Exists && !st.Deleted && st.Available == wantAvailable {
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("node %s timed out waiting for %q (available=%v)", c.Nodes[idx].Label, name, wantAvailable)
}

// WaitFileDeleted polls until a file tombstone is visible (or times out).
func (c *Cluster) WaitFileDeleted(idx int, name string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		st, err := c.StatFile(idx, name)
		if err != nil {
			return err
		}
		if st.Exists && st.Deleted {
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("node %s timed out waiting for %q deletion", c.Nodes[idx].Label, name)
}

// BridgeExport captures and publishes pending Low bundles plus file chunks.
func (c *Cluster) BridgeExport(idx int) (map[string]any, error) {
	node := c.Nodes[idx]
	resp, err := http.Post(fmt.Sprintf("https://%s/v1/admin/bridge/export", node.APIAddr), "application/json", bytes.NewReader([]byte("{}")))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 8192))
		return nil, fmt.Errorf("node %s bridge export failed (%d): %s", node.Label, resp.StatusCode, string(b))
	}
	var res map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return nil, err
	}
	return res, nil
}

// BridgeImport receives staged artifacts and drains them into High.
func (c *Cluster) BridgeImport(idx int) (map[string]any, error) {
	node := c.Nodes[idx]
	resp, err := http.Post(fmt.Sprintf("https://%s/v1/admin/bridge/import", node.APIAddr), "application/json", bytes.NewReader([]byte("{}")))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 8192))
		return nil, fmt.Errorf("node %s bridge import failed (%d): %s", node.Label, resp.StatusCode, string(b))
	}
	var res map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return nil, err
	}
	return res, nil
}

// BridgeExportOptions selects capture-only (queue without publishing).
type BridgeExportOptions struct {
	CaptureOnly bool
}

// BridgeExportWithOptions captures and optionally publishes pending bundles.
func (c *Cluster) BridgeExportWithOptions(idx int, opts BridgeExportOptions) (map[string]any, error) {
	node := c.Nodes[idx]
	payload, _ := json.Marshal(map[string]any{"capture_only": opts.CaptureOnly})
	resp, err := http.Post(fmt.Sprintf("https://%s/v1/admin/bridge/export", node.APIAddr), "application/json", bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 8192))
		return nil, fmt.Errorf("node %s bridge export failed (%d): %s", node.Label, resp.StatusCode, string(b))
	}
	var res map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return nil, err
	}
	return res, nil
}

// BridgeStatus returns the bridge outbox/inbox progress snapshot.
func (c *Cluster) BridgeStatus(idx int) (map[string]any, error) {
	node := c.Nodes[idx]
	resp, err := http.Get(fmt.Sprintf("https://%s/v1/admin/bridge/status", node.APIAddr))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("node %s bridge status failed (%d): %s", node.Label, resp.StatusCode, string(b))
	}
	var res map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return nil, err
	}
	return res, nil
}

// Provenance returns field ownership (present, owner "low"/"high").
func (c *Cluster) Provenance(idx int, table, rowHex, column string) (bool, string, error) {
	node := c.Nodes[idx]
	url := fmt.Sprintf("https://%s/v1/admin/bridge/provenance?table=%s&row=%s&column=%s", node.APIAddr, table, rowHex, column)
	resp, err := http.Get(url)
	if err != nil {
		return false, "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return false, "", fmt.Errorf("node %s provenance failed (%d): %s", node.Label, resp.StatusCode, string(b))
	}
	var res struct {
		Present bool   `json:"present"`
		Owner   string `json:"owner"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return false, "", err
	}
	return res.Present, res.Owner, nil
}

// ReleaseOwnership returns a High-overridden field to Low ownership.
func (c *Cluster) ReleaseOwnership(idx int, table, rowHex, column string) error {
	node := c.Nodes[idx]
	payload, _ := json.Marshal(map[string]string{"table": table, "column": column, "row_hex": rowHex})
	resp, err := http.Post(fmt.Sprintf("https://%s/v1/admin/bridge/release", node.APIAddr), "application/json", bytes.NewReader(payload))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("node %s release failed (%d): %s", node.Label, resp.StatusCode, string(b))
	}
	return nil
}

// Migrate applies a schema migration (full table list) on one node.
func (c *Cluster) Migrate(idx int, tables any) error {
	node := c.Nodes[idx]
	payload, _ := json.Marshal(map[string]any{"tables": tables})
	resp, err := http.Post(fmt.Sprintf("https://%s/v1/admin/migrate", node.APIAddr), "application/json", bytes.NewReader(payload))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 8192))
		return fmt.Errorf("node %s migrate failed (%d): %s", node.Label, resp.StatusCode, string(b))
	}
	return nil
}

// RotateKey rotates the node's storage key without restarting.
func (c *Cluster) RotateKey(idx int, keyID, keyHex, algorithm string) error {
	node := c.Nodes[idx]
	payload, _ := json.Marshal(map[string]string{"key_id": keyID, "key_hex": keyHex, "algorithm": algorithm})
	resp, err := http.Post(fmt.Sprintf("https://%s/v1/admin/rotate-key", node.APIAddr), "application/json", bytes.NewReader(payload))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("node %s rotate-key failed (%d): %s", node.Label, resp.StatusCode, string(b))
	}
	return nil
}

// EncryptionStatus returns the node's encryption status snapshot.
func (c *Cluster) EncryptionStatus(idx int) (map[string]any, error) {
	node := c.Nodes[idx]
	resp, err := http.Get(fmt.Sprintf("https://%s/v1/admin/encryption-status", node.APIAddr))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("node %s encryption-status failed (%d): %s", node.Label, resp.StatusCode, string(b))
	}
	var res map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return nil, err
	}
	return res, nil
}
