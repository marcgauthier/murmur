package murmur

import "encoding/json"

type Bridge struct{ object }

func ExporterConfig(domain NodeID, directory, stream string, signer, recipient Key) BridgeConfig {
	var c BridgeConfig
	if err := invoke(0, "helper_bridge_exporter_defaults", map[string]any{"domain": domain, "directory": directory, "stream": stream, "signer": signer, "recipient": recipient}, &c); err != nil {
		panic(err)
	}
	return c
}
func ReceiverConfig(domain NodeID, directory, stream string, secret Key, trustedDomains []NodeID, trustedSigners, trustedOrigins []TrustEntry) (BridgeConfig, error) {
	var c BridgeConfig
	err := invoke(0, "helper_bridge_receiver_defaults", map[string]any{"domain": domain, "directory": directory, "stream": stream, "receiver_secret": secret, "trusted_domains": nonNil(trustedDomains), "trusted_signers": nonNil(trustedSigners), "trusted_origins": nonNil(trustedOrigins)}, &c)
	return c, err
}
func OpenBridge(config BridgeConfig, backend *BackendHandle) (*Bridge, error) {
	id, err := backend.handle()
	if err != nil {
		return nil, err
	}
	var result uint64
	if err = invoke(0, "bridge_open", map[string]any{"config": config, "backend": id}, &result); err != nil {
		return nil, err
	}
	b := &Bridge{}
	b.set(result)
	return b, nil
}
func (b *Bridge) Close() error { return b.object.close("release") }
func (b *Bridge) AttachFiles(files *Objects) error {
	var id *uint64
	if files != nil {
		h, err := files.handle()
		if err != nil {
			return err
		}
		id = &h
	}
	return b.call("attach_files", map[string]any{"files": id}, nil)
}
func (b *Bridge) Capture() (uint64, error) {
	var n uint64
	err := b.call("capture", nil, &n)
	return n, err
}
func (b *Bridge) Process() (uint64, error) {
	var n uint64
	err := b.call("process", nil, &n)
	return n, err
}
func (b *Bridge) Export(payload []byte) (string, error) {
	var path string
	err := b.call("export", map[string]any{"payload": Bytes(payload)}, &path)
	return path, err
}
func (b *Bridge) ExportSnapshot() (string, error) {
	var path string
	err := b.call("export_snapshot", nil, &path)
	return path, err
}
func (b *Bridge) ExportFile(name string) error {
	return b.call("export_file", map[string]any{"name": name}, nil)
}
func (b *Bridge) OpenBundle(bytes []byte) (BridgeHeader, []byte, error) {
	var parts []json.RawMessage
	if err := b.call("open_bundle", map[string]any{"bytes": Bytes(bytes)}, &parts); err != nil {
		return BridgeHeader{}, nil, err
	}
	if len(parts) != 2 {
		return BridgeHeader{}, nil, invalidError("bundle result")
	}
	var header BridgeHeader
	var payload Bytes
	if err := json.Unmarshal(parts[0], &header); err != nil {
		return header, nil, err
	}
	err := json.Unmarshal(parts[1], &payload)
	return header, payload, err
}
func (b *Bridge) Stage(path string) (BridgeHeader, error) {
	var h BridgeHeader
	err := b.call("stage", map[string]any{"path": path}, &h)
	return h, err
}
func (b *Bridge) ReleaseOwnership(table string, key []byte, column *string) (bool, error) {
	var changed bool
	err := b.call("release_ownership", map[string]any{"table": table, "key": Bytes(key), "column": column}, &changed)
	return changed, err
}
func (b *Bridge) PublishHTTP(path, url string) error {
	return b.call("publish_http", map[string]any{"path": path, "url": url}, nil)
}
func (b *Bridge) PublishFTP(path, url string, tls *FTPOptions) error {
	return b.call("publish_ftp", map[string]any{"path": path, "url": url, "tls": tls}, nil)
}
