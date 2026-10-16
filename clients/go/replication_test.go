package murmur

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math/big"
	"net/url"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func clusterConfigs(t *testing.T, ids ...NodeID) []ReplicationConfig {
	t.Helper()
	caPublic, caPrivate, err := ed25519.GenerateKey(rand.Reader)
	must(t, err)
	now := time.Now()
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "Murmur Go test CA"}, NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	caDER, err := x509.CreateCertificate(rand.Reader, ca, ca, caPublic, caPrivate)
	must(t, err)
	var configs []ReplicationConfig
	for i, id := range ids {
		public, private, err := ed25519.GenerateKey(rand.Reader)
		must(t, err)
		uri, err := url.Parse("replicateddb://node/" + hex.EncodeToString(id[:]))
		must(t, err)
		cert := &x509.Certificate{SerialNumber: big.NewInt(int64(i + 2)), Subject: pkix.Name{CommonName: "Murmur Go test node"}, NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour), URIs: []*url.URL{uri}, KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth}}
		der, err := x509.CreateCertificate(rand.Reader, cert, ca, public, caPrivate)
		must(t, err)
		keyDER, err := x509.MarshalPKCS8PrivateKey(private)
		must(t, err)
		seed := Key{byte(i + 20)}
		config, err := NewReplicationConfig("127.0.0.1:0", Credentials{Chain: []Bytes{der}, PrivateKey: keyDER, Roots: []Bytes{caDER}}, seed)
		must(t, err)
		config.IncomingInterval = 10 * time.Millisecond
		config.AntiEntropyInterval = 30 * time.Millisecond
		configs = append(configs, config)
	}
	for i := range configs {
		for j, id := range ids {
			public, err := PublicKey(configs[j].SigningKey)
			must(t, err)
			configs[i].TrustedOrigins = append(configs[i].TrustedOrigins, TrustEntry{Node: id, Key: public})
			if i != j {
				configs[i].AllowedPeers = append(configs[i].AllowedPeers, id)
				configs[i].TrustedSnapshotSources = append(configs[i].TrustedSnapshotSources, id)
			}
		}
	}
	return configs
}
func openNetworkDB(t *testing.T, path string, config ReplicationConfig, database, node NodeID) *DB {
	t.Helper()
	options := DefaultOptions()
	options.Mode = CreateNew
	options.DatabaseID = &database
	options.NodeID = &node
	options.Replication = &config
	db, err := Open(path, testKey(), options)
	must(t, err)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	return db
}
func waitFor(t *testing.T, predicate func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if predicate() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("condition did not converge")
}
func TestNetworkPoliciesFilesAndRetention(t *testing.T) {
	ids := []NodeID{{1}, {2}}
	configs := clusterConfigs(t, ids...)
	dir := t.TempDir()
	for i := range configs {
		files, err := NewFileConfig(filepath.Join(dir, hex.EncodeToString(ids[i][:])), Key{77}, "127.0.0.1:0")
		must(t, err)
		files.FetchInterval = 20 * time.Millisecond
		configs[i].Files = &files
		configs[i].MergePolicies = []PolicyEntry{{Table: "stats", Column: "count", Policy: CounterPolicy}, {Table: "stats", Column: "tags", Policy: SetPolicy}, {Table: "stats", Column: "lowest", Policy: MinPolicy}, {Table: "stats", Column: "highest", Policy: MaxPolicy}}
	}
	database := NodeID{9}
	a := openNetworkDB(t, filepath.Join(dir, "a"), configs[0], database, ids[0])
	b := openNetworkDB(t, filepath.Join(dir, "b"), configs[1], database, ids[1])
	for _, d := range []*DB{a, b} {
		must(t, d.Migrate(NewMigration("CREATE TABLE stats(id INTEGER PRIMARY KEY,count INTEGER NOT NULL,tags BLOB,lowest INTEGER,highest INTEGER)")))
		must(t, d.Flush())
	}
	ar, err := a.Replication()
	must(t, err)
	defer ar.Release()
	br, err := b.Replication()
	must(t, err)
	defer br.Release()
	status, err := br.Status()
	must(t, err)
	must(t, ar.AddPeer(*status.Address))
	_, err = a.Exec("INSERT INTO stats VALUES(1,0,NULL,100,0)")
	must(t, err)
	must(t, a.Flush())
	waitFor(t, func() bool {
		rows, err := b.Query("SELECT count FROM stats WHERE id=1")
		return err == nil && len(rows.Rows) == 1
	})
	for _, d := range []*DB{a, b} {
		must(t, d.Transaction(func(sql *Sql) error {
			key := []Value{Integer(1)}
			if err := sql.CounterAdd("stats", key, "count", 3); err != nil {
				return err
			}
			if err := sql.SetAdd("stats", key, "tags", Text("go")); err != nil {
				return err
			}
			if err := sql.Min("stats", key, "lowest", Integer(10)); err != nil {
				return err
			}
			return sql.Max("stats", key, "highest", Integer(90))
		}))
		must(t, d.Flush())
	}
	waitFor(t, func() bool {
		for _, d := range []*DB{a, b} {
			rows, err := d.Query("SELECT count FROM stats WHERE id=1")
			if err != nil || len(rows.Rows) != 1 || rows.Rows[0][0].Integer != 6 {
				return false
			}
		}
		return true
	})
	payload := bytes.Repeat([]byte("streamed file\n"), 20000)
	_, err = a.UploadFile("report", bytes.NewReader(payload))
	must(t, err)
	waitFor(t, func() bool { ready, err := b.FileAvailable("report"); return err == nil && ready })
	var output bytes.Buffer
	_, err = b.GetFile("report", &output)
	must(t, err)
	if !bytes.Equal(output.Bytes(), payload) {
		t.Fatal("file mismatch")
	}
	_, err = a.FileAvailability("report")
	must(t, err)
	_, err = a.ListFiles()
	must(t, err)
	_, err = a.SearchFiles("rep")
	must(t, err)
	_, err = a.FileMetadata("report")
	must(t, err)
	_, err = a.FileKeyRotationStatus()
	must(t, err)
	_, err = a.RetentionStatus()
	must(t, err)
	_, err = a.CollectRetention()
	must(t, err)
	must(t, ar.ForceSync())
	must(t, a.DeleteFile("report"))
	must(t, ar.RemovePeer(ids[1]))
	must(t, a.RetirePeer(ids[1]))
}

type testBackend struct {
	BackendDefaults
	mu             sync.Mutex
	database, node NodeID
	values         map[string][]byte
}

func (b *testBackend) Identity() (NodeID, NodeID) { return b.database, b.node }
func (b *testBackend) Get(key []byte) ([]byte, bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	value, found := b.values[string(key)]
	return append([]byte{}, value...), found, nil
}
func (b *testBackend) Keys(prefix []byte) ([][]byte, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	var keys [][]byte
	for key := range b.values {
		if bytes.HasPrefix([]byte(key), prefix) {
			keys = append(keys, []byte(key))
		}
	}
	return keys, nil
}
func (b *testBackend) Commit(records []Record) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, r := range records {
		if r.Value == nil {
			delete(b.values, string(r.Key))
		} else {
			b.values[string(r.Key)] = append([]byte{}, *r.Value...)
		}
	}
	return nil
}
func (b *testBackend) ImportBridge(BridgeHeader, []byte) (bool, error)        { return true, nil }
func (b *testBackend) ReleaseOwnership(string, []byte, *string) (bool, error) { return true, nil }
func (b *testBackend) Publish(records []Record) error                         { return b.Commit(records) }
func (b *testBackend) Snapshot() ([]byte, error)                              { return []byte{0}, nil }
func (b *testBackend) SnapshotWithProgress() ([]byte, []PeerProgress, error) {
	return []byte{0}, []PeerProgress{}, nil
}
func (b *testBackend) RetentionCheckpoint() error                  { return nil }
func (b *testBackend) PruneHistory(NodeID, uint64) (uint64, error) { return 0, nil }
func (b *testBackend) ReclaimRetention() error                     { return nil }
func (b *testBackend) Schema() ([]byte, error)                     { return []byte{0}, nil }
func (b *testBackend) Apply([][]byte, []Record) (bool, error)      { return true, nil }
func (b *testBackend) Observe(uint64, uint32) error                { return nil }
func (b *testBackend) AcceptSchema([]byte) (bool, error)           { return true, nil }
func backendFor(t *testing.T, database, node NodeID) (*testBackend, *BackendHandle) {
	t.Helper()
	backend := &testBackend{database: database, node: node, values: map[string][]byte{}}
	h, err := RegisterBackend(backend)
	must(t, err)
	t.Cleanup(func() {
		if err := h.Close(); err != nil {
			t.Error(err)
		}
	})
	return backend, h
}
func TestStandaloneBackendObjectsAndBridge(t *testing.T) {
	database, node := NodeID{5}, NodeID{1}
	before := callbackCount()
	_, backend := backendFor(t, database, node)
	config := clusterConfigs(t, node)[0]
	must(t, backend.BindSigner(config))
	r, err := StartReplication(config, backend)
	must(t, err)
	must(t, r.Close())
	files, err := NewFileConfig(filepath.Join(t.TempDir(), "objects"), Key{4}, "127.0.0.1:0")
	must(t, err)
	objects, err := OpenObjects(files, backend)
	must(t, err)
	payload := bytes.Repeat([]byte{0, 1, 255}, 50000)
	metadata, err := objects.Upload("file", bytes.NewReader(payload))
	must(t, err)
	var output bytes.Buffer
	_, err = objects.Read("file", &output)
	must(t, err)
	if !bytes.Equal(output.Bytes(), payload) {
		t.Fatal("payload mismatch")
	}
	path, err := objects.Path(metadata.Digest, metadata.Generation)
	must(t, err)
	must(t, objects.Verify(path, metadata, &output))
	chunks := 0
	_, total, err := objects.StreamPlainChunks("file", 0, 4096, func(offset uint64, data []byte) error {
		if offset != uint64(chunks*4096) {
			return invalidError("chunk offset")
		}
		chunks++
		return nil
	})
	must(t, err)
	if total != uint64(len(payload)) || chunks < 2 {
		t.Fatal(total, chunks)
	}
	must(t, objects.Close())
	secret := Key{8}
	recipient, err := RecipientPublic(secret)
	must(t, err)
	exporter, err := OpenBridge(ExporterConfig(database, filepath.Join(t.TempDir(), "out"), "feed", config.SigningKey, recipient), backend)
	must(t, err)
	artifact, err := exporter.Export([]byte("bridge payload"))
	must(t, err)
	highDomain := NodeID{6}
	_, highBackend := backendFor(t, highDomain, NodeID{2})
	public, err := PublicKey(config.SigningKey)
	must(t, err)
	receiverConfig, err := ReceiverConfig(highDomain, filepath.Join(t.TempDir(), "in"), "feed", secret, []NodeID{database}, []TrustEntry{{Node: node, Key: public}}, []TrustEntry{{Node: node, Key: public}})
	must(t, err)
	receiver, err := OpenBridge(receiverConfig, highBackend)
	must(t, err)
	_, err = receiver.Stage(artifact)
	must(t, err)
	key, err := Encode("Values", []Value{Integer(1)})
	must(t, err)
	changed, err := receiver.ReleaseOwnership("docs", key, nil)
	must(t, err)
	if !changed {
		t.Fatal("ownership release")
	}
	must(t, receiver.Close())
	must(t, exporter.Close())
	must(t, highBackend.Close())
	must(t, backend.Close())
	if callbackCount() != before {
		t.Fatal("backend callback leaked")
	}
}
func TestTransportFramesAndTrust(t *testing.T) {
	ids := []NodeID{{1}, {2}}
	configs := clusterConfigs(t, ids...)
	a, err := NewEndpoint(configs[0], ids[0])
	must(t, err)
	defer a.Close()
	b, err := NewEndpoint(configs[1], ids[1])
	must(t, err)
	defer b.Close()
	address, err := b.Address()
	must(t, err)
	accepted := make(chan *Connection, 1)
	acceptError := make(chan error, 1)
	go func() {
		c, err := b.Accept()
		if err != nil {
			acceptError <- err
			return
		}
		accepted <- c
	}()
	client, err := a.Connect(address)
	must(t, err)
	defer client.Close()
	var server *Connection
	select {
	case server = <-accepted:
	case err := <-acceptError:
		t.Fatal(err)
	case <-time.After(5 * time.Second):
		t.Fatal("accept timeout")
	}
	defer server.Close()
	peer, err := client.PeerID(configs[0])
	must(t, err)
	if peer != ids[1] {
		t.Fatal(peer)
	}
	stream, recv, err := client.OpenBi()
	must(t, err)
	defer recv.Close()
	message, err := NewMessage("Ping", uint64(42))
	must(t, err)
	must(t, stream.WriteMessage(message))
	send, receive, err := server.AcceptBi()
	must(t, err)
	got, err := receive.ReadMessage()
	must(t, err)
	var n uint64
	must(t, got.DecodeBody(&n))
	if got.Kind != "Ping" || n != 42 {
		t.Fatal(got)
	}
	readDone := make(chan error, 1)
	go func() { _, err := recv.ReadMessage(); readDone <- err }()
	time.Sleep(20 * time.Millisecond)
	must(t, recv.Close())
	select {
	case err := <-readDone:
		if err == nil {
			t.Fatal("closed stream read succeeded")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("stream Close did not cancel pending read")
	}
	must(t, send.Finish())
	must(t, stream.Finish())
	must(t, receive.Close())
	must(t, send.Close())
	must(t, stream.Close())
	if _, err := CertificateNodeID([]byte("invalid")); !errors.Is(err, ErrReplicationAuth) {
		t.Fatalf("auth error: %v", err)
	}
}
func TestSnapshotHelpers(t *testing.T) {
	database, donor, recipient := NodeID{9}, NodeID{1}, NodeID{2}
	configs := clusterConfigs(t, donor, recipient)
	schema, err := SchemaDigest([]byte{0})
	must(t, err)
	now, err := NowMillis()
	must(t, err)
	manifest, records, err := BuildSnapshot([2]NodeID{database, donor}, recipient, []byte("snapshot"), nil, schema, configs[0], now)
	must(t, err)
	_, high := backendFor(t, database, recipient)
	must(t, manifest.Verify(high, configs[1], donor, now))
	must(t, AcceptSnapshot(high, configs[1], donor, manifest, now))
	incoming, err := IncomingSnapshot(high)
	must(t, err)
	if incoming == nil {
		t.Fatal("missing incoming snapshot")
	}
	indexes, err := MissingSnapshot(high, *incoming)
	must(t, err)
	if len(indexes) != 1 {
		t.Fatal(indexes)
	}
	must(t, ReceiveSnapshot(high, donor, manifest.Transfer, 0, []byte("snapshot"), now))
	done, err := FinishSnapshot(high, configs[1], donor, manifest.Transfer, now)
	must(t, err)
	if !done || len(records) == 0 {
		t.Fatal("snapshot did not finish")
	}
	_, err = json.Marshal(manifest)
	must(t, err)
}

func TestBackendBatchedReadsAndBlockingUtility(t *testing.T) {
	b, h := backendFor(t, NodeID{1}, NodeID{2})
	defer h.Close()
	must(t, b.Commit([]Record{PutRecord([]byte("a"), []byte("one")), PutRecord([]byte("empty"), []byte{})}))
	values, err := h.GetMany([][]byte{[]byte("a"), []byte("missing"), []byte("empty"), []byte("a")})
	must(t, err)
	if len(values) != 4 || values[0] == nil || string(*values[0]) != "one" || values[1] != nil || values[2] == nil || len(*values[2]) != 0 || string(*values[3]) != "one" {
		t.Fatalf("bad batch: %#v", values)
	}
	values, err = h.GetManyBounded([][]byte{[]byte("a"), []byte("empty")}, 1)
	must(t, err)
	if len(values) != 1 {
		t.Fatalf("oversized first value: %d", len(values))
	}
	called := false
	must(t, RunBlocking(func() error { called = true; return nil }))
	if !called {
		t.Fatal("callback not called")
	}
	want := errors.New("blocking callback failed")
	if err := RunBlocking(func() error { return want }); !errors.Is(err, want) {
		t.Fatalf("callback error: %v", err)
	}
	if RunBlocking(nil) == nil {
		t.Fatal("nil callback accepted")
	}
}
